#![feature(command_access)]

use anyhow::{anyhow, Result};
use std::process::Command;
use std::thread;
use std::path::{PathBuf, Path};
use std::fs;
use std::io;
use std::io::{Read, Write, Seek};
use rand::prelude::*;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

mod common;
use common::*;

mod zfs;
use zfs::*;

/*
 * Produce a "seed" dataset.  This will be filled with a set of random files,
 * and a snapshot will be taken.  This snapshot will be used to create many
 * clones in which various concurrent operations will occur.
 */
struct Seed {
    log: Logger,
    id: u64,
    dataset: String,
}

const KILOBYTE: u64 = 1024;
const MEGABYTE: u64 = KILOBYTE * 1024;

const SEED_FILE_COUNT: usize = 1_000;
const FILE_MIN: u64 = 2; /* MB */
const FILE_MAX: u64 = 32; /* MB */

fn chown_to_me<P: AsRef<Path>>(p: P) -> Result<()> {
    /*
     * Fix permissions so we can write to the directory.
     */
    Command::new("/bin/pfexec")
        .env_clear()
        .arg("/bin/chown")
        .arg("jclulow")
        .arg(p.as_ref())
        .output()?;
    Ok(())
}

impl Seed {
    fn setup(log: Logger, pool: &str, id: u64) -> Result<Seed> {
        let root = format!("{}/seed", pool);
        zfs_create(&log, &root, true)?;

        let dataset = format!("{}/{:<04}", root, id);

        if !zfs_snapshot_exists(&log, &dataset, "final")? {
            /*
             * A previous setup run did not complete.  Destroy and recreate
             * the entire thing.
             */
            zfs_destroy(&log, &dataset, true)?;
            zfs_create(&log, &dataset, false)?;

            let mountpoint = PathBuf::from(zfs_get(&log, &dataset, 
                "mountpoint")?);
            chown_to_me(&mountpoint)?;

            /*
             * Create a fan-out directory structure full of files of random
             * size.
             */
            let mut rng = rand_chacha::ChaCha20Rng::from_entropy();

            for _ in 0..SEED_FILE_COUNT {
                let l0 = rng.gen_range::<u64, _>(0..16);
                let l1 = rng.gen_range::<u64, _>(0..16);
                let l2 = rng.gen::<u64>();

                let mut fp = mountpoint.clone();
                fp.push(format!("{:<04X}", l0));
                fp.push(format!("{:<04X}", l1));
                std::fs::create_dir_all(&fp)?;
                fp.push(format!("{:<016X}.dat", l2));

                let sz_mb = rng.gen_range::<u64, _>(FILE_MIN..=FILE_MAX);

                let mut f = fs::OpenOptions::new()
                    .write(true)
                    .truncate(true)
                    .create(true)
                    .open(&fp)?;
                let mut bw = io::BufWriter::new(f);

                /*
                 * Create a file with random data:
                 */
                let mut buf = Vec::with_capacity(8192);
                for _ in 0..(sz_mb * 64) {
                    buf.clear();

                    /*
                     * Generate mostly random data, with some compressible data:
                     */
                    let random = rng.gen_bool(0.75);

                    while buf.len() < (16 * KILOBYTE) as usize {
                        if random {
                            buf.push(rng.gen::<u8>());
                        } else {
                            buf.push(b'A');
                        }
                    }

                    bw.write(&buf)?;
                }

                bw.flush()?;
            }

            /*
             * Take the "final" snapshot that we will use to create clones.
             */
            zfs_snapshot(&log, &dataset, "final", false)?;
        } else {
            info!(&log, "seed {} already setup", id);
        }

        Ok(Seed {
            log,
            id,
            dataset,
        })
    }

    fn dataset(&self) -> &str {
        &self.dataset
    }
}

struct Plant {
    log: Logger,
    id: u64,
    parent: String,
    dataset: String,
    mountpoint: PathBuf,
}

fn file_futz<P: AsRef<Path>, T: rand::Rng>(p: P, rng: &mut T,
    buf: &mut Vec<u8>)
    -> Result<()>
{
    let mut f = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(false)
        .open(p.as_ref())?;

    let sz = f.metadata()?.len();

    /*
     * Determine how many operations we will perform on this
     * file.
     */
    let iops = rng.gen_range(1..10_000);

    buf.clear();
    while buf.len() < (1 * KILOBYTE) as usize {
        buf.push(b'\0');
    }

    for _ in 0..iops {
        /*
         * Are we looking to read or write?
         */
        let write = rng.gen_bool(0.40);

        let target = rng.gen_range(0..(sz / 1024 - 1));
        f.seek(io::SeekFrom::Start(target))?;

        if write {
            let random = rng.gen_bool(0.75);

            buf.clear();
            while buf.len() < (1 * KILOBYTE) as usize {
                if random {
                    buf.push(rng.gen::<u8>());
                } else {
                    buf.push(b'A');
                }
            }

            f.write_all(buf)?;
            f.flush()?;
        } else {
            f.read_exact(buf)?;
        }
    }

    Ok(())
}

impl Plant {
    fn setup(log: Logger, pool: &str, id: u64, parent: &str) -> Result<Plant> {
        /*
         * Start with a clean slate.
         */
        let dataset = format!("{}/plant/{:<04}", pool, id);
        zfs_destroy(&log, &dataset, true)?;

        /*
         * Clone the seed:
         */
        zfs_clone(&log, parent, "final", &dataset)?;

        let mountpoint = PathBuf::from(zfs_get(&log, &dataset, "mountpoint")?);
        chown_to_me(&mountpoint)?;

        Ok(Plant {
            log,
            id,
            parent: parent.to_string(),
            mountpoint,
            dataset,
        })
    }

    fn start(&self, nthreads: u64) -> Result<()> {
        /*
         * Create I/O threads to act within this plant.
         */
        for _ in 0..nthreads {
            let log = self.log.clone();
            let mp = self.mountpoint.clone();
            thread::spawn(move || {
                let mut rng = rand_chacha::ChaCha20Rng::from_entropy();
                let mut buf = Vec::with_capacity((1 * KILOBYTE) as usize);

                loop {
                    /*
                     * List all files in the plant at this time.
                     */
                    let mut files = Vec::new();
                    let walk = walkdir::WalkDir::new(&mp);
                    for ent in walk.into_iter() {
                        match ent {
                            Ok(ent) => {
                                if !ent.file_type().is_file() {
                                    continue;
                                }
                                files.push(ent.path().to_path_buf());
                            }
                            Err(e) => {
                                error!(&log, "walk failure: {:?}", e);
                                continue;
                            }
                        }
                    }

                    /*
                     * Shuffle the deck.
                     */
                    let mut neworder = VecDeque::new();
                    for i in 0..files.len() {
                        neworder.push_back(i);
                    }

                    if !neworder.is_empty() {
                        let mut i = neworder.len() - 1;
                        while i >= 1 {
                            let j = rng.gen_range(0..i);
                            neworder.swap(i, j);
                            i -= 1;
                        }
                    }

                    while let Some(i) = neworder.pop_front() {
                        if let Err(e) = file_futz(&files[i], &mut rng,
                            &mut buf)
                        {
                            error!(&log, "file futz error: {:?}", e);
                        }
                    }
                }
            });
        }

        Ok(())
    }


    fn dataset(&self) -> &str {
        &self.dataset
    }
}

/*
 * Define a work area under the pool; e.g.,
 *  dynamite/0001/base
 *               /clone0a (from base@snap0)
 *               /clone0b (from base@snap0)
 *               /clone1a (from base@snap1)
 *          /0002/base
 *          ...
 */
struct Worker {
    log: Logger,
    pool: String,
    id: u64,
}

impl Worker {
    fn new(log: Logger, pool: &str, id: u64) -> Result<Worker> {
        Ok(Worker {
            log,
            pool: pool.to_string(),
            id,
        })
    }

    fn run(&mut self) -> Result<()> {
        let log = &self.log;
        let mut rng = rand_chacha::ChaCha20Rng::from_entropy();

        let root = format!("{}/{:<04}", self.pool, self.id);

        /*
         * First, recursively destroy the dataset.
         */
        zfs_destroy(log, &root, true)?;

        /*
         * Now, create the container dataset and the base dataset beneath.
         */
        zfs_create(log, &root, false)?;
        let base = format!("{}/base", root);
        zfs_create(log, &base, false)?;

        /*
         * Create some random data in the base dataset.
         */
        let mp = PathBuf::from(zfs_get(log, &base, "mountpoint")?);
        let mut base_num = 0;
        let base_steps = 2;
        let snap_count = 4;
        let file_count = 32;
        let file_megs = 16;

        /*
         * Fix permissions so we can write to the directory.
         */
        chown_to_me(&mp)?;

        for snap in 0..snap_count {
            /*
             * Before and between snapshots, write some new data.
             */
            for l0 in base_num..(base_num + base_steps) {
                let mut dir0 = mp.clone();
                dir0.push(format!("{:<02}", l0));

                for l1 in 0..base_steps {
                    let mut dir1 = dir0.clone();
                    dir1.push(format!("{:<02}", l1));

                    fs::create_dir_all(&dir1)?;

                    for l2 in 0..file_count {
                        let mut file = dir1.clone();
                        file.push(format!("{:<04}.dat", l2));

                        let mut f = fs::OpenOptions::new()
                            .write(true)
                            .truncate(true)
                            .create(true)
                            .open(&file)?;
                        let mut bw = io::BufWriter::new(f);

                        /*
                         * Create a file with random data:
                         */
                        for _ in 0..(file_megs * 1024) {
                            let mut buf = Vec::new();

                            /*
                             * Create a random kilobyte of data:
                             */
                            while buf.len() < 1024 {
                                buf.push(rng.gen::<u8>());
                            }
                            bw.write(&buf)?;
                        }

                        bw.flush()?;
                    }
                }
            }

            /*
             * Overwrite some, but not all, of the data between snapshots.
             */
            base_num += base_steps / 2;

            /*
             * Create a snapshot of the base dataset so that we can clone it.
             */
            let snapname = format!("snap{:<02}", snap);
            zfs_snapshot(log, &base, &snapname, false)?;

            /*
             * Create four clones of each snapshot.
             */
            for clone in 'a'..='d' {
                let clone = format!("{}/clone{}{}", root, snap, clone);

                zfs_clone(log, &base, &snapname, &clone)?;

                /*
                 * Read the data back from each clone.
                 */
                let mp = PathBuf::from(zfs_get(log, &clone, "mountpoint")?);
                for l0 in 0..(snap_count * base_steps) {
                    let mut dir0 = mp.clone();
                    dir0.push(format!("{:<02}", l0));

                    for l1 in 0..base_steps {
                        let mut dir1 = dir0.clone();
                        dir1.push(format!("{:<02}", l1));

                        for l2 in 0..=file_count {
                            let mut file = dir1.clone();
                            file.push(format!("{:<04}.dat", l2));

                            /*
                             * Skip files that we cannot open.
                             */
                            if let Ok(mut f) = fs::OpenOptions::new()
                                .read(true)
                                .open(&file)
                            {
                                debug!(log, "reading back {:?}", file);
                                let mut br = io::BufReader::new(f);
                                let mut buf = Vec::new();
                                br.read_to_end(&mut buf)?;
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

fn jobs() -> Result<usize> {
    let out = Command::new("/usr/sbin/psrinfo")
        .env_clear()
        .arg("-t")
        .output()?;

    if !out.status.success() {
        bail!("{}", out.info());
    }

    Ok(String::from_utf8(out.stdout)?.trim().parse()?)
}

fn main() -> Result<()> {
    let cmd = std::env::args().nth(1).ok_or(anyhow!("no argument?"))?;

    let log = init_log();

    info!(log, "stress: {}", cmd);

    match cmd.as_str() {
        "io" => {
            /*
             * Prepare seed datasets:
             */
            let seeds = (0..10u64).map(|id| {
                let log = log.new(o! { "seed" => id });

                info!(log, "creating seed {}", id);

                Seed::setup(log.clone(), "dynamite", id)
            }).collect::<Result<Vec<_>>>()?;

            /*
             * Destroy all previous plants:
             */
            zfs_destroy(&log, "dynamite/plant", true)?;
            zfs_create(&log, "dynamite/plant", false)?;

            /*
             * Establish plants, each from a random seed:
             */
            let mut rng = rand_chacha::ChaCha20Rng::from_entropy();
            let plants = (0..60).map(|id| {
                let log = log.new(o! { "plant" => id });

                let si = rng.gen_range(0..seeds.len());
                let seed = seeds[si].dataset().to_string();
                info!(log, "creating plant {} from {}", id, seed);

                Plant::setup(log.clone(), "dynamite", id, &seed)
            }).collect::<Result<Vec<_>>>()?;

            /*
             * Start all the I/O threads:
             */
            for p in &plants {
                p.start(4)?;
            }

            loop {
                /*
                 * XXX Could join threads.
                 */
                sleep(1_000_000);
            }
        }
        "backup" => {
            /*
             * Use the main thread to perform periodic "backup" activity.  For
             * each plant, we want to:
             *      - take a new snapshot
             *      - delete the oldest snapshot until there are only N
             *        snapshots left
             *      - if there are at least two snapshots, do an incremental
             *        zfs send of the current snapshot using the second most
             *        recent snapshot as the comparison base
             */
            let maxsnaps = 5;
            loop {
                let snapnum = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs();
                let snapname = format!("backup-{}", snapnum);

                let mut sends = Arc::new(Mutex::new(Vec::new()));

                for ds in zfs_dataset_children(&log, "dynamite/plant")? {
                    /*
                     * Take snapshot.
                     */
                    zfs_snapshot(&log, &ds, &snapname, false)?;

                    /*
                     * Age out old snapshots.
                     */
                    let snaps = loop {
                        let snaps = zfs_snapshot_list(&log, &ds)?;

                        if snaps.len() < maxsnaps {
                            break snaps;
                        }

                        zfs_destroy_snapshot(&log, &ds, &snaps[0])?;
                    };

                    if snaps.len() < 2 {
                        continue;
                    }

                    let sold = snaps[snaps.len() - 2].to_string();
                    let snew = snaps[snaps.len() - 1].to_string();

                    sends.lock().unwrap().push((ds, sold, snew));
                    //zfs_send_to_null(&log, &ds, &sold, &snew)?;
                }

                let mut threads = Vec::<thread::JoinHandle<Result<()>>>::new();
                for _ in 0..4 {
                    let log = log.clone();
                    let sends = Arc::clone(&sends);

                    threads.push(thread::spawn(move || {
                        loop {
                            let (ds, sold, snew) = {
                                let mut sends = sends.lock().unwrap();
                                if let Some(x) = sends.pop() {
                                    x
                                } else {
                                    return Ok(());
                                }
                            };

                            zfs_send_to_null(&log, &ds, &sold, &snew)?;
                        }
                    }));
                }

                while let Some(t) = threads.pop() {
                    t.join().unwrap();
                }

                sleep(5_000);
            }
        }
        n => {
            bail!("unknown command {}", n);
        }
    }
}

fn mainold() -> Result<()> {
    let log = init_log();

    info!(log, "stress");

    let j = 8 * jobs()?;

    for id in 0..j {
        let log = log.new(o! { "worker" => id });

        let mut w = Worker::new(log.clone(), "dynamite", id as u64)?;

        let mut rng = rand_chacha::ChaCha20Rng::from_entropy();

        thread::spawn(move || {
            /*
             * Random start delay.
             */
            sleep(rng.gen_range(0..30_000));

            loop {
                info!(log, "worker starting");
                if let Err(e) = w.run() {
                    error!(log, "worker failure: {:?}", e);
                    sleep(1000);
                }

                /*
                 * Random delay between cycles.
                 */
                sleep(rng.gen_range(0..30_000));
            }
        });
    }

    loop {
        sleep(1000); /* XXX join threads */
    }

    Ok(())
}
