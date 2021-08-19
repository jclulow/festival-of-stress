use std::process::Command;
use super::common::*;

const ZFS: &str = "/sbin/zfs";
const ZPOOL: &str = "/sbin/zpool";
const PFEXEC: &str = "/bin/pfexec";
const BASH: &str = "/bin/bash";

fn zfs() -> Command {
    let mut cmd = Command::new(PFEXEC);
    cmd.env_clear();
    cmd.arg(ZFS);
    cmd
}

fn zpool() -> Command {
    let mut cmd = Command::new(PFEXEC);
    cmd.env_clear();
    cmd.arg(ZPOOL);
    cmd
}

fn validate_snapshot_name(n: &str) -> Result<()> {
    if n.contains('@') || n.contains('/') {
        bail!("invalid snapshot name {}", n);
    }
    Ok(())
}

fn validate_dataset_name(n: &str) -> Result<()> {
    if n.contains('@') {
        bail!("invalid dataset name {}", n);
    }
    Ok(())
}

pub fn zfs_destroy_snapshot(log: &Logger, dataset: &str, snapname: &str)
    -> Result<()>
{
    validate_dataset_name(dataset)?;
    validate_snapshot_name(snapname)?;

    let fullname = format!("{}@{}", dataset, snapname);

    let mut cmd = zfs();
    cmd.arg("destroy");
    cmd.arg(fullname);

    info!(log, "exec: {:?}", cmd.get_args());

    let res = cmd.output()?;
    if !res.status.success() {
        if let Ok(s) = String::from_utf8(res.stderr.clone()) {
            if s.contains("dataset does not exist") {
                return Ok(());
            }
        }

        error!(log, "{:?} failed: {}", cmd.get_args(), res.info());
        bail!("{:?} failed: {}", cmd.get_args(), res.info());
    }

    Ok(())
}

pub fn zfs_destroy(log: &Logger, dataset: &str, recursive: bool) -> Result<()> {
    validate_dataset_name(dataset)?;

    let mut cmd = zfs();
    cmd.arg("destroy");
    if recursive {
        cmd.arg("-r");
    }
    cmd.arg(dataset);

    info!(log, "exec: {:?}", cmd.get_args());

    let res = cmd.output()?;
    if !res.status.success() {
        if let Ok(s) = String::from_utf8(res.stderr.clone()) {
            if s.contains("dataset does not exist") {
                return Ok(());
            }
        }

        error!(log, "{:?} failed: {}", cmd.get_args(), res.info());
        bail!("{:?} failed: {}", cmd.get_args(), res.info());
    }

    Ok(())
}

pub fn zfs_create(log: &Logger, dataset: &str, exists_ok: bool) -> Result<()> {
    validate_dataset_name(dataset)?;

    let mut cmd = zfs();
    cmd.arg("create");
    cmd.arg(dataset);

    info!(log, "exec: {:?}", cmd.get_args());

    let res = cmd.output()?;
    if !res.status.success() {
        if exists_ok {
            if let Ok(s) = String::from_utf8(res.stderr.clone()) {
                if s.contains("dataset already exists") {
                    return Ok(());
                }
            }
        }

        error!(log, "{:?} failed: {}", cmd.get_args(), res.info());
        bail!("{:?} failed: {}", cmd.get_args(), res.info());
    }

    Ok(())
}

pub fn zfs_snapshot(log: &Logger, dataset: &str, name: &str, recursive: bool)
    -> Result<()>
{
    validate_dataset_name(dataset)?;
    validate_snapshot_name(name)?;

    let fullname = format!("{}@{}", dataset, name);

    let mut cmd = zfs();
    cmd.arg("snapshot");
    if recursive {
        cmd.arg("-r");
    }
    cmd.arg(fullname);

    info!(log, "exec: {:?}", cmd.get_args());

    let res = cmd.output()?;
    if !res.status.success() {
        error!(log, "{:?} failed: {}", cmd.get_args(), res.info());
        bail!("{:?} failed: {}", cmd.get_args(), res.info());
    }

    Ok(())
}

pub fn zfs_clone(log: &Logger, dataset: &str, snapname: &str, target: &str)
    -> Result<()>
{
    validate_dataset_name(dataset)?;
    validate_snapshot_name(snapname)?;
    validate_dataset_name(target)?;

    let fullname = format!("{}@{}", dataset, snapname);

    let mut cmd = zfs();
    cmd.arg("clone");
    cmd.arg(fullname);
    cmd.arg(target);

    info!(log, "exec: {:?}", cmd.get_args());

    let res = cmd.output()?;
    if !res.status.success() {
        error!(log, "{:?} failed: {}", cmd.get_args(), res.info());
        bail!("{:?} failed: {}", cmd.get_args(), res.info());
    }

    Ok(())
}

pub fn zfs_get(log: &Logger, dataset: &str, prop: &str) -> Result<String> {
    validate_dataset_name(dataset)?;

    let mut cmd = zfs();
    cmd.arg("get");
    cmd.arg("-H");
    cmd.arg("-o");
    cmd.arg("value");
    cmd.arg(prop);
    cmd.arg(dataset);

    info!(log, "exec: {:?}", cmd.get_args());

    let res = cmd.output()?;
    if !res.status.success() {
        error!(log, "{:?} failed: {}", cmd.get_args(), res.info());
        bail!("{:?} failed: {}", cmd.get_args(), res.info());
    }

    Ok(String::from_utf8(res.stdout)?.trim_end_matches('\n').to_string())
}

pub fn zfs_snapshot_exists(log: &Logger, dataset: &str, snapname: &str)
    -> Result<bool>
{
    validate_dataset_name(dataset)?;
    validate_snapshot_name(snapname)?;

    let fullname = format!("{}@{}", dataset, snapname);

    let mut cmd = zfs();
    cmd.arg("list");
    cmd.arg("-Ho");
    cmd.arg("name");
    cmd.arg(fullname);

    info!(log, "exec: {:?}", cmd.get_args());

    let res = cmd.output()?;
    if !res.status.success() {
        if let Ok(s) = String::from_utf8(res.stderr.clone()) {
            if s.contains("dataset does not exist") {
                return Ok(false);
            }
        }

        error!(log, "{:?} failed: {}", cmd.get_args(), res.info());
        bail!("{:?} failed: {}", cmd.get_args(), res.info());
    }

    Ok(true)
}

pub fn zfs_dataset_children(log: &Logger, dataset: &str)
    -> Result<Vec<String>>
{
    validate_dataset_name(dataset)?;

    let mut cmd = zfs();
    cmd.arg("list");
    cmd.arg("-t");
    cmd.arg("filesystem");
    cmd.arg("-d");
    cmd.arg("1");
    cmd.arg("-Ho");
    cmd.arg("name");
    cmd.arg(dataset);

    info!(log, "exec: {:?}", cmd.get_args());

    let res = cmd.output()?;
    if !res.status.success() {
        error!(log, "{:?} failed: {}", cmd.get_args(), res.info());
        bail!("{:?} failed: {}", cmd.get_args(), res.info());
    }

    let s = String::from_utf8(res.stdout)?;
    Ok(s.lines().map(|s| s.to_string()).collect())
}

pub fn zfs_snapshot_list(log: &Logger, dataset: &str) -> Result<Vec<String>> {
    validate_dataset_name(dataset)?;

    let mut cmd = zfs();
    cmd.arg("list");
    cmd.arg("-t");
    cmd.arg("snapshot");
    cmd.arg("-d");
    cmd.arg("1");
    cmd.arg("-Ho");
    cmd.arg("name");
    cmd.arg("-s");
    cmd.arg("creation");
    cmd.arg(dataset);

    info!(log, "exec: {:?}", cmd.get_args());

    let res = cmd.output()?;
    if !res.status.success() {
        error!(log, "{:?} failed: {}", cmd.get_args(), res.info());
        bail!("{:?} failed: {}", cmd.get_args(), res.info());
    }

    let s = String::from_utf8(res.stdout)?;
    Ok(s.lines().map(|s| {
        let t = s.split('@').collect::<Vec<_>>();
        assert_eq!(t.len(), 2);
        t[1].to_string()
    }).collect())
}

pub fn zfs_send_to_null(log: &Logger, dataset: &str, snapold: &str,
    snapnew: &str)
    -> Result<bool>
{
    validate_dataset_name(dataset)?;
    validate_snapshot_name(snapold)?;
    validate_snapshot_name(snapnew)?;

    let fullold = format!("{}@{}", dataset, snapold);
    let fullnew = format!("{}@{}", dataset, snapnew);

    let mut script = String::new();
    script += "set -o errexit; set -o pipefail; ";
    script += &format!("{} send -i {} {} >/dev/null", ZFS, fullold, fullnew);

    let mut cmd = Command::new(PFEXEC);
    cmd.env_clear();
    cmd.arg(BASH);
    cmd.arg("-c");
    cmd.arg(&script);

    info!(log, "exec: {:?}", cmd.get_args());

    let res = cmd.output()?;
    if !res.status.success() {
        error!(log, "{:?} failed: {}", cmd.get_args(), res.info());
        bail!("{:?} failed: {}", cmd.get_args(), res.info());
    }

    Ok(true)
}
