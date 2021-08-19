#!/bin/bash

set -o errexit
set -o pipefail
set -o xtrace

cargo +nightly build --release
ssh romulus rm -f /tmp/stress
scp target/release/stress romulus:/tmp
ssh -t romulus /tmp/stress io
