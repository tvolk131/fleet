#!/bin/sh
# boot.sh <dir> [extra runner flags]: start a spike microVM in the background; logs go to <dir>/runner.log.
# Reuses the vz spike's kernel and guest initramfs, so both backends boot the identical guest.
set -eu
cd "$(dirname "$0")"
dir=$1; shift
mkdir -p "$dir"
( target/debug/krun-runner-spike run "$dir" --kernel "$PWD/../vz-runner/assets/vmlinux" \
    --initrd "$PWD/../vz-runner/guest/out/initramfs.cpio.gz" "$@" >> "$dir/runner.log" 2>&1
  echo "runner exit $?" >> "$dir/runner.log" ) &
