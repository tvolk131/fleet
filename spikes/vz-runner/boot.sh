#!/bin/sh
# boot.sh <dir> [extra runner flags]: start a spike VM in the background; logs go to <dir>/runner.log.
set -eu
cd "$(dirname "$0")"
dir=$1; shift
mkdir -p "$dir"
( target/debug/vz-runner-spike run "$dir" --kernel "$PWD/assets/vmlinux" \
    --initrd "$PWD/guest/out/initramfs.cpio.gz" "$@" >> "$dir/runner.log" 2>&1
  echo "runner exit $?" >> "$dir/runner.log" ) &
