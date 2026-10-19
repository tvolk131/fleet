#!/bin/sh
# Builds the static guest init and packs it as a newc initramfs: out/initramfs.cpio.gz
set -eu
cd "$(dirname "$0")"
cargo build --release --target aarch64-unknown-linux-musl
rm -rf out/root && mkdir -p out/root/dev out/root/proc out/root/sys
cp target/aarch64-unknown-linux-musl/release/spike-init out/root/init
(cd out/root && find . | cpio -o -H newc 2>/dev/null) | gzip -9 > out/initramfs.cpio.gz
ls -la out/initramfs.cpio.gz
