#!/bin/sh
# Builds the Linux test guest image bundles the driver conformance suite boots:
#   target/vm-guest/standard/      powers off on a graceful stop request
#   target/vm-guest/ignores-stop/  ignores it
#   target/vm-guest/agent/         serves Fleet's real guest agent on the agent port
# Needs the aarch64-unknown-linux-musl Rust target and zstd (only to fetch the kernel).
# The kernel is Kata Containers' guest kernel; set FLEET_TEST_KERNEL to use a local copy.
set -eu
here=$(cd "$(dirname "$0")" && pwd)
out=$here/../../target/vm-guest
kernel_url=https://github.com/kata-containers/kata-containers/releases/download/4.2.0/kata-static-4.2.0-arm64.tar.zst
kernel_member=./opt/kata/share/kata-containers/vmlinux-6.18.35-202
kernel_sha256=f19bef3fb5fe562c0e5a8a2ba99e53455e83de713dd0d0e98e2b03e0523ed02f

mkdir -p "$out"
if [ ! -f "$out/vmlinux" ]; then
    if [ -n "${FLEET_TEST_KERNEL:-}" ]; then
        cp "$FLEET_TEST_KERNEL" "$out/vmlinux.part"
    else
        curl -fsSL "$kernel_url" | zstd -dc | tar -xOf - "$kernel_member" > "$out/vmlinux.part"
    fi
    echo "$kernel_sha256  $out/vmlinux.part" | shasum -a 256 -c - >/dev/null
    mv "$out/vmlinux.part" "$out/vmlinux"
fi

(cd "$here" && cargo build --release --target aarch64-unknown-linux-musl)
root=$out/root
rm -rf "$root" && mkdir -p "$root/dev" "$root/proc" "$root/sys"
cp "$here/target/aarch64-unknown-linux-musl/release/fleet-test-guest" "$root/init"
(cd "$root" && find . | cpio -o -H newc 2>/dev/null) | gzip -9 > "$out/initramfs.cpio.gz"

bundle() {
    rm -rf "$out/$1" && mkdir -p "$out/$1"
    ln "$out/vmlinux" "$out/initramfs.cpio.gz" "$out/$1/"
    printf '{"guest":"linux","kernel":"vmlinux","initrd":"initramfs.cpio.gz","cmdline":"console=hvc0 rdinit=/init panic=-1%s"}\n' "$2" > "$out/$1/image.json"
}
bundle standard ""
bundle ignores-stop " fleet_test.ignore_stop"
bundle agent " fleet_test.agent"
echo "test guest bundles in $out"
