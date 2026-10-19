#!/bin/sh
# Installs Fleet's guest agent into a macOS image bundle's disk, replacing Tart's agent as the
# way the host runs commands in the guest. Run once per image, with sudo: launchd only loads
# agents owned by root, and setting ownership inside the image needs root.
#
#   sudo contrib/install-guest-agent.sh <image bundle> <fleet-guest executable>
#
# The agent runs as a LaunchAgent, so as the user the image logs in automatically, like Tart's.
# It listens on vsock port 1024 (`fleet guest-exec`) and shuts the guest down with `sudo -n`.
set -eu

if [ "$(id -u)" != 0 ]; then
    echo "Run with sudo: $0 <image bundle> <fleet-guest executable>" >&2
    exit 1
fi
if [ "$#" != 2 ] || [ ! -f "$1/image.json" ] || [ ! -f "$2" ]; then
    echo "Usage: $0 <image bundle> <fleet-guest executable>" >&2
    exit 1
fi
bundle=$1
agent=$2
if ! /usr/bin/grep -q '"guest":"macos"' "$bundle/image.json"; then
    echo "Refusing: $bundle is not a macOS image bundle" >&2
    exit 1
fi

devices=$(/usr/bin/hdiutil attach -nomount -imagekey diskimage-class=CRawDiskImage "$bundle/disk.img" | /usr/bin/awk '{print $1}')
first=$(echo "$devices" | /usr/bin/head -1)
mountpoint=$(/usr/bin/mktemp -d /tmp/fleet-image.XXXXXXXX)
cleanup() {
    /usr/sbin/diskutil unmount "$mountpoint" >/dev/null 2>&1 || true
    /usr/bin/hdiutil detach "$first" >/dev/null 2>&1 || true
    /bin/rmdir "$mountpoint" 2>/dev/null || true
}
trap cleanup EXIT

# Only devices this attach created are considered, never the host's own disks.
data=
for device in $devices; do
    case $device in /dev/disk*s*) continue ;; esac
    found=$(/usr/sbin/diskutil apfs list "$device" 2>/dev/null | /usr/bin/awk '/\(Data\)$/ {print $(NF-1)}')
    [ -n "$found" ] && data=$found
done
if [ -z "$data" ]; then
    echo "No APFS Data volume in $bundle/disk.img" >&2
    exit 1
fi
/usr/sbin/diskutil mount -mountPoint "$mountpoint" "$data" >/dev/null
/usr/sbin/diskutil enableOwnership "$mountpoint" >/dev/null

/usr/bin/install -d -o root -g wheel -m 0755 "$mountpoint/usr/local/libexec"
/usr/bin/install -o root -g wheel -m 0755 "$agent" "$mountpoint/usr/local/libexec/fleet-guest"
/bin/cat > "$mountpoint/Library/LaunchAgents/io.fleet.guest-agent.plist" <<'PLIST'
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>io.fleet.guest-agent</string>
    <key>ProgramArguments</key>
    <array>
        <string>/usr/local/libexec/fleet-guest</string>
        <string>--agent</string>
    </array>
    <key>EnvironmentVariables</key>
    <dict>
        <key>PATH</key>
        <string>/usr/local/bin:/opt/homebrew/bin:/usr/bin:/bin:/usr/sbin:/sbin</string>
    </dict>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>/tmp/fleet-guest-agent.log</string>
    <key>StandardErrorPath</key>
    <string>/tmp/fleet-guest-agent.log</string>
</dict>
</plist>
PLIST
/usr/sbin/chown root:wheel "$mountpoint/Library/LaunchAgents/io.fleet.guest-agent.plist"
/bin/chmod 0644 "$mountpoint/Library/LaunchAgents/io.fleet.guest-agent.plist"
echo "Installed the Fleet guest agent into $bundle"
