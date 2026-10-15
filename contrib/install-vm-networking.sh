#!/bin/sh
# Install the qualified Softnet helper into a root-owned location. Does not start
# Fleet, restart services, modify sudoers, or enable VM support on any worker.
set -eu

if [ "$(id -u)" != 0 ]; then
    echo "Run with sudo: $0 /absolute/path/to/softnet" >&2
    exit 1
fi
if [ "$#" != 1 ] || [ ! -f "$1" ]; then
    echo "Usage: $0 /absolute/path/to/softnet" >&2
    exit 1
fi

expected=479f4d698c5794200c2fcd09d4f606b8d675faed19ce241b0ad52df1938d95c9
actual=$(/usr/bin/shasum -a 256 "$1" | /usr/bin/awk '{print $1}')
if [ "$actual" != "$expected" ]; then
    echo "Refusing unqualified binary: expected the official Softnet 0.24.0 universal binary." >&2
    exit 1
fi

destination=/Library/PrivilegedHelperTools/com.fleet.softnet/0.24.0
for directory in /Library/PrivilegedHelperTools /Library/PrivilegedHelperTools/com.fleet.softnet "$destination"; do
    if [ -L "$directory" ]; then
        echo "Refusing symlink: $directory" >&2
        exit 1
    fi
    /usr/bin/install -d -o root -g wheel -m 0755 "$directory"
done
if [ -e "$destination/softnet" ]; then
    if [ -L "$destination/softnet" ]; then
        echo "Refusing a symlink at the helper destination." >&2
        exit 1
    fi
    actual=$(/usr/bin/shasum -a 256 "$destination/softnet" | /usr/bin/awk '{print $1}')
    if [ "$actual" != "$expected" ]; then
        echo "Refusing to replace an existing different helper." >&2
        exit 1
    fi
else
    /usr/bin/install -o root -g wheel -m 0755 "$1" "$destination/softnet"
fi
# Verify the installed copy before granting the privilege required by vmnet.
actual=$(/usr/bin/shasum -a 256 "$destination/softnet" | /usr/bin/awk '{print $1}')
[ "$actual" = "$expected" ]
/usr/sbin/chown root:wheel "$destination/softnet"
/bin/chmod 4755 "$destination/softnet"
echo "Installed Softnet 0.24.0 at $destination/softnet"
