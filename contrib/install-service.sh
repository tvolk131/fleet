#!/bin/sh
# Install a definition produced by `fleet service render`. Run with sudo.
# Never targets the legacy local.fleet.agent/coordinator services.
set -eu
[ "$(id -u)" = 0 ] || { echo 'Administrator privileges are required to install a system service.' >&2; exit 1; }
[ "$#" = 3 ] || { echo 'Usage: install-service.sh launchd|systemd service-name /absolute/definition' >&2; exit 1; }
kind=$1
name=$2
definition=$3
case "$definition" in /*) ;; *) echo 'Definition must be an absolute path.' >&2; exit 1;; esac
[ -f "$definition" ] || { echo 'Definition not found.' >&2; exit 1; }
case "$name" in *[!a-zA-Z0-9.-]*|'') echo 'Invalid service name.' >&2; exit 1;; esac
case "$name" in local.fleet.node|local.fleet.node.*|local.fleet.qualification.*|local.fleet.staging|fleet-node|fleet-node-*|fleet-qualification-*|fleet-staging) ;; *) echo 'Use a new Fleet node, qualification, or staging service name.' >&2; exit 1;; esac
case "$kind" in
launchd)
    [ "$(uname -s)" = Darwin ] || exit 1
    /usr/bin/plutil -lint "$definition"
    label=$(/usr/bin/plutil -extract Label raw -o - "$definition")
    [ "$label" = "$name" ] || { echo 'Definition label does not match service name.' >&2; exit 1; }
    user=$(/usr/bin/plutil -extract UserName raw -o - "$definition")
    [ "$user" != root ] || { echo 'Fleet must run as a regular user.' >&2; exit 1; }
    destination="/Library/LaunchDaemons/$name.plist"
    if [ -e "$destination" ]; then
        cmp -s "$definition" "$destination" || { echo 'A different service definition already exists; preserve and review it before replacement.' >&2; exit 1; }
    else
        /usr/bin/install -o root -g wheel -m 644 "$definition" "$destination"
    fi
    /bin/launchctl bootout "system/$name" 2>/dev/null || true
    /bin/launchctl bootstrap system "$destination"
    /bin/launchctl print "system/$name"
    ;;
systemd)
    [ "$(uname -s)" = Linux ] || exit 1
    # The rendered unit contains an explicit unprivileged User and absolute paths.
    python3 - "$definition" <<'PY'
import configparser,sys
c=configparser.ConfigParser(interpolation=None,strict=False)
c.read(sys.argv[1])
assert c['Service']['User'] not in ('','root'), 'Fleet must run as a regular user'
assert ' service run --config ' in c['Service']['ExecStart'], 'Expected Fleet service command'
assert c['Service']['KillMode']=='process', 'Do not terminate detached workload supervisors'
PY
    destination="/etc/systemd/system/$name.service"
    if [ -e "$destination" ]; then
        cmp -s "$definition" "$destination" || { echo 'A different service definition already exists; preserve and review it before replacement.' >&2; exit 1; }
    else
        install -o root -g root -m 644 "$definition" "$destination"
    fi
    systemctl daemon-reload
    systemctl enable --now "$name.service"
    systemctl status --no-pager "$name.service"
    ;;
*) echo 'Service kind must be launchd or systemd.' >&2; exit 1;;
esac
