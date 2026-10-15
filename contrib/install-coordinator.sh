#!/bin/sh
# Install (or reinstall) the fleet coordinator daemon (`fleet daemon`) as a launchd daemon on the laptop. Run with sudo:
#     sudo ./install-coordinator.sh [user]
# The daemon runs as `user` (default: the user who ran sudo), from ~user/fleet/bin/fleet, with its keys in ~user/.fleet.
# As a launchd daemon it is exempt from macOS's Local Network privacy, so direct (hole-punched) paths work, and it
# starts at boot. launchd restarts it whenever it exits, so replacing the binary and stopping the process upgrades it.
# The queue lives in ~user/.fleet/fleet.db, so restarting the daemon loses nothing.
# Uninstall: sudo launchctl bootout system/local.fleet.coordinator && sudo rm /Library/LaunchDaemons/local.fleet.coordinator.plist
set -eu
LABEL=local.fleet.coordinator
PLIST=/Library/LaunchDaemons/$LABEL.plist
if [ "$(id -u)" != 0 ]; then echo "run with sudo" >&2; exit 1; fi
USER_NAME=${1:-${SUDO_USER:-}}
if [ -z "$USER_NAME" ] || [ "$USER_NAME" = root ]; then echo "name the user the agent runs as" >&2; exit 1; fi
HOME_DIR=$(dscl . -read "/Users/$USER_NAME" NFSHomeDirectory | awk '{print $2}')
BIN="$HOME_DIR/fleet/bin/fleet"
[ -x "$BIN" ] || { echo "no fleet binary at $BIN" >&2; exit 1; }
[ -s "$HOME_DIR/.fleet/machines" ] || echo "note: no machines named yet (fleet machine add <name> <agent-id>)" >&2
if pgrep -U "$USER_NAME" -f "fleet/bin/fleet daemon" >/dev/null && ! launchctl print "system/$LABEL" >/dev/null 2>&1; then
    echo "a fleet daemon is already running outside launchd: stop it first (Ctrl-C)" >&2; exit 1
fi
cat > "$PLIST" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key><string>$LABEL</string>
    <key>ProgramArguments</key><array><string>$BIN</string><string>daemon</string></array>
    <key>UserName</key><string>$USER_NAME</string>
    <key>WorkingDirectory</key><string>$HOME_DIR</string>
    <key>EnvironmentVariables</key>
    <dict>
        <key>HOME</key><string>$HOME_DIR</string>
        <key>PATH</key><string>/usr/bin:/bin:/usr/sbin:/sbin</string>
        <key>RUST_LOG</key><string>fleet=info,warn</string>
    </dict>
    <key>RunAtLoad</key><true/>
    <key>KeepAlive</key><true/>
    <key>ThrottleInterval</key><integer>5</integer>
    <key>ProcessType</key><string>Standard</string>
    <key>AbandonProcessGroup</key><true/>
    <key>StandardOutPath</key><string>$HOME_DIR/fleet/coordinator.log</string>
    <key>StandardErrorPath</key><string>$HOME_DIR/fleet/coordinator.log</string>
</dict>
</plist>
PLIST
chown root:wheel "$PLIST"
chmod 644 "$PLIST"
launchctl bootout "system/$LABEL" 2>/dev/null || true
launchctl bootstrap system "$PLIST"
sleep 3
launchctl print "system/$LABEL" | grep -E "state =|pid =" || true
echo "installed $LABEL (runs $BIN daemon as $USER_NAME; log $HOME_DIR/fleet/coordinator.log)"
