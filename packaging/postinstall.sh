#!/bin/sh
set -e
install -d -m 0700 /var/lib/pollen
systemctl daemon-reload
systemctl enable pollen
# On upgrade, restart to pick up the new binary.
# On fresh install, don't start — credentials aren't enrolled yet.
if systemctl is-active --quiet pollen; then
    systemctl restart pollen
fi
