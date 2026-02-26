#!/bin/sh
set -e
getent group pollen >/dev/null 2>&1 || groupadd --system pollen
install -d -m 0750 -g pollen /var/lib/pollen
systemctl daemon-reload
systemctl enable pollen
# On upgrade, restart to pick up the new binary.
# On fresh install, don't start — credentials aren't enrolled yet.
if systemctl is-active --quiet pollen; then
    systemctl restart pollen
fi
