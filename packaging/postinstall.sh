#!/bin/sh
set -e
getent group pln >/dev/null 2>&1 || groupadd --system pln
# Group-writable so pln group members can generate identity keys.
install -d -m 0770 -g pln /var/lib/pln
systemctl daemon-reload
systemctl enable pln
# On upgrade, restart to pick up the new binary.
# On fresh install, don't start — credentials aren't enrolled yet.
if systemctl is-active --quiet pln; then
    systemctl restart pln
fi
