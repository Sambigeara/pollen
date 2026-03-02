#!/bin/sh
set -e
getent group pln >/dev/null 2>&1 || groupadd --system pln
id -u pln >/dev/null 2>&1 || useradd -r -s /usr/sbin/nologin -g pln pln
# Group-writable so pln group members can generate identity keys.
install -d -m 0770 -o pln -g pln /var/lib/pln
install -d -m 0770 -o pln -g pln /var/lib/pln/keys
if command -v systemctl >/dev/null 2>&1; then
    systemctl daemon-reload
    systemctl enable pln
    # On upgrade, restart to pick up the new binary.
    # On fresh install, don't start — credentials aren't enrolled yet.
    if systemctl is-active --quiet pln; then
        systemctl restart pln
    fi
fi
