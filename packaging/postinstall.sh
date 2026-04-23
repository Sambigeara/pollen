#!/bin/sh
set -e
/usr/bin/pln daemon install
if command -v systemctl >/dev/null 2>&1; then
    # On upgrade, restart to pick up the new binary.
    # On fresh install, don't start — credentials aren't enrolled yet.
    if systemctl is-active --quiet pln; then
        systemctl restart pln
    fi
fi
