#!/bin/sh
set -e
pln provision
if command -v systemctl >/dev/null 2>&1; then
    systemctl daemon-reload
    systemctl enable pln
    # On upgrade, restart to pick up the new binary.
    # On fresh install, don't start — credentials aren't enrolled yet.
    if systemctl is-active --quiet pln; then
        systemctl restart pln
    fi
fi
