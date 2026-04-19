#!/bin/sh
set -e
# deb: $1 = "remove"; rpm: $1 = 0
if [ "$1" = "remove" ] || [ "$1" = "0" ]; then
    /usr/bin/pln service uninstall 2>/dev/null || true
fi
