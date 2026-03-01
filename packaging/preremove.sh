#!/bin/sh
set -e
# deb: $1 = "remove"; rpm: $1 = 0
if [ "$1" = "remove" ] || [ "$1" = "0" ]; then
    systemctl stop pln || true
    systemctl disable pln || true
fi
