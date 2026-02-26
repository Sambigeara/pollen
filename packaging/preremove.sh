#!/bin/sh
set -e
# deb: $1 = "remove"; rpm: $1 = 0
if [ "$1" = "remove" ] || [ "$1" = "0" ]; then
    systemctl stop pollen || true
    systemctl disable pollen || true
fi
