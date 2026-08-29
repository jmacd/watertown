#!/bin/sh
set -eu
if [ "$#" -ne 2 ]; then
  printf '%s\n' "usage: $0 CAPSULE MATERIALIZED" >&2
  exit 2
fi
PYTHON=${PYTHON:-python3}
"$PYTHON" "$1/capsule.py" verify "$1"
"$PYTHON" "$1/capsule.py" materialize "$1" "$2"
