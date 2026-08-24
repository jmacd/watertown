#!/bin/sh
set -eu
umask 077

usage() {
    printf '%s\n' \
        "usage: sh $0 CAPSULE NEW_DESTINATION [WHEELHOUSE]" \
        "" \
        "Verifies and materializes a pondcapsule.1 without Pond." \
        "Set PYTHON to override python3.13 and RECOVERY_VENV to choose the venv." \
        "If WHEELHOUSE is given, package installation is strictly offline." >&2
    exit 2
}

[ "$#" -ge 2 ] && [ "$#" -le 3 ] || usage

CAPSULE=$1
DEST=$2
WHEELHOUSE=${3:-}
PYTHON=${PYTHON:-python3.13}
VENV=${RECOVERY_VENV:-watertown-capsule-venv}
KIT=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)

case "$DEST" in
    ''|'/'|'.'|'..'|-*) printf '%s\n' 'unsafe destination' >&2; exit 2 ;;
esac
[ -d "$CAPSULE" ] || {
    printf '%s\n' "capsule directory does not exist: $CAPSULE" >&2
    exit 2
}
[ ! -e "$DEST" ] || {
    printf '%s\n' "destination already exists: $DEST" >&2
    exit 2
}
command -v "$PYTHON" >/dev/null 2>&1 || {
    printf '%s\n' "$PYTHON is not installed; Python 3.13 is required" >&2
    exit 2
}
"$PYTHON" -c 'import sys; raise SystemExit(sys.version_info < (3, 13))' || {
    printf '%s\n' "$PYTHON is older than the required Python 3.13" >&2
    exit 2
}

for name in CAPSULE-README.md CAPSULE-FORMAT.md capsule.py capsule-requirements.lock recover.sh; do
    [ -f "$CAPSULE/$name" ] || {
        printf '%s\n' "capsule is missing recovery aid: $name" >&2
        exit 1
    }
    cmp "$KIT/$name" "$CAPSULE/$name" >/dev/null || {
        printf '%s\n' \
            "capsule recovery aid differs from this trusted kit: $name" \
            "Use the authenticated recovery-kit copy, not the capsule copy." >&2
        exit 1
    }
done

if [ ! -d "$VENV" ]; then
    "$PYTHON" -m venv "$VENV"
fi
VPYTHON=$VENV/bin/python
if [ ! -x "$VPYTHON" ]; then
    VPYTHON=$VENV/Scripts/python.exe
fi
[ -x "$VPYTHON" ] || {
    printf '%s\n' "virtual environment has no Python executable: $VENV" >&2
    exit 1
}

if ! "$VPYTHON" -c '
import importlib.metadata
import pathlib
import sys

for line in pathlib.Path(sys.argv[1]).read_text().splitlines():
    line = line.strip()
    if not line or line.startswith("#"):
        continue
    name, expected = line.split("==", 1)
    if importlib.metadata.version(name) != expected:
        raise SystemExit(1)
' "$KIT/capsule-requirements.lock" >/dev/null 2>&1; then
    if [ -n "$WHEELHOUSE" ]; then
        [ -d "$WHEELHOUSE" ] || {
            printf '%s\n' "wheelhouse directory does not exist: $WHEELHOUSE" >&2
            exit 2
        }
        "$VPYTHON" -m pip install --no-index --find-links "$WHEELHOUSE" \
            -r "$KIT/capsule-requirements.lock"
    else
        "$VPYTHON" -m pip install -r "$KIT/capsule-requirements.lock"
    fi
fi

"$VPYTHON" "$KIT/capsule.py" verify "$CAPSULE"
"$VPYTHON" "$KIT/capsule.py" materialize "$CAPSULE" "$DEST"
printf '%s\n' \
    "Recovery completed without Pond." \
    "Read $DEST/README.txt, then use $DEST/inventory.json to locate each logical path."
