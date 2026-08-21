#!/bin/sh
set -eu
umask 077

if [ "$#" -ne 2 ]; then
    printf '%s\n' "usage: sh $0 ALIAS/BUCKET/PREFIX DESTINATION" >&2
    exit 2
fi

SOURCE=${1%/}
DEST=$2
case "$SOURCE" in
    ''|/*|.*|-*|*'/../'*|*'/./'*) printf '%s\n' 'unsafe MinIO source' >&2; exit 2 ;;
    */*/*) ;;
    *) printf '%s\n' 'source must be ALIAS/BUCKET/PREFIX' >&2; exit 2 ;;
esac
case "$DEST" in
    ''|'/'|'.'|'..'|-*) printf '%s\n' 'unsafe destination' >&2; exit 2 ;;
esac
if [ -e "$DEST" ]; then
    printf '%s\n' "destination already exists: $DEST" >&2
    exit 2
fi

command -v mc >/dev/null 2>&1 || {
    printf '%s\n' 'MinIO Client (mc) is not installed' >&2
    exit 2
}

mkdir -m 700 "$DEST"
mc mirror --preserve "$SOURCE" "$DEST"
if [ ! -d "$DEST/_delta_log" ]; then
    printf '%s\n' "download has no _delta_log directory: $DEST" >&2
    exit 1
fi
printf '%s\n' "Native backup downloaded to $DEST"
