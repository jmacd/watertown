# Watertown `watertown.commit.v1` recovery kit

This kit extracts the current native Watertown backup format into a portable
`pondcapsule.4`. It contains no historical format readers or migration paths.

## Safety

Review every file and verify `SHA256SUMS` before supplying credentials.
Extraction is create-only: the destination must not exist and must not be
inside the source backup. Dynamic recipes are copied as inert data and are
never executed.

## Install dependencies

Use an isolated Python 3.13 environment and install the versions in
`requirements.lock`. Object-store CLIs may be obtained with the reviewed
`download-azcopy.sh` or `download-mc.sh` helpers when required by the source
backend.

## Extract

From a local native backup:

```sh
python extract.py /path/to/native-backup ./capsule \
  --ref main --birthplace recovered
```

Or select an exact commit:

```sh
python extract.py /path/to/native-backup ./capsule \
  --commit <64-hex-commit> --birthplace recovered
```

Then verify and materialize:

```sh
python ./capsule/capsule.py verify ./capsule
python ./capsule/capsule.py materialize ./capsule ./recovered
```

The extractor accepts only:

- `watertown.commit.v1`
- `watertown.tree.v1`
- `watertown.manifest.v1`
- `watertown.series.v2`
- `watertown.series-pack.v2`
- `watertown.recipe.v1`

It preserves the current stable logical hashing domains documented in
`FORMAT.md`. Any obsolete native object, recipe, series, pack, capsule, or
partition layout is rejected rather than interpreted or migrated.

Run the focused self-tests with:

```sh
python capsule_test.py
python integration_test.py
```
