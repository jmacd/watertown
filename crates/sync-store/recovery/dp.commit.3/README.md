# Watertown `dp.commit.3` Recovery Kit

This kit extracts a portable recovery capsule from a downloaded Watertown
ContentRemote backup without running a source-format `pond` binary.

## Safety

1. Verify the `README.sh` hash supplied outside the backup.
2. Review `README.sh` before running it.
3. Run it only with an empty destination directory name.
4. Review every extracted file before following later instructions.
5. Authenticate Azure CLI or MinIO Client separately. No credential belongs in
   this kit.
6. Keep the source backup read-only.

`README.sh` only creates files and prints instructions. It performs no network
access, executes no extracted file, modifies no pond, and deletes no storage.

## Recovery stages

1. Download the complete native Delta backup, including `_delta_log/` and
   `_blobs/`, with reviewed `az`, `azcopy`, or `mc` commands.
2. Create a disposable Python 3.13 virtual environment and install the exact
   packages in `requirements.lock`.
3. Select a native ref or exact commit hash.
4. Run one of:

   ```sh
   python extract.py BACKUP CAPSULE --ref REF --birthplace LABEL
   python extract.py BACKUP CAPSULE --commit HASH --birthplace LABEL
   ```

   `BACKUP` is a complete local download and `CAPSULE` must not exist.
5. Run a current `pond capsule verify CAPSULE` before import.

The extractor:

- reads the native Delta table and `_blobs/` only;
- resolves live rows by greatest transaction sequence;
- rejects duplicate winners, malformed objects, unsafe paths, and hash
  mismatches;
- decodes the selected `dp.commit.3` graph without invoking `pond`; and
- creates a new portable capsule without changing the backup.

Internally it performs these steps:

1. Select a native ref or exact commit hash.
2. Read the current Delta snapshot and resolve the live `objects` and `refs`
   rows by greatest `txn_seq`.
3. Decode the selected `dp.commit.3` graph and materialize the portable capsule
   manifest and `recovery/objects/blake3=<hash>` files.
4. Verify every physical object, logical leaf, series root, and capsule root.
5. Import only into a new empty target pond.

Before using a newly published kit, run its dependency-free decoder check:

```sh
python extract.py --verify-fixtures native-fixtures.json
```

This checks the independent decoder against byte-for-byte fixtures also
validated by the Rust source codecs. Production publication remains gated on a
full native-backup-to-capsule integration test.
