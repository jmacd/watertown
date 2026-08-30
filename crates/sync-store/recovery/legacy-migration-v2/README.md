# Legacy opaque migration recovery kit

This new recipe converts a frozen `dp.commit.3` backup into the distinct
immutable `pondcapsule.legacy.2` migration envelope. It does not alter or
replace the published `dp.commit.3`/`pondcapsule.1` recipe.

Install the exact extractor dependencies:

```sh
python3 -m venv recovery-venv
. recovery-venv/bin/activate
python -m pip install -r requirements.lock
python extract.py --verify-fixtures native-fixtures.json
```

Extract by ref or exact commit:

```sh
python extract.py BACKUP CAPSULE --ref main --birthplace LABEL
python CAPSULE/capsule.py verify CAPSULE
```

The source side verifies the legacy physical graph and raw hashes only. It does
not import PyArrow, inspect Parquet schemas/rows, require homogeneous table
schemas, or calculate Watertown semantic leaf identity. Target Watertown owns
per-version Parquet decoding, replay, corrected per-leaf schema fingerprints,
logical hashes, deterministic replay validation, and atomic promotion.

Failed extraction leaves its hidden sibling staging directory for inspection.
The source backup and requested destination are never mutated in place.

Maintainers can generate a synthetic evolving-schema Delta backup and prove the
extractor succeeds under an import hook that rejects and records every PyArrow
import attempt:

```sh
python -m pip install -r integration-requirements.lock
python integration_test.py
```
