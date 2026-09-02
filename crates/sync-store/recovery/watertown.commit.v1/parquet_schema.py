"""Recover the logical Arrow schema embedded in Parquet metadata."""

from __future__ import annotations

import base64
from typing import Any


def _value_type(data_type: Any, pa: Any) -> Any:
    return data_type.value_type if pa.types.is_dictionary(data_type) else data_type


def read_parquet_schema(parquet: Any, pa: Any, error_type: type[Exception]) -> Any:
    """Return the Arrow schema, preserving logical types stored by ArrowWriter."""
    physical_schema = parquet.schema_arrow
    metadata = parquet.metadata.metadata or {}
    encoded = metadata.get(b"ARROW:schema")
    if encoded is None:
        return physical_schema
    if not isinstance(encoded, bytes):
        raise error_type("Parquet ARROW:schema metadata is not bytes")
    try:
        serialized = base64.b64decode(encoded, validate=True)
        schema = pa.ipc.read_schema(pa.BufferReader(serialized))
    except (ValueError, pa.ArrowInvalid, OSError) as error:
        raise error_type(f"invalid Parquet ARROW:schema metadata: {error}") from error

    if len(schema) != len(physical_schema):
        raise error_type("Parquet ARROW:schema field count differs from physical schema")
    for physical, logical in zip(physical_schema, schema):
        if physical.name != logical.name or physical.nullable != logical.nullable:
            raise error_type(
                "Parquet ARROW:schema field names or nullability differ from physical schema"
            )
        physical_type = _value_type(physical.type, pa)
        logical_type = _value_type(logical.type, pa)
        if physical_type == logical_type:
            continue
        if pa.types.is_int64(physical_type) and pa.types.is_timestamp(logical_type):
            continue
        if (
            pa.types.is_timestamp(physical_type)
            and pa.types.is_timestamp(logical_type)
            and physical_type.tz == logical_type.tz
        ):
            continue
        raise error_type(
            f"Parquet ARROW:schema type {logical_type} differs from physical type "
            f"{physical_type} for {physical.name!r}"
        )
    return schema
