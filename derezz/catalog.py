from __future__ import annotations

from pathlib import Path

from daft.catalog import Catalog

from typing import Literal

from pyiceberg.catalog import load_catalog as _load_catalog
from pyiceberg.catalog import Catalog as PyIcebergCatalog
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import IdentityTransform
from pyiceberg.schema import Schema as PyIcebergSchema
from pyiceberg.types import (
    BinaryType,
    StringType,
    LongType,
    FloatType,
    ListType,
    IntegerType,
    FixedType,
    NestedField,
)


Stage = Literal["prod", "dev"]


videos_table_schema = PyIcebergSchema(
    NestedField(1, "v_uuid", FixedType(16), required=True),
    NestedField(2, "v_name", StringType(), required=True),
    NestedField(3, "v_location", StringType(), required=True)
)

frames_table_schema = PyIcebergSchema(
    NestedField(1, "v_uuid", FixedType(16), required=True),  # <-- partition!
    NestedField(2, "v_name", StringType(), required=True),
    NestedField(3, "f_number", LongType(), required=True),
    NestedField(4, "f_image", BinaryType(), required=True),
)

frames_table_partitioning = PartitionSpec(
    PartitionField(
        source_id=1,
        field_id=1000,
        transform=IdentityTransform(),
        name="v_uuid"
    )
)

features_table_schema = PyIcebergSchema(
    NestedField(1, "v_uuid", FixedType(16), required=True),
    NestedField(2, "v_name", StringType(), required=True),
    NestedField(3, "f_number", LongType(), required=True),
    NestedField(4, "ft_label", StringType(), required=True),  # <-- partition!
    NestedField(5, "ft_image", BinaryType(), required=True),
    NestedField(6, "ft_confidence", FloatType(), required=True),
    NestedField(8, "ft_bbox", ListType(
        element_id=7,
        element=IntegerType(),
    ), required=False),
)


features_table_partitioning = PartitionSpec(
    PartitionField(
        source_id=4,
        field_id=1000,
        transform=IdentityTransform(),
        name="ft_label"
    )
)


def load_catalog(stage: Stage) -> Catalog:
    if stage == "prod":
        raise NotImplementedError()
    else:
        catalog = _load_catalog_dev()
    catalog.create_namespace_if_not_exists("test")
    catalog.create_table_if_not_exists(
        identifier="test.videos",
        schema=videos_table_schema,
    )
    catalog.create_table_if_not_exists(
        identifier="test.frames",
        schema=frames_table_schema,
        partition_spec=frames_table_partitioning,
    )
    catalog.create_table_if_not_exists(
        identifier="test.features",
        schema=features_table_schema,
        partition_spec=features_table_partitioning,
    )
    return Catalog.from_iceberg(catalog)


def _load_catalog_dev() -> PyIcebergCatalog:
    """Loads the sqlite development catalog."""
    warehouse_dir = Path.home() / ".derezz" / "warehouse"
    warehouse_dir.mkdir(parents=True, exist_ok=True)
    warehouse_path = str(warehouse_dir)
    return _load_catalog(
        "default",
        **{
            "type": "sql",
            "uri": f"sqlite:///{warehouse_path}/pyiceberg_catalog.db",
            "warehouse": f"file://{warehouse_path}",
        },
    )
