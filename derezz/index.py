from __future__ import annotations

import uuid
import logging

from pathlib import Path

from derezz.util import VideoSource, image_dhash, extract_image_features, to_jpg, items

import daft
from daft.datatype import ImageMode
from daft.catalog import Catalog
from daft.expressions import lit

# YOLO expects 640x640
image_height=640
image_width=640


logger = logging.getLogger(__name__)


def index(catalog: Catalog, filepath: Path) -> None:
    """Indexes the video at the given filepath in the provided Catalog."""
    # Video metadata
    v_uuid = uuid.uuid4().bytes
    v_name = filepath.name # basename
    v_location = str(filepath)

    # Customer video source which streams frames into daft.
    source = VideoSource(
        filepath=filepath,
        image_mode=ImageMode.RGB,
        image_height=image_height,
        image_width=image_width,
    )

    # Read the video, deduplicate frames, then extract features
    logger.info("Processing features...")
    df = source.read()
    df = df.with_column("dhash", image_dhash(df["data"]))
    df = df.groupby("dhash").any_value("data", "frame_number")
    df = df.limit(5) # !! uncomment for production run !!
    df = df.with_column("features", extract_image_features(df["data"])) # type: ignore
    df = df.filter(~df["frame_number"].is_null())
    df = df.collect()

    logger.info("Writing video metadata to S3 Tables...")
    catalog.write_table("test.videos", daft.from_pylist([
        {
            "v_uuid": v_uuid,
            "v_name": v_name,
            "v_location": v_location,
        }
    ]))

    logger.info("Writing frame data to S3 Tables...")
    catalog.write_table("test.frames", df = df.select(*items({
        "v_uuid": lit(v_uuid),
        "v_name": lit(v_name),
        "f_number": df["frame_number"],
        "f_image": to_jpg(df["data"]),
    })))

    logger.info("Inserting features into index...")
    df = df.select(df["frame_number"], df["data"], df["features"].explode()["*"])
    df = df.filter("(label IS NOT NULL) AND (bbox IS NOT NULL)")
    df = df.select(*items({
        "v_uuid": lit(v_uuid),
        "v_name": lit(v_name),
        "f_number": df["frame_number"],
        "ft_label": df["label"],
        "ft_image": to_jpg(df["data"].image.crop(df["bbox"])),
        "ft_confidence": df["confidence"],
        "ft_bbox": df["bbox"],
    }))
    catalog.write_table("test.features", df)

    logger.info(f"Video `{v_name}` was indexed!")
