from __future__ import annotations

from pathlib import Path

from derezz.util import VideoSource, image_dhash, extract_image_features
from daft.datatype import ImageMode

# For YOLO
image_height=640
image_width=640


def index(filepath: Path) -> None:
    source = VideoSource(
        filepath=filepath,
        image_mode=ImageMode.RGB,
        image_height=image_height,
        image_width=image_width,
    )

    df = source.read()
    df = df.with_column("dhash", image_dhash(df["data"]))
    df = df.groupby("dhash").any_value("data", "frame_number")

    # embed images for feature extraction
    df = df.with_column("features", extract_image_features(df["data"])) # type: ignore

    # flatten the feature list with its associated frame number
    df = df.select("frame_number", df["features"].explode())
    df = df.select("frame_number", df["features"]["*"])
    df = df.write_parquet("out")

    # 


    # df.show()
