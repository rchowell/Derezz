import cv2
from pathlib import Path
import logging
import os

import torch
import imagehash
import yt_dlp
import numpy as np

from torchvision import transforms

from PIL import Image
from ultralytics import YOLO
from ultralytics.engine.results import Results, Boxes

import daft
from daft.io import DataSource, DataSourceTask
from daft.io.pushdowns import Pushdowns
from daft.schema import Schema
from daft.datatype import DataType, ImageMode
from daft.recordbatch import MicroPartition
from daft.expressions import Expression
from daft.series import Series

from dataclasses import dataclass
from typing import Iterator, TypedDict

logger = logging.getLogger(__name__)

to_tensor = transforms.ToTensor()


class Feature(TypedDict):
    label: str
    confidence: float
    bbox: list[int]  # x1, y1, x2, y2


FeatureType = DataType.struct(
    {
        "label": DataType.string(),
        "confidence": DataType.float64(),
        "bbox": DataType.fixed_size_list(DataType.int64(), 4),  # x1, y1, x2, y2
    }
)


def get_cachedir() -> Path:
    return Path.home() / ".derezz" / "cache"


def get_tempdir() -> Path:
    import time
    timestamp = int(time.time())
    tempdir = get_cachedir() / str(timestamp)
    tempdir.mkdir(parents=True, exist_ok=True)
    return tempdir


@daft.udf(return_dtype=DataType.binary())
def to_jpg(images: Series) -> list[bytes]:
    """Encodes the RGB vector to JPEG bytes."""
    return [ cv2.imencode(".jpg", image)[1].tobytes() for image in images ]


@daft.udf(return_dtype=DataType.binary())
def draw_box(images: Series, bboxes: Series, color = (255, 0, 255)): # magenta
    """Draws the bbox[min_x, min_y, max_x, max_y] on the JPEG encoded image."""
    def draw_box_on_image(image, bbox):
        img = cv2.imdecode(np.frombuffer(image, np.uint8), cv2.IMREAD_COLOR)
        if bbox is not None:
            x1, y1, x2, y2 = [int(v) for v in bbox]
            cv2.rectangle(img, (x1, y1), (x2, y2), color=color, thickness=2)
        return cv2.imencode(".jpg", img)[1].tobytes()
    return [draw_box_on_image(image, bbox) for image, bbox in zip(images, bboxes)]


@daft.udf(return_dtype=DataType.list(FeatureType))
class ExtractImageFeatures:
    def __init__(self) -> None:
        self.model = YOLO("yolo11n.pt")
        if torch.cuda.is_available():
            self.model.to("cuda")

    def __call__(self, images: Series) -> list[list[Feature]]:
        if (len(images) == 0):
            return []
        # convert daft images into a pytorch stack
        batch = [to_tensor(Image.fromarray(image)) for image in images]
        stack = torch.stack(batch, dim=0)
        return [self.to_features(res) for res in self.model(stack)]

    def to_features(self, res: Results) -> list[Feature]:
        """Converts YOLO Results to a list[Feature]."""
        return [self.to_feature(box) for box in res.boxes]

    def to_feature(self, box: Boxes) -> Feature:
        """Converts YOLO Box to a Feature."""
        return {
            "label": self.model.names[int(box.cls[0])],
            "confidence": float(box.conf[0]),
            "bbox": box.xyxy[0].tolist(),
        }


extract_image_features = ExtractImageFeatures


def items(projections: dict[str, Expression]) -> list[Expression]:
    """Helper to convert projection items (dict) into a projection item list."""
    return [ expr.alias(alias) for (alias, expr) in projections.items() ]


@dataclass
class VideoSource(DataSource):
    """
    DataSource for reading video files and streaming frames as images.

    Attributes:
        filepath (str): Path to the video file.
        image_height (int): Height to which each frame will be resized.
        image_width (int): Width to which each frame will be resized.
    """
    filepath: str
    image_height: int
    image_width: int

    @property
    def name(self) -> str:
        return "VideoSource"

    @property
    def schema(self) -> Schema:
        return Schema.from_pydict(
            {
                "frame_number": DataType.int64(),
                "frame_size_bytes": DataType.int64(),
                "timestamp_ms": DataType.int64(),
                "pts": DataType.int64(),  # presentation timestamp
                "dts": DataType.int64(),  # decode timestamp
                "fps": DataType.float64(),
                "data": DataType.image(
                    height=self.image_height,
                    width=self.image_width,
                    mode=ImageMode.RGB,
                ),
            }
        )

    def get_tasks(self, pushdowns: Pushdowns) -> Iterator[DataSourceTask]:
        yield VideoSourceTask(
            filepath=self.filepath,
            image_height=self.image_height,
            image_width=self.image_width,
        )


@dataclass
class VideoSourceTask(DataSourceTask):
    filepath: str
    image_height: int
    image_width: int

    @property
    def schema(self) -> Schema:
        return Schema.from_pydict(
            {
                "frame_number": DataType.int64(),
                "frame_size_bytes": DataType.int64(),
                "timestamp_ms": DataType.int64(),
                "pts": DataType.int64(),  # presentation timestamp
                "dts": DataType.int64(),  # decode timestamp
                "fps": DataType.float64(),
                "data": DataType.image(
                    height=self.image_height,
                    width=self.image_width,
                    mode=ImageMode.RGB,
                ),
            }
        )

    def create_empty_partition(self):
        return {
            "frame_number": [],
            "timestamp_ms": [],
            "pts": [],
            "dts": [],
            "fps": [],
            "frame_size_bytes": [],
            "data": [],
        }

    def download_youtube_video(self, quality="best[height<=720]", audio=False) -> str:
        url = self.filepath
        cache_dir = get_cachedir()
        tempfile = os.path.join(cache_dir, f"youtube_{hash(url) % 10000}.%(ext)s")
        logger.info(f"Downloading youtube video to {tempfile}...")

        if audio:
            format_str = quality
        else:
            # try best non-AV1, then fallback to best non-AV1
            format_str = f"{quality}[vcodec!*=av01]/best[vcodec!*=av01]"

        ydl_opts = {
            "outtmpl": tempfile,
            "format": format_str,
            "quiet": True,
            "no_warnings": True,
        }
        
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(url, download=False)
                # title = info.get("title", "video")
                ext = info.get("ext", "mp4")
                ydl.download([url])
                return tempfile.replace("%(ext)s", ext)
        except Exception as e:
            raise Exception(f"Failed to download video: {str(e)}")


    def get_micro_partitions(self) -> Iterator[MicroPartition]:
        if ("youtube.com" in str(self.filepath)):
            filepath = self.download_youtube_video()
        else:
            filepath = self.filepath

        cap = cv2.VideoCapture(str(filepath))

        if not cap.isOpened():
            logger.error(f"Failed to open video file {filepath}")
            return

        try:
            fps = cap.get(cv2.CAP_PROP_FPS)

            # target ~10MB partitions
            max_partition_size = 10 * 1024 * 1024 # 10 MB

            # build a partition
            frame_count = 0
            curr_partition = self.create_empty_partition()
            curr_partition_size = 0

            while True:
                curr_pos_ms = cap.get(cv2.CAP_PROP_POS_MSEC) # current timestamp

                ret, frame = cap.read()
                if not ret:
                    break

                try:
                    # calculating various timestamps
                    if curr_pos_ms > 0:
                        timestamp_ms = int(curr_pos_ms)
                    else:
                        timestamp_ms = (int(frame_count * (1000.0 / fps)) if fps > 0 else 0)
                    pts_us = timestamp_ms * 1000  # ms to us
                    dts_us = pts_us

                    frame_rgb = frame # already RGB
                    frame_resized = cv2.resize(
                        frame_rgb,
                        (self.image_width, self.image_height),
                        interpolation=cv2.INTER_AREA,
                    )
                    frame_size_bytes = frame_resized.nbytes

                    # append column entries
                    curr_partition["frame_number"].append(frame_count)
                    curr_partition["timestamp_ms"].append(timestamp_ms)
                    curr_partition["pts"].append(pts_us)
                    curr_partition["dts"].append(dts_us)
                    curr_partition["fps"].append(fps)
                    curr_partition["frame_size_bytes"].append(frame_size_bytes)
                    curr_partition["data"].append(frame_resized)
                    curr_partition_size += frame_size_bytes + 64

                    # yield partition once its big enough
                    if (curr_partition_size >= max_partition_size):
                        yield MicroPartition.from_pydict(curr_partition)
                        curr_partition = self.create_empty_partition()
                        curr_partition_size = 0
                    frame_count += 1

                except Exception as e:
                    logger.warning(f"Skipping corrupted frame {frame_count}: {e}")
                    frame_count += 1
                    continue
            
            # don't forget to flush!
            if curr_partition_size > 0:
                yield MicroPartition.from_pydict(curr_partition)

        except Exception as e:
            logger.error(f"Error processing video {self.filepath}: {e}")
        finally:
            cap.release()


@daft.udf(return_dtype=bytes)
def image_dhash(images: Series):
    """The dhash algorithm is faster than ahash and phash.

    https://www.hackerfactor.com/blog/index.php?/archives/529-Kind-of-Like-That.html
    """
    return [imagehash.dhash(Image.fromarray(img)).hash.tobytes() for img in images]
