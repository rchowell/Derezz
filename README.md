```sh
# 1. Setup a Package
uv init --name derezzed --app derezzed
Initialized project `derezzed` at `/Users/rch/Projects/derezzed`

# 2. Install daft with AWS and Iceberg extras.
uv add "daft[aws,iceberg]"
```

## Model

```sql
CREATE TABLE videos (
    v_uuid UUID,
    v_name STRING
)

CREATE TABLE frames (
    v_uuid UUID,
    f_number LONG,
    f_image BINARY -- compressed jpg
)

CREATE TABLE IF NOT EXISTS features (
    ft_frame_uuid,
    ft_label STRING,
    ft_confidence DOUBLE,
    ft_bbox LIST<INT>
)
```

## Usage

```
# Uploads this video to the search index.
derezz index ./path/to/video.mp4

# Returns a list of all videos + timestamps which have a cat.
derezz find 'cat'
```
