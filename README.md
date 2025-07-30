```sh
# 1. Setup a Package
uv init --name derezzed --app derezzed
Initialized project `derezzed` at `/Users/rch/Projects/derezzed`

# 2. Install daft with AWS and Iceberg extras.
uv add "daft[aws,iceberg]"
```

## Usage

```
# Uploads this video to the search index.
derezz index ./path/to/video.mp4

# Returns a list of all videos + timestamps which have a cat.
derezz find 'cat'
```
