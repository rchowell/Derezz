import webbrowser
import warnings

from daft import col
from daft.catalog import Catalog
from daft.expressions import lit

from derezz.util import get_tempdir

warnings.filterwarnings('ignore')


def ls(catalog: Catalog) -> None:
    df = catalog.read_table("test.features")
    df = df.groupby("ft_label").agg(col("ft_label").count().alias("count")).sort("count", desc=True)
    print(f'{"count":>5} | labels')
    print("------+-------------------")
    for row in df.to_pylist():
        print(f'{row["count"]:>5} | {row["ft_label"]}')


def find(catalog: Catalog, pattern: str) -> None:
    terms = get_terms(pattern)
    df = catalog.read_table("test.features")
    df = df.filter(col("ft_label").is_in(terms))
    df = df.groupby("v_name").agg_list("ft_label")
    df = df.with_columns_renamed({
        "v_name": "video",
        "ft_label": "hits",
    })
    df.show()


def open_(catalog: Catalog, pattern: str) -> None:
    terms = get_terms(pattern)
    df = catalog.read_table("test.features")
    df = df.filter(col("ft_label").is_in(terms))

    # materialize and cache all results images so we can open them in file explorer
    out_dir = get_tempdir()
    for row in df.to_pylist():
        fname = f"{row['v_name']}-{row['f_number']}.jpg"
        fpath = out_dir / fname
        with open(fpath, "wb") as f:
            print(f"Writing {fpath}")
            f.write(row['ft_image'])

    # show the results
    webbrowser.open(f"file://{out_dir}")


def get_terms(pattern: str) -> list[str]:
    # could get more clever here later
    return [ term.strip() for term in pattern.split(",") ]
