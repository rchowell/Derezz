import pytest
import daft
from daft import col


def test_group_labels_as_list():
    df = daft.from_pylist(
        [
            {"v_uuid": "a", "label": "cat"},
            {"v_uuid": "a", "label": "dog"},
            {"v_uuid": "b", "label": "cat"},
            {"v_uuid": "b", "label": "bird"},
            {"v_uuid": "b", "label": "dog"},
        ]
    )
    grouped = df.groupby("v_uuid").agg_list("label")
    grouped.show()

    # # Convert to pylist and sort for comparison
    # result = sorted(
    #     [
    #         {"v_uuid": row["v_uuid"], "labels": sorted(row["labels"])}
    #         for row in grouped.to_pylist()
    #     ],
    #     key=lambda x: x["v_uuid"]
    # )
    # expected_sorted = sorted(
    #     [
    #         {"v_uuid": row["v_uuid"], "labels": sorted(row["labels"])}
    #         for row in expected
    #     ],
    #     key=lambda x: x["v_uuid"]
    # )
    # assert result == expected_sorted
