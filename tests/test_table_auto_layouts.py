import sys
from pathlib import Path

APP_ROOT = Path(__file__).resolve().parents[1] / "blueprints-app"
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))

from app.table_auto_layouts import build_auto_layout  # noqa: E402
from app.table_layouts import encode_bucket_code  # noqa: E402


def _columns():
    return [
        {
            "column_key": "display_name",
            "display_name": "Display Name",
            "sqlite_column": "display_name",
            "width_px": 180,
            "min_width_px": 40,
            "max_width_px": 900,
            "position": 0,
            "hidden": False,
            "data_type": "TEXT",
            "sample_max_length": 28,
        },
        {
            "column_key": "addresses",
            "display_name": "Addresses",
            "sqlite_column": "addresses",
            "width_px": 220,
            "min_width_px": 40,
            "max_width_px": 900,
            "position": 1,
            "hidden": True,
            "data_type": "TEXT",
            "sample_max_length": 52,
        },
        {
            "column_key": "commit_hash",
            "display_name": "Commit Hash",
            "sqlite_column": "commit_hash",
            "width_px": 160,
            "min_width_px": 40,
            "max_width_px": 900,
            "position": 2,
            "hidden": True,
            "data_type": "TEXT",
            "sample_max_length": 40,
        },
        {
            "column_key": "_actions",
            "display_name": "Actions",
            "sqlite_column": None,
            "width_px": 96,
            "min_width_px": 40,
            "max_width_px": 900,
            "position": 3,
            "hidden": True,
            "data_type": None,
            "sample_max_length": None,
        },
    ]


def test_horizontal_scroll_auto_layout_keeps_every_column_visible():
    bucket_code = encode_bucket_code(
        {
            "horizontal_scroll": True,
            "mobile": True,
            "portrait": True,
        }
    )

    layout, planner = build_auto_layout(
        _columns(),
        bucket_code,
        table_name="fleet-nodes",
        viewport={"width_px": 360, "height_px": 800, "available_table_width_px": 360},
    )

    assert layout["algorithm_version"] == "auto-horizontal-v1"
    assert all(column["hidden"] is False for column in layout["columns"])
    assert planner["visible_count"] == 4
    assert planner["hidden_count"] == 0
    assert planner["max_estimated_cell_lines"] <= 4
    assert "horizontal_scroll_all_columns" in planner["reason_codes"]


def test_mobile_portrait_widths_stay_below_two_thirds_viewport_when_possible():
    bucket_code = encode_bucket_code(
        {
            "horizontal_scroll": True,
            "mobile": True,
            "portrait": True,
        }
    )

    layout, _planner = build_auto_layout(
        _columns(),
        bucket_code,
        table_name="fleet-nodes",
        viewport={"width_px": 360, "height_px": 800, "available_table_width_px": 360},
    )

    non_action_widths = [
        column["width_px"] for column in layout["columns"] if column["column_key"] != "_actions"
    ]
    assert max(non_action_widths) <= 238
    assert min(column["width_px"] for column in layout["columns"]) >= 64


def test_desktop_horizontal_layout_uses_wider_columns_than_mobile_portrait():
    mobile_bucket = encode_bucket_code(
        {
            "horizontal_scroll": True,
            "mobile": True,
            "portrait": True,
        }
    )
    desktop_bucket = encode_bucket_code({"horizontal_scroll": True})

    mobile_layout, _ = build_auto_layout(
        _columns(),
        mobile_bucket,
        table_name="fleet-nodes",
        viewport={"width_px": 360, "height_px": 800, "available_table_width_px": 360},
    )
    desktop_layout, _ = build_auto_layout(
        _columns(),
        desktop_bucket,
        table_name="fleet-nodes",
        viewport={"width_px": 1366, "height_px": 768, "available_table_width_px": 1280},
    )

    mobile_total = sum(column["width_px"] for column in mobile_layout["columns"])
    desktop_total = sum(column["width_px"] for column in desktop_layout["columns"])
    assert desktop_total > mobile_total
