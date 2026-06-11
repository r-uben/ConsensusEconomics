"""Variable concept mapping — the versioned judgment layer over raw labels.

The Parquet archive stays vintage-faithful (raw variable names); this map
records which raw labels denote the same economic concept, with validity
ranges and review status. See SCHEMA.md.
"""

from pathlib import Path

import pandas as pd

MAP_PATH = Path(__file__).parent / "variable_map.csv"

MAP_COLUMNS = [
    "country",
    "raw_variable",
    "valid_from",
    "valid_to",
    "concept_id",
    "concept_label",
    "unit_code",
    "scale_factor",
    "mapping_status",
    "break_type",
    "evidence_note",
    "map_version",
]


def load_variable_map() -> pd.DataFrame:
    """Load the variable map; raises if it has not been generated yet."""
    if not MAP_PATH.exists():
        raise FileNotFoundError(
            f"{MAP_PATH} not found — run `build-variable-map` first"
        )
    df = pd.read_csv(MAP_PATH, dtype=str).fillna("")
    missing = set(MAP_COLUMNS) - set(df.columns)
    if missing:
        raise ValueError(f"variable_map.csv missing columns: {sorted(missing)}")
    return df
