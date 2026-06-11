"""Generate or update the variable concept map skeleton.

Reads the country-variable inventory (data/output/variables.csv, produced by
consolidate-output) and writes src/consensus_economics/mappings/variable_map.csv.

Merge semantics: rows already in the map are preserved untouched — they carry
human judgments. Only inventory pairs not yet in the map are appended, with
mapping_status="new" and a mechanical concept_id slug, for later review.
"""

import argparse
import re

import pandas as pd

from consensus_economics.mappings import MAP_COLUMNS, MAP_PATH
from consensus_economics.paths import Paths

MAP_VERSION = "0.1"


def slug(raw_name: str) -> str:
    """Mechanical concept-id draft: 'Indust / Manuf Production' -> 'INDUST_MANUF_PRODUCTION'."""
    cleaned = raw_name.strip().upper().replace("&", "AND")
    return re.sub(r"[^A-Z0-9]+", "_", cleaned).strip("_")


def load_inventory() -> pd.DataFrame:
    path = Paths().output / "variables.csv"
    if not path.exists():
        raise FileNotFoundError(f"{path} not found — run `consolidate-output` first")
    return pd.read_csv(path, dtype=str)


def skeleton_rows(inventory: pd.DataFrame) -> pd.DataFrame:
    rows = pd.DataFrame(
        {
            "country": inventory["country"],
            "raw_variable": inventory["variable"],
            "valid_from": inventory["first_survey"],
            "valid_to": inventory["last_survey"],
            "concept_id": inventory["variable"].map(slug),
            "concept_label": inventory["variable"].str.strip(),
            "unit_code": "",
            "scale_factor": "1",
            "mapping_status": "new",
            "break_type": "",
            "evidence_note": "",
            "map_version": MAP_VERSION,
        }
    )
    return rows[MAP_COLUMNS]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate/update the variable concept map skeleton"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Rebuild from scratch, discarding existing judgments",
    )
    args = parser.parse_args()

    fresh = skeleton_rows(load_inventory())

    if MAP_PATH.exists() and not args.force:
        existing = pd.read_csv(MAP_PATH, dtype=str).fillna("")
        known = set(zip(existing["country"], existing["raw_variable"]))
        additions = fresh[
            ~fresh.apply(lambda r: (r["country"], r["raw_variable"]) in known, axis=1)
        ]
        combined = pd.concat([existing, additions], ignore_index=True)
        print(f"kept {len(existing)} existing rows, added {len(additions)} new")
    else:
        combined = fresh
        print(f"generated {len(combined)} rows from scratch")

    combined = combined.sort_values(["country", "raw_variable", "valid_from"])
    combined.to_csv(MAP_PATH, index=False)
    print(f"-> {MAP_PATH}")

    by_status = combined["mapping_status"].value_counts().to_dict()
    print(f"status counts: {by_status}")


if __name__ == "__main__":
    main()
