"""Consolidate per-month output CSVs into single Parquet files.

Produces data/output/forecasters.parquet and data/output/forex.parquet with
typed columns (categoricals for repeated strings, dates as dates) — the file
you point an analysis at, instead of 800+ CSVs.
"""

import argparse

import pandas as pd
from tqdm import tqdm

from consensus_economics.paths import Paths

CATEGORICAL_COLUMNS = {
    "forecasters": ["country", "variable", "source", "statistic", "unit"],
    "forex": ["currency", "reference"],
}


def collect_kind(kind: str) -> pd.DataFrame:
    """Read every <year>/<kind>/<YYYYMM>.csv under data/output into one frame."""
    output = Paths().output
    files = sorted(output.glob(f"*/{kind}/*.csv"))
    if not files:
        raise FileNotFoundError(f"No {kind} CSVs found under {output}")

    frames = []
    for path in tqdm(files, desc=f"Reading {kind}", ncols=100):
        df = pd.read_csv(path, dtype={"release_date": "string"})
        # The survey month only lives in the filename; release_date can be
        # empty when the workbook's date cell was unparseable
        df["survey_date"] = path.stem
        frames.append(df)

    combined = pd.concat(frames, ignore_index=True)

    combined["release_date"] = pd.to_datetime(
        combined["release_date"], format="%Y%m%d", errors="coerce"
    )
    combined["survey_date"] = pd.to_datetime(
        combined["survey_date"], format="%Y%m", errors="raise"
    )
    for col in CATEGORICAL_COLUMNS[kind]:
        combined[col] = combined[col].astype("category")

    return combined


def write_variable_inventory(combined: pd.DataFrame) -> None:
    """Inventory of raw variable names — the input for any canonicalization map."""
    inventory = (
        combined.groupby(["country", "variable"], observed=True)
        .agg(
            n_obs=("value", "size"),
            first_survey=("survey_date", "min"),
            last_survey=("survey_date", "max"),
            units=("unit", lambda u: " | ".join(sorted(set(u.dropna().astype(str))))),
        )
        .reset_index()
        .sort_values(["country", "variable"])
    )
    target = Paths().output / "variables.csv"
    inventory.to_csv(target, index=False)
    print(f"variables: {len(inventory):,} country-variable pairs -> {target}")


def consolidate(kind: str) -> None:
    combined = collect_kind(kind)
    target = Paths().output / f"{kind}.parquet"
    combined.to_parquet(target, index=False)
    print(f"{kind}: {len(combined):,} rows -> {target}")
    if kind == "forecasters":
        write_variable_inventory(combined)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Consolidate output CSVs into Parquet files"
    )
    parser.add_argument(
        "--kind",
        choices=["forecasters", "forex"],
        help="Consolidate only one kind (default: both)",
    )
    args = parser.parse_args()

    kinds = [args.kind] if args.kind else ["forecasters", "forex"]
    for kind in kinds:
        consolidate(kind)


if __name__ == "__main__":
    main()
