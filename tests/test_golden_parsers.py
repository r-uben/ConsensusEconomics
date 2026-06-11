"""Golden-file regression tests for the worksheet parsers.

These parse real Consensus Economics workbooks from data/xlsx (licensed data,
never committed) and pin the output schema plus a handful of reference values
across vintages. The hardcoded row offsets in the parsers are the most likely
thing to break when the workbook layout changes — these tests are the alarm.

Skipped automatically when the data files are not available.
"""

import pytest

from consensus_economics.paths import Paths
from consensus_economics.worksheets.country_worksheet import CountryWorksheet
from consensus_economics.worksheets.forex_worksheet import ForexWorksheet

EXPECTED_COUNTRY_COLUMNS = [
    "country", "variable", "source", "statistic",
    "year", "value", "unit", "release_date",
]

EXPECTED_FOREX_COLUMNS = [
    "currency", "reference", "year", "horizon",
    "current_value", "forecasted_value", "release_date",
]

# (date, country, n_rows, release_date, variable, year, consensus_mean)
COUNTRY_CASES = [
    ("199101", "USA", 714, "19910107", "10 Year Govt Bond Yield", 1991, 7.793182),
    ("200506", "Canada", 481, "20050613", "Consumer Prices", 2005, 2.06875),
    ("202409", "USA", 861, "20240909", "10 Year Govt Bond Yield", 2024, 3.809253),
]

# (date, n_rows, release_date, currency, horizon, forecasted_value)
FOREX_CASES = [
    ("200506", 39, "20050613", "CAD", 3, 1.241),
    ("202409", 39, "20240909", "CAD", 3, 1.369377),
]


def _require_xlsx(date: str) -> None:
    try:
        available = (Paths().xlsx / f"{date}.xlsx").exists()
    except FileNotFoundError:
        available = False
    if not available:
        pytest.skip(f"data/xlsx/{date}.xlsx not available")


@pytest.mark.parametrize(
    "date,country,n_rows,release_date,variable,year,mean", COUNTRY_CASES
)
def test_country_worksheet_golden(date, country, n_rows, release_date, variable, year, mean):
    _require_xlsx(date)
    ws = CountryWorksheet(date, country)
    df = ws.forecasters_data

    assert list(df.columns) == EXPECTED_COUNTRY_COLUMNS
    assert len(df) == n_rows
    assert ws.release_date == release_date
    assert df["value"].notna().all()

    row = df[
        (df["variable"] == variable)
        & (df["year"] == year)
        & (df["source"] == "Consensus")
        & (df["statistic"] == "mean")
    ]
    assert len(row) == 1
    assert row["value"].iloc[0] == pytest.approx(mean, abs=1e-6)


@pytest.mark.parametrize(
    "date,n_rows,release_date,currency,horizon,forecasted", FOREX_CASES
)
def test_forex_worksheet_golden(date, n_rows, release_date, currency, horizon, forecasted):
    _require_xlsx(date)
    ws = ForexWorksheet(date)
    df = ws.forecasters_data

    assert list(df.columns) == EXPECTED_FOREX_COLUMNS
    assert len(df) == n_rows
    assert ws.release_date == release_date

    row = df[
        (df["currency"] == currency)
        & (df["reference"] == "USD")
        & (df["horizon"] == horizon)
    ]
    assert len(row) == 1
    assert row["forecasted_value"].iloc[0] == pytest.approx(forecasted, abs=1e-6)
