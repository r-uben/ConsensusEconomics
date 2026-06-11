"""Integrity tests for the variable concept map."""

import re

import pytest

from consensus_economics.mappings import MAP_COLUMNS, MAP_PATH, load_variable_map
from consensus_economics.paths import Paths

VALID_STATUSES = {"confirmed", "needs_review", "new"}


@pytest.fixture(scope="module")
def variable_map():
    if not MAP_PATH.exists():
        pytest.skip("variable_map.csv not generated")
    return load_variable_map()


def test_columns_complete(variable_map):
    assert list(variable_map.columns) == MAP_COLUMNS


def test_no_duplicate_validity_keys(variable_map):
    dupes = variable_map.duplicated(["country", "raw_variable", "valid_from"])
    assert not dupes.any(), variable_map[dupes][["country", "raw_variable"]]


def test_statuses_valid(variable_map):
    assert set(variable_map["mapping_status"]) <= VALID_STATUSES


def test_concept_ids_wellformed(variable_map):
    pattern = re.compile(r"^[A-Z0-9_]+$")
    bad = variable_map[~variable_map["concept_id"].str.match(pattern)]
    assert bad.empty, bad[["country", "raw_variable", "concept_id"]]


def test_inventory_fully_mapped(variable_map):
    """Every country-variable pair in the corpus inventory has a map entry."""
    inventory_path = Paths().output / "variables.csv"
    if not inventory_path.exists():
        pytest.skip("inventory not generated (licensed data absent)")
    import pandas as pd

    inventory = pd.read_csv(inventory_path, dtype=str)
    inv_keys = set(zip(inventory["country"], inventory["variable"]))
    map_keys = set(zip(variable_map["country"], variable_map["raw_variable"]))
    assert inv_keys <= map_keys, sorted(inv_keys - map_keys)[:10]
