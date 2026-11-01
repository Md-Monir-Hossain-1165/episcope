"""Contract tests for the committed EpiScope serving artefacts.

Run from the repository root::

    python -m pytest -q

The tests exercise the dashboard payloads that are committed to the
repository and the Makefile entry points. They deliberately avoid Spark,
network access and the generated Bronze/Silver/Gold trees, so a clean
checkout can run them in CI.
"""

import json
import re
from functools import lru_cache
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "dashboard" / "data"

PAYLOADS = (
    "epi_weekly.json",
    "forecasts.json",
    "alerts.json",
    "allocation.json",
    "calendar.json",
    "bangladesh_districts.geojson",
)

DISEASES = {"dengue", "diarrhoea", "ili", "malaria", "other"}
N_DISTRICTS = 64
HORIZONS = {1, 2, 3, 4}
MAX_ALERTS = 2000
OPERATING_Z = 3.0

PERSON_TOKENS = (
    "patient",
    "name",
    "phone",
    "mobile",
    "nid",
    "address",
    "birth",
    "dob",
    "email",
)


@lru_cache(maxsize=None)
def load(name):
    path = DATA / name
    if not path.is_file():
        pytest.skip(f"{name} is not committed in this checkout")
    return json.loads(path.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def weekly():
    return load("epi_weekly.json")


@pytest.fixture(scope="module")
def forecasts():
    return load("forecasts.json")


@pytest.fixture(scope="module")
def alerts():
    return load("alerts.json")


@pytest.fixture(scope="module")
def allocation():
    return load("allocation.json")


def test_required_payloads_exist():
    missing = [name for name in PAYLOADS if not (DATA / name).is_file()]
    assert not missing, f"dashboard payloads missing: {missing}"


def test_weekly_covers_the_national_grid(weekly):
    assert weekly, "weekly payload is empty"
    districts = {row["district"] for row in weekly}
    diseases = {row["disease_group"] for row in weekly}
    assert len(districts) == N_DISTRICTS
    assert diseases == DISEASES


def test_weekly_rows_are_aggregate_only(weekly):
    expected = {
        "district",
        "division",
        "disease_group",
        "epi_year",
        "epi_week",
        "case_count",
        "incidence_per_100k",
    }
    assert expected <= set(weekly[0])
    assert all(row["case_count"] is None or row["case_count"] >= 0 for row in weekly)


def test_forecast_grid_is_complete(forecasts):
    assert len(forecasts) == N_DISTRICTS * len(DISEASES) * len(HORIZONS)
    assert {row["horizon_weeks"] for row in forecasts} == HORIZONS
    assert len({(row["district"], row["disease_group"]) for row in forecasts}) == (
        N_DISTRICTS * len(DISEASES)
    )


def test_forecast_intervals_are_ordered(forecasts):
    bad = [
        row
        for row in forecasts
        if not (
            0 <= row["lower_95"] <= row["forecast_cases"] <= row["upper_95"]
        )
    ]
    assert not bad, f"{len(bad)} forecast rows have invalid intervals"


def test_alerts_respect_the_serving_cap(alerts):
    assert 0 < len(alerts) <= MAX_ALERTS
    assert all(row["c2_score"] >= OPERATING_Z for row in alerts)
    assert {row["alert_level"] for row in alerts} <= {"high", "watch"}


def test_allocation_respects_budget_and_demand(allocation):
    assert len(allocation) == N_DISTRICTS
    budgets = {row["national_budget"] for row in allocation}
    assert len(budgets) == 1
    budget = budgets.pop()
    assert sum(row["allocated_units"] for row in allocation) == budget
    for row in allocation:
        assert 0 <= row["allocated_units"] <= row["need"]
        assert row["unmet_need"] >= 0


def test_no_person_level_fields():
    offenders = {}
    for name in PAYLOADS:
        if name.endswith(".geojson"):
            # Boundary files legitimately carry place names, not patients.
            continue
        rows = load(name)
        keys = {key.lower() for row in rows for key in row}
        hits = sorted(
            key for key in keys if any(token in key for token in PERSON_TOKENS)
        )
        if hits:
            offenders[name] = hits
    assert not offenders, f"person-level fields exposed: {offenders}"


def test_dashboard_matches_serving_export():
    serving = ROOT / "serving" / "data"
    if not serving.is_dir():
        pytest.skip("serving/data has not been exported in this checkout")
    mismatched = []
    for name in PAYLOADS:
        dashboard_file = DATA / name
        serving_file = serving / name
        if not serving_file.is_file():
            continue
        if json.loads(dashboard_file.read_text(encoding="utf-8")) != json.loads(
            serving_file.read_text(encoding="utf-8")
        ):
            mismatched.append(name)
    assert not mismatched, f"dashboard/serving payloads differ: {mismatched}"


def test_makefile_entry_points_exist():
    makefile = ROOT / "Makefile"
    assert makefile.is_file()
    targets = set(re.findall(r"^([A-Za-z0-9_-]+):", makefile.read_text(), re.M))
    required = {
        "setup",
        "s1",
        "s3",
        "generate",
        "pipeline",
        "analytics",
        "serve",
        "dashboard",
        "scale-experiment",
        "clean",
    }
    assert required <= targets, f"missing Makefile targets: {sorted(required - targets)}"
