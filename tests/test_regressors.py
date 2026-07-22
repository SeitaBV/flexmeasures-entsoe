"""
Unit tests for the novel price-regressor helpers:

- the residual-load derivation, and
- the generation-outage aggregation with its day-ahead knowledge horizon.

These test only the pure functions, so they need neither a FlexMeasures app nor a
database. To avoid importing the whole ``flexmeasures_entsoe`` package (whose __init__
pulls in FlexMeasures), we load ``regressors.py`` directly from its file path.
"""

import importlib.util
import os

import pandas as pd
import pytest


# Load flexmeasures_entsoe/prices/regressors.py in isolation.
_HERE = os.path.dirname(os.path.abspath(__file__))
_REGRESSORS_PATH = os.path.join(
    _HERE, "..", "flexmeasures_entsoe", "prices", "regressors.py"
)
_spec = importlib.util.spec_from_file_location("_entsoe_regressors", _REGRESSORS_PATH)
regressors = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(regressors)

TZ = "Europe/Amsterdam"


def _ts(value: str) -> pd.Timestamp:
    return pd.Timestamp(value, tz=TZ)


# ---------------------------------------------------------------------------
# Residual load
# ---------------------------------------------------------------------------


def test_compute_residual_load_subtracts_all_green_components():
    index = pd.date_range("2025-06-01", periods=3, freq="1h", tz=TZ)
    load = pd.Series([10000.0, 11000.0, 12000.0], index=index)
    solar = pd.Series([2000.0, 3000.0, 500.0], index=index)
    wind_offshore = pd.Series([1000.0, 1000.0, 1000.0], index=index)
    wind_onshore = pd.Series([500.0, 700.0, 900.0], index=index)

    residual = regressors.compute_residual_load(
        load, solar, wind_offshore, wind_onshore
    )

    expected = pd.Series([6500.0, 6300.0, 9600.0], index=index)
    pd.testing.assert_series_equal(
        residual, expected.rename("Residual load"), check_freq=False
    )


def test_compute_residual_load_treats_missing_green_as_zero():
    """A green component that is absent (NaN after alignment) should not raise the residual."""
    index = pd.date_range("2025-06-01", periods=2, freq="1h", tz=TZ)
    load = pd.Series([10000.0, 11000.0], index=index)
    solar = pd.Series([2000.0, 3000.0], index=index)
    # Wind offshore only reported for the first hour.
    wind_offshore = pd.Series([1000.0], index=index[:1])
    wind_onshore = pd.Series([500.0, 700.0], index=index)

    residual = regressors.compute_residual_load(
        load, solar, wind_offshore, wind_onshore
    )

    # Hour 0: 10000 - 2000 - 1000 - 500 = 6500
    # Hour 1: 11000 - 3000 -    0 - 700 = 7300 (missing offshore treated as 0)
    assert residual.iloc[0] == pytest.approx(6500.0)
    assert residual.iloc[1] == pytest.approx(7300.0)


def test_compute_residual_load_without_any_green_equals_load():
    """When no wind & solar is published (all green missing), residual load == load."""
    index = pd.date_range("2025-06-01", periods=3, freq="1h", tz=TZ)
    load = pd.Series([9000.0, 9500.0, 10000.0], index=index)
    empty = pd.Series(dtype="float64")

    residual = regressors.compute_residual_load(load, empty, empty, empty)

    pd.testing.assert_series_equal(
        residual, load.rename("Residual load"), check_freq=False
    )


# ---------------------------------------------------------------------------
# Neighbour mapping and green-column tolerance
# ---------------------------------------------------------------------------


def test_interconnected_neighbours_for_nl():
    assert regressors.INTERCONNECTED_NEIGHBOURS["NL"] == [
        "DE_LU",
        "BE",
        "GB",
        "NO_2",
        "DK_1",
    ]


def test_interconnected_neighbours_unknown_country_defaults_to_empty():
    # The command uses .get(country, []) so unlisted countries are a no-op, not an error.
    assert regressors.INTERCONNECTED_NEIGHBOURS.get("XX", []) == []


def test_green_column_returns_none_when_forecast_is_none():
    # Happens for neighbours (e.g. NO_2 / DK_1) that publish no wind & solar forecast.
    for column in regressors.GREEN_COLUMNS:
        assert regressors.green_column(None, column) is None


def test_green_column_returns_none_when_column_absent():
    df = pd.DataFrame({"Solar": [1.0, 2.0]})
    assert regressors.green_column(df, "Wind Offshore") is None
    assert regressors.green_column(df, "Solar") is not None


# ---------------------------------------------------------------------------
# Outage aggregation with knowledge horizon
# ---------------------------------------------------------------------------


def _outages_df(rows) -> pd.DataFrame:
    """Build an outages DataFrame shaped like entsoe-py's, indexed by created_doc_time."""
    df = pd.DataFrame(
        rows,
        columns=[
            "created_doc_time",
            "nominal_power",
            "start",
            "end",
            "avail_qty",
        ],
    )
    return df.set_index("created_doc_time")


def test_outage_unavailable_mw_is_nominal_minus_available():
    # Delivery day is 2025-06-02; published the day before -> counts.
    outages = _outages_df(
        [
            (
                _ts("2025-06-01 09:00"),  # created_doc_time (before delivery day)
                500.0,  # nominal_power
                _ts("2025-06-02 00:00"),  # start
                _ts("2025-06-03 00:00"),  # end (whole delivery day)
                200.0,  # avail_qty -> unavailable = 300 MW
            )
        ]
    )
    from_time = _ts("2025-06-02 00:00")
    until_time = _ts("2025-06-03 00:00")

    result = regressors.aggregate_outages_to_hourly_unavailable_mw(
        outages, from_time, until_time
    )

    assert len(result) == 24
    assert (result == 300.0).all()


def test_outage_published_on_delivery_day_is_excluded_no_lookahead_leak():
    """An outage published on/after the delivery day must not be counted (look-ahead leak)."""
    delivery_day_outage = _outages_df(
        [
            (
                _ts("2025-06-02 06:00"),  # published DURING the delivery day
                500.0,
                _ts("2025-06-02 00:00"),
                _ts("2025-06-03 00:00"),
                0.0,  # fully unavailable
            )
        ]
    )
    from_time = _ts("2025-06-02 00:00")
    until_time = _ts("2025-06-03 00:00")

    result = regressors.aggregate_outages_to_hourly_unavailable_mw(
        delivery_day_outage, from_time, until_time
    )

    # Nothing may be counted: it was not known before the delivery day.
    assert (result == 0.0).all()


def test_outage_knowledge_horizon_is_per_delivery_day_over_multiday_range():
    """Same outage doc counts for a later delivery day but not for one before it was published."""
    outages = _outages_df(
        [
            (
                _ts("2025-06-02 10:00"),  # published on June 2
                400.0,
                _ts("2025-06-02 00:00"),
                _ts("2025-06-04 00:00"),  # spans June 2 and June 3
                0.0,  # fully unavailable -> 400 MW
            )
        ]
    )
    from_time = _ts("2025-06-02 00:00")
    until_time = _ts("2025-06-04 00:00")  # two delivery days

    result = regressors.aggregate_outages_to_hourly_unavailable_mw(
        outages, from_time, until_time
    )

    june_2 = result[result.index.normalize() == _ts("2025-06-02")]
    june_3 = result[result.index.normalize() == _ts("2025-06-03")]

    # June 2 delivery: outage published same day -> not known before -> excluded.
    assert (june_2 == 0.0).all()
    # June 3 delivery: outage was published on June 2 (before that delivery day) -> counted.
    assert (june_3 == 400.0).all()


def test_outage_only_counts_overlapping_hours():
    outages = _outages_df(
        [
            (
                _ts("2025-06-01 09:00"),
                600.0,
                _ts("2025-06-02 03:00"),  # outage from 03:00 ...
                _ts("2025-06-02 06:00"),  # ... until 06:00 (exclusive)
                100.0,  # unavailable = 500 MW
            )
        ]
    )
    from_time = _ts("2025-06-02 00:00")
    until_time = _ts("2025-06-03 00:00")

    result = regressors.aggregate_outages_to_hourly_unavailable_mw(
        outages, from_time, until_time
    )

    # Hours 03:00, 04:00, 05:00 overlap; others do not.
    overlapping = [
        _ts("2025-06-02 03:00"),
        _ts("2025-06-02 04:00"),
        _ts("2025-06-02 05:00"),
    ]
    for hour in result.index:
        if hour in overlapping:
            assert result[hour] == pytest.approx(500.0)
        else:
            assert result[hour] == pytest.approx(0.0)


def test_outage_multiple_overlapping_outages_are_summed():
    outages = _outages_df(
        [
            (
                _ts("2025-06-01 09:00"),
                500.0,
                _ts("2025-06-02 00:00"),
                _ts("2025-06-03 00:00"),
                0.0,  # 500 MW all day
            ),
            (
                _ts("2025-06-01 10:00"),
                300.0,
                _ts("2025-06-02 10:00"),
                _ts("2025-06-02 12:00"),
                0.0,  # extra 300 MW during 10:00-12:00
            ),
        ]
    )
    from_time = _ts("2025-06-02 00:00")
    until_time = _ts("2025-06-03 00:00")

    result = regressors.aggregate_outages_to_hourly_unavailable_mw(
        outages, from_time, until_time
    )

    assert result[_ts("2025-06-02 09:00")] == pytest.approx(500.0)
    assert result[_ts("2025-06-02 10:00")] == pytest.approx(800.0)
    assert result[_ts("2025-06-02 11:00")] == pytest.approx(800.0)
    assert result[_ts("2025-06-02 12:00")] == pytest.approx(500.0)


def test_outage_empty_frame_returns_zero_series():
    empty = _outages_df([])
    from_time = _ts("2025-06-02 00:00")
    until_time = _ts("2025-06-03 00:00")

    result = regressors.aggregate_outages_to_hourly_unavailable_mw(
        empty, from_time, until_time
    )

    assert len(result) == 24
    assert (result == 0.0).all()


def test_outage_ignores_rows_without_nominal_power():
    outages = _outages_df(
        [
            (
                _ts("2025-06-01 09:00"),
                float("nan"),  # unknown nominal power -> skipped
                _ts("2025-06-02 00:00"),
                _ts("2025-06-03 00:00"),
                0.0,
            )
        ]
    )
    from_time = _ts("2025-06-02 00:00")
    until_time = _ts("2025-06-03 00:00")

    result = regressors.aggregate_outages_to_hourly_unavailable_mw(
        outages, from_time, until_time
    )

    assert (result == 0.0).all()
