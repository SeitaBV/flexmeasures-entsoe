"""
Tests for the neighbour-country regressor collection.

These exercise the orchestration in ``prices/day_ahead.py`` with a *mocked* ENTSO-E
client and a stubbed ``ensure_sensors`` (so no live API and no database are touched). We
focus on the graceful handling of neighbours that publish no wind & solar forecast (e.g.
NO_2 / DK_1): residual load must fall back to the plain load forecast instead of crashing.

``flexmeasures`` must be importable for these (the package __init__ pulls it in); if it is
not installed, the whole module is skipped.
"""

import pandas as pd
import pytest

pytest.importorskip("flexmeasures")

from flexmeasures_entsoe.prices import day_ahead  # noqa: E402

TZ = "Europe/Amsterdam"
FROM_TIME = pd.Timestamp("2025-06-02 00:00", tz=TZ)
UNTIL_TIME = pd.Timestamp("2025-06-03 00:00", tz=TZ)
HOURLY = pd.date_range(FROM_TIME, UNTIL_TIME, freq="1h", inclusive="left", tz=TZ)


class FakeSensor:
    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return f"<FakeSensor {self.name}>"


class FakeLog:
    def __init__(self):
        self.warnings = []

    def info(self, *args):
        pass

    def debug(self, *args):
        pass

    def warning(self, msg, *args):
        self.warnings.append(msg)


class NoData(Exception):
    """Stand-in for entsoe.exceptions.NoMatchingDataError."""


class FakeClient:
    """Configurable fake ENTSO-E client.

    wind_solar_mode: 'ok' | 'raise' | 'empty' | 'partial'
    """

    def __init__(self, wind_solar_mode="ok", load_mode="ok"):
        self.wind_solar_mode = wind_solar_mode
        self.load_mode = load_mode

    def query_load_forecast(self, country_code, start, end):
        if self.load_mode == "raise":
            raise NoData("no load forecast")
        if self.load_mode == "empty":
            return pd.DataFrame({"Forecasted Load": []})
        return pd.DataFrame({"Forecasted Load": 12000.0}, index=HOURLY)

    def query_wind_and_solar_forecast(self, country_code, start, end, psr_type=None):
        if self.wind_solar_mode == "raise":
            raise NoData("no wind & solar forecast")
        if self.wind_solar_mode == "empty":
            return pd.DataFrame()
        if self.wind_solar_mode == "partial":
            return pd.DataFrame({"Solar": 2000.0}, index=HOURLY)
        return pd.DataFrame(
            {"Solar": 2000.0, "Wind Onshore": 1000.0, "Wind Offshore": 500.0},
            index=HOURLY,
        )


@pytest.fixture
def stub_ensure_sensors(monkeypatch):
    """Return sensors named exactly as requested (suffixed), without touching the DB."""

    def _fake_ensure(specs, country_code, timezone):
        return {spec[0]: FakeSensor(spec[0]) for spec in specs}

    monkeypatch.setattr(day_ahead, "ensure_sensors", _fake_ensure)


def _collect(client, **kwargs):
    log = FakeLog()
    defaults = dict(
        client=client,
        log=log,
        data_country_code="NO_2",
        sensor_timezone=TZ,
        from_time=FROM_TIME,
        until_time=UNTIL_TIME,
        include_load_forecast=True,
        include_wind_solar_forecast=True,
        include_residual_load=True,
        include_outages=False,
        suffix=" (NO_2)",
        strict=False,
    )
    defaults.update(kwargs)
    triples = day_ahead._collect_country_regressors(**defaults)
    return triples, log


def _by_sensor_name(triples):
    return {sensor.name: series for sensor, series, _ in triples}


@pytest.mark.parametrize("wind_solar_mode", ["raise", "empty"])
def test_neighbour_without_wind_solar_falls_back_to_load(
    stub_ensure_sensors, wind_solar_mode
):
    """A neighbour that publishes no wind & solar must not crash; residual == load."""
    client = FakeClient(wind_solar_mode=wind_solar_mode)
    triples, log = _collect(client)

    saved = _by_sensor_name(triples)
    # Load forecast is still saved.
    assert "Day-ahead load forecast (NO_2)" in saved
    # No wind/solar sensors were produced.
    assert not any(name.startswith(("Solar", "Wind")) for name in saved)
    # Residual load exists and equals the load forecast (green fell back to zero).
    assert "Residual load (NO_2)" in saved
    residual = saved["Residual load (NO_2)"]
    assert residual.to_numpy() == pytest.approx([12000.0] * len(HOURLY))
    # A warning was logged about the missing wind & solar data.
    assert any("wind & solar" in w for w in log.warnings)


def test_neighbour_partial_wind_solar_used_for_residual(stub_ensure_sensors):
    """If only Solar is published, residual subtracts just Solar (missing wind = 0)."""
    client = FakeClient(wind_solar_mode="partial")
    triples, _ = _collect(client)

    saved = _by_sensor_name(triples)
    # Only the Solar sensor is produced (wind columns absent).
    assert "Solar (NO_2)" in saved
    assert "Wind Onshore (NO_2)" not in saved
    assert "Wind Offshore (NO_2)" not in saved
    # Residual = 12000 - 2000 = 10000.
    residual = saved["Residual load (NO_2)"]
    assert residual.to_numpy() == pytest.approx([10000.0] * len(HOURLY))


def test_neighbour_missing_load_skips_residual(stub_ensure_sensors):
    """Without a load forecast we cannot derive residual load, but must not crash."""
    client = FakeClient(load_mode="raise", wind_solar_mode="ok")
    triples, log = _collect(client)

    saved = _by_sensor_name(triples)
    assert "Day-ahead load forecast (NO_2)" not in saved
    assert "Residual load (NO_2)" not in saved
    # Wind & solar are still saved (they don't depend on load).
    assert "Solar (NO_2)" in saved
    assert any("cannot derive residual load" in w for w in log.warnings)


def test_strict_mode_aborts_on_empty_price_country_data(stub_ensure_sensors):
    """For the price country (strict), empty ENTSO-E data aborts, as the price import does."""
    import click

    client = FakeClient(load_mode="empty")
    with pytest.raises(click.Abort):
        _collect(
            client,
            data_country_code="NL",
            suffix="",
            strict=True,
            include_wind_solar_forecast=False,
            include_residual_load=False,
        )


def test_full_neighbour_data_produces_all_sensors(stub_ensure_sensors):
    """With complete data a neighbour yields load, three green, and residual sensors."""
    client = FakeClient(wind_solar_mode="ok")
    triples, _ = _collect(client)

    names = set(_by_sensor_name(triples))
    assert names == {
        "Day-ahead load forecast (NO_2)",
        "Solar (NO_2)",
        "Wind Onshore (NO_2)",
        "Wind Offshore (NO_2)",
        "Residual load (NO_2)",
    }
    # Residual = 12000 - 2000 - 1000 - 500 = 8500.
    residual = _by_sensor_name(triples)["Residual load (NO_2)"]
    assert residual.to_numpy() == pytest.approx([8500.0] * len(HOURLY))
