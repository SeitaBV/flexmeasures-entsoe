"""
Tests for the neighbour-country regressor collection and saving.

These exercise the orchestration in ``prices/regressors_import.py`` with a *mocked* ENTSO-E
client and stubbed DB helpers (so no live API and no database are touched). Two things are
covered:

- graceful handling of neighbours that publish no wind & solar forecast (e.g. NO_2 / DK_1):
  residual load must fall back to the plain load forecast instead of crashing;
- that each country's sensors are ensured/saved under *its own* timezone and with plain
  (unsuffixed) sensor names, and that neighbours come from ENTSO-E's own NEIGHBOURS mapping.

``flexmeasures`` must be importable for these (the package __init__ pulls it in); if it is
not installed, the whole module is skipped.
"""

import pandas as pd
import pytest

pytest.importorskip("flexmeasures")

from flexmeasures_entsoe.prices import regressors_import  # noqa: E402

TZ = "Europe/Amsterdam"
FROM_TIME = pd.Timestamp("2025-06-02 00:00", tz=TZ)
UNTIL_TIME = pd.Timestamp("2025-06-03 00:00", tz=TZ)
HOURLY = pd.date_range(FROM_TIME, UNTIL_TIME, freq="1h", inclusive="left", tz=TZ)


class FakeSensor:
    def __init__(self, name, event_resolution=None):
        self.name = name
        self.event_resolution = event_resolution

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
    load_mode: 'ok' | 'raise' | 'empty'
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


# ---------------------------------------------------------------------------
# _collect_country_regressors: graceful missing-data handling
# ---------------------------------------------------------------------------


def _collect(client, **kwargs):
    log = FakeLog()
    defaults = dict(
        client=client,
        log=log,
        country_code="NO_2",
        from_time=FROM_TIME,
        until_time=UNTIL_TIME,
        include_load_forecast=True,
        include_wind_solar_forecast=True,
        include_residual_load=True,
        include_outages=False,
        strict=False,
    )
    defaults.update(kwargs)
    results = regressors_import._collect_country_regressors(**defaults)
    return results, log


def _by_name(results):
    """Map base sensor name -> series (results are (spec, override, series, is_entsoe))."""
    return {spec[0]: series for spec, _override, series, _is_entsoe in results}


@pytest.mark.parametrize("wind_solar_mode", ["raise", "empty"])
def test_neighbour_without_wind_solar_falls_back_to_load(wind_solar_mode):
    """A neighbour that publishes no wind & solar must not crash; residual == load."""
    client = FakeClient(wind_solar_mode=wind_solar_mode)
    results, log = _collect(client)

    saved = _by_name(results)
    assert "Day-ahead load forecast" in saved
    assert not any(name.startswith(("Solar", "Wind")) for name in saved)
    # Residual load exists and equals the load forecast (green fell back to zero).
    assert "Residual load" in saved
    assert saved["Residual load"].to_numpy() == pytest.approx([12000.0] * len(HOURLY))
    assert any("wind & solar" in w for w in log.warnings)


def test_neighbour_partial_wind_solar_used_for_residual():
    """If only Solar is published, residual subtracts just Solar (missing wind = 0)."""
    client = FakeClient(wind_solar_mode="partial")
    results, _ = _collect(client)

    saved = _by_name(results)
    assert "Solar" in saved
    assert "Wind Onshore" not in saved
    assert "Wind Offshore" not in saved
    # Residual = 12000 - 2000 = 10000.
    assert saved["Residual load"].to_numpy() == pytest.approx([10000.0] * len(HOURLY))


def test_neighbour_missing_load_skips_residual():
    """Without a load forecast we cannot derive residual load, but must not crash."""
    client = FakeClient(load_mode="raise", wind_solar_mode="ok")
    results, log = _collect(client)

    saved = _by_name(results)
    assert "Day-ahead load forecast" not in saved
    assert "Residual load" not in saved
    assert "Solar" in saved  # wind & solar don't depend on load
    assert any("cannot derive residual load" in w for w in log.warnings)


def test_full_neighbour_data_produces_all_specs():
    """With complete data a neighbour yields load, three green, and residual specs."""
    client = FakeClient(wind_solar_mode="ok")
    results, _ = _collect(client)

    saved = _by_name(results)
    assert set(saved) == {
        "Day-ahead load forecast",
        "Solar",
        "Wind Onshore",
        "Wind Offshore",
        "Residual load",
    }
    # Residual = 12000 - 2000 - 1000 - 500 = 8500.
    assert saved["Residual load"].to_numpy() == pytest.approx([8500.0] * len(HOURLY))
    # Residual load is flagged as derived (not straight-from-ENTSO-E) data.
    residual_is_entsoe = [
        is_entsoe for spec, _o, _s, is_entsoe in results if spec[0] == "Residual load"
    ][0]
    assert residual_is_entsoe is False


def test_strict_mode_aborts_on_empty_price_country_data():
    """For the price country (strict), empty ENTSO-E data aborts, as the price import does."""
    import click

    client = FakeClient(load_mode="empty")
    with pytest.raises(click.Abort):
        _collect(
            client,
            country_code="NL",
            strict=True,
            include_wind_solar_forecast=False,
            include_residual_load=False,
        )


# ---------------------------------------------------------------------------
# collect_and_save_regressors: per-country timezone, base names, neighbours
# ---------------------------------------------------------------------------


def test_collect_and_save_uses_each_country_own_timezone_and_base_names(monkeypatch):
    ensure_calls = []
    save_calls = []

    def fake_ensure(specs, country_code, timezone):
        ensure_calls.append((country_code, timezone, [s[0] for s in specs]))
        return {s[0]: FakeSensor(s[0]) for s in specs}

    def fake_save(series, sensor, source, timezone, now):
        save_calls.append((sensor.name, timezone, source))

    monkeypatch.setattr(regressors_import, "ensure_sensors", fake_ensure)
    monkeypatch.setattr(regressors_import, "save_entsoe_series", fake_save)
    monkeypatch.setattr(regressors_import, "resample_if_needed", lambda s, sensor: s)
    monkeypatch.setattr(
        regressors_import, "ensure_data_source_for_derived_data", lambda: "DERIVED"
    )

    regressors_import.collect_and_save_regressors(
        client=FakeClient(wind_solar_mode="ok"),
        log=FakeLog(),
        dryrun=False,
        country_code="NL",
        country_timezone="Europe/Amsterdam",
        from_time=FROM_TIME,
        until_time=UNTIL_TIME,
        now=None,
        entsoe_data_source="ENTSOE",
        include_load_forecast=False,
        include_wind_solar_forecast=False,
        include_residual_load=True,
        include_outages=False,
        include_neighbours=True,
    )

    tz_by_country = {country: tz for country, tz, _names in ensure_calls}
    # Price country and neighbours (from entsoe.mappings.NEIGHBOURS) each ensured.
    assert tz_by_country["NL"] == "Europe/Amsterdam"
    assert "BE" in tz_by_country  # a neighbour from the ENTSO-E mapping
    # Each sensor carries its OWN country's timezone.
    assert tz_by_country["GB"] == "Europe/London"
    assert tz_by_country["NO_2"] == "Europe/Oslo"
    assert tz_by_country["DK_1"] == "Europe/Copenhagen"

    # Sensor names are plain base names (identity comes from the asset, not a suffix).
    all_names = [name for _c, _tz, names in ensure_calls for name in names]
    assert "Residual load" in all_names
    assert all("(" not in name for name in all_names)

    # Residual load is saved from the derived source, with the country's own timezone.
    assert ("Residual load", "Europe/London", "DERIVED") in save_calls


# ---------------------------------------------------------------------------
# resample_if_needed and _save_results: irregular (gappy) neighbour data
# ---------------------------------------------------------------------------


def test_resample_if_needed_tolerates_gaps():
    """Hourly data with missing hours (no inferable frequency) must survive as-is.

    Neighbour zones (e.g. DK_1) can return series with holes, for which
    ``pd.infer_freq`` finds no frequency; the most common timestamp spacing
    still identifies the resolution.
    """
    from datetime import timedelta

    from flexmeasures_entsoe.utils import resample_if_needed

    gappy_index = HOURLY.delete([3, 4, 10])  # knock holes into the hourly grid
    series = pd.Series(12000.0, index=gappy_index)
    sensor = FakeSensor("Day-ahead load forecast", event_resolution=timedelta(hours=1))

    result = resample_if_needed(series, sensor)

    assert result.equals(series)


def test_resample_if_needed_still_raises_without_any_spacing():
    """A single timestamp offers no spacing at all, so the clear error remains."""
    from datetime import timedelta

    from flexmeasures_entsoe.utils import resample_if_needed

    series = pd.Series([12000.0], index=HOURLY[:1])
    sensor = FakeSensor("Day-ahead load forecast", event_resolution=timedelta(hours=1))

    with pytest.raises(ValueError, match="no discernible frequency"):
        resample_if_needed(series, sensor)


def _run_save_results_with_unalignable_series(monkeypatch, strict):
    """Prepare _save_results with one unalignable series among good ones."""
    saved = []
    log = FakeLog()

    monkeypatch.setattr(
        regressors_import,
        "ensure_sensors",
        lambda specs, country_code, timezone: {s[0]: FakeSensor(s[0]) for s in specs},
    )
    monkeypatch.setattr(
        regressors_import,
        "save_entsoe_series",
        lambda series, sensor, source, timezone, now: saved.append(sensor.name),
    )

    def picky_resample(series, sensor):
        if sensor.name == "Day-ahead load forecast":
            raise ValueError("no discernible frequency")
        return series

    monkeypatch.setattr(regressors_import, "resample_if_needed", picky_resample)

    good = pd.Series(1.0, index=HOURLY)
    results = [
        (("Day-ahead load forecast", "MW", None, True), None, good, True),
        (("Residual load", "MW", None, True), None, good, True),
    ]

    def run():
        regressors_import._save_results(
            results, "DK_1", "Europe/Copenhagen", log, "ENTSOE", None, strict=strict
        )

    return run, saved, log


def test_save_results_lenient_skips_unalignable_series(monkeypatch):
    """In lenient (neighbour) mode, one bad series is skipped; the rest still save."""
    run, saved, log = _run_save_results_with_unalignable_series(
        monkeypatch, strict=False
    )
    run()
    assert saved == ["Residual load"]
    assert any("skipping" in w for w in log.warnings)


def test_save_results_strict_raises_on_unalignable_series(monkeypatch):
    """In strict (price country) mode, an unalignable series still aborts."""
    run, _saved, _log = _run_save_results_with_unalignable_series(
        monkeypatch, strict=True
    )
    with pytest.raises(ValueError):
        run()
