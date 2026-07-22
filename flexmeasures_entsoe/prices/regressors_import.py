"""
Orchestration for importing extra price-forecasting regressors from ENTSO-E.

This module does the FlexMeasures/ENTSO-E work (querying the client, ensuring sensors and
saving beliefs) and is kept separate from the pure helpers in ``regressors.py`` (which stay
importable without a FlexMeasures app or database). ``day_ahead.py`` only wires the CLI and
calls :func:`collect_and_save_regressors`.

Regressor sensors are identified by their *asset*, not by their name: a country's series
are saved to plainly-named sensors (``Residual load``, ``Day-ahead load forecast``, ...)
that live on that country's own transmission-zone asset, each carrying that country's own
timezone. So NL's ``Residual load`` and DE_LU's ``Residual load`` are distinct sensors on
distinct assets.
"""

from typing import List, Optional, Tuple

import pandas as pd
from flexmeasures import Sensor, Source

from . import regressors
from ..utils import (
    ensure_data_source_for_derived_data,
    ensure_sensors,
    abort_if_data_empty,
    save_entsoe_series,
    resample_if_needed,
)

# One regressor result: (base sensor spec, optional sensor override, the series, whether
# the data comes straight from ENTSO-E (True) or is derived by us (False)).
RegressorResult = Tuple[Tuple, Optional[Sensor], pd.Series, bool]

# Map a green-forecast column to the sensor spec it is saved into.
_GREEN_SPEC_BY_NAME = {spec[0]: spec for spec in regressors.WIND_SOLAR_SENSOR_SPECS}


def collect_and_save_regressors(
    client,
    log,
    dryrun: bool,
    country_code: str,
    country_timezone: str,
    from_time: pd.Timestamp,
    until_time: pd.Timestamp,
    now,
    entsoe_data_source: Source,
    include_load_forecast: bool,
    include_wind_solar_forecast: bool,
    include_residual_load: bool,
    include_outages: bool,
    include_neighbours: bool = False,
    load_forecast_sensor: Optional[Sensor] = None,
    residual_load_sensor: Optional[Sensor] = None,
    outages_sensor: Optional[Sensor] = None,
):
    """Query and save the requested regressor series for the price country (and neighbours).

    Data is only fetched from ENTSO-E when needed: residual load implies fetching the load
    and wind & solar forecasts, even when those are not saved themselves.

    With ``include_neighbours``, the same regressors (except domestic outages) are also
    collected for the price country's interconnected neighbours (from
    ``entsoe.mappings.NEIGHBOURS``). Missing neighbour data is skipped gracefully rather
    than aborting the import, and residual load then falls back to the plain load forecast.
    """
    # The price country: strict (abort on empty data), may use sensor overrides & outages.
    _collect_ensure_and_save(
        client=client,
        log=log,
        dryrun=dryrun,
        country_code=country_code,
        timezone=country_timezone,
        from_time=from_time,
        until_time=until_time,
        now=now,
        entsoe_data_source=entsoe_data_source,
        include_load_forecast=include_load_forecast,
        include_wind_solar_forecast=include_wind_solar_forecast,
        include_residual_load=include_residual_load,
        include_outages=include_outages,
        strict=True,
        load_forecast_sensor=load_forecast_sensor,
        residual_load_sensor=residual_load_sensor,
        outages_sensor=outages_sensor,
    )

    if not include_neighbours:
        return

    neighbours = regressors.neighbours_for(country_code)
    if not neighbours:
        log.warning(
            f"ENTSO-E lists no interconnected neighbours for {country_code}; "
            "--include-neighbours has no effect."
        )
    for neighbour in neighbours:
        log.info(f"Collecting regressors for neighbour {neighbour} ...")
        # Neighbours: lenient (skip missing data), no outages, no sensor overrides, and
        # each on its own transmission-zone asset with its own timezone.
        _collect_ensure_and_save(
            client=client,
            log=log,
            dryrun=dryrun,
            country_code=neighbour,
            timezone=regressors.timezone_for(neighbour) or country_timezone,
            from_time=from_time,
            until_time=until_time,
            now=now,
            entsoe_data_source=entsoe_data_source,
            include_load_forecast=include_load_forecast,
            include_wind_solar_forecast=include_wind_solar_forecast,
            include_residual_load=include_residual_load,
            include_outages=False,  # outages stay domestic-only
            strict=False,
        )


def _collect_ensure_and_save(
    client,
    log,
    dryrun: bool,
    country_code: str,
    timezone: str,
    from_time: pd.Timestamp,
    until_time: pd.Timestamp,
    now,
    entsoe_data_source: Source,
    include_load_forecast: bool,
    include_wind_solar_forecast: bool,
    include_residual_load: bool,
    include_outages: bool,
    strict: bool,
    load_forecast_sensor: Optional[Sensor] = None,
    residual_load_sensor: Optional[Sensor] = None,
    outages_sensor: Optional[Sensor] = None,
):
    """Collect one country's regressor series, then (unless dryrun) ensure sensors & save."""
    results = _collect_country_regressors(
        client=client,
        log=log,
        country_code=country_code,
        from_time=from_time,
        until_time=until_time,
        include_load_forecast=include_load_forecast,
        include_wind_solar_forecast=include_wind_solar_forecast,
        include_residual_load=include_residual_load,
        include_outages=include_outages,
        strict=strict,
        load_forecast_sensor=load_forecast_sensor,
        residual_load_sensor=residual_load_sensor,
        outages_sensor=outages_sensor,
    )
    if not dryrun:
        _save_results(results, country_code, timezone, log, entsoe_data_source, now)


def _collect_country_regressors(
    client,
    log,
    country_code: str,
    from_time: pd.Timestamp,
    until_time: pd.Timestamp,
    include_load_forecast: bool,
    include_wind_solar_forecast: bool,
    include_residual_load: bool,
    include_outages: bool,
    strict: bool = True,
    load_forecast_sensor: Optional[Sensor] = None,
    residual_load_sensor: Optional[Sensor] = None,
    outages_sensor: Optional[Sensor] = None,
) -> List[RegressorResult]:
    """Fetch one country's data and pair each resulting series with its sensor spec.

    Returns a list of :data:`RegressorResult`. With ``strict=False`` (used for neighbours),
    missing data is skipped with a warning instead of aborting the whole import; sensors are
    only referenced for series that actually have data (so dead zones create no clutter).
    """
    # Query ENTSO-E only for what we need (residual load implies both forecasts).
    load_series = None
    green_forecast = None
    if include_load_forecast or include_residual_load:
        load_forecast = _query_series(
            lambda: client.query_load_forecast(
                country_code, start=from_time, end=until_time
            ),
            strict=strict,
            log=log,
            description=f"day-ahead load forecast for {country_code}",
        )
        if load_forecast is not None:
            load_series = regressors.load_series_from_forecast(load_forecast)
            log.debug("Load forecast: \n%s" % load_series)
    if include_wind_solar_forecast or include_residual_load:
        green_forecast = _query_series(
            lambda: client.query_wind_and_solar_forecast(
                country_code, start=from_time, end=until_time, psr_type=None
            ),
            strict=strict,
            log=log,
            description=f"day-ahead wind & solar forecast for {country_code}",
        )
        if green_forecast is not None:
            log.debug("Wind & solar forecast: \n%s" % green_forecast)

    results: List[RegressorResult] = []
    if include_load_forecast and load_series is not None:
        results.append(
            (
                regressors.LOAD_FORECAST_SENSOR_SPEC,
                load_forecast_sensor,
                load_series,
                True,
            )
        )
    if include_wind_solar_forecast and green_forecast is not None:
        for column in regressors.GREEN_COLUMNS:
            series = regressors.green_column(green_forecast, column)
            if series is None:
                log.warning(
                    f"ENTSO-E did not report '{column}' for {country_code}; skipping that sensor."
                )
                continue
            results.append((_GREEN_SPEC_BY_NAME[column], None, series, True))
    if include_residual_load:
        if load_series is None:
            log.warning(
                f"No load forecast for {country_code}; cannot derive residual load. Skipping."
            )
        else:
            residual_load = _build_residual_load(load_series, green_forecast, log)
            results.append(
                (
                    regressors.RESIDUAL_LOAD_SENSOR_SPEC,
                    residual_load_sensor,
                    residual_load,
                    False,
                )
            )
    if include_outages:
        log.info("Getting generation outages (unavailable capacity) ...")
        outages = client.query_unavailability_of_generation_units(
            country_code, start=from_time, end=until_time
        )
        # Note: outages may legitimately be empty (no announced outages), so we do not abort.
        unavailable_mw = regressors.aggregate_outages_to_hourly_unavailable_mw(
            outages, from_time, until_time
        )
        log.debug("Hourly unavailable capacity (MW): \n%s" % unavailable_mw)
        results.append(
            (regressors.OUTAGES_SENSOR_SPEC, outages_sensor, unavailable_mw, True)
        )
    return results


def _save_results(
    results: List[RegressorResult],
    country_code: str,
    timezone: str,
    log,
    entsoe_data_source: Source,
    now,
):
    """Ensure the needed sensors (once) and save each regressor series to its sensor."""
    if not results:
        return

    # Ensure sensors for results that do not carry an explicit override, in one call, so the
    # country's transmission-zone asset is created once and only for series we actually have.
    specs = tuple(spec for spec, override, _, _ in results if override is None)
    ensured = ensure_sensors(specs, country_code, timezone) if specs else {}

    derived_data_source = None  # created lazily, only if we save derived data
    for spec, override, series, is_entsoe_data in results:
        sensor = override if override is not None else ensured[spec[0]]
        series = resample_if_needed(series, sensor)
        if is_entsoe_data:
            source = entsoe_data_source
        else:
            if derived_data_source is None:
                derived_data_source = ensure_data_source_for_derived_data()
            source = derived_data_source
        log.info(f"Saving {len(series)} beliefs for Sensor {sensor.name} ...")
        save_entsoe_series(series, sensor, source, timezone, now)


def _query_series(client_call, strict: bool, log, description: str):
    """Run an ENTSO-E query, returning None on missing data when not strict.

    In strict mode (price country) an empty result aborts, matching the price import. In
    lenient mode (neighbours) both a raised exception (e.g. entsoe's NoMatchingDataError)
    and an empty result become a warning + None, so one missing neighbour series never
    crashes the import.
    """
    try:
        data = client_call()
    except Exception as e:  # noqa: B902 - any ENTSO-E failure for a neighbour is non-fatal
        if strict:
            raise
        log.warning(f"Could not get {description} ({e}); skipping.")
        return None
    if data is None or (hasattr(data, "empty") and data.empty):
        if strict:
            abort_if_data_empty(data)  # raises click.Abort
        log.warning(f"No {description} available; skipping.")
        return None
    return data


def _build_residual_load(
    load_series: pd.Series, green_forecast: Optional[pd.DataFrame], log
) -> pd.Series:
    """Derive residual load, aligning all inputs to hourly means first.

    ``green_forecast`` may be None (no wind & solar published for this country), in which
    case residual load falls back to the plain load forecast.
    """
    log.info("Computing residual load (load − wind & solar) ...")

    def to_hourly(series: Optional[pd.Series]) -> pd.Series:
        if series is None or series.empty:
            return pd.Series(dtype="float64")
        return series.resample("1h").mean()

    residual_load = regressors.compute_residual_load(
        load_forecast=to_hourly(load_series),
        solar=to_hourly(regressors.green_column(green_forecast, "Solar")),
        wind_offshore=to_hourly(
            regressors.green_column(green_forecast, "Wind Offshore")
        ),
        wind_onshore=to_hourly(regressors.green_column(green_forecast, "Wind Onshore")),
    )
    log.debug("Residual load: \n%s" % residual_load)
    return residual_load
