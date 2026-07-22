from typing import Dict, List, Optional, Tuple
from datetime import datetime

import click
from flask.cli import with_appcontext
import pandas as pd
from flexmeasures import Source, Sensor

from flexmeasures.data.transactional import task_with_status_report

from flexmeasures.data.schemas import SensorIdField
from flexmeasures.data.schemas.sources import DataSourceIdField


from . import pricing_sensors
from . import regressors
from .. import (
    entsoe_data_bp,
)  # noqa: E402
from ..utils import (
    create_entsoe_client,
    ensure_country_code_and_timezone,
    ensure_data_source,
    ensure_data_source_for_derived_data,
    parse_from_and_to_dates,
    ensure_sensors,
    save_entsoe_series,
    abort_if_data_empty,
    abort_if_data_incomplete,
    resample_if_needed,
    start_import_log,
)


@entsoe_data_bp.cli.command("import-day-ahead-prices")
@click.option(
    "--from-date",
    required=False,
    type=click.DateTime(["%Y-%m-%d"]),
    help="Query data from this date onwards. If not specified, defaults to today",
)
@click.option(
    "--to-date",
    required=False,
    type=click.DateTime(["%Y-%m-%d"]),
    help="Query data until this date (inclusive). If not specified, defaults to tomorrow.",
)
@click.option(
    "--dryrun/--no-dryrun",
    default=False,
    help="In dry run mode, do not save the data to the db.",
)
@click.option(
    "--country",
    "country_code",
    required=False,
    help="ENTSO-E country code (such as BE, DE, FR or NL).",
)
@click.option(
    "--timezone",
    "country_timezone",
    required=False,
    help="Timezone for the country (such as 'Europe/Amsterdam').",
)
@click.option(
    "--sensor",
    "sensor",
    type=SensorIdField(),
    required=False,
    help="Sensor to store the data into. If not provided, the sensor `Day-ahead prices` is used.",
)
@click.option(
    "--source",
    "source",
    type=DataSourceIdField(),
    required=False,
    help="Source of the price data. If not provided, the source `ENTSO-E` is used.",
)
@click.option(
    "--include-load-forecast/--no-include-load-forecast",
    default=False,
    help="Also import the ENTSO-E day-ahead load forecast into a `Day-ahead load forecast` sensor (MW).",
)
@click.option(
    "--include-wind-solar-forecast/--no-include-wind-solar-forecast",
    default=False,
    help="Also import the ENTSO-E day-ahead wind & solar forecast into the `Solar`, `Wind Onshore` and `Wind Offshore` sensors (MW).",
)
@click.option(
    "--include-residual-load/--no-include-residual-load",
    default=False,
    help="Also derive and import residual load (load forecast − wind & solar forecast) into a `Residual load` sensor (MW). "
    "The strongest single regressor for day-ahead prices. Implies fetching the load and wind & solar forecasts.",
)
@click.option(
    "--include-outages/--no-include-outages",
    default=False,
    help="Also import day-ahead-known generation outages (unavailable capacity) into a `Generation outages` sensor (MW).",
)
@click.option(
    "--include-neighbours/--no-include-neighbours",
    default=False,
    help="Also import the selected regressors (except outages) for interconnected neighbouring countries "
    "(for NL: DE_LU, BE, GB, NO_2, DK_1), into per-country sensors (e.g. `Residual load (DE_LU)`). "
    "Neighbour scarcity drives NL price spikes, so this improves spike/evening-peak forecasts.",
)
@click.option(
    "--load-forecast-sensor",
    "load_forecast_sensor",
    type=SensorIdField(),
    required=False,
    help="Sensor to store the load forecast into. Defaults to the `Day-ahead load forecast` sensor.",
)
@click.option(
    "--residual-load-sensor",
    "residual_load_sensor",
    type=SensorIdField(),
    required=False,
    help="Sensor to store residual load into. Defaults to the `Residual load` sensor.",
)
@click.option(
    "--outages-sensor",
    "outages_sensor",
    type=SensorIdField(),
    required=False,
    help="Sensor to store generation outages into. Defaults to the `Generation outages` sensor.",
)
@click.option(
    "--for",
    "default_import_timerange",
    required=False,
    default="today-and-tomorrow",
    type=click.Choice(["today", "tomorrow", "today-and-tomorrow"]),
    help="Easy-to-use time range setting, which defines the defaults for start and end to be used when --from-date and/or --to-date are not used. Can be set to 'today' or 'tomorrow' or 'today-and-tomorrow' (which is the default value).",
)
@click.option(
    "--fail-on-incomplete-data",
    "fail_on_incomplete_data",
    is_flag=True,
    default=False,
    help="If set, the import will abort if the data received is incomplete.",
)
@with_appcontext
@task_with_status_report("entsoe-import-day-ahead-prices")
def import_day_ahead_prices(
    dryrun: bool = False,
    from_date: Optional[datetime] = None,
    to_date: Optional[datetime] = None,
    country_code: Optional[str] = None,
    country_timezone: Optional[str] = None,
    sensor: Optional[Sensor] = None,
    source: Optional[Source] = None,
    include_load_forecast: bool = False,
    include_wind_solar_forecast: bool = False,
    include_residual_load: bool = False,
    include_outages: bool = False,
    include_neighbours: bool = False,
    load_forecast_sensor: Optional[Sensor] = None,
    residual_load_sensor: Optional[Sensor] = None,
    outages_sensor: Optional[Sensor] = None,
    default_import_timerange: str = "today-and-tomorrow",
    fail_on_incomplete_data: bool = False,
):
    """
    Import forecasted prices for any date range, defaulting to today and tomorrow.
    Possibly best to run this script somewhere around or maybe two or three hours after 13:00,
    when tomorrow's prices are announced.

    Optionally, also import extra regressor series that improve day-ahead *price*
    forecasting. Use the --include-* flags to choose which of these to pull; each is
    saved to its own sensor (MW):

    \b
      --include-load-forecast        ENTSO-E day-ahead load forecast.
      --include-wind-solar-forecast  ENTSO-E day-ahead wind & solar forecast
                                     (Solar, Wind Onshore, Wind Offshore).
      --include-residual-load        Residual load = load forecast − wind & solar
                                     forecast. The single strongest regressor for NL
                                     day-ahead prices. Implies fetching the two forecasts
                                     above (but only saves them if their own flag is set).
      --include-outages              Day-ahead-known generation outages (unavailable MW).
                                     Only outages published before a target hour's delivery
                                     day are counted, to avoid a look-ahead leak.
      --include-neighbours           Also import the selected regressors (except outages)
                                     for interconnected neighbours, into per-country sensors
                                     (e.g. `Residual load (DE_LU)`). Regional NW-European
                                     scarcity drives NL price spikes.
    """
    # Set up FlexMeasures data structure
    country_code, country_timezone = ensure_country_code_and_timezone(
        country_code, country_timezone
    )

    if source is None:
        entsoe_data_source = ensure_data_source()
    else:
        entsoe_data_source = source

    if sensor is None:
        # For now, we only have one pricing sensor ...
        sensors = ensure_sensors(pricing_sensors, country_code, country_timezone)
        pricing_sensor = sensors["Day-ahead prices"]
        assert pricing_sensor.name == "Day-ahead prices"
    else:
        pricing_sensor = sensor

    # Parse CLI options (or set defaults)
    from_time, until_time = parse_from_and_to_dates(
        from_date, to_date, country_timezone, default_to=default_import_timerange
    )

    # Start import
    client = create_entsoe_client()
    log, now = start_import_log(
        "day-ahead price", from_time, until_time, country_code, country_timezone
    )

    log.info("Getting prices ...")
    prices: pd.Series = client.query_day_ahead_prices(
        country_code, start=from_time, end=until_time
    )
    abort_if_data_empty(prices)
    if fail_on_incomplete_data:
        abort_if_data_incomplete(
            prices, from_time, until_time, pricing_sensor.event_resolution
        )
    prices = resample_if_needed(prices, pricing_sensor)
    log.debug("Prices: \n%s" % prices)

    if not dryrun:
        log.info(f"Saving {len(prices)} beliefs for Sensor {pricing_sensor.name} ...")
        save_entsoe_series(
            prices, pricing_sensor, entsoe_data_source, country_timezone, now
        )

    # Optionally, import extra regressor series for price forecasting.
    if (
        include_load_forecast
        or include_wind_solar_forecast
        or include_residual_load
        or include_outages
    ):
        collect_and_save_regressors(
            client=client,
            log=log,
            dryrun=dryrun,
            country_code=country_code,
            country_timezone=country_timezone,
            from_time=from_time,
            until_time=until_time,
            now=now,
            entsoe_data_source=entsoe_data_source,
            include_load_forecast=include_load_forecast,
            include_wind_solar_forecast=include_wind_solar_forecast,
            include_residual_load=include_residual_load,
            include_outages=include_outages,
            include_neighbours=include_neighbours,
            load_forecast_sensor=load_forecast_sensor,
            residual_load_sensor=residual_load_sensor,
            outages_sensor=outages_sensor,
        )


def collect_and_save_regressors(
    client,
    log,
    dryrun: bool,
    country_code: str,
    country_timezone: str,
    from_time: pd.Timestamp,
    until_time: pd.Timestamp,
    now: datetime,
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
    """
    Query and save the requested extra regressor series (each to its own sensor).

    Data is only fetched from ENTSO-E when needed: residual load implies fetching the
    load and wind & solar forecasts, even when those are not saved themselves.

    When ``include_neighbours`` is set, the same regressors (except domestic outages) are
    also collected for the price country's interconnected neighbours, saved to per-country
    sensors (e.g. ``Residual load (DE_LU)``) under each neighbour's own transmission zone.
    Missing neighbour data is skipped gracefully rather than aborting the import.
    """
    # Collect the domestic (price-country) regressors. This is strict: if ENTSO-E has no
    # data for the price country itself, we abort (as the price import already does).
    to_save = _collect_country_regressors(
        client=client,
        log=log,
        data_country_code=country_code,
        sensor_timezone=country_timezone,
        from_time=from_time,
        until_time=until_time,
        include_load_forecast=include_load_forecast,
        include_wind_solar_forecast=include_wind_solar_forecast,
        include_residual_load=include_residual_load,
        include_outages=include_outages,
        suffix="",
        strict=True,
        load_forecast_sensor=load_forecast_sensor,
        residual_load_sensor=residual_load_sensor,
        outages_sensor=outages_sensor,
    )

    # Optionally, collect the same regressors (minus outages) for each neighbour. This is
    # lenient: neighbours with no published data (e.g. no wind & solar for NO_2 / DK_1)
    # are skipped, and residual load then falls back to the plain load forecast.
    if include_neighbours:
        neighbours = regressors.INTERCONNECTED_NEIGHBOURS.get(country_code, [])
        if not neighbours:
            log.warning(
                f"No interconnected neighbours are defined for {country_code}; "
                "--include-neighbours has no effect."
            )
        for neighbour in neighbours:
            log.info(f"Collecting regressors for neighbour {neighbour} ...")
            to_save += _collect_country_regressors(
                client=client,
                log=log,
                data_country_code=neighbour,
                sensor_timezone=country_timezone,
                from_time=from_time,
                until_time=until_time,
                include_load_forecast=include_load_forecast,
                include_wind_solar_forecast=include_wind_solar_forecast,
                include_residual_load=include_residual_load,
                include_outages=False,  # outages stay domestic-only
                suffix=f" ({neighbour})",
                strict=False,
            )

    if not dryrun:
        _save_regressor_series(to_save, log, entsoe_data_source, country_timezone, now)


def _collect_country_regressors(
    client,
    log,
    data_country_code: str,
    sensor_timezone: str,
    from_time: pd.Timestamp,
    until_time: pd.Timestamp,
    include_load_forecast: bool,
    include_wind_solar_forecast: bool,
    include_residual_load: bool,
    include_outages: bool,
    suffix: str = "",
    strict: bool = True,
    load_forecast_sensor: Optional[Sensor] = None,
    residual_load_sensor: Optional[Sensor] = None,
    outages_sensor: Optional[Sensor] = None,
) -> List[Tuple[Sensor, pd.Series, bool]]:
    """Collect the (sensor, series, is_entsoe_data) triples for one country's regressors.

    ``data_country_code`` is queried at ENTSO-E; the sensors are named with ``suffix``
    (empty for the price country, e.g. " (DE_LU)" for a neighbour) and live under that
    country's own transmission zone. With ``strict=False`` (used for neighbours), missing
    data is skipped with a warning instead of aborting the whole import.
    """
    ensured_sensors = _ensure_regressor_sensors(
        country_code=data_country_code,
        country_timezone=sensor_timezone,
        include_load_forecast=include_load_forecast and load_forecast_sensor is None,
        include_wind_solar_forecast=include_wind_solar_forecast,
        include_residual_load=include_residual_load and residual_load_sensor is None,
        include_outages=include_outages and outages_sensor is None,
        suffix=suffix,
    )

    # Query ENTSO-E only for what we need (residual load implies both forecasts).
    load_series = None
    green_forecast = None
    if include_load_forecast or include_residual_load:
        load_forecast = _query_series(
            lambda: client.query_load_forecast(
                data_country_code, start=from_time, end=until_time
            ),
            strict=strict,
            log=log,
            description=f"day-ahead load forecast for {data_country_code}",
        )
        if load_forecast is not None:
            load_series = regressors.load_series_from_forecast(load_forecast)
            log.debug("Load forecast: \n%s" % load_series)
    if include_wind_solar_forecast or include_residual_load:
        green_forecast = _query_series(
            lambda: client.query_wind_and_solar_forecast(
                data_country_code, start=from_time, end=until_time, psr_type=None
            ),
            strict=strict,
            log=log,
            description=f"day-ahead wind & solar forecast for {data_country_code}",
        )
        if green_forecast is not None:
            log.debug("Wind & solar forecast: \n%s" % green_forecast)

    # Assemble the (sensor, series, is_entsoe_data) triples to save.
    to_save: List[Tuple[Sensor, pd.Series, bool]] = []
    if include_load_forecast and load_series is not None:
        sensor = (
            load_forecast_sensor or ensured_sensors["Day-ahead load forecast" + suffix]
        )
        to_save.append((sensor, load_series, True))
    if include_wind_solar_forecast and green_forecast is not None:
        to_save.extend(
            _wind_solar_to_save(
                green_forecast, ensured_sensors, data_country_code, suffix, log
            )
        )
    if include_residual_load:
        if load_series is None:
            log.warning(
                f"No load forecast for {data_country_code}; cannot derive residual load. Skipping."
            )
        else:
            residual_load = _build_residual_load(load_series, green_forecast, log)
            sensor = residual_load_sensor or ensured_sensors["Residual load" + suffix]
            to_save.append((sensor, residual_load, False))
    if include_outages:
        log.info("Getting generation outages (unavailable capacity) ...")
        outages = client.query_unavailability_of_generation_units(
            data_country_code, start=from_time, end=until_time
        )
        # Note: outages may legitimately be empty (no announced outages), so we do not abort.
        unavailable_mw = regressors.aggregate_outages_to_hourly_unavailable_mw(
            outages, from_time, until_time
        )
        log.debug("Hourly unavailable capacity (MW): \n%s" % unavailable_mw)
        sensor = outages_sensor or ensured_sensors["Generation outages" + suffix]
        to_save.append((sensor, unavailable_mw, True))
    return to_save


def _query_series(client_call, strict: bool, log, description: str):
    """Run an ENTSO-E query, returning None on missing data when not strict.

    In strict mode (price country) an empty result aborts, matching the price import.
    In lenient mode (neighbours) both a raised exception (e.g. entsoe's NoMatchingDataError)
    and an empty result are turned into a warning + None, so one missing neighbour series
    never crashes the import.
    """
    try:
        data = client_call()
    except (
        Exception
    ) as e:  # noqa: B902 - any ENTSO-E failure for a neighbour is non-fatal
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


def _ensure_regressor_sensors(
    country_code: str,
    country_timezone: str,
    include_load_forecast: bool,
    include_wind_solar_forecast: bool,
    include_residual_load: bool,
    include_outages: bool,
    suffix: str = "",
) -> Dict[str, Sensor]:
    """Ensure the sensors for the requested regressors exist, returning them by name.

    ``suffix`` is appended to each sensor name (e.g. " (DE_LU)") so neighbour regressors
    get their own per-country sensors.
    """
    specs: List[Tuple] = []
    if include_load_forecast:
        specs.append(_suffix_spec(regressors.LOAD_FORECAST_SENSOR_SPEC, suffix))
    if include_wind_solar_forecast:
        specs.extend(
            _suffix_spec(spec, suffix) for spec in regressors.WIND_SOLAR_SENSOR_SPECS
        )
    if include_residual_load:
        specs.append(_suffix_spec(regressors.RESIDUAL_LOAD_SENSOR_SPEC, suffix))
    if include_outages:
        specs.append(_suffix_spec(regressors.OUTAGES_SENSOR_SPEC, suffix))
    if not specs:
        return {}
    return ensure_sensors(tuple(specs), country_code, country_timezone)


def _suffix_spec(spec: Tuple, suffix: str) -> Tuple:
    """Append ``suffix`` to a sensor spec's name, leaving unit/resolution/source as-is."""
    name, unit, event_resolution, data_by_entsoe = spec
    return (name + suffix, unit, event_resolution, data_by_entsoe)


def _wind_solar_to_save(
    green_forecast: pd.DataFrame,
    ensured_sensors: Dict[str, Sensor],
    country_code: str,
    suffix: str,
    log,
) -> List[Tuple[Sensor, pd.Series, bool]]:
    """Pair each reported green column with its sensor (skipping columns not reported)."""
    triples: List[Tuple[Sensor, pd.Series, bool]] = []
    for column in regressors.GREEN_COLUMNS:
        series = regressors.green_column(green_forecast, column)
        if series is None:
            log.warning(
                f"ENTSO-E did not report '{column}' for {country_code}; skipping that sensor."
            )
            continue
        triples.append((ensured_sensors[column + suffix], series, True))
    return triples


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


def _save_regressor_series(
    to_save: List[Tuple[Sensor, pd.Series, bool]],
    log,
    entsoe_data_source: Source,
    country_timezone: str,
    now: datetime,
):
    """Save each regressor series, using the derived data source for derived series."""
    derived_data_source = None  # created lazily, only if we save derived data
    for sensor, series, is_entsoe_data in to_save:
        series = resample_if_needed(series, sensor)
        if is_entsoe_data:
            regressor_source = entsoe_data_source
        else:
            if derived_data_source is None:
                derived_data_source = ensure_data_source_for_derived_data()
            regressor_source = derived_data_source
        log.info(f"Saving {len(series)} beliefs for Sensor {sensor.name} ...")
        save_entsoe_series(series, sensor, regressor_source, country_timezone, now)
