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
    parse_from_and_to_dates_default_today_and_tomorrow,
    ensure_sensors,
    save_entsoe_series,
    abort_if_data_empty,
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
    load_forecast_sensor: Optional[Sensor] = None,
    residual_load_sensor: Optional[Sensor] = None,
    outages_sensor: Optional[Sensor] = None,
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
    from_time, until_time = parse_from_and_to_dates_default_today_and_tomorrow(
        from_date, to_date, country_timezone
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
    load_forecast_sensor: Optional[Sensor] = None,
    residual_load_sensor: Optional[Sensor] = None,
    outages_sensor: Optional[Sensor] = None,
):
    """
    Query and save the requested extra regressor series (each to its own sensor).

    Data is only fetched from ENTSO-E when needed: residual load implies fetching the
    load and wind & solar forecasts, even when those are not saved themselves.
    """
    # Ensure/lookup the sensors we will write to.
    ensured_sensors = _ensure_regressor_sensors(
        country_code=country_code,
        country_timezone=country_timezone,
        include_load_forecast=include_load_forecast and load_forecast_sensor is None,
        include_wind_solar_forecast=include_wind_solar_forecast,
        include_residual_load=include_residual_load and residual_load_sensor is None,
        include_outages=include_outages and outages_sensor is None,
    )

    # Query ENTSO-E only for what we need (residual load implies both forecasts).
    load_series = green_forecast = None
    if include_load_forecast or include_residual_load:
        log.info("Getting day-ahead load forecast ...")
        load_forecast = client.query_load_forecast(
            country_code, start=from_time, end=until_time
        )
        abort_if_data_empty(load_forecast)
        load_series = regressors.load_series_from_forecast(load_forecast)
        log.debug("Load forecast: \n%s" % load_series)
    if include_wind_solar_forecast or include_residual_load:
        log.info("Getting day-ahead wind & solar forecast ...")
        green_forecast = client.query_wind_and_solar_forecast(
            country_code, start=from_time, end=until_time, psr_type=None
        )
        abort_if_data_empty(green_forecast)
        log.debug("Wind & solar forecast: \n%s" % green_forecast)

    # Assemble the (sensor, series, is_entsoe_data) triples to save.
    to_save: List[Tuple[Sensor, pd.Series, bool]] = []
    if include_load_forecast:
        sensor = load_forecast_sensor or ensured_sensors["Day-ahead load forecast"]
        to_save.append((sensor, load_series, True))
    if include_wind_solar_forecast:
        to_save.extend(
            _wind_solar_to_save(green_forecast, ensured_sensors, country_code, log)
        )
    if include_residual_load:
        residual_load = _build_residual_load(load_series, green_forecast, log)
        sensor = residual_load_sensor or ensured_sensors["Residual load"]
        to_save.append((sensor, residual_load, False))
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
        sensor = outages_sensor or ensured_sensors["Generation outages"]
        to_save.append((sensor, unavailable_mw, True))

    if not dryrun:
        _save_regressor_series(to_save, log, entsoe_data_source, country_timezone, now)


def _ensure_regressor_sensors(
    country_code: str,
    country_timezone: str,
    include_load_forecast: bool,
    include_wind_solar_forecast: bool,
    include_residual_load: bool,
    include_outages: bool,
) -> Dict[str, Sensor]:
    """Ensure the sensors for the requested regressors exist, returning them by name."""
    specs: List[Tuple] = []
    if include_load_forecast:
        specs.append(regressors.LOAD_FORECAST_SENSOR_SPEC)
    if include_wind_solar_forecast:
        specs.extend(regressors.WIND_SOLAR_SENSOR_SPECS)
    if include_residual_load:
        specs.append(regressors.RESIDUAL_LOAD_SENSOR_SPEC)
    if include_outages:
        specs.append(regressors.OUTAGES_SENSOR_SPEC)
    if not specs:
        return {}
    return ensure_sensors(tuple(specs), country_code, country_timezone)


def _wind_solar_to_save(
    green_forecast: pd.DataFrame,
    ensured_sensors: Dict[str, Sensor],
    country_code: str,
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
        triples.append((ensured_sensors[column], series, True))
    return triples


def _build_residual_load(
    load_series: pd.Series, green_forecast: pd.DataFrame, log
) -> pd.Series:
    """Derive residual load, aligning all inputs to hourly means first."""
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
