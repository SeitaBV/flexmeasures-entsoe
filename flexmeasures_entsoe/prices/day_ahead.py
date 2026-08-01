from typing import Optional
from datetime import datetime

import click
from flask.cli import with_appcontext
import pandas as pd
from flexmeasures import Source, Sensor

from flexmeasures.data.transactional import task_with_status_report

from flexmeasures.data.schemas import SensorIdField
from flexmeasures.data.schemas.sources import DataSourceIdField


from . import pricing_sensors
from .regressors_import import collect_and_save_regressors
from .. import (
    entsoe_data_bp,
)  # noqa: E402
from ..utils import (
    create_entsoe_client,
    ensure_country_code_and_timezone,
    ensure_data_source,
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
    "--include-all/--no-include-all",
    default=False,
    help="Shorthand for 'import everything': turns on every regressor "
    "(load forecast, wind & solar forecast, residual load, outages) AND --include-neighbours. "
    "Combines with any explicitly set flags.",
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
    "(from ENTSO-E's own neighbour mapping; for NL: BE, DE_LU, DE_AT_LU, GB, NO_2, DK_1). "
    "Each neighbour's series are saved to plainly-named sensors on that neighbour's own transmission-zone "
    "asset. Neighbour scarcity drives NL price spikes, so this improves spike/evening-peak forecasts.",
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
    include_all: bool = False,
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
      --include-all                  Shorthand: turn on all of the below (and neighbours).
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
                                     for interconnected neighbours, on each neighbour's own
                                     transmission-zone asset. Regional NW-European scarcity
                                     drives NL price spikes.
    """
    # --include-all is a shorthand that turns everything on; it composes with explicit flags.
    if include_all:
        include_load_forecast = True
        include_wind_solar_forecast = True
        include_residual_load = True
        include_outages = True
        include_neighbours = True

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
