from typing import Optional
from datetime import datetime

import click
from flask.cli import with_appcontext
from flask import current_app

# from entsoe.entsoe import URL
import pandas as pd
from flexmeasures.data.transactional import task_with_status_report

from .. import (
    entsoe_data_bp,
)  # noqa: E402
from . import generation_sensors
from ..utils import (
    create_entsoe_client,
    ensure_country_code_and_timezone,
    ensure_data_source,
    ensure_data_source_for_derived_data,
    abort_if_data_empty,
    parse_from_and_to_dates_default_today_and_tomorrow,
    save_entsoe_series,
    ensure_sensors,
    resample_if_needed,
    start_import_log,
)


"""
Get the CO₂ content from tomorrow's generation forecasts.
We get the overall forecast and the solar&wind forecast, so we know the share of green energy.
For now, we'll compute the CO₂ mix from some assumptions.
"""

# TODO: Decide which sources to use ― https://github.com/SeitaBV/flexmeasures-entsoe/issues/2

# Source for these ratios: https://ourworldindata.org/energy/country/netherlands#what-sources-does-the-country-get-its-electricity-from (2020 data)
grey_energy_mix = dict(gas=0.598, oil=0.045, coal=0.0718)

# Source for kg CO₂ per MWh: https://energy.utexas.edu/news/nuclear-and-wind-power-estimated-have-lowest-levelized-co2-emissions
kg_CO2_per_MWh = dict(
    coal=870,  # lignite
    gas=464,  # natural
    solar=44.5,  # mix of utility/residential, difference isn't large
    oil=652,  # ca. 75% of coal, see https://www.volker-quaschning.de/datserv/CO2-spez/index_e.php
    wind_onshore=14,
    wind_offshore=17,  # factor of ~ 1.1, see https://www.mdpi.com/2071-1050/10/6/2022
)


@entsoe_data_bp.cli.command("import-day-ahead-generation")
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
@with_appcontext
@task_with_status_report("entsoe-import-day-ahead-generation")
def import_day_ahead_generation(
    dryrun: bool = False,
    from_date: Optional[datetime] = None,
    to_date: Optional[datetime] = None,
    country_code: Optional[str] = None,
    country_timezone: Optional[str] = None,
):
    """
    Import forecasted generation for any date range, defaulting to today and tomorrow.
    This will save overall generation, solar, offshore and onshore wind, and the estimated CO₂ content per hour.
    Possibly best to run this script somewhere around or maybe two or three hours after 13:00,
    when tomorrow's prices are announced.
    """
    # Set up FlexMeasures data structure
    country_code, country_timezone = ensure_country_code_and_timezone(
        country_code, country_timezone
    )
    entsoe_data_source = ensure_data_source()
    derived_data_source = ensure_data_source_for_derived_data()
    sensors = ensure_sensors(generation_sensors, country_code, country_timezone)
    # Parse CLI options (or set defaults)
    from_time, until_time = parse_from_and_to_dates_default_today_and_tomorrow(
        from_date, to_date, country_timezone
    )

    # Start import
    client = create_entsoe_client()
    log, now = start_import_log(
        "day-ahead generation", from_time, until_time, country_code, country_timezone
    )

    log.info("Getting scheduled generation ...")
    # We assume that the green (solar & wind) generation is not included in this (it is not scheduled)
    scheduled_generation: pd.Series = client.query_generation_forecast(
        country_code, start=from_time, end=until_time
    )
    abort_if_data_empty(scheduled_generation)
    log.debug("Overall aggregated generation: \n%s" % scheduled_generation)

    scheduled_generation = resample_if_needed(
        scheduled_generation,
        sensors["Scheduled generation"],
    )

    log.info("Getting green generation ...")
    green_generation_df: pd.DataFrame = client.query_wind_and_solar_forecast(
        country_code, start=from_time, end=until_time, psr_type=None
    )
    abort_if_data_empty(green_generation_df)
    log.debug("Green generation: \n%s" % green_generation_df)

    log.info("Aggregating green energy columns ...")
    all_green_generation = green_generation_df.sum(axis="columns")
    log.debug("Aggregated green generation: \n%s" % all_green_generation)

    log.info("Computing combined generation forecast ...")
    all_generation = scheduled_generation + all_green_generation
    log.debug("Combined generation: \n%s" % all_generation)

    log.info("Computing CO₂ content from the MWh values ...")
    co2_in_kg = calculate_CO2_content_in_kg(scheduled_generation, green_generation_df)
    log.debug("Overall CO₂ content (kg): \n%s" % co2_in_kg)
    forecasted_kg_CO2_per_MWh = co2_in_kg / all_generation
    log.debug("Overall CO₂ content (kg/MWh): \n%s" % forecasted_kg_CO2_per_MWh)

    def get_series_for_sensor(sensor):
        if sensor.name == "Scheduled generation":
            return scheduled_generation
        elif sensor.name == "Solar":
            return green_generation_df["Solar"]
        elif sensor.name == "Wind Onshore":
            return green_generation_df["Wind Onshore"]
        elif sensor.name == "Wind Offshore":
            return green_generation_df["Wind Offshore"]
        elif sensor.name == "CO₂ intensity":
            return forecasted_kg_CO2_per_MWh
        else:
            log.error(f"Cannot connect data to sensor {sensor.name}.")
            raise click.Abort

    if not dryrun:
        for sensor in sensors.values():
            series = get_series_for_sensor(sensor)
            log.info(f"Saving {len(series)} beliefs for Sensor {sensor.name} ...")
            series.name = "event_value"  # required by timely_beliefs, TODO: check if that still is the case, see https://github.com/SeitaBV/timely-beliefs/issues/64
            entsoe_source = (
                entsoe_data_source if sensor.data_by_entsoe else derived_data_source
            )
            save_entsoe_series(series, sensor, entsoe_source, country_timezone, now)


def calculate_CO2_content_in_kg(
    grey_generation: pd.Series, green_generation: pd.DataFrame
) -> pd.Series:
    grey_CO2_intensity_factor = (  # TODO: a factor per hour of the day
        (grey_energy_mix["coal"] * kg_CO2_per_MWh["coal"])
        + (grey_energy_mix["gas"] * kg_CO2_per_MWh["gas"])
        + (grey_energy_mix["oil"] * kg_CO2_per_MWh["oil"])
    )
    current_app.logger.debug(f"Grey intensity factor: {grey_CO2_intensity_factor}")
    grey_CO2_content = grey_generation * grey_CO2_intensity_factor
    current_app.logger.debug("Grey CO₂ content (tonnes): \n%s" % grey_CO2_content)

    green_generation["solar CO₂"] = (
        green_generation["Solar"] * kg_CO2_per_MWh["solar"] / 1000.0
    )
    green_generation["wind_onshore CO₂"] = (
        green_generation["Wind Onshore"] * kg_CO2_per_MWh["wind_onshore"]
    )
    green_generation["wind_offshore CO₂"] = (
        green_generation["Wind Offshore"] * kg_CO2_per_MWh["wind_offshore"]
    )

    current_app.logger.debug(
        "Green generation and CO₂ content: \n%s" % green_generation
    )

    return (
        grey_CO2_content
        + green_generation["solar CO₂"]
        + green_generation["wind_onshore CO₂"]
        + green_generation["wind_offshore CO₂"]
    )
