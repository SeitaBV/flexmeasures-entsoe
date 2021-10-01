from typing import Optional, Union
from datetime import datetime
import pytz

import click
from flask.cli import with_appcontext
from flask import current_app
import entsoe
from entsoe import EntsoePandasClient

# from entsoe.entsoe import URL
import pandas as pd
from flexmeasures.utils.time_utils import server_now
from flexmeasures.data.services.time_series import drop_unchanged_beliefs
from flexmeasures.data.transactional import task_with_status_report
from flexmeasures.api.common.utils.api_utils import save_to_db
from timely_beliefs import BeliefsDataFrame

from .. import (
    entsoe_data_bp,
    DEFAULT_COUNTRY_CODE,
    DEFAULT_COUNTRY_TIMEZONE,
)  # noqa: E402
from ..utils import ensure_data_source, ensure_data_source_for_derived_data
from .utils import ensure_generation_sensors


"""
Get the CO2 content from tomorrow's generation forecasts.
We get the overall forecast and the solar&wind forecast, so we know the share of green energy.
For now, we'll compute the CO2 mix from some assumptions.
"""

# TODO: Decide which sources to use ― https://github.com/SeitaBV/flexmeasures-entsoe/issues/2

# Source for these ratios: https://ourworldindata.org/energy/country/netherlands#what-sources-does-the-country-get-its-electricity-from (2020 data)
grey_energy_mix = dict(gas=0.598, oil=0.045, coal=0.0718)

# Source for kg CO2 per MWh: https://energy.utexas.edu/news/nuclear-and-wind-power-estimated-have-lowest-levelized-co2-emissions
kg_CO2_per_MWh = dict(
    coal=870,  # lignite
    gas=464,  # natural
    solar=44.5,  # mix of utility/residential, difference isn't large
    oil=652,  # ca. 75% of coal, see https://www.volker-quaschning.de/datserv/CO2-spez/index_e.php
    wind_onshore=14,
    wind_offshore=17,  # factor of ~ 1.1, see https://www.mdpi.com/2071-1050/10/6/2022
)

# Use this if you are testing / developing (iop is provided by ENTSO-E for this purpose)
# You'll need a separate account & access token from ENTSO-E for this platform, though.
# TODO: move to main __init__, and document it in Readme
# entsoe.entsoe.URL = "https://iop-transparency.entsoe.eu/api"


@entsoe_data_bp.cli.command("import-day-ahead-generation")
@click.option(
    "--from-date",
    required=False,
    type=click.DateTime(["%Y-%m-%d"]),
    help="Query data from this date onwards. If not specified, defaults to --to-date",
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
@with_appcontext
@task_with_status_report
def import_day_ahead_generation(
    dryrun: bool = False,
    from_date: Optional[datetime] = None,
    to_date: Optional[datetime] = None,
):
    """
    Import forecasted generation for any date range, defaulting to tomorrow.
    This will save overall generation, solar, offshore and onshore wind, and the estimated CO2 content per hour.
    Possibly best to run this script somewhere around or maybe two or three hours after 13:00,
    when tomorrow's prices are announced.
    """
    log = current_app.logger
    country_code = current_app.config.get("ENTSOE_COUNTRY_CODE", DEFAULT_COUNTRY_CODE)
    country_timezone = current_app.config.get(
        "ENTSOE_COUNTRY_TIMEZONE", DEFAULT_COUNTRY_TIMEZONE
    )

    log.info(
        f"Will contact ENTSO-E at {entsoe.entsoe.URL}, country code: {country_code}, country timezone {country_timezone} ..."
    )

    entsoe_data_source = ensure_data_source()
    derived_data_source = ensure_data_source_for_derived_data()
    sensors = ensure_generation_sensors()

    # Parse CLI options (or set defaults)
    # entsoe-py expects time params as pd.Timestamp
    if to_date is None:
        today_start = datetime.today().replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        to_date = pd.Timestamp(
            today_start, tzinfo=pytz.timezone(country_timezone)
        ) + pd.offsets.DateOffset(
            days=1
        )  # Add a calendar day instead of just 24 hours, from https://github.com/gweis/isodate/pull/64
    else:
        to_date = pd.Timestamp(to_date, tzinfo=pytz.timezone(country_timezone))
    if from_date is None:
        from_time = to_date
    else:
        from_time = pd.Timestamp(from_date, tzinfo=pytz.timezone(country_timezone))
    until_time = to_date + pd.offsets.DateOffset(days=1)  # because to_date is inclusive
    log.info(
        f"Importing generation data from ENTSO-E, starting at {from_time}, up until {until_time} ..."
    )

    auth_token = current_app.config.get("ENTSOE_AUTH_TOKEN")
    if not auth_token:
        click.echo("Setting ENTSOE_AUTH_TOKEN seems empty!")
        raise click.Abort

    def check_empty(data: Union[pd.DataFrame, pd.Series]):
        if data.empty:
            click.echo(
                "Result is empty. Probably ENTSO-E does not provide these forecasts yet ..."
            )
            raise click.Abort

    now = server_now().astimezone(pytz.timezone(country_timezone))
    client = EntsoePandasClient(api_key=auth_token)
    log.info("Getting scheduled generation ...")
    # We assume that the green (solar & wind) generation is not included in this (it is not scheduled)
    scheduled_generation: pd.Series = client.query_generation_forecast(
        country_code, start=from_time, end=until_time
    )
    check_empty(scheduled_generation)
    log.debug("Overall aggregated generation: \n%s" % scheduled_generation)

    log.info("Getting green generation ...")
    green_generation_df: pd.DataFrame = client.query_wind_and_solar_forecast(
        country_code, start=from_time, end=until_time, psr_type=None
    )
    check_empty(green_generation_df)
    log.debug("Green generation: \n%s" % green_generation_df)

    log.info("Down-sampling green energy forecast ...")
    green_generation_df = green_generation_df.resample(
        "60T"
    ).mean()  # ENTSO-E data is in MW
    log.debug("Resampled green generation: \n%s" % green_generation_df)

    log.info("Aggregating green energy columns ...")
    all_green_generation = green_generation_df.sum(axis="columns")
    log.debug("Aggregated green generation: \n%s" % all_green_generation)

    log.info("Computing combined generation forecast ...")
    all_generation = scheduled_generation + all_green_generation
    log.debug("Combined generation: \n%s" % all_generation)

    log.info("Computing CO2 content from the MWh values ...")
    co2_in_kg = calculate_CO2_content_in_kg(scheduled_generation, green_generation_df)
    log.debug("Overall CO2 content (kg): \n%s" % co2_in_kg)
    forecasted_kg_CO2_per_MWh = co2_in_kg / all_generation
    log.debug("Overall CO2 content (kg/MWh): \n%s" % forecasted_kg_CO2_per_MWh)

    def get_series_for_sensor(sensor):
        if sensor.name == "Scheduled generation":
            return scheduled_generation
        elif sensor.name == "Solar":
            return green_generation_df["Solar"]
        elif sensor.name == "Onshore wind":
            return green_generation_df["Wind Onshore"]
        elif sensor.name == "Offshore wind":
            return green_generation_df["Wind Offshore"]
        elif sensor.name == "CO2 intensity":
            return forecasted_kg_CO2_per_MWh
        else:
            log.error(f"Cannot connect data to sensor {sensor.name}.")
            raise click.Abort

    if not dryrun:
        for sensor in sensors:
            log.debug(f"Saving data for Sensor {sensor.name} ...")
            series = get_series_for_sensor(sensor)
            series.name = "event_value"  # required by timely_beliefs, TODO: check if that still is the case, see https://github.com/SeitaBV/timely-beliefs/issues/64
            belief_times = (
                (series.index.floor("D") - pd.Timedelta("6H"))
                .to_frame(name="clipped_belief_times")
                .clip(upper=now)
                .set_index("clipped_belief_times")
                .index
            )  # published no later than D-1 18:00 Brussels time
            bdf = BeliefsDataFrame(
                series,
                source=entsoe_data_source
                if sensor.data_by_entsoe
                else derived_data_source,
                sensor=sensor,
                belief_time=belief_times,
            )

            # Drop beliefs that haven't changed
            bdf = bdf.groupby(level=["belief_time"], as_index=False).apply(
                drop_unchanged_beliefs
            )

            # Work around bug in which groupby still introduces an index level, even though we asked it not to
            if None in bdf.index.names:
                bdf.index = bdf.index.droplevel(None)

            # TODO: evaluate some traits of the data via FlexMeasures, see https://github.com/SeitaBV/flexmeasures-entsoe/issues/3
            save_to_db(bdf)


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
    current_app.logger.debug("Grey CO2 content (tonnes): \n%s" % grey_CO2_content)

    green_generation["solar CO2"] = (
        green_generation["Solar"] * kg_CO2_per_MWh["solar"] / 1000.0
    )
    green_generation["wind_onshore CO2"] = (
        green_generation["Wind Onshore"] * kg_CO2_per_MWh["wind_onshore"]
    )
    green_generation["wind_offshore CO2"] = (
        green_generation["Wind Offshore"] * kg_CO2_per_MWh["wind_offshore"]
    )

    current_app.logger.debug(
        "Green generation and CO2 content: \n%s" % green_generation
    )

    return (
        grey_CO2_content
        + green_generation["solar CO2"]
        + green_generation["wind_onshore CO2"]
        + green_generation["wind_offshore CO2"]
    )
