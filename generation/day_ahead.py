from typing import Optional
from datetime import timedelta
import entsoe
import pytz

import click
from flask.cli import with_appcontext
from flask import current_app
import entsoe
from entsoe import EntsoePandasClient
from entsoe.entsoe import URL
import pandas as pd
from flexmeasures.utils.time_utils import server_now
from flexmeasures.data.transactional import task_with_status_report

from .. import entsoe_data_bp, DEFAULT_COUNTRY_CODE, DEFAULT_TIMEZONE  # noqa: E402
from ..utils import ensure_data_source
from .utils import ensure_generation_sensors 


"""
Get the CO2 content from tomorrow's generation forecasts.
We get the overall forecast and the solar&wind forecast, so we know the share of green energy.
For now, we'll compute the CO2 mix from some assumptions.
"""


# Source for these ratios: https://ourworldindata.org/energy/country/netherlands#what-sources-does-the-country-get-its-electricity-from (2020 data)
grey_energy_mix = dict(
    gas= .598,
    oil= .045,
    coal= .0718
)
# Source for kg CO2 per MWh: https://energy.utexas.edu/news/nuclear-and-wind-power-estimated-have-lowest-levelized-co2-emissions
# TODO: one can still use other sources, e.g. https://ourworldindata.org/grapher/carbon-dioxide-emissions-factor has only fossil fuels and different numbers
# If there is one (trustable) source that lists all of the below sources, let's use that.
kg_CO2_per_MWh = dict(
    coal=870,        # lignite
    gas=464,         # natural
    solar=44.5,      # mix of utility/residential, difference isn't large
    oil=652,         # ca. 75% of coal, see https://www.volker-quaschning.de/datserv/CO2-spez/index_e.php
    wind_onshore=14,
    wind_offshore=17  # factor of ~ 1.1, see https://www.mdpi.com/2071-1050/10/6/2022 
)

# Use this if you are testing / developing (iop is provided by ENTSO-E for this purpose)
# You'll need a separate account & access token from ENTSO-E for this platform, though.
# TODO: move to main __init__, and document it in Readme
# entsoe.entsoe.URL = "https://iop-transparency.entsoe.eu/api"

@entsoe_data_bp.cli.command("import-day-ahead-generation")
@click.option(
    "--dryrun/--no-dryrun",
    default=False,
    help="In Dry run, do not save the data to the db.",
)
@with_appcontext
@task_with_status_report
def import_day_ahead_generation(dryrun: bool=False):
    """
    Import forecasted generation for the upcoming day.
    This will save overall generation, solar, offshore and onshore wind, and the estimated CO2 content per hour.
    Possibly best to run this script somewhere around or maybe two or three hours after 13:00,
    when prices are announced.
    """
    log = current_app.logger
    country_code = current_app.config.get("ENTSOE_COUNTRY_CODE", DEFAULT_COUNTRY_CODE)
    timezone = current_app.config.get("ENTSOE_TIMEZONE", DEFAULT_TIMEZONE)

    log.info(f"Will contact ENSO-E at {entsoe.entsoe.URL}, country code: {country_code}, timezone {timezone} ...")

    ensure_data_source()
    ensure_generation_sensors()

    now = server_now().astimezone(pytz.timezone(timezone)).replace(minute=0, second=0, microsecond=0)
    from_time = (now + timedelta(hours=24)).replace(hour=0)
    until_time = from_time + timedelta(hours=24)
    log.info(
        f"Importing generation data from ENTSO-E, starting at {from_time}, up until {until_time} ..."
    )
    from_time = from_time.astimezone(pytz.utc)
    until_time = until_time.astimezone(pytz.utc)

    auth_token = current_app.config.get("ENTSOE_AUTH_TOKEN")
    if not auth_token:
        click.echo("Setting ENTSOE_AUTH_TOKEN seems empty!")
        raise click.Abort
    
    client = EntsoePandasClient(api_key=auth_token)
    log.info("Getting all generation ...")
    all_generation: pd.DataFrame= client.query_generation_forecast(country_code, start=from_time, end=until_time)
    log.debug("Overall aggregated generation: \n%s" % all_generation)

    log.info("Getting green generation ...")
    green_generation_df: pd.DataFrame = client.query_wind_and_solar_forecast(country_code, start=from_time, end=until_time, psr_type=None)
    log.debug("Green generation: \n%s" % green_generation_df)

    log.info("Down-sampling green energy forecast ...")
    green_generation = green_generation_df.resample("60T").mean()    
    log.debug("Resampled green generation: \n%s" % green_generation)

    log.info("Aggregating green energy columns ...")
    all_green_generation = green_generation.sum(axis="columns")
    log.debug("Aggregated green generation: \n%s" % all_green_generation)

    log.info("Computing grey generation forecast ...")
    grey_generation = all_generation - all_green_generation
    log.debug("Grey generation: \n%s" % grey_generation)

    log.info("Computing CO2 content from the MWh values ...")
    co2 = calculate_CO2_content_in_kg_per_MWh(grey_generation, green_generation)
    log.debug("Overall CO2 content (tonnes): \n%s" % co2)

    # TODO: save values for each sensor we use, via fm.api.common.api_utils.save_to_db (make BeliefsDataFrames first)  
    if not dryrun:
        pass


def calculate_CO2_content_in_kg_per_MWh(grey_generation: pd.Series, green_generation: pd.DataFrame) -> pd.Series:
    grey_CO2_intensity_factor = (  # TODO: a factor per hour of the day
        (grey_energy_mix["coal"] * kg_CO2_per_MWh["coal"])
         + (grey_energy_mix["gas"] * kg_CO2_per_MWh["gas"])
         + (grey_energy_mix["oil"] * kg_CO2_per_MWh["oil"])
    )
    current_app.logger.debug(f"Grey intensity factor: {grey_CO2_intensity_factor}")
    grey_CO2_content_in_tonnes = grey_generation * grey_CO2_intensity_factor / 1000.
    current_app.logger.debug("Grey CO2 content (tonnes): \n%s" % grey_CO2_content_in_tonnes)
    
    green_generation["solar CO2 tonnes"] = green_generation["Solar"] * kg_CO2_per_MWh["solar"] / 1000. 
    green_generation["wind_onshore CO2 tonnes"] = green_generation["Wind Onshore"] * kg_CO2_per_MWh["wind_onshore"] / 1000.
    green_generation["wind_offshore CO2 tonnes"] = green_generation["Wind Offshore"] * kg_CO2_per_MWh["wind_offshore"] / 1000.
    
    current_app.logger.debug("Green generation and CO2 content (tonnes): \n%s" % green_generation)

    return grey_CO2_content_in_tonnes + green_generation["solar CO2 tonnes"] + green_generation["wind_onshore CO2 tonnes"] + green_generation["wind_offshore CO2 tonnes"]