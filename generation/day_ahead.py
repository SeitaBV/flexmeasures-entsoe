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


gray_energy_mix = dict(
    gas= .598,
    oil= .045,
    coal= .0718
)
# Source for these ratios: https://ourworldindata.org/energy/country/netherlands#what-sources-does-the-country-get-its-electricity-from
kg_CO2_per_MWh_of_coal = 363.6  # lignite
kg_CO2_per_MWh_of_gas = 201.96
# Source for kg CO2 per MWh: https://ourworldindata.org/grapher/carbon-dioxide-emissions-factor

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
    log.debug(all_generation)
    log.info("Getting green generation ...")
    green_generation_df: pd.DataFrame = client.query_wind_and_solar_forecast(country_code, start=from_time, end=until_time, psr_type=None)
    log.debug(green_generation_df)

    log.info("Down-sampling green energy forecast ...")
    green_generation_df = green_generation_df.resample("60T").mean()    
    log.debug(green_generation_df)

    log.info("Aggregating green energy columns ...")
    green_generation = green_generation_df.agg(["sum"], axis="columns")
    log.debug(green_generation)

    log.info("Computing grey generation forecast ...")
    grey_generation = all_generation - green_generation["sum"]
    log.debug(grey_generation)

    # TODO: compute CO2 content from these MWh values
    
    # TODO: save values for each sensor we use, via fm.api.common.api_utils.save_to_db (make BeliefsDataFrames first)  
