from typing import Optional
from datetime import datetime

import click
from flask.cli import with_appcontext
from flask import current_app
import pandas as pd

import entsoe
from entsoe import EntsoePandasClient
from flexmeasures.data.transactional import task_with_status_report

from . import pricing_sensors
from .. import (
    entsoe_data_bp,
    DEFAULT_COUNTRY_CODE,
    DEFAULT_COUNTRY_TIMEZONE,
)  # noqa: E402
from ..utils import (
    ensure_data_source,
    parse_from_and_to_dates_default_tomorrow,
    ensure_sensors,
    save_entsoe_series,
    get_auth_token_from_config_and_set_server_url,
    abort_if_data_empty,
)


@entsoe_data_bp.cli.command("import-day-ahead-prices")
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
@task_with_status_report("entsoe-import-day-ahead-prices")
def import_day_ahead_prices(
    dryrun: bool = False,
    from_date: Optional[datetime] = None,
    to_date: Optional[datetime] = None,
):
    """
    Import forecasted prices for any date range, defaulting to tomorrow.
    Possibly best to run this script somewhere around or maybe two or three hours after 13:00,
    when tomorrow's prices are announced.
    """
    log = current_app.logger
    country_code = current_app.config.get("ENTSOE_COUNTRY_CODE", DEFAULT_COUNTRY_CODE)
    country_timezone = current_app.config.get(
        "ENTSOE_COUNTRY_TIMEZONE", DEFAULT_COUNTRY_TIMEZONE
    )

    auth_token = get_auth_token_from_config_and_set_server_url()
    log.info(
        f"Will contact ENTSO-E at {entsoe.entsoe.URL}, country code: {country_code}, country timezone: {country_timezone} ..."
    )

    entsoe_data_source = ensure_data_source()

    from_time, until_time = parse_from_and_to_dates_default_tomorrow(
        from_date, to_date, country_timezone
    )
    log.info(
        f"Importing generation data from ENTSO-E, starting at {from_time}, up until {until_time} ..."
    )

    sensors = ensure_sensors(pricing_sensors)
    # For now, we only have one pricing sensor ...
    pricing_sensor = sensors["Day-ahead prices"]
    assert pricing_sensor.name == "Day-ahead prices"

    client = EntsoePandasClient(api_key=auth_token)

    log.info("Getting prices ...")
    prices: pd.Series = client.query_day_ahead_prices(
        country_code, start=from_time, end=until_time
    )
    abort_if_data_empty(prices)
    log.debug("Prices: \n%s" % prices)

    if not dryrun:
        log.info(f"Saving {len(prices)} beliefs for Sensor {pricing_sensor.name} ...")
        prices.name = "event_value"  # required by timely_beliefs, TODO: check if that still is the case, see https://github.com/SeitaBV/timely-beliefs/issues/64
        save_entsoe_series(prices, pricing_sensor, entsoe_data_source, country_timezone)
