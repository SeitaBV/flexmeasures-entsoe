from typing import Tuple, List, Union
from datetime import datetime, timedelta

from flask import current_app
import pandas as pd
import click
import pytz
import entsoe

from flexmeasures.data.utils import get_data_source, save_to_db
from flexmeasures.data.models.time_series import Sensor
from flexmeasures.data.models.generic_assets import GenericAsset, GenericAssetType
from flexmeasures.data.models.data_sources import DataSource
from flexmeasures.utils.time_utils import server_now
from flexmeasures.data.config import db
from timely_beliefs import BeliefsDataFrame

from . import DEFAULT_DERIVED_DATA_SOURCE, DEFAULT_COUNTRY_CODE, DEFAULT_COUNTRY_TIMEZONE  # noqa: E402


def ensure_data_source() -> DataSource:
    return get_data_source(
        data_source_name="ENTSO-E",
        data_source_type="forecasting script",
    )


def ensure_data_source_for_derived_data() -> DataSource:
    return get_data_source(
        data_source_name=current_app.config.get("ENTSOE_DERIVED_DATA_SOURCE", DEFAULT_DERIVED_DATA_SOURCE),
        data_source_type="forecasting script",
    )


def ensure_transmission_zone_asset() -> GenericAsset:
    """
    Ensure a GenericAsset exists to model the transmission zone for which this plugin gathers data.
    """
    country_code = current_app.config.get("ENTSOE_COUNTRY_CODE", DEFAULT_COUNTRY_CODE)
    transmission_zone_type = GenericAssetType.query.filter(
        GenericAssetType.name == "transmission zone"
    ).one_or_none()
    if not transmission_zone_type:
        current_app.logger.info("Adding transmission zone type ...")
        transmission_zone_type = GenericAssetType(
            name="transmission zone",
            description="A grid regulated & balanced as a whole, usually a national grid.",
        )
        db.session.add(transmission_zone_type)
    ga_name = f"{country_code} transmission zone"
    transmission_zone = GenericAsset.query.filter(
        GenericAsset.name == ga_name
    ).one_or_none()
    if not transmission_zone:
        current_app.logger.info(f"Adding {ga_name} ...")
        transmission_zone = GenericAsset(
            name=ga_name,
            generic_asset_type=transmission_zone_type,
            account_id=None,  # public
        )
    return transmission_zone


def ensure_sensors(sensor_specifications: Tuple[Tuple]) -> List[Sensor]:
    """
    Ensure a GenericAsset exists to model the transmission zone for which this plugin gathers
    generation data, then add specified sensors for relevant data we collect.

    If new sensors got created, the session has been flushed.
    """
    sensors = []
    sensors_created: bool = False
    timezone = current_app.config.get(
        "ENTSOE_COUNTRY_TIMEZONE", DEFAULT_COUNTRY_TIMEZONE
    )
    transmission_zone = ensure_transmission_zone_asset() 
    for sensor_name, unit, data_by_entsoe in sensor_specifications:
        sensor = Sensor.query.filter(Sensor.name == sensor_name).one_or_none()
        if not sensor:
            current_app.logger.info(f"Adding sensor {sensor_name} ...")
            sensor = Sensor(
                name=sensor_name,
                unit=unit,
                generic_asset=transmission_zone,
                timezone=timezone,
                event_resolution=timedelta(hours=1),
            )
            db.session.add(sensor)
            sensors_created = True
        sensor.data_by_entsoe = data_by_entsoe
        sensors.append(sensor)
    if sensors_created:
        db.session.flush()
    return sensors


def get_auth_token_from_config_and_set_server_url() -> str:
    """
    Read ENTSOE auth token from config, raise if not given.
    If test server is supposed to be used, we'll try to read the token
    usable for that, and also change the URL.
    """
    use_test_server = current_app.config.get("ENTSOE_USE_TEST_SERVER", False)
    if use_test_server:
        auth_token = current_app.config.get("ENTSOE_AUTH_TOKEN_TEST_SERVER")
        entsoe.entsoe.URL = "https://iop-transparency.entsoe.eu/api"
    else:
        auth_token = current_app.config.get("ENTSOE_AUTH_TOKEN")
        entsoe.entsoe.URL = "https://transparency.entsoe.eu/api"
    if not auth_token:
        click.echo("Setting ENTSOE_AUTH_TOKEN seems empty!")
        raise click.Abort
    return auth_token


def abort_if_data_empty(data: Union[pd.DataFrame, pd.Series]):
    if data.empty:
        click.echo(
            "Result is empty. Probably ENTSO-E does not provide these forecasts yet ..."
        )
        raise click.Abort


def parse_from_and_to_dates(from_date: datetime, to_date: datetime, country_timezone: str) -> Tuple[datetime, datetime]:
    """
    Parse CLI options (or set defaults)
    Note:  entsoe-py expects time params as pd.Timestamp
    """
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
    return from_time, until_time


def save_entsoe_series(series: pd.Series, sensor: Sensor, entsoe_source: DataSource, country_timezone: str):
    """
    Save a series gotten from ENTSO-E to a Flexeasures database.
    """
    now = server_now().astimezone(pytz.timezone(country_timezone))
    belief_times = (
        (series.index.floor("D") - pd.Timedelta("6H"))
        .to_frame(name="clipped_belief_times")
        .clip(upper=now)
        .set_index("clipped_belief_times")
        .index
    )  # published no later than D-1 18:00 Brussels time
    bdf = BeliefsDataFrame(
        series,
        source=entsoe_source,
        sensor=sensor,
        belief_time=belief_times,
    )

    # TODO: evaluate some traits of the data via FlexMeasures, see https://github.com/SeitaBV/flexmeasures-entsoe/issues/3
    save_to_db(bdf)
