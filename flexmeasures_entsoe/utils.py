from typing import Dict, Optional, Tuple, Union
from datetime import datetime
from logging import Logger

from entsoe import EntsoePandasClient
from flask import current_app
from pandas.tseries.frequencies import to_offset
import pandas as pd
import click
import pytz
import entsoe

from flexmeasures.data.utils import get_data_source, save_to_db
from flexmeasures import Asset, AssetType, Sensor, Source
from flexmeasures.data import db
from flexmeasures.utils.time_utils import server_now
from timely_beliefs import BeliefsDataFrame

from . import (
    DEFAULT_DERIVED_DATA_SOURCE,
    DEFAULT_COUNTRY_CODE,
    DEFAULT_COUNTRY_TIMEZONE,
)  # noqa: E402


def ensure_data_source() -> Source:
    return get_data_source(
        data_source_name="ENTSO-E",
        data_source_type="forecasting script",
    )


def ensure_data_source_for_derived_data() -> Source:
    return get_data_source(
        data_source_name=current_app.config.get(
            "ENTSOE_DERIVED_DATA_SOURCE", DEFAULT_DERIVED_DATA_SOURCE
        ),
        data_source_type="forecasting script",
    )


def ensure_transmission_zone_asset(country_code: str) -> Asset:
    """
    Ensure a GenericAsset exists to model the transmission zone for which this plugin gathers data.
    """
    transmission_zone_type = AssetType.query.filter(
        AssetType.name == "transmission zone"
    ).one_or_none()
    if not transmission_zone_type:
        current_app.logger.info("Adding transmission zone type ...")
        transmission_zone_type = AssetType(
            name="transmission zone",
            description="A grid regulated & balanced as a whole, usually a national grid.",
        )
        db.session.add(transmission_zone_type)
    ga_name = f"{country_code} transmission zone"
    transmission_zone = Asset.query.filter(Asset.name == ga_name).one_or_none()
    if not transmission_zone:
        current_app.logger.info(f"Adding {ga_name} ...")
        transmission_zone = Asset(
            name=ga_name,
            generic_asset_type=transmission_zone_type,
            account_id=None,  # public
        )
        db.session.add(transmission_zone)
    db.session.commit()
    return transmission_zone


def ensure_sensors(
    sensor_specifications: Tuple[Tuple],
    country_code: str,
    timezone: str,
) -> Dict[str, Sensor]:
    """
    Ensure a GenericAsset exists to model the transmission zone for which this plugin gathers
    generation data, then add specified sensors for relevant data we collect.

    If new sensors got created, the session has been flushed.
    """
    sensors = {}
    sensors_created: bool = False
    transmission_zone = ensure_transmission_zone_asset(country_code)
    for sensor_name, unit, event_resolution, data_by_entsoe in sensor_specifications:
        sensor = Sensor.query.filter(
            Sensor.name == sensor_name,
            Sensor.unit == unit,
            Sensor.generic_asset == transmission_zone,
        ).one_or_none()
        if not sensor:
            current_app.logger.info(f"Adding sensor {sensor_name} ...")
            sensor = Sensor(
                name=sensor_name,
                unit=unit,
                generic_asset=transmission_zone,
                timezone=timezone,
                event_resolution=event_resolution,
            )
            db.session.add(sensor)
            sensors_created = True
        sensor.data_by_entsoe = data_by_entsoe
        sensors[sensor_name] = sensor
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
        entsoe.entsoe.URL = "https://web-api.tp.entsoe.eu/api"
    if not auth_token:
        click.echo("Setting ENTSOE_AUTH_TOKEN seems empty!")
        raise click.Abort
    return auth_token


def ensure_country_code_and_timezone(
    country_code: Optional[str] = None,
    country_timezone: Optional[str] = None,
) -> Tuple[str, str]:
    if country_code is None:
        country_code = current_app.config.get(
            "ENTSOE_COUNTRY_CODE", DEFAULT_COUNTRY_CODE
        )
    if country_timezone is None:
        country_timezone = current_app.config.get(
            "ENTSOE_COUNTRY_TIMEZONE", DEFAULT_COUNTRY_TIMEZONE
        )
    return country_code, country_timezone


def create_entsoe_client() -> EntsoePandasClient:
    auth_token = get_auth_token_from_config_and_set_server_url()
    client = EntsoePandasClient(api_key=auth_token)
    return client


def abort_if_data_empty(data: Union[pd.DataFrame, pd.Series]):
    if data.empty:
        click.echo(
            "Result is empty. Probably ENTSO-E does not provide these forecasts yet ..."
        )
        raise click.Abort


def parse_from_and_to_dates_default_today_and_tomorrow(
    from_date: Optional[datetime], to_date: Optional[datetime], country_timezone: str
) -> Tuple[datetime, datetime]:
    """
    Parse CLI options (or set default to today and tomorrow)
    Note:  entsoe-py expects time params as pd.Timestamp
    """
    today_start = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
    if to_date is None:
        to_date = pd.Timestamp(
            today_start, tzinfo=pytz.timezone(country_timezone)
        ) + pd.offsets.DateOffset(
            days=1
        )  # Add a calendar day instead of just 24 hours, from https://github.com/gweis/isodate/pull/64
    else:
        to_date = pd.Timestamp(to_date, tzinfo=pytz.timezone(country_timezone))
    if from_date is None:
        from_date = pd.Timestamp(today_start, tzinfo=pytz.timezone(country_timezone))
    else:
        from_date = pd.Timestamp(from_date, tzinfo=pytz.timezone(country_timezone))
    from_time, until_time = date_range_to_time_range(from_date, to_date)
    return from_time, until_time


def parse_from_and_to_dates_default_yesterday(
    from_date: Optional[datetime], to_date: Optional[datetime], country_timezone: str
) -> Tuple[datetime, datetime]:
    """
    Parse CLI options (or set default to yesterday)
    Note:  entsoe-py expects time params as pd.Timestamp
    """
    if from_date is None:
        today_start = datetime.today().replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        from_date = pd.Timestamp(
            today_start, tzinfo=pytz.timezone(country_timezone)
        ) - pd.offsets.DateOffset(
            days=1
        )  # Deduct a calendar day instead of just 24 hours, from https://github.com/gweis/isodate/pull/64
    else:
        from_date = pd.Timestamp(from_date, tzinfo=pytz.timezone(country_timezone))
    if to_date is None:
        to_date = from_date
    else:
        to_date = pd.Timestamp(to_date, tzinfo=pytz.timezone(country_timezone))
    from_time, until_time = date_range_to_time_range(from_date, to_date)
    return from_time, until_time


def resample_if_needed(s: pd.Series, sensor: Sensor) -> pd.Series:
    inferred_frequency = pd.infer_freq(s.index)
    if inferred_frequency is None:
        raise ValueError(
            "Data has no discernible frequency from which to derive an event resolution."
        )
    inferred_resolution = pd.to_timedelta(to_offset(inferred_frequency))
    target_resolution = sensor.event_resolution
    if inferred_resolution == target_resolution:
        return s
    elif inferred_resolution > target_resolution:
        current_app.logger.debug(f"Upsampling data for {sensor.name} ...")
        index = pd.date_range(
            s.index[0],
            s.index[-1] + inferred_resolution,
            freq=target_resolution,
            closed="left",
        )
        s = s.reindex(index).pad()
    elif inferred_resolution < target_resolution:
        current_app.logger.debug(f"Downsampling data for {sensor.name} ...")
        s = s.resample(target_resolution).mean()
    current_app.logger.debug(f"Resampled data for {sensor.name}: \n%s" % s)
    return s


def save_entsoe_series(
    series: pd.Series,
    sensor: Sensor,
    entsoe_source: Source,
    country_timezone: str,
    now: Optional[datetime] = None,
):
    """
    Save a series gotten from ENTSO-E to a FlexMeasures database.
    """
    if not now:
        now = server_now().astimezone(pytz.timezone(country_timezone))
    belief_times = (
        (series.index.floor("D") - pd.Timedelta("6h"))
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
    status = save_to_db(bdf)
    if status == "success_but_nothing_new":
        current_app.logger.info("Done. These beliefs had already been saved before.")
    elif status == "success_with_unchanged_beliefs_skipped":
        current_app.logger.info("Done. Some beliefs had already been saved before.")


def date_range_to_time_range(
    from_date: pd.Timestamp, to_date: pd.Timestamp
) -> Tuple[pd.Timestamp, pd.Timestamp]:
    """Because to_date is inclusive, we add one calendar day."""
    return from_date, to_date + pd.offsets.DateOffset(days=1)


def start_import_log(
    import_type: str,
    from_time: pd.Timestamp,
    until_time: pd.Timestamp,
    country_code: str,
    country_timezone: str,
) -> Tuple[Logger, datetime]:
    log = current_app.logger
    log.info(
        f"Importing {import_type} data for {country_code} (timezone {country_timezone}), starting at {from_time}, up until {until_time}, from ENTSO-E at {entsoe.entsoe.URL} ..."
    )
    now = server_now().astimezone(pytz.timezone(country_timezone))
    return log, now
