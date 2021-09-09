from typing import List
from datetime import timedelta
from flask import current_app

from flexmeasures.data.models.generic_assets import GenericAsset, GenericAssetType
from flexmeasures.data.models.time_series import Sensor
from flexmeasures.data.config import db

from .. import DEFAULT_COUNTRY_CODE, DEFAULT_COUNTRY_TIMEZONE  # noqa: E402


generation_sensors = (
    ("Scheduled generation", "MWh"),
    ("Solar", "MWh"),
    ("Onshore wind", "MWh"),
    ("Offshore wind", "MWh"),
    ("CO2 intensity", "kg/MWh"),
)


def ensure_generation_sensors() -> List[Sensor]:
    """
    Ensure a GenericAsset exists to model the transmission zone for which this plugin gathers
    generation data, plus sensors for relevant data we collect.
    """
    sensors = []
    country_code = current_app.config.get("ENTSOE_COUNTRY_CODE", DEFAULT_COUNTRY_CODE)
    timezone = current_app.config.get("ENTSOE_COUNTRY_TIMEZONE", DEFAULT_COUNTRY_TIMEZONE)

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
    for sensor_name, unit in generation_sensors:
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
        sensors.append(sensor)
    db.session.commit()
    return sensors
