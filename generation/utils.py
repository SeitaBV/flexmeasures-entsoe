from datetime import timedelta
from flask import current_app

from flexmeasures.data.models.generic_assets import GenericAsset, GenericAssetType
from flexmeasures.data.models.time_series import Sensor
from flexmeasures.data.config import db
from timely_beliefs.sensors.func_store import knowledge_horizons

from .. import DEFAULT_COUNTRY_CODE, DEFAULT_TIMEZONE  # noqa: E402



def ensure_generation_sensors():
    """
    Ensure a GenericAsset exists to model the transmission zone for which this plugin gathers
    generation data, plus sensors for relevant data we collect.
    """
    country_code = current_app.config.get("ENTSOE_COUNTRY_CODE", DEFAULT_COUNTRY_CODE)
    timezone = current_app.config.get("ENTSOE_TIMEZONE", DEFAULT_TIMEZONE)

    transmission_zone_type = GenericAssetType.query.filter(
        GenericAssetType.name == "transmission zone"
    ).one_or_none()
    if not transmission_zone_type:
        current_app.logger.info("Adding transmission zone type ...")
        transmission_zone_type = GenericAssetType(name="transmission zone", description="A grid regulated & balanced as a whole, usually a national grid.")
        db.session.add(transmission_zone_type)
    ga_name = f"{country_code} transmission zone"
    transmission_zone = GenericAsset.query.filter(GenericAsset.name == ga_name).one_or_none()
    if not transmission_zone:
        current_app.logger.info(f"Adding {ga_name} ...")
        transmission_zone = GenericAsset(
            name=ga_name,
            generic_asset_type=transmission_zone_type,
            account_id=None  # public
        )
    for sensor_name, unit in (("Overall generation", "MWh"), ("Solar", "MWh"), ("Onshore wind", "MWh"), ("Offshore wind", "MWh"), ("CO2", "kg/h")):
        sensor = Sensor.query.filter(Sensor.name == sensor_name).one_or_none()
        if not sensor:
            current_app.logger.info(f"Adding sensor {sensor_name} ...")
            sensor = Sensor(
                name=sensor_name,
                unit=unit,
                generic_asset=transmission_zone,
                timezone=timezone,
                event_resolution=timedelta(hours=1),
                knowledge_horizon=(knowledge_horizons.x_days_ago_at_y_oclock, dict(x=1, y=13, z="Europe/Amsterdam")),  # publishing time is 13:00
            )
            db.session.add(sensor)
    db.session.commit()
