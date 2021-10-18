from typing import List
from datetime import timedelta
from flask import current_app
import pandas as pd

from flexmeasures.data.models.generic_assets import GenericAsset, GenericAssetType
from flexmeasures.data.models.time_series import Sensor
from flexmeasures.data.config import db

from .. import DEFAULT_COUNTRY_CODE, DEFAULT_COUNTRY_TIMEZONE  # noqa: E402


# sensor_name, unit, data sourced directly by ENTSO-E or not (i.e. derived)
generation_sensors = (
    ("Scheduled generation", "MWh", True),
    ("Solar", "MWh", True),
    ("Onshore wind", "MWh", True),
    ("Offshore wind", "MWh", True),
    ("CO2 intensity", "kg/MWh", False),
)


def determine_net_emission_factors(shares: pd.DataFrame) -> pd.Series:
    """Given production shares, determine the net emission factors.

    Use column headers that match production types listed below.
    Use any index.

    For example:

        print(shares)

              fossil_gas     other  fossil_hard_coal     waste   nuclear
        hour
        0       0.443685  0.206033          0.237596  0.050915  0.059455
        1       0.443910  0.205065          0.235022  0.052614  0.060987

        print(determine_net_emission_factors(shares))

        hour
        0     644.753221
        1     641.410093
        Name: Average emissions from Dutch electricity production (kg CO₂ eq/MWh), dtype: float64
    """
    emission_factors = dict(
        biomass=50.4,
        fossil_brown_coal_or_lignite=None,  # unknown
        fossil_coal_derived_gas=None,  # unknown
        fossil_gas=464,
        fossil_hard_coal=1030,
        fossil_oil=1010,
        fossil_oil_shale=None,  # unknown
        fossil_peat=None,  # unknown
        geothermal=0.00664,
        hydro_pumped_storage=611,
        hydro_run_of_river_and_poundage=0.0253,
        hydro_water_reservoir=8.13,
        marine=None,  # unknown
        nuclear=10.1,
        other=927,  # for EU28
        other_renewable=None,  # unknown
        solar=0.00591,
        waste=None,  # unknown
        wind_offshore=0.133,
        wind_onshore=0.133,
    )  # supplementary material from "Real-time carbon accounting method for the European electricity markets, Tranberg et al. (2019)"
    # todo: substitute placeholder for unknown emission factor of waste
    emission_factors["waste"] = emission_factors["biomass"]
    for production_type in shares.columns:
        shares[production_type] = (
            shares[production_type] * emission_factors[production_type]
        )
    return shares.sum(axis=1).rename(
        "Average emissions from Dutch electricity production (kg CO₂ eq/MWh)"
    )


def ensure_generation_sensors() -> List[Sensor]:
    """
    Ensure a GenericAsset exists to model the transmission zone for which this plugin gathers
    generation data, plus sensors for relevant data we collect.
    """
    sensors = []
    country_code = current_app.config.get("ENTSOE_COUNTRY_CODE", DEFAULT_COUNTRY_CODE)
    timezone = current_app.config.get(
        "ENTSOE_COUNTRY_TIMEZONE", DEFAULT_COUNTRY_TIMEZONE
    )

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
    for sensor_name, unit, data_by_entsoe in generation_sensors:
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
        sensor.data_by_entsoe = data_by_entsoe
        sensors.append(sensor)
    db.session.commit()
    return sensors
