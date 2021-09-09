from flask import current_app

from flexmeasures.data.models.data_sources import DataSource
from flexmeasures.data.config import db


def ensure_data_source() -> DataSource:
    entsoe_data_source = DataSource.query.filter(
        DataSource.name == "ENTSO-E"
    ).one_or_none()
    if not entsoe_data_source:
        current_app.logger.info("Adding ENTSO-E data source ...")
        entsoe_data_source = DataSource(name="ENTSO-E", type="forecasting script")
        db.session.add(entsoe_data_source)
    return entsoe_data_source
