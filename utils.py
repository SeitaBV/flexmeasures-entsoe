from flask import current_app

from flexmeasures.data.models.data_sources import DataSource
from flexmeasures.data.utils import get_data_source

from . import DEFAULT_DERIVED_DATA_SOURCE


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
