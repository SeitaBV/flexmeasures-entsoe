"""Automation support for importing ENTSO-E day-ahead prices."""

import pandas as pd
from flask import current_app
from marshmallow import fields, Schema, validate

from flexmeasures.data.models.data_sources import DataGenerator
from flexmeasures.data.schemas import SensorIdField

from .. import DEFAULT_DATA_SOURCE_NAME
from ..utils import (
    create_entsoe_client,
    drop_data_at_or_after,
    ensure_country_code_and_timezone,
    entsoe_series_to_beliefs,
    parse_from_and_to_dates,
    resample_if_needed,
    start_import_log,
)


class EntsoeDayAheadPriceConfigSchema(Schema):
    sensor = SensorIdField(required=True)
    country_code = fields.String(
        data_key="country-code", load_default=None, allow_none=True
    )
    country_timezone = fields.String(
        data_key="country-timezone", load_default=None, allow_none=True
    )


class EntsoeDayAheadPriceParametersSchema(Schema):
    import_range = fields.String(
        data_key="import-range",
        load_default="today-and-tomorrow",
        validate=validate.OneOf(["today", "tomorrow", "today-and-tomorrow"]),
    )
    fail_on_incomplete_data = fields.Boolean(
        data_key="fail-on-incomplete-data", load_default=False
    )


class EntsoeDayAheadPriceImporter(DataGenerator):
    """Fetch recurring day-ahead prices from ENTSO-E for one output sensor."""

    __data_generator_base__ = "market"
    __version__ = "0.10"
    _config_schema = EntsoeDayAheadPriceConfigSchema()
    _parameters_schema = EntsoeDayAheadPriceParametersSchema()

    @classmethod
    def get_data_source_info(cls) -> dict:
        """Name the automation's source after ENTSO-E, as the CLI import names its source.

        Without this, the source is named after FLEXMEASURES_DEFAULT_DATASOURCE ("FlexMeasures" by default),
        so moving a sensor from the CLI import to the automation would change the name of the source its prices are recorded under.
        """
        source_info = super().get_data_source_info()
        source_info["source"] = current_app.config.get(
            "ENTSOE_DATA_SOURCE_NAME", DEFAULT_DATA_SOURCE_NAME
        )
        return source_info

    @property
    def output_sensors(self) -> list:
        return self._resolve_sensors(self._config["sensor"])

    def _compute(
        self,
        import_range: str = "today-and-tomorrow",
        fail_on_incomplete_data: bool = False,
    ) -> list[dict]:
        pricing_sensor = self._config["sensor"]
        country_code, country_timezone = ensure_country_code_and_timezone(
            self._config.get("country_code"),
            self._config.get("country_timezone"),
        )
        from_time, until_time = parse_from_and_to_dates(
            None, None, country_timezone, default_to=import_range
        )
        client = create_entsoe_client()
        log, now = start_import_log(
            "day-ahead price",
            from_time,
            until_time,
            country_code,
            country_timezone,
        )
        log.info("Getting prices ...")
        prices: pd.Series = client.query_day_ahead_prices(
            country_code, start=from_time, end=until_time
        )
        prices = drop_data_at_or_after(prices, until_time=until_time)
        if prices.empty:
            raise ValueError(
                "ENTSO-E returned no day-ahead prices for the requested period."
            )
        if fail_on_incomplete_data:
            expected_periods = int(
                (until_time - from_time) / pricing_sensor.event_resolution
            )
            if len(prices) < expected_periods:
                raise ValueError(
                    "ENTSO-E returned incomplete day-ahead prices: "
                    f"expected {expected_periods} periods but got {len(prices)}."
                )
        prices = resample_if_needed(prices, pricing_sensor)
        log.debug("Prices: \n%s", prices)
        beliefs = entsoe_series_to_beliefs(
            prices,
            pricing_sensor,
            self.data_source,
            country_timezone,
            now,
        )
        return [{"sensor": pricing_sensor, "data": beliefs}]
