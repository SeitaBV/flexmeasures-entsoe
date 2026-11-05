"""Registration and schema tests for the ENTSO-E price automation."""

from datetime import datetime, timedelta, timezone

from marshmallow import ValidationError
import pandas as pd
import pytest

pytest.importorskip("flexmeasures.data.automations")

from flexmeasures_entsoe import __automation_types__  # noqa: E402
from flexmeasures_entsoe.prices.automation import (  # noqa: E402
    EntsoeDayAheadPriceConfigSchema,
    EntsoeDayAheadPriceImporter,
    EntsoeDayAheadPriceParametersSchema,
)
from flexmeasures_entsoe.prices import automation as automation_module  # noqa: E402


class FakeSensor:
    id = 42
    name = "Day-ahead prices"
    event_resolution = timedelta(minutes=15)


class FakeLog:
    def info(self, *args):
        pass

    def debug(self, *args):
        pass


class FakeClient:
    def __init__(self, prices):
        self.prices = prices
        self.calls = []

    def query_day_ahead_prices(self, country_code, start, end):
        self.calls.append((country_code, start, end))
        return self.prices


def make_generator():
    generator = object.__new__(EntsoeDayAheadPriceImporter)
    generator._config = {
        "sensor": FakeSensor(),
        "country_code": "NL",
        "country_timezone": "Europe/Amsterdam",
    }
    generator._data_source = "automation source"
    return generator


def prepare_compute(monkeypatch, prices):
    start = pd.Timestamp("2026-09-24", tz="Europe/Amsterdam")
    end = start + pd.Timedelta(hours=1)
    now = datetime(2026, 9, 23, 18, tzinfo=timezone.utc)
    client = FakeClient(prices)
    monkeypatch.setattr(
        automation_module,
        "parse_from_and_to_dates",
        lambda *args, **kwargs: (start, end),
    )
    monkeypatch.setattr(automation_module, "create_entsoe_client", lambda: client)
    monkeypatch.setattr(
        automation_module, "start_import_log", lambda *args: (FakeLog(), now)
    )
    monkeypatch.setattr(
        automation_module, "resample_if_needed", lambda series, sensor: series
    )
    monkeypatch.setattr(
        automation_module,
        "entsoe_series_to_beliefs",
        lambda *args: "beliefs",
    )
    return client, start, end


def test_day_ahead_price_automation_registration():
    handler = __automation_types__[0]

    assert handler.type_id == "entsoe-day-ahead-prices"
    assert handler.generator_class is EntsoeDayAheadPriceImporter
    assert handler.queue == "ingestion"


def test_automation_fields_use_wire_safe_names():
    config_fields = EntsoeDayAheadPriceConfigSchema().fields
    parameter_fields = EntsoeDayAheadPriceParametersSchema().fields

    assert config_fields["country_code"].data_key == "country-code"
    assert config_fields["country_timezone"].data_key == "country-timezone"
    assert parameter_fields["import_range"].data_key == "import-range"
    assert (
        parameter_fields["fail_on_incomplete_data"].data_key
        == "fail-on-incomplete-data"
    )


def test_automation_parameter_defaults_and_validation():
    schema = EntsoeDayAheadPriceParametersSchema()

    assert schema.load({}) == {
        "import_range": "today-and-tomorrow",
        "fail_on_incomplete_data": False,
    }
    with pytest.raises(ValidationError):
        schema.load({"import-range": "next-week"})


def test_compute_queries_prices_and_returns_declared_sensor(monkeypatch):
    index = pd.date_range("2026-09-24", periods=4, freq="15min", tz="Europe/Amsterdam")
    prices = pd.Series([80.0, 81.0, 82.0, 83.0], index=index)
    client, start, end = prepare_compute(monkeypatch, prices)
    generator = make_generator()

    results = generator._compute(import_range="tomorrow")

    assert client.calls == [("NL", start, end)]
    assert results == [{"sensor": generator._config["sensor"], "data": "beliefs"}]


def test_compute_rejects_incomplete_prices_in_strict_mode(monkeypatch):
    index = pd.date_range("2026-09-24", periods=3, freq="15min", tz="Europe/Amsterdam")
    prices = pd.Series([80.0, 81.0, 82.0], index=index)
    prepare_compute(monkeypatch, prices)

    with pytest.raises(ValueError, match="expected 4 periods but got 3"):
        make_generator()._compute(fail_on_incomplete_data=True)


def test_compute_leaves_out_the_price_at_the_end_of_the_period(monkeypatch):
    """entsoe-py includes the end of the queried period, which the import excludes."""
    index = pd.date_range("2026-09-24", periods=5, freq="15min", tz="Europe/Amsterdam")
    prices = pd.Series([80.0, 81.0, 82.0, 83.0, 84.0], index=index)
    _, _, end = prepare_compute(monkeypatch, prices)
    converted = []
    monkeypatch.setattr(
        automation_module,
        "entsoe_series_to_beliefs",
        lambda series, *args: converted.append(series),
    )

    make_generator()._compute()

    assert converted[0].index[-1] == end - timedelta(minutes=15)
    assert len(converted[0]) == 4


def test_strict_mode_does_not_count_the_price_at_the_end_of_the_period(monkeypatch):
    index = pd.date_range("2026-09-24", periods=4, freq="15min", tz="Europe/Amsterdam")
    prices = pd.Series([80.0, 81.0, 82.0, 83.0], index=index).drop(index[1])
    prices.loc[index[-1] + pd.Timedelta(minutes=15)] = 84.0
    prepare_compute(monkeypatch, prices)

    with pytest.raises(ValueError, match="expected 4 periods but got 3"):
        make_generator()._compute(fail_on_incomplete_data=True)
