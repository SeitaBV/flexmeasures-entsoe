from datetime import datetime, timedelta
from types import SimpleNamespace

import click
import pandas as pd
import pytz
import pytest

from flexmeasures_entsoe import DEFAULT_DATA_SOURCE_NAME, DEFAULT_DERIVED_DATA_SOURCE
from flexmeasures_entsoe.utils import (
    FM_SUPPORTS_ACCOUNT_LINKED_SOURCES,
    _ensure_entsoe_source,
    abort_if_data_incomplete,
    ensure_data_source,
    ensure_data_source_for_derived_data,
    parse_from_and_to_dates,
)


def test_abort_if_data_incomplete():
    """
    Tests that the function raises click.Abort if data is incomplete.
    1. Data is complete: No exception raised.
    2. Data is incomplete: click.Abort is raised.
    """
    start = pd.Timestamp("2025-01-01 00:00")
    end = pd.Timestamp("2025-01-02 00:00")
    resolution = pd.Timedelta(hours=1)

    # Case 1: Data is complete (24 items for 24 hours)
    complete_data = pd.DataFrame({"val": range(24)})
    try:
        abort_if_data_incomplete(complete_data, start, end, resolution)
    except click.Abort:
        pytest.fail("Function raised Abort unexpectedly on complete data")

    # Case 2: Data is incomplete (20 items for 24 hours)
    incomplete_data = pd.DataFrame({"val": range(20)})
    with pytest.raises(click.Abort):
        abort_if_data_incomplete(incomplete_data, start, end, resolution)


def test_parse_from_and_to_dates():
    """
    Tests CLI date parsing logic:
    1. Explicit dates are timezone-localized correctly.
    2. 'None' defaults to tomorrow (start of day) -> day after tomorrow.
    """
    tz_str = "UTC"
    tz = pytz.timezone(tz_str)
    now = datetime.now(tz)
    today = datetime(now.year, now.month, now.day, tzinfo=tz)

    # Case 1: Explicit inputs
    input_start = datetime(2025, 5, 1)
    input_end = datetime(2025, 5, 2)

    s, e = parse_from_and_to_dates(
        from_date=input_start, until_date=input_end, country_timezone=tz_str
    )

    assert s.tzinfo.zone == tz.zone
    assert (e - s) == timedelta(days=2)
    assert e == datetime(2025, 5, 3, tzinfo=tz)

    # Case 2: default_to="tomorrow"
    s_tom, e_tom = parse_from_and_to_dates(
        from_date=None, until_date=None, country_timezone=tz_str, default_to="tomorrow"
    )

    assert e_tom - s_tom == timedelta(days=1)
    assert s_tom == today + timedelta(days=1)
    assert e_tom == today + timedelta(days=2)

    # Case 3: default_to="today-and-tomorrow"
    s_tod, e_tod = parse_from_and_to_dates(
        from_date=None, until_date=None, country_timezone=tz_str
    )

    assert e_tod - s_tod == timedelta(days=2)
    assert s_tod == today
    assert e_tod == today + timedelta(days=2)

    # Case 4: only providing until_date (today midnight == start of tomorrow), while start comes from "today-and-tomorrow"
    today_midnight = datetime(now.year, now.month, now.day) + timedelta(days=1)
    s_none, e_none = parse_from_and_to_dates(
        from_date=None, until_date=today_midnight, country_timezone=tz_str
    )

    assert e_none - s_none == timedelta(days=2)
    assert s_none == today
    assert e_none == today + timedelta(days=2)


# The version-branch tests below still use monkeypatching to isolate source
# creation side effects and to simulate upgrade reuse of legacy ENTSO-E
# sources without requiring multiple FlexMeasures installs in one test run.
@pytest.mark.skipif(
    not FM_SUPPORTS_ACCOUNT_LINKED_SOURCES,
    reason="Account-linked ENTSO-E sources are only supported on FlexMeasures >= 0.32.",
)
def test_ensure_data_source_passes_entsoe_account_when_supported(monkeypatch):
    """Test that ensure_data_source() creates a market-type source and passes the ENTSO-E account."""
    from flask import Flask

    app = Flask(__name__)
    captured_kwargs = {}

    def fake_get_or_create_source(source, source_type, account, flush):
        captured_kwargs.update(
            dict(
                source=source,
                source_type=source_type,
                account=account,
                flush=flush,
            )
        )
        return SimpleNamespace(type=source_type, account=account, name=source)

    fake_account = SimpleNamespace(name=DEFAULT_DATA_SOURCE_NAME)

    monkeypatch.setattr(
        "flexmeasures_entsoe.utils.get_or_create_source",
        fake_get_or_create_source,
    )
    monkeypatch.setattr(
        "flexmeasures_entsoe.utils._find_existing_source",
        lambda source_name, source_type: None,
    )
    monkeypatch.setattr(
        "flexmeasures_entsoe.utils.get_or_create_entsoe_account",
        lambda: fake_account,
    )

    with app.app_context():
        data_source = ensure_data_source()

    assert data_source.type == "market"
    assert captured_kwargs["source"] == DEFAULT_DATA_SOURCE_NAME
    assert captured_kwargs["account"].name == DEFAULT_DATA_SOURCE_NAME


@pytest.mark.skipif(
    not FM_SUPPORTS_ACCOUNT_LINKED_SOURCES,
    reason="Account-linked ENTSO-E sources are only supported on FlexMeasures >= 0.32.",
)
def test_ensure_data_source_for_derived_data_passes_entsoe_account_when_supported(
    monkeypatch,
):
    """Test that ensure_data_source_for_derived_data() passes the ENTSO-E account."""
    from flask import Flask

    app = Flask(__name__)
    captured_kwargs = {}

    def fake_get_or_create_source(source, source_type, account, flush):
        captured_kwargs.update(
            dict(
                source=source,
                source_type=source_type,
                account=account,
                flush=flush,
            )
        )
        return SimpleNamespace(type=source_type, account=account, name=source)

    fake_account = SimpleNamespace(name=DEFAULT_DATA_SOURCE_NAME)

    monkeypatch.setattr(
        "flexmeasures_entsoe.utils.get_or_create_source",
        fake_get_or_create_source,
    )
    monkeypatch.setattr(
        "flexmeasures_entsoe.utils._find_existing_source",
        lambda source_name, source_type: None,
    )
    monkeypatch.setattr(
        "flexmeasures_entsoe.utils.get_or_create_entsoe_account",
        lambda: fake_account,
    )

    with app.app_context():
        data_source = ensure_data_source_for_derived_data()

    assert data_source.type == "forecasting script"
    assert captured_kwargs["source"] == DEFAULT_DERIVED_DATA_SOURCE
    assert captured_kwargs["account"].name == DEFAULT_DATA_SOURCE_NAME


@pytest.mark.skipif(
    FM_SUPPORTS_ACCOUNT_LINKED_SOURCES,
    reason="Legacy get_data_source fallback is only used on FlexMeasures < 0.32.",
)
def test_ensure_data_source_omits_account_when_not_supported(monkeypatch):
    """Test that ensure_data_source() falls back to the legacy source factory without an account."""
    from flask import Flask

    app = Flask(__name__)
    captured_kwargs = {}

    def fake_get_data_source(data_source_name, data_source_type):
        captured_kwargs.update(
            data_source_name=data_source_name,
            data_source_type=data_source_type,
        )
        return SimpleNamespace(name=data_source_name, type=data_source_type)

    monkeypatch.setattr(
        "flexmeasures_entsoe.utils._find_existing_source",
        lambda source_name, source_type: None,
    )
    monkeypatch.setattr(
        "flexmeasures_entsoe.utils.get_data_source",
        fake_get_data_source,
    )

    with app.app_context():
        data_source = ensure_data_source()

    assert data_source.type == "market"
    assert captured_kwargs == {
        "data_source_name": DEFAULT_DATA_SOURCE_NAME,
        "data_source_type": "market",
    }


@pytest.mark.skipif(
    FM_SUPPORTS_ACCOUNT_LINKED_SOURCES,
    reason="Legacy get_data_source fallback is only used on FlexMeasures < 0.32.",
)
def test_ensure_data_source_for_derived_data_omits_account_when_not_supported(
    monkeypatch,
):
    """Test that ensure_data_source_for_derived_data() falls back to the legacy source factory."""
    from flask import Flask

    app = Flask(__name__)
    captured_kwargs = {}

    def fake_get_data_source(data_source_name, data_source_type):
        captured_kwargs.update(
            data_source_name=data_source_name,
            data_source_type=data_source_type,
        )
        return SimpleNamespace(name=data_source_name, type=data_source_type)

    monkeypatch.setattr(
        "flexmeasures_entsoe.utils._find_existing_source",
        lambda source_name, source_type: None,
    )
    monkeypatch.setattr(
        "flexmeasures_entsoe.utils.get_data_source",
        fake_get_data_source,
    )

    with app.app_context():
        data_source = ensure_data_source_for_derived_data()

    assert data_source.type == "forecasting script"
    assert captured_kwargs == {
        "data_source_name": DEFAULT_DERIVED_DATA_SOURCE,
        "data_source_type": "forecasting script",
    }


@pytest.mark.skipif(
    not FM_SUPPORTS_ACCOUNT_LINKED_SOURCES,
    reason="Legacy source upgrade reuse matters in the account-linked source path only.",
)
def test_ensure_entsoe_source_reuses_legacy_source_and_sets_account(monkeypatch):
    legacy_source = SimpleNamespace(
        name=DEFAULT_DATA_SOURCE_NAME,
        type="forecasting script",
        account=None,
    )
    fake_account = SimpleNamespace(name=DEFAULT_DATA_SOURCE_NAME)

    def fake_find_existing_source(source_name, source_type):
        if source_type == "market":
            return None
        if source_type == "forecasting script":
            return legacy_source
        return None

    monkeypatch.setattr(
        "flexmeasures_entsoe.utils._find_existing_source",
        fake_find_existing_source,
    )
    monkeypatch.setattr(
        "flexmeasures_entsoe.utils.get_or_create_entsoe_account",
        lambda: fake_account,
    )
    monkeypatch.setattr(
        "flexmeasures_entsoe.utils.get_or_create_source",
        lambda **kwargs: pytest.fail(
            "Should reuse a legacy ENTSO-E source before creating a new one."
        ),
    )

    data_source = _ensure_entsoe_source(
        source_name=DEFAULT_DATA_SOURCE_NAME,
        source_type="market",
        legacy_source_type="forecasting script",
    )

    assert data_source is legacy_source
    assert data_source.type == "market"
    assert data_source.account is fake_account
