import pytz
import pytest
import pandas as pd
import click
from datetime import datetime, timedelta

from flexmeasures_entsoe.utils import (
    abort_if_data_incomplete,
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
