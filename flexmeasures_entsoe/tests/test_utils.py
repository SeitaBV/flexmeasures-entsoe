import pytz
import pytest
import pandas as pd
import click
from datetime import datetime

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
    today_midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)

    # Case 1: Explicit inputs

    input_start = datetime(2025, 5, 1)
    input_end = datetime(2025, 5, 2)

    s, e = parse_from_and_to_dates(
        from_time=input_start, until_time=input_end, country_timezone=tz_str
    )
    assert s.tzinfo.zone == tz.zone
    assert (
        e.normalize() - s.normalize()
    ).days == 2  # increases by one because of date_range_to_time_range util

    # Case 2: default_to="tomorrow"
    s_def, e_def = parse_from_and_to_dates(
        from_time=None, until_time=None, country_timezone=tz_str, default_to="tomorrow"
    )

    assert (s_def.normalize() - today_midnight).days == 1
    assert e_def > s_def

    # Case 3: default_to="today-and-tomorrow"
    s_def2, e_def2 = parse_from_and_to_dates(
        from_time=None, until_time=None, country_timezone=tz_str
    )

    assert (s_def2.normalize() - today_midnight).days == 0
    assert e_def2 > s_def2
