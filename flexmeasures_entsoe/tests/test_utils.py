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
    tz_str = "Europe/Paris"

    # Case 1: Explicit inputs
    input_start = datetime(2025, 5, 1, 10, 0)
    input_end = datetime(2025, 5, 2, 10, 0)

    s, e = parse_from_and_to_dates(input_start, input_end, tz_str)
    assert str(s.tz) == tz_str
    assert s.hour == 10

    # Case 2: Defaults (None passed) -> Should default to Tomorrow
    s_def, e_def = parse_from_and_to_dates(
        from_time=None, until_time=None, country_timezone="UTC", default_to="tomorrow"
    )
    now = pd.Timestamp.now("UTC").normalize()

    assert s_def >= now + pd.Timedelta(days=1)
    assert e_def > s_def
