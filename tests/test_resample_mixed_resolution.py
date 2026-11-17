"""
Tests for ``resample_if_needed`` on series that mix resolutions, as ENTSO-E day-ahead prices do
around 2025-10-01 (hourly before, 15-minute after).

``flexmeasures`` must be importable for these; if it is not installed, the module is skipped.
"""

import numpy as np
import pandas as pd
from flask import Flask
import pytest

pytest.importorskip("flexmeasures")

from flexmeasures_entsoe.utils import resample_if_needed  # noqa: E402

TZ = "Europe/Amsterdam"
HOUR = pd.Timedelta(hours=1)
QUARTER = pd.Timedelta(minutes=15)


@pytest.fixture(autouse=True)
def app_context():
    with Flask("test").app_context():
        yield


class FakeSensor:
    def __init__(self, name, event_resolution):
        self.name = name
        self.event_resolution = event_resolution


def make_series(*parts):
    """Concatenate (start, end_inclusive, freq) ranges into a series with distinct values."""
    index = pd.date_range(parts[0][0], parts[0][1], freq=parts[0][2], tz=TZ)
    for start, end, freq in parts[1:]:
        index = index.append(pd.date_range(start, end, freq=freq, tz=TZ))
    return pd.Series(np.arange(1, len(index) + 1, dtype=float), index=index)


def hourly_then_quarterly(hourly_days=1):
    first = pd.Timestamp("2025-10-01") - pd.Timedelta(days=hourly_days)
    return make_series(
        (first, "2025-09-30 23:00", "1h"),
        ("2025-10-01 00:00", "2025-10-01 23:45", "15min"),
    )


def test_hourly_then_quarterly_on_quarterly_sensor():
    s = hourly_then_quarterly()
    out = resample_if_needed(s, FakeSensor("price", QUARTER))
    assert len(out) == 192
    assert out.index[-1] == pd.Timestamp("2025-10-01 23:45", tz=TZ)
    assert (out.index.to_series().diff().dropna() == QUARTER).all()
    hourly = s[:24]
    for t, v in hourly.items():
        assert (out[t : t + 3 * QUARTER] == v).all()
    quarterly = s[24:]
    assert (out[quarterly.index] == quarterly).all()


def test_hourly_majority_on_quarterly_sensor_has_no_trailing_rows():
    s = hourly_then_quarterly(hourly_days=5)
    out = resample_if_needed(s, FakeSensor("price", QUARTER))
    assert len(out) == 576
    assert out.index[-1] == pd.Timestamp("2025-10-01 23:45", tz=TZ)
    assert (out.index.to_series().diff().dropna() == QUARTER).all()


@pytest.mark.parametrize("hourly_days", [1, 5])
def test_mixed_on_hourly_sensor(hourly_days):
    s = hourly_then_quarterly(hourly_days)
    out = resample_if_needed(s, FakeSensor("price", HOUR))
    assert len(out) == 24 * (hourly_days + 1)
    n_hourly = 24 * hourly_days
    hourly = s[:n_hourly]
    assert (out[hourly.index] == hourly).all()
    quarterly = s[n_hourly:]
    expected = quarterly.resample("1h").mean()
    assert (out[expected.index] == expected).all()


def test_missing_observations_stay_missing():
    s = hourly_then_quarterly()
    missing_hour = s.index[5]
    missing_quarter = s.index[24 + 10]
    s = s.drop([missing_hour, missing_quarter])
    out = resample_if_needed(s, FakeSensor("price", QUARTER))
    assert len(out) == 192 - 4 - 1
    for k in range(4):
        assert missing_hour + k * QUARTER not in out.index
    assert missing_quarter not in out.index
    t = s.index[4]  # hour before the missing one
    assert (out[t : t + 3 * QUARTER] == s[t]).all()
    t = s.index[5]  # hour after the missing one
    assert (out[t : t + 3 * QUARTER] == s[t]).all()
    assert out[missing_quarter - QUARTER] == s[missing_quarter - QUARTER]
    assert out[missing_quarter + QUARTER] == s[missing_quarter + QUARTER]


def test_dst_change_day():
    s = make_series(
        ("2025-10-26 00:00", "2025-10-26 23:00", "1h"),
        ("2025-10-27 00:00", "2025-10-27 23:45", "15min"),
    )
    assert len(s) == 25 + 96  # the 26th has 25 local hours
    out = resample_if_needed(s, FakeSensor("price", QUARTER))
    assert len(out) == 25 * 4 + 96
    assert (out.index.to_series().diff().dropna() == QUARTER).all()
    for t, v in s[:25].items():
        assert (out[t : t + 3 * QUARTER] == v).all()
    assert (out[s.index[25:]] == s[25:]).all()


def test_gappy_single_resolution_is_unchanged():
    s = make_series(("2025-06-02 00:00", "2025-06-02 23:00", "1h"))
    s = s.drop(s.index[[3, 4, 10]])
    out = resample_if_needed(s, FakeSensor("price", HOUR))
    pd.testing.assert_series_equal(out, s)


def test_repeated_short_gaps_are_not_a_resolution_hourly_sensor():
    hourly = pd.date_range("2025-06-01 00:00", "2025-06-01 05:00", freq="1h", tz=TZ)
    gaps = pd.date_range("2025-06-01 08:00", periods=6, freq="2h", tz=TZ)  # < 12 h
    more = pd.date_range("2025-06-01 20:00", "2025-06-02 02:00", freq="1h", tz=TZ)
    index = hourly.append(gaps).append(more)
    s = pd.Series(np.arange(1, len(index) + 1, dtype=float), index=index)
    out = resample_if_needed(s, FakeSensor("price", HOUR))
    pd.testing.assert_series_equal(out, s)


def test_alternating_missing_quarters_are_not_a_resolution():
    full = pd.date_range("2025-06-01 00:00", "2025-06-01 03:45", freq="15min", tz=TZ)
    alternate = pd.date_range(
        "2025-06-01 04:00", "2025-06-01 08:00", freq="30min", tz=TZ
    )
    index = full.append(alternate)
    s = pd.Series(np.arange(1, len(index) + 1, dtype=float), index=index)
    out = resample_if_needed(s, FakeSensor("price", QUARTER))
    pd.testing.assert_series_equal(out, s)
    assert out.index[-1] == pd.Timestamp("2025-06-01 08:00", tz=TZ)
    assert pd.Timestamp("2025-06-01 04:15", tz=TZ) not in out.index


def test_only_two_hour_and_one_hour_spacings_are_not_mixed():
    two_hourly = pd.date_range(
        "2025-06-01 00:00", periods=6, freq="2h", tz=TZ
    )  # < 12 h
    hourly = pd.date_range("2025-06-01 11:00", "2025-06-01 16:00", freq="1h", tz=TZ)
    index = two_hourly.append(hourly)
    s = pd.Series(np.arange(1, len(index) + 1, dtype=float), index=index)
    out = resample_if_needed(s, FakeSensor("price", HOUR))
    pd.testing.assert_series_equal(out, s)
