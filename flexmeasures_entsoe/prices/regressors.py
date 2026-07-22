"""
Extra regressor series that improve day-ahead electricity-price forecasting.

Research on the Dutch (NL) day-ahead market established that ENTSO-E day-ahead
*fundamentals* dramatically improve price forecasting, especially for hard cases
(public holidays and the solar-driven midday price collapse):

- **Residual load** (= day-ahead load forecast − day-ahead wind & solar forecast) is
  the single best regressor. Because the day-ahead *price* is formed against these very
  forecasts, residual load flattens the spurious holiday-morning price peak and captures
  the midday collapse.
- **Generation outages** (day-ahead-known unavailable capacity) are a weaker, but
  physically relevant, regressor for scarcity.

This module only holds the *pure* helpers (outage aggregation and residual-load
derivation) and the sensor specifications, so it can be imported and unit-tested without
a FlexMeasures app or database. The orchestration (querying ENTSO-E and saving beliefs)
lives in ``day_ahead.py``.
"""

from datetime import timedelta
from typing import Optional

import pandas as pd


# Machine-readable choices for the CLI and the names of the sensors each regressor
# writes to. Wind & solar reuse the sensor names already created by the generation
# import command, so those series are not duplicated.
LOAD_FORECAST_REGRESSOR = "load-forecast"
WIND_SOLAR_FORECAST_REGRESSOR = "wind-solar-forecast"
RESIDUAL_LOAD_REGRESSOR = "residual-load"
OUTAGES_REGRESSOR = "outages"

# sensor_name, unit, event_resolution, data sourced directly by ENTSO-E (True) or derived (False)
LOAD_FORECAST_SENSOR_SPEC = ("Day-ahead load forecast", "MW", timedelta(hours=1), True)
WIND_SOLAR_SENSOR_SPECS = (
    ("Solar", "MW", timedelta(hours=1), True),
    ("Wind Onshore", "MW", timedelta(hours=1), True),
    ("Wind Offshore", "MW", timedelta(hours=1), True),
)
RESIDUAL_LOAD_SENSOR_SPEC = ("Residual load", "MW", timedelta(hours=1), False)
OUTAGES_SENSOR_SPEC = ("Generation outages", "MW", timedelta(hours=1), True)

# The three green columns we subtract from the load forecast to obtain residual load.
GREEN_COLUMNS = ("Solar", "Wind Offshore", "Wind Onshore")


def compute_residual_load(
    load_forecast: pd.Series,
    solar: pd.Series,
    wind_offshore: pd.Series,
    wind_onshore: pd.Series,
) -> pd.Series:
    """Derive residual load from a day-ahead load forecast and green forecasts.

    residual load = load forecast − solar − wind offshore − wind onshore

    All inputs should be (hourly) MW series; they are aligned on their datetime index
    before subtracting, so missing green components simply do not lower the residual.
    Residual load is the strongest single regressor for NL day-ahead prices: the price is
    set against these very forecasts, so residual load tracks the scarcity that drives it
    (and flattens the spurious holiday-morning peak while capturing the midday collapse).
    """
    frame = pd.concat(
        {
            "load": load_forecast,
            "solar": solar,
            "wind_offshore": wind_offshore,
            "wind_onshore": wind_onshore,
        },
        axis="columns",
    )
    residual = (
        frame["load"]
        - frame["solar"].fillna(0.0)
        - frame["wind_offshore"].fillna(0.0)
        - frame["wind_onshore"].fillna(0.0)
    )
    return residual.rename("Residual load")


def aggregate_outages_to_hourly_unavailable_mw(
    outages: pd.DataFrame,
    from_time: pd.Timestamp,
    until_time: pd.Timestamp,
) -> pd.Series:
    """Aggregate generation-unit outages into an hourly series of total unavailable MW.

    ``outages`` is the DataFrame returned by
    ``EntsoePandasClient.query_unavailability_of_generation_units``: it is indexed by the
    outage's publication time (``created_doc_time``) and has (at least) the columns
    ``nominal_power``, ``start``, ``end`` and ``avail_qty``. Each row describes one
    available-period of an outage; the unavailable capacity during that period is
    ``nominal_power − avail_qty`` MW.

    For every target hour in ``[from_time, until_time)`` we sum the unavailable capacity
    of every outage period that overlaps that hour.

    Knowledge-horizon correctness (IMPORTANT, avoids a look-ahead leak)
    ------------------------------------------------------------------
    Day-ahead prices are formed with only the information published *before* the delivery
    day. We therefore count an outage for a target hour **only if its publication time
    (``created_doc_time``) is strictly before the start of that hour's delivery day**
    (local midnight). An outage announced on or after the delivery day was not known when
    the day-ahead auction cleared, so feeding it into the regressor would leak future
    information and inflate the apparent forecasting skill. Because a single outage's
    publication time is compared against each target hour's own delivery day, the same
    raw outage table can be aggregated once for a multi-day range without leaking.

    Returns an hourly ``pd.Series`` (0.0 where nothing applies) indexed at the start of
    each hour, in the timezone of ``from_time``.
    """
    hours = pd.date_range(
        start=from_time.floor("h"),
        end=until_time,
        freq="1h",
        inclusive="left",
    )
    unavailable_mw = pd.Series(0.0, index=hours, name="Generation outages")
    if outages is None or outages.empty:
        return unavailable_mw

    # 'created_doc_time' is the index of the ENTSO-E outages DataFrame.
    rows = outages.reset_index()
    for _, row in rows.iterrows():
        created: pd.Timestamp = row["created_doc_time"]
        outage_start: pd.Timestamp = row["start"]
        outage_end: pd.Timestamp = row["end"]
        nominal_power = row["nominal_power"]
        avail_qty = row["avail_qty"]

        if pd.isna(nominal_power):
            # Without a nominal power we cannot quantify the outage.
            continue
        if pd.isna(avail_qty):
            # A void available quantity means the unit is fully unavailable.
            avail_qty = 0.0
        unavailable = float(nominal_power) - float(avail_qty)
        if unavailable <= 0:
            continue

        for hour_start in hours:
            delivery_day = hour_start.normalize()  # local midnight of the delivery day
            if created >= delivery_day:
                # Not known before the delivery day -> would be a look-ahead leak.
                continue
            hour_end = hour_start + pd.Timedelta(hours=1)
            if outage_start < hour_end and outage_end > hour_start:
                unavailable_mw[hour_start] += unavailable
    return unavailable_mw


def load_series_from_forecast(load_forecast: pd.DataFrame) -> pd.Series:
    """Extract the forecasted-load series from ENTSO-E's load-forecast DataFrame."""
    if "Forecasted Load" in load_forecast.columns:
        return load_forecast["Forecasted Load"]
    return load_forecast.iloc[:, 0]


def green_column(green_forecast: pd.DataFrame, column: str) -> Optional[pd.Series]:
    """Return a green-forecast column if ENTSO-E reported it for this country, else None."""
    if column in green_forecast.columns:
        return green_forecast[column]
    return None
