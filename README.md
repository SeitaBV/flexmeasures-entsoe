# ENTSO-E forecasts & data

Importing data which can be relevant for energy flexibility services via ENTSO-E's API into FlexMeasures.

We start with data about the upcoming day.

- Generation forecasts for the upcoming day
- Based on these, CO2 content for the upcoming day
- Day-ahead prices


## Usage

Importing tomorrow's prices:

    flexmeasures entsoe import-day-ahead-prices

Importing tomorrow's generation (incl. CO2 estimated content):

    flexmeasures entsoe import-day-ahead-generation

Use ``--help`` to learn more usage details.


### Importing extra regressors for price forecasting

Research on the Dutch (NL) day-ahead market established that ENTSO-E day-ahead *fundamentals* dramatically improve day-ahead **price** forecasting, especially for hard cases (public holidays and the solar-driven midday price collapse):

- **Residual load** (= day-ahead load forecast − day-ahead wind & solar forecast) is the single best regressor. Because the day-ahead *price* is formed against these very forecasts, residual load flattens the spurious holiday-morning price peak and captures the midday collapse.
- **Generation outages** (day-ahead-known unavailable capacity) are a weaker, but physically relevant, regressor for scarcity.
- **Neighbouring countries' regressors** matter too: a €500+/MWh NL scarcity spike is usually a *regional* NW-European event (Germany, Belgium, Great Britain and the Nordics tight at the same time, so imports dry up). Neighbours' day-ahead residual-load forecasts measurably improve NL spike and evening-peak forecasts.

While importing day-ahead prices, you can *also* pull these regressor series, each into its own sensor (in MW). Choose what to import with the `--include-*` flags (all default to off):

    flexmeasures entsoe import-day-ahead-prices \
        --include-load-forecast \
        --include-wind-solar-forecast \
        --include-residual-load \
        --include-outages

The flags are independent, so you can import just what you need, e.g. only residual load:

    flexmeasures entsoe import-day-ahead-prices --include-residual-load

Or use `--include-all` as a shorthand for "import everything" (all regressors *and* neighbours):

    flexmeasures entsoe import-day-ahead-prices --include-all

| Flag | Sensor(s) created/used | Unit | Source |
| --- | --- | --- | --- |
| `--include-load-forecast` | `Day-ahead load forecast` | MW | ENTSO-E |
| `--include-wind-solar-forecast` | `Solar`, `Wind Onshore`, `Wind Offshore` (shared with the generation import) | MW | ENTSO-E |
| `--include-residual-load` | `Residual load` | MW | derived (`ENTSOE_DERIVED_DATA_SOURCE`) |
| `--include-outages` | `Generation outages` | MW | ENTSO-E |
| `--include-neighbours` | The selected regressors above (except outages), for each neighbour, on the neighbour's own transmission-zone asset | MW | ENTSO-E / derived |
| `--include-all` | Shorthand: turns on all of the above (every regressor **and** `--include-neighbours`) | MW | ENTSO-E / derived |

Notes:

- `--include-residual-load` implies fetching both the load forecast and the wind & solar forecast (it needs them to derive residual load), but it only *saves* those series if their own flag is also set.
- `--include-all` composes with the individual flags — setting it simply forces everything on.
- Each single-sensor regressor can be pointed at a specific existing sensor with `--load-forecast-sensor`, `--residual-load-sensor` and `--outages-sensor` (by sensor ID). Otherwise a sensibly-named sensor is created/looked up automatically.
- **Generation outages and the day-ahead knowledge horizon.** For each target hour we sum the unavailable capacity (`nominal_power − avail_qty`) of every outage overlapping that hour, but *only* if the outage was **published (`created_doc_time`) before that hour's delivery day** (local midnight). Day-ahead prices are set with only the information known before the delivery day, so counting an outage announced on or after the delivery day would leak look-ahead information into the regressor. The publication time is checked per target hour, so a multi-day range aggregates correctly without leaking.

#### Importing neighbours' regressors

Add `--include-neighbours` to *also* pull the selected regressors (all except outages, which stay domestic-only) for the price country's interconnected neighbours:

    flexmeasures entsoe import-day-ahead-prices \
        --include-residual-load \
        --include-neighbours

The neighbour set comes straight from ENTSO-E's own maintained mapping (`entsoe.mappings.NEIGHBOURS`), so it covers every country and tracks interconnection changes as the `entsoe-py` library is updated. For NL this yields `BE`, `DE_LU`, `DE_AT_LU`, `GB`, `NO_2` and `DK_1`.

Each neighbour's series are saved to **plainly-named** sensors (`Residual load`, `Day-ahead load forecast`, ...) that live on **that neighbour's own transmission-zone asset** and carry **that country's own timezone** (e.g. `Europe/London` for GB, `Europe/Oslo` for NO_2). So a country's identity comes from the asset, not from a name suffix: NL's `Residual load` and DE_LU's `Residual load` are distinct sensors on distinct assets.

- Neighbour data is fetched **leniently**: if a neighbour publishes no data for a series (e.g. `NO_2` / `DK_1` have little or no wind & solar forecast, or a deprecated zone like `DE_AT_LU` has no day-ahead data), that series is skipped with a warning instead of aborting the import — and residual load simply falls back to the plain load forecast there. No empty sensors are created for series with no data.
- Countries that ENTSO-E lists no neighbours for make `--include-neighbours` a harmless no-op.


### October 1st 2025 go-live for ENTSO-E moving to 15-minute day-ahead prices

ENTSO-E is moving from 1-hour day-ahead prices 15-minute day-ahead prices on October 1st 2025.
To prepare for this transition, you have two choices:

1. resample your existing price sensor in FlexMeasures from 1 hour to 15 minutes, or
2. get a new sensor for the 15-minute data.

If you do this *after* the go-live moment, the `flexmeasures-entsoe` package just keeps resampling the 15-minute ENTSO-E data to hourly data.

#### 1. Resampling

**The upside** of resampling your existing price data is that the sensor ID of your price sensor in FlexMeasures will remain the same.
Depending on your system setup, `Forecaster`/`Reporter`/`Scheduler` configurations (such as an asset's `flex-context`) may depend on it, and your users may expect the 15-minute data to live under the same sensor.

**The downside** is that it quadruples your data for that sensor, due to the fact that FlexMeasures only supports a fixed resolution for any given sensor. Although there should be no noticeable hit in performance, it obviously leads to redundant data in the price history before October 1st 2025.  

**To resample** your historical data, use:

```bash
flexmeasures edit resample-data --sensor <ID of your day-ahead price sensor> --event-resolution 15
```

The `flexmeasures-entsoe` package already automatically resamples the ENTSO-E data to the resolution of your sensor.

If you use a `Reporter` to derive retail prices or to compute energy costs, there is no need to update its configuration; just resample these sensors too, using the previous command (replacing the sensor ID as needed).
Alternatively, if you want to keep these sensors in their original resolution, and find that your reporters fail with an `AssertionError` about mismatched resolutions, you may need to add the `--resolution PT1H` option when using the `flexmeasures add report` command.

#### 2. Getting a new sensor

**The upside** is that this doesn't quadruple your historic data (see *the downside* of resampling, above).

**The downside** is that you may need to revise `Forecaster`/`Reporter`/`Scheduler` configurations (such as an asset's `flex-context`) and notify users (see *the upside* of resampling, above).

**To get a new sensor**, rename your existing *Day-ahead prices* sensor in the FlexMeasures UI.

The `flexmeasures-entsoe` package will then automatically create a new 15-minute price sensor the next time `flexmeasures entsoe import-day-ahead-prices` is run, assigning it a new sensor ID.

If you have any price or costs sensors using a `Reporter` to derive values from the day-ahead wholesale prices, update the sensor ID in the configuration of each `Reporter`.
Finally, either resample each derived sensor using:

```bash
flexmeasures edit resample-data --sensor <ID of your derivative sensor> --event-resolution 15
```

or, if you want to keep these sensors in their original resolution, and find that your reporters fail with an `AssertionError` about mismatched resolutions, you may need to add the `--resolution PT1H` option when using the `flexmeasures add report` command.

## Installation

First of all, this is a FlexMeasures plugin. Consult the FlexMeasures documentation for setup.

1. Add the plugin to [the `FLEXMEASURES_PLUGINS` setting](https://flexmeasures.readthedocs.io/stable/configuration.html#flexmeasures-plugins). Either use `/path/to/flexmeasures-entsoe/flexmeasures_entsoe` or `flexmeasures_entsoe` if you installed this as a package locally (see below).

2. Add `ENTSOE_AUTH_TOKEN` to your FlexMeasures config (e.g. ~/.flexmeasures.cfg).
You can generate this token after you made an account at ENTSO-E, read more [here](https://transparencyplatform.zendesk.com/hc/en-us/articles/12845911031188-How-to-get-security-token). 

   Optionally, override other settings (defaults shown here):

       ENTSOE_COUNTRY_CODE = "NL"
       ENTSOE_COUNTRY_TIMEZONE = "Europe/Amsterdam"
       ENTSOE_DERIVED_DATA_SOURCE = "FlexMeasures ENTSO-E"

   The `ENTSOE_DERIVED_DATA_SOURCE` option is used to name the source of data that this plugin derives from ENTSO-E data, like a CO₂ signal.
   Original ENTSO-E data is reported as being sourced by `"ENTSO-E"`.

3. To install this plugin locally as a package, try `pip install .`.


## Testing

ENTSO-E provides a test server (iop) for development. It's good practice not to overwhelm their production server.

Set ``ENTSOE_USE_TEST_SERVER=True`` to enable this.

In that case, this plugin will look for the auth token in the config setting ``ENTSOE_AUTH_TOKEN_TEST_SERVER``.

Note, however, that ENTSO-E usually does not seem to make the latest data available there. Asking for the next day can often get an empty response.


## Supported FlexMeasures versions

This plugin targets two distinct FlexMeasures capability tiers:

| FlexMeasures version | Behavior |
|---|---|
| `< 0.32` | Uses the legacy `get_data_source` factory; no account is linked to the ENTSO-E source. |
| `>= 0.32` | Uses the account-linked source API (`get_or_create_source` with an `Account`). |

This package supports Python 3.10 through 3.12, following the Python support policy of the currently supported FlexMeasures releases.

The oldest supported FlexMeasures release line is `0.31.*`.
CI is run against `0.31.*` (minimum supported legacy release), `0.32.*` (first account-linked release), and the latest released FlexMeasures version across all supported Python versions.
When a new FlexMeasures release introduces breaking changes the matrix should be updated accordingly.


## Development

To keep our code quality high, we use pre-commit:

    pip install pre-commit black flake8 mypy
    pre-commit install

or:
    
    make install-for-dev

Try it:

    pre-commit run --all-files --show-diff-on-failure
