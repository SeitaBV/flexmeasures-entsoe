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


### October 1st 2025 go-live for ENTSO-E moving to 15-minute day-ahead prices

ENTSO-E is moving from 1-hour day-ahead prices 15-minute day-ahead prices on October 1st 2025.
To prepare for this transition, you have two choices:

1. resample your existing price sensor in FlexMeasures from 1 hour to 15 minutes, or
2. create a new price sensor in FlexMeasures with a 15-min resolution.

#### 1. Resampling

**The upside** of resampling your existing price data is that the sensor ID of your price sensor in FlexMeasures will remain the same. Depending on your system setup, Forecaster/Reporter/Scheduler configurations (such as an asset's `flex-context`) may depend on it, and your users may expect the 15-minute data to live under the same sensor.

**The downside** is that it quadruples your data for that sensor, due to the fact that FlexMeasures only supports a fixed resolution for any given sensor. Although there should be no noticeable hit in performance, it obviously leads to redundant data in the price history before October 1st 2025.  

**To resample** your historical data, use:

```bash
flexmeasures edit resample-data --sensor <ID of your day-ahead price sensor> --event-resolution 15
```

The `flexmeasures-entsoe` package already automatically resamples the ENTSO-E data to the resolution of your sensor.

#### 2. Creating a new sensor

**The upside** is that this doesn't quadruple your historic data (see *the downside* of resampling, above).

**The downside** is that you may need to revise Forecaster/Reporter/Scheduler configurations (such as an asset's `flex-context`) and notify users (see *the upside* of resampling, above).

**To create a new sensor**, in this example for the NL bidding zone, use:

```bash
flexmeasures add sensor --name '15-min day-ahead prices' --unit EUR/MWh --event-resolution PT15M --timezone Europe/Amsterdam --asset <ID of your 'NL transmission zone' asset>
```

and pass your new sensor ID when calling the price import command:

```bash
flexmeasures entsoe import-day-ahead-prices --sensor <new sensor ID> --country NL
```

The `flexmeasures-entsoe` package already automatically resamples the ENTSO-E data to the resolution of your sensor.

## Installation

First of all, this is a FlexMeasures plugin. Consult the FlexMeasures documentation for setup.

1. Add the plugin to [the `FLEXMEASURES_PLUGINS` setting](https://flexmeasures.readthedocs.io/en/latest/configuration.html#plugin-config). Either use `/path/to/flexmeasures-entsoe/flexmeasures_entsoe` or `flexmeasures_entsoe` if you installed this as a package locally (see below).

2. Add `ENTSOE_AUTH_TOKEN` to your FlexMeasures config (e.g. ~/.flexmeasures.cfg).
You can generate this token after you made an account at ENTSO-E, read more [here](https://transparency.entsoe.eu/content/static_content/Static%20content/web%20api/Guide.html#_authentication_and_authorisation). 

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


## Development

To keep our code quality high, we use pre-commit:

    pip install pre-commit black flake8 mypy
    pre-commit install

or:
    
    make install-for-dev

Try it:

    pre-commit run --all-files --show-diff-on-failure
