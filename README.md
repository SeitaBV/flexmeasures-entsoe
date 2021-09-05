# ENTSO-E forecasts & data

Importing data which can be relevant for energy flexibility services via ENTSO-E's API into FlexMeasures.

We start with data about the upcoming day.

- CO2 content
- Prices (planned)


## Installation

1. Add the path to this directory to your FlexMeasures (>v0.4.0) config file,
using the FLEXMEASURES_PLUGIN_PATHS setting.

2. Add ENTSOE_AUTH_TOKEN to your FlexMeasures config (e.g. ~/.flexmeasures.cfg).
You can generate this token after you made an account at ENTSO-E, read more [here](https://transparency.entsoe.eu/content/static_content/Static%20content/web%20api/Guide.html#_authentication_and_authorisation). 

3. pip install entsoe-py


## Usage

Importing tomorrow's CO2 mix:

    flexmeasures entsoe get_CO2_content_forecasts


## Development

We use pre-commit to keep code quality up:

    pip install pre-commit black flake8 mypy
    pre-commit install
    pre-commit run --all-files --show-diff-on-failure
