import os
import sys

from flask import Blueprint


HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
DEFAULT_COUNTRY_CODE = "NL"
DEFAULT_COUNTRY_TIMEZONE = "Europe/Amsterdam"  # This is what we receive, even if ENTSO-E documents Europe/Brussels
DEFAULT_DERIVED_DATA_SOURCE = "FlexMeasures ENTSO-E"

__version__ = "0.8"
__settings__ = {
    "ENTSOE_AUTH_TOKEN": dict(
        description="You can generate this token after you made an account at ENTSO-E.",
        level="error",
    ),
    "ENTSOE_COUNTRY_CODE": dict(
        level="warning",
        message_if_missing=f"'{DEFAULT_COUNTRY_CODE}' will be used as a default.",
    ),
    "ENTSOE_COUNTRY_TIMEZONE": dict(
        description="IANA timezone name used to localize ENTSO-E sensors.",
        level="info",
        message_if_missing=f"'{DEFAULT_COUNTRY_TIMEZONE}' will be used as a default.",
    ),
    "ENTSOE_USE_TEST_SERVER": dict(
        description="Boolean to indicate whether to use the ENTSO-E's iop test server instead of their production server",
        level="debug",
    ),
    "ENTSOE_AUTH_TOKEN_TEST_SERVER": dict(
        description="You can generate this token after you made an account at ENTSO-E.",
        level="debug",
    ),
    "ENTSOE_DERIVED_DATA_SOURCE": dict(
        description="String used to name the source of data that this plugin derives from ENTSO-E data, like a CO₂ signal.",
        level="info",
        message_if_missing=f"'{DEFAULT_DERIVED_DATA_SOURCE}' will be used as a default.",
    ),
}

entsoe_data_bp = Blueprint("entsoe", __name__, cli_group="entsoe")
entsoe_data_bp.cli.help = "ENTSO-E Data commands"


from .generation import day_ahead as day_ahead_generation  # noqa: E402,F401
from .prices import day_ahead as day_ahead_prices  # noqa: E402,F401
