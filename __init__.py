import os
import sys

from flask import Blueprint


HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

__version__ = "0.1"

entsoe_data_bp = Blueprint(
    "entsoe", __name__
)

DEFAULT_COUNTRY_CODE = "NL"
DEFAULT_TIMEZONE = "Europe/Brussels"


entsoe_data_bp.cli.help = "ENTSO-E Data commands"


from .generation import day_ahead  # noqa: E402,F401