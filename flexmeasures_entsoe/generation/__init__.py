from datetime import timedelta

# sensor_name, unit, event_resolution, data sourced directly by ENTSO-E or not (i.e. derived)
generation_sensors = (
    ("Scheduled generation", "MW", timedelta(minutes=15), True),
    ("Solar", "MW", timedelta(hours=1), True),
    ("Wind Onshore", "MW", timedelta(hours=1), True),
    ("Wind Offshore", "MW", timedelta(hours=1), True),
    ("CO₂ intensity", "kg/MWh", timedelta(minutes=15), False),
)
