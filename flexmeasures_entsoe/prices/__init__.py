from datetime import timedelta

# sensor_name, unit, even_resolution, data sourced directly by ENTSO-E or not (i.e. derived)
pricing_sensors = (("Day-ahead prices", "EUR/MWh", timedelta(minutes=15), True),)
