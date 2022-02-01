import pandas as pd


def determine_net_emission_factors(shares: pd.DataFrame) -> pd.Series:
    """Given production shares, determine the net emission factors.
    Or given production by type, determine the net emissions.

    Use column headers that match production types listed below.
    Use any index.

    For example:

        print(shares)

              fossil_gas     other  fossil_hard_coal     waste   nuclear
        hour
        0       0.443685  0.206033          0.237596  0.050915  0.059455
        1       0.443910  0.205065          0.235022  0.052614  0.060987

        print(determine_net_emission_factors(shares))

        hour
        0     644.753221
        1     641.410093
        Name: Average emissions from Dutch electricity production (kg CO₂ eq/MWh), dtype: float64
    """
    emission_factors = dict(
        biomass=50.4,
        fossil_brown_coal_or_lignite=None,  # unknown
        fossil_coal_derived_gas=None,  # unknown
        fossil_gas=464,
        fossil_hard_coal=1030,
        fossil_oil=1010,
        fossil_oil_shale=None,  # unknown
        fossil_peat=None,  # unknown
        geothermal=0.00664,
        hydro_pumped_storage=611,
        hydro_run_of_river_and_poundage=0.0253,
        hydro_water_reservoir=8.13,
        marine=None,  # unknown
        nuclear=10.1,
        other=927,  # for EU28
        other_renewable=None,  # unknown
        solar=0.00591,
        waste=None,  # unknown
        wind_offshore=0.133,
        wind_onshore=0.133,
    )  # supplementary material from "Real-time carbon accounting method for the European electricity markets, Tranberg et al. (2019)"
    # todo: substitute placeholder for unknown emission factor of waste
    emission_factors["waste"] = emission_factors["biomass"]
    for production_type in shares.columns:
        shares[production_type] = (
            shares[production_type] * emission_factors[production_type]
        )
    return shares.sum(axis=1).rename(
        "Average emissions from Dutch electricity production (kg CO₂ eq/MWh)"
    )
