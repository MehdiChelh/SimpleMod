from virtual_dataframe import delayed
from df_types import *
from typing import Tuple


@delayed
@pandera.check_types
def pol_bef_ps(pol_data: PolInfoOpening, year: int) -> PolInfoBefPs:
    pol_data["math_res_bef_ps"] = pol_data["math_res_opening"]
    return pol_data


@delayed
@pandera.check_types
# FIXME this function could be optimized with multindex / xarray
def pol_aft_ps(pol_data: PolInfoBefPs, pool_ps_rates: PoolPsRates, year: int) -> PolInfoClosing:
    if "ps_rate" in pol_data.columns:
        pol_data = pol_data.drop("ps_rate", axis=1)

    pol_data = pol_data.merge(pool_ps_rates[["id_sim", "id_pool", "ps_rate"]],
                              on=["id_sim", "id_pool"], how="left")
    pol_data["math_res_closing"] = pol_data["math_res_bef_ps"] * \
        (pol_data["ps_rate"] + 1)
    return pol_data


@delayed
@pandera.check_types
def pool_closing(pol_data: PolInfoClosing, pool_data: PoolInfoBefPs, year: int) -> PoolInfoClosing:
    pool_data[["math_res_opening", "math_res_bef_ps", "math_res_closing"]] = pol_data.groupby(
        ["id_sim", "id_pool"])[["math_res_opening", "math_res_bef_ps", "math_res_closing"]].sum().reset_index()[["math_res_opening", "math_res_bef_ps", "math_res_closing"]]
    return pool_data


@delayed
@pandera.check_types
def pool_opening(pool_data: PoolInfoClosing, pol_data: PolInfoOpening, year: int) -> PoolInfoOpening:
    if year == 0:
        pool_data["math_res_opening"] = pol_data.groupby(
            ["id_sim", "id_pool"])[["math_res_opening"]].sum().reset_index()["math_res_opening"]  # FIXME the syntax is not very clean (but will probably become clearer if we can use xarrays)
    else:
        pool_data["math_res_opening"] = pool_data["math_res_closing"]
    return pool_data


@ delayed
@ pandera.check_types
def pol_opening(input_data_pol: InputDataPol, pol_data: PolInfoOpening, year: int, nb_sim: int) -> PolInfoOpening:  # FIXME first argument type
    if year == 0:
        # FIXME : quickfix, but it would be better if we can broadcast the asignement along the axis (may be easier once we have xarrays)
        pol_data["id_pool"] = input_data_pol["id_pool"].to_list() * nb_sim
        # FIXME : quickfix, but it would be better if we can broadcast the asignement along the axis (may be easier once we have xarrays)
        pol_data["math_res_opening"] = input_data_pol["math_res"].to_list() * \
            nb_sim
    else:
        pol_data["math_res_opening"] = pol_data["math_res_closing"]
    return pol_data


@ delayed
@ pandera.check_types
def pool_ps(scen_eco_equity: InputDataScenEcoEquityDF, input_data_pool: InputDataPool, pool_data: PoolPsRatesWithSpread, year: int, nb_sim: int) -> PoolPsRates:
    if year == 0:
        pool_data["ps_rate"] = 0.
        # FIXME index alignement for pool_data & input_data_pool
        pool_data["spread"] = input_data_pool["spread"].to_list() * nb_sim
        return pool_data

    # FIXME without xarray or multiindex I don't know how to really increase the readability and efficiency of this function (without inverting the order of id_pool/id_sim indexes)
    rdt_year_n1 = scen_eco_equity.loc[:,
                                      [f"year_{(BEGIN_YEAR_POS+year-1)}"]].rename({f"year_{(BEGIN_YEAR_POS+year-1)}": "rdt_year_n1"}, axis=1)  # FIXME for now we apply the calculation to 1 simulation only (quick(temp)fix to the index alignement issue)
    rdt_year_n = scen_eco_equity.loc[:,
                                     [f"year_{(BEGIN_YEAR_POS+year)}"]].rename({f"year_{(BEGIN_YEAR_POS+year)}": "rdt_year_n"}, axis=1)  # FIXME for now we apply the calculation to 1 simulation only (quick(temp)fix to the index alignement issue)
    pool_data["ps_rate"] = 0.

    pool_data = pool_data.merge(
        rdt_year_n, left_on="id_sim", right_index=True, how="left")
    pool_data = pool_data.merge(
        rdt_year_n1, left_on="id_sim", right_index=True, how="left")

    pool_data.loc[pool_data["math_res_bef_ps"] > 1000000, "ps_rate"] = (pool_data.loc[pool_data["math_res_bef_ps"] > 1000000, "rdt_year_n"] /
                                                                        pool_data.loc[pool_data["math_res_bef_ps"] > 1000000, "rdt_year_n1"] - 1 + pool_data.loc[pool_data["math_res_bef_ps"] > 1000000, "spread"])

    pool_data.loc[pool_data["math_res_bef_ps"] <= 1000000, "ps_rate"] = (pool_data.loc[pool_data["math_res_bef_ps"] <= 1000000, "rdt_year_n"] /
                                                                         pool_data.loc[pool_data["math_res_bef_ps"] <= 1000000, "rdt_year_n1"] - 1)
    pool_data = pool_data.drop(["rdt_year_n", "rdt_year_n1"], axis=1)
    return pool_data


@delayed
@pandera.check_types
# FIXME not sure for pool_datda:PoolInfoOpening as argument (not present in the initial architecture)
def pool_bef_ps(pol_data: PolInfoBefPs, pool_data: PoolInfoOpening) -> PoolInfoBefPs:
    pool_data[["math_res_bef_ps"]] = (
        pol_data.groupby(["id_pool", "id_sim"])[
            ["math_res_bef_ps"]].sum().reset_index()[["math_res_bef_ps"]]  # FIXME the syntax is not very clean (but will probably become clearer if we can use xarrays)
    )
    return pool_data
