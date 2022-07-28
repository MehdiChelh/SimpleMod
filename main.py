from virtual_dataframe import VDataFrame, VClient, delayed
from df_types import ResultsDF
from df_types.PolDF import *
from df_types import *


@delayed
@pandera.check_types
def pol_bef_ps(pol_data: PolInfoOpening, year: int) -> PolInfoBefPs:
    pol_data["math_res_bef_ps"] = pol_data["math_res_opening"]
    return pol_data


@delayed
@pandera.check_types
def pol_aft_ps(pol_data: PolInfoBefPs, pool_ps_rates: PoolPsRates, year: int) -> PolInfoClosing:
    pol_data["math_res_closing"] = pol_data["math_rs_bef_ps"] * \
        (pool_ps_rates["ps_rate"] + 1)
    return pol_data


@delayed
@pandera.check_types
def pool_closing(pol_data: PolInfoClosing, year: int) -> PoolInfoClosing:
    return pol_data  # FIXME groupby


@delayed
@pandera.check_types
def pool_opening(pool_data: PoolInfoClosing, pol_data: PolInfoOpening, year: int) -> PoolInfoOpening:
    if year == 0:
        pool_data["math_res_opening"] = pol_data["math_res_opening"].groupby(
            "id_pool").sum()
    else:
        pool_data["math_res_opening"] = pool_data["math_res_closing"]
    return pool_data


@delayed
@pandera.check_types
def pol_opening(pol_data: PoldataDF, pol_data_input: PoldataDF, year: int) -> PolInfoOpening:
    if year == 0:
        pol_data["math_res_opening"] = pol_data_input["math_res"]
    else:
        pol_data["math_res_opening"] = pol_data["math_res_closing"]
    return pol_data


@delayed
@pandera.check_types
def pool_ps(scen_eco_equity: ScenEcoEquityDF, pool_data_input: PooldataDF, pool_data: PoolPsRatesWithSpread, year: int) -> PoolPsRates:
    if year == 0:
        pool_data["ps_rate"] = 0
        return pool_data

    year_df_before = scen_eco_equity.iloc[:,
                                          (BEGIN_YEAR_POS+year):(BEGIN_YEAR_POS+year+1)]
    year_df_current = scen_eco_equity.iloc[:,
                                           (BEGIN_YEAR_POS+year):(BEGIN_YEAR_POS+year+1)]
    pool_data[pool_data["math_res_bef_ps"] > 1000000]["ps_rate"] = (year_df_current /
                                                                    year_df_before - 1 + pool_data["spread"])  # FIXME index alignement

    pool_data[pool_data["math_res_bef_ps"] <= 1000000]["ps_rate"] = (year_df_current /
                                                                     year_df_before - 1)
    return pool_data
