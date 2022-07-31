from dataclasses import dataclass
from tarfile import ExFileObject
from virtual_dataframe import VDataFrame, VClient, delayed
from df_types import *
from typing import Tuple
import pandas as pd
from utils import init_vdf_from_schema


SIM_ID = 0


@delayed
@pandera.check_types
def pol_bef_ps(pol_data: PolInfoOpening, year: int) -> PolInfoBefPs:
    pol_data["math_res_bef_ps"] = pol_data["math_res_opening"]
    return pol_data


@delayed
@pandera.check_types
def pol_aft_ps(pol_data: PolInfoBefPs, pool_ps_rates: PoolPsRates, year: int) -> PolInfoClosing:
    pol_data["math_res_closing"] = pol_data["math_res_bef_ps"] * \
        (pool_ps_rates["ps_rate"] + 1)
    return pol_data


@delayed
@pandera.check_types
def pool_closing(pol_data: PolInfoClosing, pool_data: PoolInfoBefPs, year: int) -> PoolInfoClosing:
    pool_data[["math_res_opening", "math_res_before_ps", "math_res_closing"]] = pol_data.groupby(
        "id_pool")[["math_res_opening", "math_res_before_ps", "math_res_closing"]].sum()
    return pool_data


@delayed
@pandera.check_types
def pool_opening(pool_data: PoolInfoClosing, pol_data: PolInfoOpening, year: int) -> PoolInfoOpening:
    if year == 0:
        pool_data["math_res_opening"] = pol_data.groupby(
            "id_pool")[["math_res_opening"]].sum()
    else:
        pool_data["math_res_opening"] = pool_data["math_res_closing"]
    return pool_data


@delayed
@pandera.check_types
def pol_opening(input_data_pol: InputDataPol, pol_data: PolInfoOpening, year: int) -> PolInfoOpening:  # FIXME first argument type
    if year == 0:
        pol_data["math_res_opening"] = input_data_pol["math_res"]
    else:
        pol_data["math_res_opening"] = pol_data["math_res_closing"]
    return pol_data


@delayed
@pandera.check_types
def pool_ps(scen_eco_equity: InputDataScenEcoEquityDF, input_data_pool: InputDataPool, pool_data: PoolPsRatesWithSpread, year: int) -> PoolPsRates:
    if year == 0:
        pool_data["ps_rate"] = 0.
        # FIXME index alignement for pool_data & input_data_pool
        pool_data["spread"] = input_data_pool["spread"]
        return pool_data

    rdt_year_n1 = scen_eco_equity.iloc[SIM_ID,
                                       (BEGIN_YEAR_POS+year):(BEGIN_YEAR_POS+year+1)]  # FIXME for now we apply the calculation to 1 simulation only (quick(temp)fix to the index alignement issue)
    rdt_year_n = scen_eco_equity.iloc[SIM_ID,
                                      (BEGIN_YEAR_POS+year):(BEGIN_YEAR_POS+year+1)]  # FIXME for now we apply the calculation to 1 simulation only (quick(temp)fix to the index alignement issue)
    pool_data[pool_data["math_res_bef_ps"] > 1000000]["ps_rate"] = (rdt_year_n /
                                                                    rdt_year_n1 - 1 + pool_data["spread"])

    pool_data[pool_data["math_res_bef_ps"] <= 1000000]["ps_rate"] = (rdt_year_n /
                                                                     rdt_year_n1 - 1)
    return pool_data


# Strategies :
# - perform a join on simulations
#   - then we should have nsim in pool_data and pol_data index

@delayed
@pandera.check_types
# FIXME not sure for pool_datda:PoolInfoOpening as argument (not present in the initial architecture)
def pool_bef_ps(pol_data: PolInfoBefPs, pool_data: PoolInfoOpening) -> PoolInfoBefPs:
    pool_data["math_res_bef_ps"] = pol_data.groupby(
        "id_pool")["math_res_bef_ps"].sum()
    return pool_data


@delayed
@pandera.check_types
def one_year(
        input_data_pol: InputDataPol,
        input_data_pool: InputDataPool,
        input_data_scen_eco_equity: InputDataScenEcoEquityDF,
        pol_data: PolDF,
        pool_data: PoolDF,
        year: int
) -> Tuple[PoolInfoClosing, PolInfoClosing]:
    pol_data: PolInfoOpening = pol_opening(input_data_pol, pol_data, year)
    pol_data: PolInfoBefPs = pol_bef_ps(pol_data, year)
    pool_data: PoolInfoBefPs = pool_bef_ps(pol_data, pool_data)
    pool_ps_rates: PoolPsRates = pool_ps(input_data_scen_eco_equity,
                                         input_data_pool, pool_data, year)
    pol_data: PolInfoClosing = pol_aft_ps(pol_data, pool_ps_rates, year)
    pool_data: PoolInfoClosing = pool_closing(pol_data, pool_data, year)
    pool_data: PoolInfoOpening = pool_opening(pool_data, pol_data, year)
    return pool_data, pol_data


if __name__ == "__main__":
    # load data
    input_data_pol = VDataFrame(pd.read_csv("./data/mp_policies.csv"))
    input_data_pool = VDataFrame(pd.read_csv("./data/mp_pool.csv"))
    input_data_scen_eco_equity = VDataFrame(
        pd.read_csv("./data/scen_eco_sample.csv",
                    dtype={
                        "id_sim": int,
                        "year_0": float,
                        "year_1": float,
                        "year_2": float,
                        "year_3": float,
                        "measure": str
                    }))  # FIXME implement read_csv_with_schema / DataFrame_with_schema

    nb_pol, _ = input_data_pol.shape
    nb_pool, _ = input_data_pool.shape
    # nb_scenarios = input_data_scen_eco_equity.shape  # FIXME
    nb_scenarios = 1

    # FIXME devrait prendre en compte le nombre de simulations
    pol_data = init_vdf_from_schema(
        Pol_schema, nrows=nb_pol, default_data=0)
    # FIXME devrait prendre en compte le nombre de simulations
    pool_data = init_vdf_from_schema(
        Pool_schema, nrows=nb_pool, default_data=0)
    one_year(
        input_data_pol,
        input_data_pool,
        input_data_scen_eco_equity,
        pol_data,
        pool_data,
        0
    )


# FAQ :
#  Problème de titre dans l'index (designé par 'None')
#  error in check_types decorator of function 'one_year': expected series 'None' to have type str, got int64
