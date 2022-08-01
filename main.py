from dataclasses import dataclass
from tarfile import ExFileObject
from virtual_dataframe import VDataFrame, VClient, delayed
from df_types import *
from typing import Tuple
import pandas as pd
from utils import init_vdf_from_schema, save_outputs


SIM_ID = 0
SIM_COUNT = 3


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
def pol_opening(input_data_pol: InputDataPol, pol_data: PolInfoOpening, year: int) -> PolInfoOpening:  # FIXME first argument type
    if year == 0:
        # pol_data[["id_pool", "math_res_opening"]
        #          ] = input_data_pol[["id_pool", "math_res"]]
        # pol_data = pol_data.drop("math_res_opening", axis=1).merge(input_data_pol.rename(
        #     {"math_res": "math_res_opening"}, axis=1), on="id_pool", how="left")
        pol_data["id_pool"] = input_data_pol["id_pool"].to_list(
        ) * int(len(pol_data.index) / len(input_data_pol.index))  # FIXME : quickfix, but it would be better if we can broadcast the asignement along the axis (may be easier once we have xarrays)
        pol_data["math_res_opening"] = input_data_pol["math_res"].to_list(
        ) * int(len(pol_data.index) / len(input_data_pol.index))  # FIXME : quickfix, but it would be better if we can broadcast the asignement along the axis (may be easier once we have xarrays)
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
    rdt_year_n1 = scen_eco_equity.loc[:SIM_COUNT,
                                      [f"year_{(BEGIN_YEAR_POS+year-1)}"]].rename({f"year_{(BEGIN_YEAR_POS+year-1)}": "rdt_year_n1"}, axis=1)  # FIXME for now we apply the calculation to 1 simulation only (quick(temp)fix to the index alignement issue)
    rdt_year_n = scen_eco_equity.loc[:SIM_COUNT,
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


@delayed
@pandera.check_types
def one_year(
        pol_writer,
        pool_writer,
        input_data_pol: InputDataPol,
        input_data_pool: InputDataPool,
        input_data_scen_eco_equity: InputDataScenEcoEquityDF,
        pol_data: PolDF,
        pool_data: PoolDF,
        year: int,
        nb_sim: int
) -> Tuple[PoolInfoClosing, PolInfoClosing]:
    # Initialisation
    if year == 0:
        # FIXME need to be changed when simulation is added to pol_data.index
        # FIXME : quickfix, but it would be better if we can broadcast the asignement along the axis (may be easier once we have xarrays)
        pol_data["id_pol"] = input_data_pol.index.to_list() * nb_sim
        # FIXME need to be changed when simulation is added to pool_data.index
        # FIXME : quickfix, but it would be better if we can broadcast the asignement along the axis (may be easier once we have xarrays)
        pool_data["id_pool"] = input_data_pool.index.to_list() * nb_sim

    pol_data: PolInfoOpening = pol_opening(input_data_pol, pol_data, year)
    # save_outputs(pol_data, pol_writer, f"pol_opening_{year}")
    # save_outputs(pool_data, pool_writer, f"pol_opening_{year}")

    pol_data: PolInfoBefPs = pol_bef_ps(pol_data, year)
    # save_outputs(pol_data, pol_writer, f"pol_bef_ps_{year}")
    # save_outputs(pool_data, pool_writer, f"pol_bef_ps_{year}")

    pool_data: PoolInfoBefPs = pool_bef_ps(pol_data, pool_data)
    # save_outputs(pol_data, pol_writer, f"pool_bef_ps_{year}")
    # save_outputs(pool_data, pool_writer, f"pool_bef_ps_{year}")

    pool_data: PoolPsRates = pool_ps(input_data_scen_eco_equity,
                                     input_data_pool, pool_data, year, nb_sim)
    # save_outputs(pol_data, pol_writer, f"pool_ps_{year}")
    # save_outputs(pool_data, pool_writer, f"pool_ps_{year}")

    pol_data: PolInfoClosing = pol_aft_ps(pol_data, pool_data, year)
    # save_outputs(pol_data, pol_writer, f"pol_aft_ps_{year}")
    # save_outputs(pool_data, pool_writer, f"pol_aft_ps_{year}")

    pool_data: PoolInfoClosing = pool_closing(pol_data, pool_data, year)
    # save_outputs(pol_data, pol_writer, f"pool_closing_{year}")
    # save_outputs(pool_data, pool_writer, f"pool_closing_{year}")

    pool_data: PoolInfoOpening = pool_opening(pool_data, pol_data, year)
    # save_outputs(pol_data, pol_writer, f"pool_opening_{year}")
    # save_outputs(pool_data, pool_writer, f"pool_opening_{year}")

    return pol_data, pool_data


if __name__ == "__main__":
    # load data
    input_data_pol = VDataFrame(
        pd.read_csv("./data/mp_policies.csv",
                    dtype={
                        "id_policy": int,
                        "id_pool": int,
                        "age": int,
                        "math_res": float
                    }, index_col=0))
    input_data_pool = VDataFrame(
        pd.read_csv("./data/mp_pool.csv", index_col=0))
    input_data_scen_eco_equity = VDataFrame(
        pd.read_csv("./data/scen_eco_sample.csv",
                    dtype={
                        "id_sim": int,
                        "year_0": float,
                        "year_1": float,
                        "year_2": float,
                        "year_3": float,
                        "measure": str
                    },
                    index_col=0))  # FIXME implement read_csv_with_schema / DataFrame_with_schema

    nb_pol, _ = input_data_pol.shape
    nb_pool, _ = input_data_pool.shape
    # nb_scenarios = input_data_scen_eco_equity.shape  # FIXME
    nb_scenarios = SIM_COUNT

    pol_data = init_vdf_from_schema(
        Pol_schema, nrows=nb_scenarios * nb_pol, default_data=0)
    pool_data = init_vdf_from_schema(
        Pool_schema, nrows=nb_scenarios * nb_pool, default_data=0)

    # FIXME this section could be improved (would be straightforward if we have xarrays)
    pol_data["id_sim"] = np.array(
        [[i for j in range(nb_pol)] for i in range(nb_scenarios)]).reshape(nb_scenarios * nb_pol)
    pool_data["id_sim"] = np.array(
        [[i for j in range(nb_pool)] for i in range(nb_scenarios)]).reshape(nb_scenarios * nb_pool)

    pol_writer = pd.ExcelWriter("outputs/pol_data.xlsx", engine='xlsxwriter')
    pool_writer = pd.ExcelWriter("outputs/pool_data.xlsx", engine='xlsxwriter')
    for year in range(3):
        print(f"--> year: {year}")
        pol_data, pool_data = one_year(
            pol_writer,
            pool_writer,
            input_data_pol,
            input_data_pool,
            input_data_scen_eco_equity,
            pol_data,
            pool_data,
            year,
            nb_scenarios
        )
    pol_writer.save()
    pol_writer.close()
    pool_writer.save()
    pool_writer.close()


# FAQ :
#  Problème de titre dans l'index (designé par 'None')
#  error in check_types decorator of function 'one_year': expected series 'None' to have type str, got int64
