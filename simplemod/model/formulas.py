from typing import Any

from pandera import check_types, Int
from virtual_dataframe import delayed
from ..types import *
import numpy as np


# @delayed
# @check_types
def pol_opening(pol_data: PolInfoOpening,
                year: Int) -> PolInfoOpening:
    pol_data['math_res_opening'] = pol_data['math_res_closing']
    return pol_data


# @delayed
# @check_types
def pol_bef_ps(pol_data: PolInfoOpening,
               year: Int) -> PolInfoBefPs:
    pol_data['math_res_bef_ps'] = pol_data['math_res_opening']
    return pol_data


# @delayed
# @check_types
def pool_bef_ps(pol_data: PolInfoBefPs,
                pool_data: PoolDFFull,
                year: Int) -> PoolInfoBefPs:

    agg = pol_data.groupby(['id_sim', 'id_pool'])[["math_res_bef_ps"]].sum().reset_index()
    agg.index = pool_data.index  # FIX for dask
    pool_data = pool_data.drop("math_res_bef_ps", axis=1).merge(agg, on=["id_sim", "id_pool"])

    return pool_data


# @delayed
# @check_types
def pool_ps(econ_data: InputDataScenEcoEquityDF,
            pol_data: PolInfoBefPs,
            pool_data: PoolDFFull,
            year: Int) -> PoolDFFull:

    if year == 0:
        prev_year = 'y0'
        curr_year = 'y0'
    else:
        curr_year = 'y' + str(year)
        prev_year = 'y' + str(year-1)

    eq_return = econ_data.loc[:, curr_year] / econ_data.loc[:, prev_year] - 1
    pool_data = pool_data.merge(eq_return.rename("eq_return").to_frame("eq_return"),
                                left_on="id_sim", right_index=True, how="left")
    pool_data['ps_rate'] = pool_data["eq_return"]
    pool_data['tot_return'] = pool_data["eq_return"]
    pool_data = pool_data.drop("eq_return", axis=1)
    # pool_data.loc[:, 'tot_return'].where(pool_data.loc[:, 'math_res_bef_ps'] < 1000000,
    #                                      pool_data.loc[:, 'ps_rate'] + pool_data.loc[:, 'spread'], inplace=True)
    pool_data['tot_return'] = pool_data['tot_return'].where(pool_data['math_res_bef_ps'] < 1000000,
                                                            pool_data['ps_rate'] + pool_data['spread'])

    return pool_data


# @delayed
# @check_types
def pol_aft_ps(pol_data: PolInfoBefPs,
               pool_data: PoolDFFull,
               year: Int) -> PolInfoClosing:

    pol_data = pol_data.merge(pool_data[['id_pool', 'id_sim', 'tot_return']],
                              on=['id_pool', 'id_sim'], how='left')
    pol_data['math_res_closing'] = pol_data['math_res_bef_ps'] * (1 + pol_data['tot_return'])

    pol_data = pol_data.drop('tot_return', axis=1)
    return pol_data


# @delayed
# @check_types
def pool_opening(pool_data: PoolInfoClosing, pol_data: PolInfoOpening, year: int) -> PoolInfoOpening:
    if year == 0:
        agg = pol_data.groupby(
            ["id_sim", "id_pool"])[["math_res_opening"]].sum().reset_index()
        # agg.index = pool_data.index  # FIX for dask
        pool_data = pool_data.drop("math_res_opening", axis=1).merge(agg, on=["id_sim", "id_pool"])
    else:
        pool_data["math_res_opening"] = pool_data["math_res_closing"]
    return pool_data


# @delayed
# @check_types
def pool_closing(pol_data: PolInfoClosing, pool_data: PoolInfoBefPs, year: int) -> PoolInfoClosing:
    agg = pol_data.groupby(
        ["id_sim", "id_pool"])[["math_res_opening", "math_res_bef_ps", "math_res_closing"]].sum().reset_index()
    agg.index = pool_data.index  # FIX for dask
    pool_data = pool_data.drop(["math_res_opening", "math_res_bef_ps", "math_res_closing"],
                               axis=1).merge(agg, on=["id_sim", "id_pool"])
    return pool_data
