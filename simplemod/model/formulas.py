from typing import Any

from pandera import check_types, Int
from virtual_dataframe import delayed
from ..types import *
import numpy as np


# @delayed
# @check_types
def pol_opening(pol_data: PolInfoOpening,
                year: Int,
                sim: Int) -> PolInfoOpening:
    pol_data.loc[:, 'math_res_opening'] = pol_data.loc[:, 'math_res_closing']
    pol_data.loc[:, 'math_res_closing'] = 0

    return pol_data


# @delayed
# @check_types
def pol_bef_ps(pol_data: PolInfoOpening,
               year: Int,
               sim: Int) -> PolInfoBefPs:
    pol_data.loc[:, 'math_res_bef_ps'] = pol_data.loc[:, 'math_res_opening']
    return pol_data


# @delayed
# @check_types
def pool_bef_ps(pol_data: PolInfoBefPs,
                pool_data: PoolDFFull,
                year: Int,
                sim: Int) -> PoolInfoBefPs:

    # FIXME make it work on multiple sims simultaneously
    agg = pol_data.groupby(['id_pool', 'id_sim']).sum().reset_index().set_index('id_pool').loc[:, 'math_res_bef_ps']
    pool_data.loc[:, 'math_res_bef_ps'] = agg
    return pool_data


# @delayed
# @check_types
def pool_ps(econ_data: InputDataScenEcoEquityDF,
            pol_data: PolInfoBefPs,
            pool_data: PoolDFFull,
            year: Int,
            sim: Int) -> PoolDFFull:

    # FIXME make it work on multiple sims simultaneously

    if year == 0:
        prev_year = 'y0'
        curr_year = 'y0'
    else:
        curr_year = 'y' + str(year)
        prev_year = 'y' + str(year-1)

    eq_return = econ_data.loc[:, curr_year] / econ_data.loc[:, prev_year] - 1

    pool_data.loc[:, 'ps_rate'] = eq_return
    pool_data.loc[:, 'tot_return'] = eq_return
    # pool_data.loc[:, 'tot_return'].where(pool_data.loc[:, 'math_res_bef_ps'] < 1000000,
    #                                      pool_data.loc[:, 'ps_rate'] + pool_data.loc[:, 'spread'], inplace=True)
    pool_data.loc[:, 'tot_return'] = pool_data.loc[:, 'tot_return'].where(pool_data.loc[:, 'math_res_bef_ps'] < 1000000,
                                                                          pool_data.loc[:, 'ps_rate'] + pool_data.loc[:, 'spread'])

    return pool_data


# @delayed
# @check_types
def pol_aft_ps(pol_data: PolInfoBefPs,
               pool_data: PoolDFFull,
               year: Int,
               sim: Int) -> PolInfoClosing:

    # FIXME make it work on multiple sims simultaneously
    # FIXME sort_index is temporary for debugging / checking result consistency, it shouldn't be necessary in the future and should be avoid in the testing
    pol_data = pol_data.merge(pool_data[['tot_return']], on='id_pool', how='left').sort_index()
    pol_data.loc[:, 'math_res_closing'] = pol_data.loc[:, 'math_res_bef_ps'] * (1 + pol_data.loc[:, 'tot_return'])

    pol_data.drop('tot_return', axis=1, inplace=True)
    return pol_data


# @delayed
# @check_types
def pool_opening(pool_data: PoolInfoClosing, pol_data: PolInfoOpening, year: int) -> PoolInfoOpening:
    if year == 0:
        pool_data["math_res_opening"] = pol_data.groupby(
            ["id_sim", "id_pool"])[["math_res_opening"]].sum().reset_index().set_index("id_pool")["math_res_opening"]  # FIXME the syntax is not very clean (but will probably become clearer if we can use xarrays)
    else:
        pool_data["math_res_opening"] = pool_data["math_res_closing"]
    return pool_data


# @delayed
# @check_types
def pool_closing(pol_data: PolInfoClosing, pool_data: PoolInfoBefPs, year: int) -> PoolInfoClosing:
    pool_data[["math_res_opening", "math_res_bef_ps", "math_res_closing"]] = pol_data.groupby(
        ["id_sim", "id_pool"])[["math_res_opening", "math_res_bef_ps", "math_res_closing"]].sum().reset_index().set_index("id_pool")[["math_res_opening", "math_res_bef_ps", "math_res_closing"]]
    return pool_data
