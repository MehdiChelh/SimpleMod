from typing import Any

from pandera import check_types, Int
from virtual_dataframe import delayed
from ..types import *
import numpy as np


@delayed
@check_types
def pol_opening(pol_data: PolInfoOpening,
                year: Int,
                sim: Int) -> PolInfoOpening:  
    pol_data.loc[:, 'math_res_opening'] = pol_data.loc[:, 'math_res_closing']
    pol_data.loc[:, 'math_res_closing'] = 0

    return pol_data


@delayed
@check_types
def pol_bef_ps(pol_data: PolInfoOpening,
               year: Int,
               sim: Int) -> PolInfoBefPs:
    pol_data.loc[:, 'math_res_bef_ps'] = pol_data.loc[:, 'math_res_opening']
    return pol_data


@delayed
@check_types
def pool_bef_ps(pol_data: PolInfoBefPs,
                pool_data: PoolDFFull, 
                year: Int,
                sim: Int) -> PoolInfoBefPs:

    # FIXME make it work on multiple sims simultaneously
    agg = pol_data.groupby(['id_pool', 'id_sim']).sum().reset_index().set_index('id_pool').loc[:, 'math_res_bef_ps']
    pool_data.loc[:, 'math_res_bef_ps'] = agg 
    #print(pool_data)
    return pool_data


@delayed
@check_types
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

    eq_return = econ_data.loc[sim, curr_year] / econ_data.loc[sim, prev_year] - 1
    
    pool_data.loc[:, 'ps_rate'] = eq_return
    pool_data.loc[:, 'tot_return'] = eq_return
    pool_data.loc[:, 'tot_return'].where(pool_data.loc[:, 'math_res_bef_ps'] < 1000000, pool_data.loc[:, 'ps_rate'] + pool_data.loc[:, 'spread'], inplace=True)

    #print(pool_data)

    return pool_data


@delayed
@check_types
def pol_aft_ps(pol_data: PolInfoBefPs,
               pool_data: PoolDFFull,
               year: Int,
               sim: Int) -> PolInfoClosing:

    # FIXME make it work on multiple sims simultaneously
    #print(pool_data)
    
    pol_data = pol_data.merge(pool_data[['tot_return']], on = 'id_pool', how='left')
    pol_data.loc[:, 'math_res_closing'] = pol_data.loc[:, 'math_res_bef_ps'] * (1+ pol_data.loc[:, 'tot_return'])
    #print(pol_data)

    pol_data.drop('tot_return', axis=1, inplace=True)
    return pol_data
