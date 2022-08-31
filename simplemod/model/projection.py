import numpy as np
from time import time

from typing import Tuple
from pandera import Int 

from simplemod.constants import SIM_COUNT, POL_COUNT, POOL_COUNT
from .formulas import *
from simplemod.utils import init_vdf_from_schema, schema_to_dtypes
from virtual_dataframe import compute

@check_types
def init_model(input_data_pol: InputDataPol, input_data_pool: InputDataPool) -> Tuple[PolDF, PoolDFFull]:

    nb_scenarios = SIM_COUNT
    nb_pol = POL_COUNT 
    nb_pool = POOL_COUNT

    id_scen = 0

    pol_data = init_vdf_from_schema(
        PolDF,
        nrows=nb_scenarios * nb_pol,  # FIXME
        default_data=0)
    pol_data.loc[:, 'id_policy'] = list(range(1, nb_pol+1)) * nb_scenarios
    pol_data.loc[:, 'id_sim'] = np.repeat(range(nb_scenarios), nb_pol)
    pol_data = pol_data.set_index('id_policy')

    pool_data = init_vdf_from_schema(
        PoolDFFull ,
        nrows=nb_scenarios * nb_pool,  # FIXME
        default_data=0)
    pool_data.loc[:, 'id_pool'] = list(range(1,nb_pool+1))*nb_scenarios
    pool_data.loc[:, 'id_sim'] = np.repeat(range(nb_scenarios), nb_pool)
    pool_data = pool_data.set_index('id_pool')
    
    pol_data.loc[:, 'id_pool'] = input_data_pol.loc[:, 'id_pool']
    pol_data.loc[:, 'math_res_closing'] = input_data_pol.loc[:, 'math_res']
    pool_data.loc[:, 'spread'] = input_data_pool.loc[:, 'spread']

    return pol_data, pool_data

@delayed
@check_types
def one_year(
        pol_data: PolDF,
        pool_data: PoolDFFull,
        econ_data: InputDataScenEcoEquityDF,
        year: Int,
        sim: Int
) -> Tuple[PolInfoClosing, PoolInfoClosing]:

    #print(pol_data.head())

    pol_data: PolInfoOpening = pol_opening(pol_data, year, sim)
    pol_data: PolInfoBefPs = pol_bef_ps(pol_data, year, sim)
    pool_data: PoolDFFull = pool_bef_ps(pol_data, pool_data, year, sim)
    pool_data: PoolDFFull = pool_ps(econ_data, pol_data, pool_data, year, sim)
    pol_data: PolInfoClosing = pol_aft_ps(pol_data, pool_data, year, sim)

    return pol_data, pool_data


def projection(input_data_pol: InputDataPol,
               input_data_pool: InputDataPool,
               input_data_scen_eco_equity: InputDataScenEcoEquityDF) -> Tuple[PolInfoClosing, PoolDFFull]:
    
    nb_scenarios = 0
    nb_years = 100
    max_scen_year = 3

    t_start = time()

    pol_data, pool_data = init_model(input_data_pol, input_data_pool)
#    print(compute(pol_data[pol_data.id_sim==0]))

    print()


    for year in range(nb_years+1):
        print(f"--> year: {year}")
        pol_data, pool_data = one_year(
            pol_data,
            pool_data,
            input_data_scen_eco_equity,
            min(year, max_scen_year),
            nb_scenarios
        )
        # TODO: result must be input
        # TODO: save all years into results
    
    pol_data.compute()
    t_end = time()

    print('Computed in {:.04f}s'.format((t_end-t_start)))

    return pol_data, pool_data