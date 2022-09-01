import numpy as np
import pandas as pd
from time import time

from typing import Tuple
from pandera import Int 

from simplemod.constants import SIM_COUNT, POL_COUNT, POOL_COUNT
from .formulas import *
from simplemod.utils import init_vdf_from_schema, schema_to_dtypes
from virtual_dataframe import compute, VSeries

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
    pol_data['id_policy'] = VSeries(list(range(nb_pol)) * nb_scenarios)
    pol_data['id_sim'] = VSeries(np.repeat(range(nb_scenarios), nb_pol))
    pol_data = pol_data.set_index('id_policy')

    pool_data = init_vdf_from_schema(
        PoolDFFull ,
        nrows=nb_scenarios * nb_pool,  # FIXME
        default_data=0)
    pool_data['id_pool'] = VSeries(list(range(nb_pool)) * nb_scenarios)
    pool_data['id_sim'] = VSeries(np.repeat(range(nb_scenarios), nb_pool))
    pool_data = pool_data.set_index('id_pool')
    pol_data['id_pool'] = VSeries(input_data_pol.loc[:, 'id_pool'])
    
    pol_data['math_res_closing'] = VSeries(input_data_pol.loc[:, 'math_res'])
    pool_data['spread'] = VSeries(input_data_pool.loc[:, 'spread'])
    return pol_data, pool_data

@delayed(nout=2)
# @check_types
def one_year(
        pol_writer,
        pool_writer,
        pol_data: PolDF,
        pool_data: PoolDFFull,
        econ_data: InputDataScenEcoEquityDF,
        year: Int,
        sim: Int
) -> Tuple[PolInfoClosing, PoolInfoClosing]:

    #print(pol_data.head())

    pol_data: PolInfoOpening = pol_opening(pol_data, year, sim)
    pol_data.to_excel(pol_writer, f"pol_opening_{year}")
    pool_data.to_excel(pool_writer, f"pol_opening_{year}")

    pool_data: PoolInfoOpening = pool_opening(pool_data, pol_data, year)
    pol_data.to_excel(pol_writer, f"pool_opening_{year}")
    pool_data.to_excel(pool_writer, f"pool_opening_{year}")

    pol_data: PolInfoBefPs = pol_bef_ps(pol_data, year, sim)
    pol_data.to_excel(pol_writer, f"pol_bef_ps_{year}")
    pool_data.to_excel(pool_writer, f"pol_bef_ps_{year}")

    pool_data: PoolDFFull = pool_bef_ps(pol_data, pool_data, year, sim)
    pol_data.to_excel(pol_writer, f"pool_bef_ps_{year}")
    pool_data.to_excel(pool_writer, f"pool_bef_ps_{year}")

    pool_data: PoolDFFull = pool_ps(econ_data, pol_data, pool_data, year, sim)
    pol_data.to_excel(pol_writer, f"pool_ps_{year}")
    pool_data.to_excel(pool_writer, f"pool_ps_{year}")

    pol_data: PolInfoClosing = pol_aft_ps(pol_data, pool_data, year, sim)
    pol_data.to_excel(pol_writer, f"pool_aft_ps_{year}")
    pool_data.to_excel(pool_writer, f"pool_aft_ps_{year}")

    pool_data: PoolInfoClosing = pool_closing(pol_data, pool_data, year)
    pol_data.to_excel(pol_writer, f"pool_closing_{year}")
    pool_data.to_excel(pool_writer, f"pool_closing_{year}")

    return pol_data, pool_data


def projection(input_data_pol: InputDataPol,
               input_data_pool: InputDataPool,
               input_data_scen_eco_equity: InputDataScenEcoEquityDF) -> Tuple[PolInfoClosing, PoolDFFull]:
    
    nb_scenarios = 0
    nb_years = 3  #FIXME update to 100 once calculation has been checked
    max_scen_year = 3

    t_start = time()

    pol_data, pool_data = init_model(input_data_pol, input_data_pool)
#    print(compute(pol_data[pol_data.id_sim==0]))

    print()

    pol_writer = pd.ExcelWriter("outputs/pol_data.xlsx", engine='xlsxwriter')
    pool_writer = pd.ExcelWriter("outputs/pool_data.xlsx", engine='xlsxwriter')

    for year in range(nb_years+1):
        print(f"--> year: {year}")
        pol_data, pool_data = one_year(
            pol_writer,
            pool_writer,
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

    pol_writer.save()
    pool_writer.save()

    return pol_data, pool_data