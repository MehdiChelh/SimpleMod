import numpy as np
import pandas as pd
import cudf
from time import time

from typing import Tuple
from pandera import Int

from simplemod.constants import SIM_COUNT, POL_COUNT, POOL_COUNT
from .formulas import *
from simplemod.utils import init_vdf_from_schema, schema_to_dtypes
from virtual_dataframe import compute, VSeries, VDF_MODE, Mode


@check_types
def init_model(input_data_pol: InputDataPol, input_data_pool: InputDataPool) -> Tuple[PolDF, PoolDFFull]:

    nb_scenarios = 1  # SIM_COUNT
    nb_pol = POL_COUNT
    nb_pool = POOL_COUNT

    # id_scen = 0 FIXME to remove

    pol_data = init_vdf_from_schema(
        PolDF,
        nrows=nb_scenarios * nb_pol,  # FIXME
        default_data=0)
    pol_data['id_policy'] = VSeries(list(range(nb_pol)) * nb_scenarios)
    pol_data['id_sim'] = VSeries(np.repeat(range(nb_scenarios), nb_pol))
    pol_data = pol_data.set_index('id_policy')

    pool_data = init_vdf_from_schema(
        PoolDFFull,
        nrows=nb_scenarios * nb_pool,  # FIXME
        default_data=0)
    pool_data['id_pool'] = VSeries(list(range(nb_pool)) * nb_scenarios)
    pool_data['id_sim'] = VSeries(np.repeat(range(nb_scenarios), nb_pool))
    pool_data = pool_data.set_index('id_pool')
    pol_data['id_pool'] = VSeries(input_data_pol.loc[:, 'id_pool'])

    pol_data['math_res_closing'] = VSeries(input_data_pol.loc[:, 'math_res'])
    pool_data['spread'] = VSeries(input_data_pool.loc[:, 'spread'])
    return pol_data, pool_data


# @delayed(nout=2)
# @check_types
def one_year(
        pol_writer,
        pool_writer,
        pol_data: PolDF,
        pool_data: PoolDFFull,
        econ_data: InputDataScenEcoEquityDF,
        year: Int,
        sim: Int,
        nb_sim: Int
) -> Tuple[PolInfoClosing, PoolInfoClosing]:

    pol_data: PolInfoOpening = pol_opening(pol_data, year, sim)
    # pol_data.to_excel(pol_writer, f"pol_opening_{year}")
    # pool_data.to_excel(pool_writer, f"pol_opening_{year}")

    pool_data: PoolInfoOpening = pool_opening(pool_data, pol_data, year)
    # pol_data.to_excel(pol_writer, f"pool_opening_{year}")
    # pool_data.to_excel(pool_writer, f"pool_opening_{year}")

    pol_data: PolInfoBefPs = pol_bef_ps(pol_data, year, sim)
    # pol_data.to_excel(pol_writer, f"pol_bef_ps_{year}")
    # pool_data.to_excel(pool_writer, f"pol_bef_ps_{year}")

    pool_data: PoolDFFull = pool_bef_ps(pol_data, pool_data, year, sim)
    # pol_data.to_excel(pol_writer, f"pool_bef_ps_{year}")
    # pool_data.to_excel(pool_writer, f"pool_bef_ps_{year}")

    pool_data: PoolDFFull = pool_ps(econ_data, pol_data, pool_data, year, sim)
    # pol_data.to_excel(pol_writer, f"pool_ps_{year}")
    # pool_data.to_excel(pool_writer, f"pool_ps_{year}")

    pol_data: PolInfoClosing = pol_aft_ps(pol_data, pool_data, year, sim)
    # pol_data.to_excel(pol_writer, f"pool_aft_ps_{year}")
    # pool_data.to_excel(pool_writer, f"pool_aft_ps_{year}")

    pool_data: PoolInfoClosing = pool_closing(pol_data, pool_data, year)
    # pol_data.to_excel(pol_writer, f"pool_closing_{year}")
    # pool_data.to_excel(pool_writer, f"pool_closing_{year}")

    return pol_data, pool_data


@delayed(nout=2)
# @check_types
def multiple_years(
        pol_writer,
        pool_writer,
        pol_data: PolDF,
        pool_data: PoolDFFull,
        econ_data: InputDataScenEcoEquityDF,
        nb_years: Int,
        max_scen_year: Int,
        sim: Int,
        nb_sim: Int
) -> Tuple[PolInfoClosing, PoolInfoClosing]:
    for year in range(nb_years+1):
        print(f"--> year: {year}")
        pol_data, pool_data = one_year(
            pol_writer,
            pool_writer,
            pol_data,
            pool_data,
            econ_data,
            min(year, max_scen_year),
            sim,
            SIM_COUNT
        )
    return pol_data, pool_data
    # TODO: result must be input
    # TODO: save all years into results


def projection(input_data_pol: InputDataPol,
               input_data_pool: InputDataPool,
               input_data_scen_eco_equity: InputDataScenEcoEquityDF) -> Tuple[PolInfoClosing, PoolDFFull]:

    nb_scenarios = SIM_COUNT
    nb_years = 3  # FIXME update to 100 once calculation has been checked
    max_scen_year = 3

    t_start = time()

    pol_data, pool_data = init_model(input_data_pol, input_data_pool)
#    print(compute(pol_data[pol_data.id_sim==0]))

    print()

    pol_writer = pd.ExcelWriter("outputs/pol_data.xlsx", engine='xlsxwriter')
    pool_writer = pd.ExcelWriter("outputs/pool_data.xlsx", engine='xlsxwriter')

    # for year in range(nb_years+1):
    #     print(f"--> year: {year}")
    #     pol_data, pool_data = one_year(
    #         pol_writer,
    #         pool_writer,
    #         pol_data,
    #         pool_data,
    #         input_data_scen_eco_equity,
    #         min(year, max_scen_year),
    #         None,
    #         SIM_COUNT
    #     )
    #     # TODO: result must be input
    #     # TODO: save all years into results

    # pol_data_ = pol_data.compute()
    # # pool_data_ = pool_data.compute()

    pol_data_list = []
    pool_data_list = []
    for sim in range(nb_scenarios+1):
        _call = multiple_years(
            pol_writer,
            pool_writer,
            pol_data,
            pool_data,
            input_data_scen_eco_equity.loc[:nb_scenarios, :],
            nb_years,
            max_scen_year,
            sim,
            SIM_COUNT
        )

        if VDF_MODE in (Mode.dask, Mode.dask_cudf):  # FIXME this should be in virtual_dataframe package
            pol_data, pool_data = _call.compute()
        else:
            pol_data, pool_data = _call
        pol_data_list.append(pol_data)
        pool_data_list.append(pool_data)

    if VDF_MODE in (Mode.dask, Mode.pandas):
        pol_data = pd.concat(pol_data_list)
        pool_data = pd.concat(pool_data_list)
    elif VDF_MODE in (Mode.dask_cudf, Mode.cudf):
        pol_data = cudf.concat(pol_data_list)
        pool_data = cudf.concat(pool_data_list)

    t_end = time()

    print('Computed in {:.04f}s'.format((t_end-t_start)))

    print(f"outputs/pol_final_{VDF_MODE}.csv")
    pol_data.to_csv(f"outputs/pol_final_{VDF_MODE}.csv")
    print(f"outputs/pool_final_{VDF_MODE}.csv")
    pool_data.to_csv(f"outputs/pool_final_{VDF_MODE}.csv")

    pol_writer.save()
    pool_writer.save()

    return pol_data, pool_data
