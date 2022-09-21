from operator import index
import pickle
import numpy as np
import pandas as pd
from time import time
import dask
from dask.distributed import performance_report
import ray
import cudf

from typing import List, Tuple
from pandera import Int

from simplemod.constants import DISTRIBUTION_STRATEGY, NB_YEARS, POL_PARTITIONS, POOL_PARTITIONS, POL_COUNT, POOL_COUNT, SIM_COUNT, SIM_TOTAL_COUNT, SIM_STRATEGY, DistributionStrategy, SimStrategy
from .formulas import *
from simplemod.utils import init_vdf_from_schema, schema_to_dtypes
from virtual_dataframe import compute, VSeries, VDF_MODE, Mode, VClient, visualize


# @check_types
def init_model(input_data_pol: InputDataPol, input_data_pool: InputDataPool) -> Tuple[PolDF, PoolDFFull]:

    nb_scenarios = SIM_COUNT
    print("---- nb_scenarios:", nb_scenarios)
    nb_pol = POL_COUNT
    nb_pool = POOL_COUNT

    # id_scen = 0 FIXME to remove

    pol_data = init_vdf_from_schema(
        PolDF,
        nrows=nb_scenarios * nb_pol,  # FIXME
        default_data=0,
        npartitions=POL_PARTITIONS)
    pol_data['id_policy'] = VSeries(list(range(nb_pol)) * nb_scenarios)
    pol_data['id_sim'] = VSeries(np.repeat(range(nb_scenarios), nb_pol))
    # pol_data = pol_data.set_index('id_policy')

    pool_data = init_vdf_from_schema(
        PoolDFFull,
        nrows=nb_scenarios * nb_pool,  # FIXME
        default_data=0,
        npartitions=POOL_PARTITIONS)
    # FIXME rather use the index in the input dataframe instead of range(...)
    pool_data['id_pool'] = VSeries(list(range(nb_pool)) * nb_scenarios)
    pool_data['id_sim'] = VSeries(np.repeat(range(nb_scenarios), nb_pool))
    # pool_data = pool_data.set_index('id_pool')

    pol_data = pol_data.drop("id_pool", axis=1).merge(
        input_data_pol[["id_policy", "id_pool"]], on="id_policy", how="left")

    pol_data = pol_data.drop("math_res_closing", axis=1).merge(
        input_data_pol[["id_policy", "math_res"]].rename(columns={"math_res": "math_res_closing"}), on="id_policy", how="left")

    pool_data = pool_data.drop("spread", axis=1).merge(
        input_data_pool[["id_pool", "spread"]], on="id_pool", how="left")
    # print(concat([input_data_pol.loc[:, 'id_pool']] * nb_scenarios, pol_data).compute())
    # pol_data['id_pool'] = concat([input_data_pol.loc[:, 'id_pool']] * nb_scenarios, pol_data)['id_pool']
    # pol_data['math_res_closing'] = concat([input_data_pol.loc[:, 'math_res']]
    #                                       * nb_scenarios, pol_data)
    # pool_data['spread'] = concat([input_data_pool.loc[:, 'spread']] * nb_scenarios, pool_data)
    return pol_data, pool_data


# @delayed(nout=2)
# @check_types
def one_year(
        pol_data: PolDF,
        pool_data: PoolDFFull,
        econ_data: InputDataScenEcoEquityDF,
        year: Int,


) -> Tuple[PolInfoClosing, PoolInfoClosing]:

    pol_data: PolInfoOpening = pol_opening(pol_data, year)

    pool_data: PoolInfoOpening = pool_opening(pool_data, pol_data, year)

    pol_data: PolInfoBefPs = pol_bef_ps(pol_data, year)

    pool_data: PoolDFFull = pool_bef_ps(pol_data, pool_data, year)

    pool_data: PoolDFFull = pool_ps(econ_data, pol_data, pool_data, year)

    pol_data: PolInfoClosing = pol_aft_ps(pol_data, pool_data, year)

    pool_data: PoolInfoClosing = pool_closing(pol_data, pool_data, year)

    return pol_data, pool_data


# @check_types
# @delayed(nout=2)
def _multiple_years(
        pol_data: PolDF,
        pool_data: PoolDFFull,
        econ_data: InputDataScenEcoEquityDF,
        nb_years: Int,
        max_scen_year: Int,
) -> Tuple[PolInfoClosing, PoolInfoClosing]:
    for year in range(nb_years+1):
        print(f"--> year: {year}")
        t_start = time()
        pol_data, pool_data = one_year(
            pol_data,
            pool_data,
            econ_data,
            min(year, max_scen_year),
        )
        t_end = time()
    return pol_data, pool_data
    # TODO: result must be input
    # TODO: save all years into results


multiple_years_delayed = delayed(_multiple_years, nout=2)
multiple_years_remote = ray.remote(_multiple_years).remote


# @delayed(nout=2)
def projection(input_data_pol: InputDataPol,
               input_data_pool: InputDataPool,
               input_data_scen_eco_equity: InputDataScenEcoEquityDF,
               client: Any = None) -> Tuple[PolInfoClosing, PoolDFFull]:

    nb_years = NB_YEARS
    max_scen_year = 3

    t_start = time()

    pol_data, pool_data = init_model(input_data_pol, input_data_pool)

    print()
    if DISTRIBUTION_STRATEGY == DistributionStrategy.dask_delayed:
        multiple_years = multiple_years_delayed
        def finalize(_list): return compute(*_list)
    elif DISTRIBUTION_STRATEGY == DistributionStrategy.virtual_dataframe:
        multiple_years = _multiple_years
        def finalize(_list): return compute(*_list)
    # FIXME : see if we want this to work only when using dask (potentially 2 levels of parallelizations ?) or also when using pandas (1 level of parallelization) ?
    elif DISTRIBUTION_STRATEGY == DistributionStrategy.dask_submit:
        try:
            multiple_years = lambda *args, **kwargs: client.submit(_multiple_years, *args, **kwargs)
            finalize = client.gather
        except:
            multiple_years = lambda *args, **kwargs: _multiple_years(*args, **kwargs)
            def finalize(x): return x
    elif DISTRIBUTION_STRATEGY == DistributionStrategy.ray_remote:
        multiple_years = multiple_years_remote
        finalize = ray.get
    else:
        multiple_years = _multiple_years
        def finalize(x): return x

    if DISTRIBUTION_STRATEGY in (DistributionStrategy.dask_submit, DistributionStrategy.ray_remote):
        pol_data = pol_data.compute()
        pool_data = pool_data.compute()
        input_data_scen_eco_equity = input_data_scen_eco_equity.compute()

    try:
        nb_partitions = pol_data.npartitions
    except:
        nb_partitions = 1

    if SIM_STRATEGY == SimStrategy.loop:
        __call_list = []
        for s in range(SIM_TOTAL_COUNT):
            print(f"==========> sim: {s}")
            tmp_pol_data, tmp_pool_data = pol_data.copy(),  pool_data.copy()  # FIXME see what is the best solution
            tmp_pol_data["id_sim"], tmp_pool_data["id_sim"] = s, s
            __call = multiple_years(
                tmp_pol_data,
                tmp_pool_data,
                input_data_scen_eco_equity.loc[s:s, :],
                nb_years,
                max_scen_year,
            )
            __call_list.append(__call)

        t_compute_call = time()
        res = finalize(__call_list)
        t_end = time()
        if VDF_MODE in (Mode.cudf, Mode.dask_cudf):
            concat = cudf.concat
        else:
            concat = pd.concat
        pol_data = concat([_pol_data for _pol_data, _ in res]).reset_index(drop=True)
        pool_data = concat([_pool_data for _, _pool_data in res]).reset_index(drop=True)

    else:
        _pol_data, _pool_data = multiple_years(
            pol_data,
            pool_data,
            input_data_scen_eco_equity,
            nb_years,
            max_scen_year,
        )
        t_compute_call = time()
        pol_data, pool_data = finalize([_pol_data, _pool_data])
        t_end = time()

    if VDF_MODE in (Mode.cudf, Mode.dask_cudf):
        pol_data = pol_data.to_pandas().sort_values(["id_sim", "id_policy"]).reset_index().drop("index", axis=1)
        pool_data = pool_data.to_pandas().sort_values(["id_sim", "id_pool"]).reset_index().drop("index", axis=1)

    print("=============================")
    print(pool_data)
    print("=============================")
    print(pol_data)

    print('Computed in {:.04f}s'.format((t_end-t_start)))
    print('Compute call lasted {:.04f}s'.format((t_end-t_compute_call)))

    nb_mp, = pol_data["id_policy"].value_counts().shape
    nb_sim, = pol_data["id_sim"].value_counts().shape

    print("Number of MPs: {:}".format(nb_mp))
    print("Number of SIM: {:}".format(nb_sim))

    # pol_data.to_csv(
    #     f"outputs/pol_final_{SIM_STRATEGY.value}_{VDF_MODE.value}_{DISTRIBUTION_STRATEGY.value}.csv", index=False)
    # pool_data.to_csv(
    #     f"outputs/pool_final_{SIM_STRATEGY.value}_{VDF_MODE.value}_{DISTRIBUTION_STRATEGY.value}.csv", index=False)

    return pol_data, pool_data, t_end-t_compute_call, nb_mp, nb_sim, nb_partitions
