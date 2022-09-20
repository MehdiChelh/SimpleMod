import time
import pandas as pd
import os
from unittest import mock
from importlib import reload
import sys
import dask.dataframe
import cudf
import dask_cudf

import virtual_dataframe
from simplemod.types import (InputDataPol, InputDataPool, InputDataScenEcoEquityDF)
from simplemod.utils import schema_to_dtypes
from simplemod.constants import SIM_TOTAL_COUNT
from simplemod.model.projection import projection


def dep_reload():
    for k, v in list(sys.modules.items()):
        if k.startswith('virtual_dataframe') or k.startswith('simplemod'):
            reload(v)


def _projection(*args, **kwargs):
    input_data_pol = virtual_dataframe.read_csv(
        "./data/mp_policies_1k.csv",
        dtype=schema_to_dtypes(InputDataPol, "id_policy"),
    )  # .set_index("id_policy", drop=True)

    input_data_pool = virtual_dataframe.read_csv(
        "./data/mp_pool*.csv",
        dtype=schema_to_dtypes(InputDataPool, "id_pool")
    )  # .set_index("id_pool")

    input_data_scen_eco_equity = virtual_dataframe.read_csv(
        "./data/scen_eco_sample*.csv",
        dtype=schema_to_dtypes(InputDataScenEcoEquityDF, "id_sim")
    ).loc[:SIM_TOTAL_COUNT, :]  # .set_index("id_sim").loc[:SIM_TOTAL_COUNT, :]  # FIXME implement read_csv_with_schema / DataFrame_with_schema

    with virtual_dataframe.VClient() as client:
        pol_data, pool_data = projection(input_data_pol,
                                         input_data_pool,
                                         input_data_scen_eco_equity,
                                         client)
    return pol_data, pool_data


def test_projection_1y_pandas() -> None:
    os.environ["VDF_MODE"] = "pandas"
    dep_reload()

    assert (virtual_dataframe.env.VDF_MODE == virtual_dataframe.env.Mode.pandas)
    assert isinstance(virtual_dataframe.vpandas.VDataFrame(), pd.DataFrame)

    pol_data, pool_data = _projection()

    expected_pol_data = pd.read_csv("./tests/pol_1y.csv")
    expected_pool_data = pd.read_csv("./tests/pool_1y.csv")

    max_pol_data_error = (pol_data - expected_pol_data).abs().max().max()
    max_pool_data_error = (pool_data - expected_pool_data).abs().max().max()

    assert max(max_pol_data_error, max_pool_data_error) < 1


def test_projection_1y_dask() -> None:
    os.environ["VDF_MODE"] = "dask"
    dep_reload()

    assert virtual_dataframe.env.VDF_MODE == virtual_dataframe.env.Mode.dask
    assert isinstance(virtual_dataframe.vpandas.VDataFrame(), dask.dataframe.DataFrame)

    pol_data, pool_data = _projection()

    expected_pol_data = pd.read_csv("./tests/pol_1y.csv")
    expected_pool_data = pd.read_csv("./tests/pool_1y.csv")

    assert (pol_data - expected_pol_data).abs().max().max() < 1
    assert (pool_data - expected_pool_data).abs().max().max() < 1


def test_projection_1y_cudf() -> None:
    os.environ["VDF_MODE"] = "cudf"
    dep_reload()

    assert (virtual_dataframe.env.VDF_MODE == virtual_dataframe.env.Mode.cudf)
    assert isinstance(virtual_dataframe.vpandas.VDataFrame(), cudf.DataFrame)

    pol_data, pool_data = _projection()

    expected_pol_data = pd.read_csv("./tests/pol_1y.csv")
    expected_pool_data = pd.read_csv("./tests/pool_1y.csv")

    max_pol_data_error = (pol_data - expected_pol_data).abs().max().max()
    max_pool_data_error = (pool_data - expected_pool_data).abs().max().max()

    assert max(max_pol_data_error, max_pool_data_error) < 1


def test_projection_1y_dask_cudf() -> None:
    os.environ["VDF_MODE"] = "dask_cudf"
    dep_reload()

    assert (virtual_dataframe.env.VDF_MODE == virtual_dataframe.env.Mode.dask_cudf)
    assert isinstance(virtual_dataframe.vpandas.VDataFrame(), dask_cudf.DataFrame)

    pol_data, pool_data = _projection()

    expected_pol_data = pd.read_csv("./tests/pol_1y.csv")
    expected_pool_data = pd.read_csv("./tests/pool_1y.csv")

    max_pol_data_error = (pol_data - expected_pol_data).abs().max().max()
    max_pool_data_error = (pool_data - expected_pool_data).abs().max().max()

    assert max(max_pol_data_error, max_pool_data_error) < 1
