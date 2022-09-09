from operator import index
import sys
from typing import get_args

import click
import dotenv
import pandas as pd
import time
import os
try:
    from dask.distributed import performance_report
except:
    pass


from simplemod.constants import SIM_COUNT, POL_COUNT, POOL_COUNT
from simplemod.types import InputDataPol, InputDataPool, InputDataScenEcoEquityDF, PolDF, PoolDF
from simplemod.model import one_year, projection
from simplemod.tools import init_logger, logging
from simplemod.utils import init_vdf_from_schema, schema_to_dtypes
from virtual_dataframe import read_csv, compute, VClient, VDF_MODE, Mode

LOGGER = logging.getLogger(__name__)


# %%

@click.command(short_help="Sample for Cardif")
def main() -> int:
    # %%
    input_data_pol = read_csv(
        "./data/mp_policies_10k.csv",
        dtype=schema_to_dtypes(InputDataPol, "id_policy"),
    ).set_index("id_policy", drop=True)

    # %%
    input_data_pool = read_csv(
        "./data/mp_pool*.csv",
        dtype=schema_to_dtypes(InputDataPool, "id_pool")
    ).set_index("id_pool")

    # %%
    input_data_scen_eco_equity = read_csv(
        "./data/scen_eco_sample*.csv",
        dtype=schema_to_dtypes(InputDataScenEcoEquityDF, "id_sim")
    ).set_index("id_sim").loc[:SIM_COUNT, :]  # FIXME implement read_csv_with_schema / DataFrame_with_schema

    # print(input_data_scen_eco_equity.head())

    BENCH = os.getenv("BENCH")
    if not BENCH:
        with VClient():
            pol_data, pool_data = projection(input_data_pol,
                                             input_data_pool,
                                             input_data_scen_eco_equity)
            print(pol_data.head())
            print(pol_data.shape)
    else:
        ROOT_PATH = os.getenv("ROOT_PATH")
        CODE_HASH = os.getenv("CODE_HASH")

        t0 = time.time()
        # print(input_data_scen_eco_equity.head())
        with VClient():
            if VDF_MODE in (Mode.dask, Mode.dask_cudf):
                with performance_report(filename=f"{ROOT_PATH}/dask_reports/SIM_{SIM_COUNT}-MP_{POL_COUNT}-MODE_{VDF_MODE}_report.html"):
                    # %% -- projection
                    pol_data, pool_data = projection(input_data_pol,
                                                     input_data_pool,
                                                     input_data_scen_eco_equity)
            else:
                pol_data, pool_data = projection(input_data_pol,
                                                 input_data_pool,
                                                 input_data_scen_eco_equity)
        t1 = time.time()
        result = pd.DataFrame({
            "time": [t0 - t1],
            "VDF_MODE": [VDF_MODE],
            "SIM_COUNT": [SIM_COUNT],
            "POL_COUNT": [POL_COUNT],
            "CODE_HASH": [CODE_HASH]
        })

        df = pd.read_csv(f"{ROOT_PATH}/results.csv")
        pd.concat([df, result]).to_csv(f"{ROOT_PATH}/results.csv", index=False)
    return 0


if __name__ == '__main__':
    init_logger(LOGGER, logging.INFO)

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    if not hasattr(sys, 'frozen') and hasattr(sys, '_MEIPASS'):
        dotenv.load_dotenv(dotenv.find_dotenv())

    sys.exit(main(standalone_mode=False))  # pylint: disable=no-value-for-parameter,unexpected-keyword-arg
