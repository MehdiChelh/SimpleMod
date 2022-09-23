from http.client import OK
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


from simplemod.constants import DISTRIBUTION_STRATEGY, NB_YEARS, POL_PARTITIONS, POOL_PARTITIONS, SIM_COUNT, POL_COUNT, POOL_COUNT, SIM_ID, SIM_STRATEGY, SIM_TOTAL_COUNT
from simplemod.types import InputDataPol, InputDataPool, InputDataScenEcoEquityDF, PolDF, PoolDF
from simplemod.model import one_year, projection
from simplemod.tools import init_logger, logging
from simplemod.utils import init_vdf_from_schema, schema_to_dtypes
from virtual_dataframe import read_csv, compute, VClient, VDF_MODE, Mode

LOGGER = logging.getLogger(__name__)


# %%

@click.command(short_help="Sample for Cardif")
@click.option("--out", default="./tmp", help="Folder for saving outputs")
@click.option("--suffix", default="", help="Suffix for saving pol_data and pool_data")
def main(out, suffix) -> int:
    input_data_pol = read_csv(
        {
            # "./data/mp_policies_1k.csv",
            # ["http://10.65.1.133:8000/mp_policies_1k.csv" for i in range(1)],
            1038: "http://10.65.1.133:8000/mp_policies_1k.csv",
            # "./data/mp_policies_1k-*.csv",
            # [f"http://10.65.1.133:8000/mp_policies_10k.csv" for i in range(1)],
            10380: "http://10.65.1.133:8000/mp_policies_10k.csv",
            # "./data/mp_policies_10k-*.csv",
            # [f"http://10.65.1.133:8000/mp_policies_100k.csv" for i in range(1)],
            103800: "http://10.65.1.133:8000/mp_policies_100k.csv",
            # "./data/mp_policies_100k-*.csv",
            # ["http://10.65.1.133:8000/mp_policies_1M.csv" for i in range(1)],
            1038000: "http://10.65.1.133:8000/mp_policies_1M.csv",
        }[(POL_COUNT)],
        dtype=schema_to_dtypes(InputDataPol, "id_policy"),
    )  # .set_index("id_policy", drop=True)

    input_data_pool = read_csv(
        "./data/mp_pool*.csv",
        dtype=schema_to_dtypes(InputDataPool, "id_pool")
    )  # .set_index("id_pool")

    input_data_scen_eco_equity = read_csv(
        "./data/scen_eco_sample*.csv",
        dtype=schema_to_dtypes(InputDataScenEcoEquityDF, "id_sim")
    ).loc[:SIM_TOTAL_COUNT, :]  # .set_index("id_sim").loc[:SIM_TOTAL_COUNT, :]  # FIXME implement read_csv_with_schema / DataFrame_with_schema

    BENCH = os.getenv("BENCH")
    if not BENCH:
        with VClient() as client:

            t_start = time.time()
            pol_data, pool_data, *rest = projection(input_data_pol,
                                                    input_data_pool,
                                                    input_data_scen_eco_equity,
                                                    client)
            t_end = time.time()
        pol_data.to_csv(f"{out}/pol_data{suffix}.csv", index=False)
        pool_data.to_csv(f"{out}/pool_data{suffix}.csv", index=False)
    else:
        ROOT_PATH = os.environ["ROOT_PATH"]
        RUN_PATH = os.environ["RUN_PATH"]
        CODE_HASH = os.getenv("CODE_HASH")

        with VClient() as client:
            if VDF_MODE in (Mode.dask, Mode.dask_cudf):
                client.restart()
                with performance_report(
                        filename=f"{RUN_PATH}/{SIM_STRATEGY}-{DISTRIBUTION_STRATEGY}-{VDF_MODE}-report.html"):
                    t0 = time.time()
                    pol_data, pool_data, compute_time, nb_mp, nb_sim, nb_partitions = projection(input_data_pol,
                                                                                                 input_data_pool,
                                                                                                 input_data_scen_eco_equity,
                                                                                                 client)
                    t1 = time.time()
            else:
                t0 = time.time()
                pol_data, pool_data, compute_time, nb_mp, nb_sim, nb_partitions = projection(input_data_pol,
                                                                                             input_data_pool,
                                                                                             input_data_scen_eco_equity,
                                                                                             client)
                t1 = time.time()

        pol_data.to_csv(f"{RUN_PATH}/pol_data-{SIM_STRATEGY.value}-{DISTRIBUTION_STRATEGY.value}-{VDF_MODE.value}.csv")
        pool_data.to_csv(
            f"{RUN_PATH}/pool_data-{SIM_STRATEGY.value}-{DISTRIBUTION_STRATEGY.value}-{VDF_MODE.value}.csv")

        data = {"CODE_HASH": [CODE_HASH],
                "NB_YEARS": [NB_YEARS],
                "POOL_COUNT": [POOL_COUNT],
                "POOL_PARTITIONS": [POOL_PARTITIONS],
                "POL_COUNT": [POL_COUNT],
                "POL_PARTITIONS": [POL_PARTITIONS],
                "SIM_COUNT": [SIM_COUNT],
                "SIM_TOTAL_COUNT": [SIM_TOTAL_COUNT],
                "SIM_ID": [SIM_ID],
                "SIM_STRATEGY": [SIM_STRATEGY],
                "DISTRIBUTION_STRATEGY": [DISTRIBUTION_STRATEGY],
                "VDF_MODE": [VDF_MODE],
                "time": [t1 - t0],
                "compute_time": [compute_time],
                "check_POL_COUNT": [nb_mp],
                "check_SIM_COUNT": [nb_sim],
                "check_nb_partitions": [nb_partitions],
                "outputs": [RUN_PATH]}

        pd.concat([pd.read_csv(f"{ROOT_PATH}/results.csv"), pd.DataFrame(data)]
                  ).to_csv(f"{ROOT_PATH}/results.csv", index=False)
    return 0


if __name__ == '__main__':
    init_logger(LOGGER, logging.INFO)

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    if not hasattr(sys, 'frozen') and hasattr(sys, '_MEIPASS'):
        dotenv.load_dotenv(dotenv.find_dotenv())

    sys.exit(main(standalone_mode=False))  # pylint: disable=no-value-for-parameter,unexpected-keyword-arg
