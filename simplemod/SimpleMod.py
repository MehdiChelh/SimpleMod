import sys
from typing import get_args

import click
import dotenv
import pandas as pd


from simplemod.constants import SIM_COUNT, POL_COUNT, POOL_COUNT
from simplemod.types import InputDataPol, InputDataPool, InputDataScenEcoEquityDF, PolDF, PoolDF
from simplemod.model import one_year, projection
from simplemod.tools import init_logger, logging
from simplemod.utils import init_vdf_from_schema, schema_to_dtypes
from virtual_dataframe import read_csv, compute

LOGGER = logging.getLogger(__name__)


# %%

@click.command(short_help="Sample for Cardif")
def main() -> int:
    # %%
    input_data_pol = read_csv(
        "./data/mp_policies_1k*.csv",
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

    # %% -- projection
    pol_data, pool_data = projection(input_data_pol,
                                     input_data_pool,
                                     input_data_scen_eco_equity)

    return 0


if __name__ == '__main__':
    init_logger(LOGGER, logging.INFO)

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    if not hasattr(sys, 'frozen') and hasattr(sys, '_MEIPASS'):
        dotenv.load_dotenv(dotenv.find_dotenv())

    sys.exit(main(standalone_mode=False))  # pylint: disable=no-value-for-parameter,unexpected-keyword-arg
