import sys
from tkinter.ttk import Separator
from typing import get_args

import os
import time
import click
import dotenv
from itertools import product
import pandas as pd
import importlib
import subprocess
try:
    from dask.distributed import performance_report
except:
    pass

from simplemod.constants import SIM_COUNT, POL_COUNT, POOL_COUNT
from simplemod.types import InputDataPol, InputDataPool, InputDataScenEcoEquityDF, PolDF, PoolDF
from simplemod.model import one_year, projection
from simplemod.tools import init_logger, logging
from simplemod.utils import init_vdf_from_schema, schema_to_dtypes, get_git_revision_hash
from virtual_dataframe import read_csv, compute, VClient, Mode

LOGGER = logging.getLogger(__name__)


# %%

@click.command(short_help="Sample for Cardif")
def main() -> int:
    CODE_HASH = get_git_revision_hash()
    ROOT_PATH = f"./outputs/benchmarking/{CODE_HASH}"
    os.environ["ROOT_PATH"] = ROOT_PATH
    benchmark_plan = pd.read_csv(f"{ROOT_PATH}/benchmark_plan.csv", sep=",")

    if not os.path.isdir(f"{ROOT_PATH}/dask_reports/"):
        os.makedirs(f"{ROOT_PATH}/dask_reports/")

    results = []

    os.environ["BENCH"] = "yes"

    for i, run_config in benchmark_plan.iterrows():
        print("=================================================================================== row :", i)
        for key, val in run_config.items():
            if key not in ("SIM_ID", "NB_MP"):
                os.environ[key] = str(val)
                print(f"------ {key}:", val)

        RUN_PATH = f"{ROOT_PATH}/{run_config['POL_COUNT']}/{run_config['SIM_TOTAL_COUNT']}/"
        os.makedirs(f"{RUN_PATH}/output/", exist_ok=True)
        os.makedirs(f"{RUN_PATH}/error/", exist_ok=True)
        os.environ["RUN_PATH"] = RUN_PATH

        bashCommand = "python simplemod/SimpleMod.py"
        process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)

        output, error = process.communicate()
        log_name = f"{run_config['SIM_STRATEGY']}-{run_config['DISTRIBUTION_STRATEGY']}-{run_config['VDF_MODE']}"
        with open(f"{RUN_PATH}/output/{log_name}.log", "w") as f:
            f.write(output.decode())
        if error:
            with open(f"{RUN_PATH}/error/{log_name}.log", "w") as f:
                f.write(error.decode())

        print(output.decode() if output else output)
        print(error.decode() if error else error)

    return 0


if __name__ == '__main__':
    init_logger(LOGGER, logging.INFO)

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    if not hasattr(sys, 'frozen') and hasattr(sys, '_MEIPASS'):
        dotenv.load_dotenv(dotenv.find_dotenv())

    sys.exit(main(standalone_mode=False))  # pylint: disable=no-value-for-parameter,unexpected-keyword-arg
