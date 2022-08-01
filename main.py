from virtual_dataframe import VDataFrame, VClient
from df_types import *
import pandas as pd
from utils import init_vdf_from_schema, save_outputs
from model import *
from constants import *


if __name__ == "__main__":
    # -- load data
    input_data_pol = VDataFrame(
        pd.read_csv("./data/mp_policies.csv",
                    dtype={
                        "id_policy": int,
                        "id_pool": int,
                        "age": int,
                        "math_res": float
                    }, index_col=0))
    input_data_pool = VDataFrame(
        pd.read_csv("./data/mp_pool.csv", index_col=0))
    input_data_scen_eco_equity = VDataFrame(
        pd.read_csv("./data/scen_eco_sample.csv",
                    dtype={
                        "id_sim": int,
                        "year_0": float,
                        "year_1": float,
                        "year_2": float,
                        "year_3": float,
                        "measure": str
                    },
                    index_col=0)).loc[:SIM_COUNT, :]  # FIXME implement read_csv_with_schema / DataFrame_with_schema

    # -- dimensions
    nb_pol, _ = input_data_pol.shape
    nb_pool, _ = input_data_pool.shape
    nb_scenarios = SIM_COUNT if SIM_COUNT else input_data_scen_eco_equity.shape

    # -- initialize calculation dataframes
    pol_data = init_vdf_from_schema(
        Pol_schema, nrows=nb_scenarios * nb_pol, default_data=0)
    pool_data = init_vdf_from_schema(
        Pool_schema, nrows=nb_scenarios * nb_pool, default_data=0)
    # FIXME this section could be improved (would be straightforward if we have xarrays)
    pol_data["id_sim"] = np.array(
        [[i for j in range(nb_pol)] for i in range(nb_scenarios)]).reshape(nb_scenarios * nb_pol)
    pool_data["id_sim"] = np.array(
        [[i for j in range(nb_pool)] for i in range(nb_scenarios)]).reshape(nb_scenarios * nb_pool)

    pol_writer = pd.ExcelWriter("outputs/pol_data.xlsx", engine='xlsxwriter')
    pool_writer = pd.ExcelWriter("outputs/pool_data.xlsx", engine='xlsxwriter')

    # -- projection
    for year in range(3):
        print(f"--> year: {year}")
        pol_data, pool_data = one_year(
            pol_writer,
            pool_writer,
            input_data_pol,
            input_data_pool,
            input_data_scen_eco_equity,
            pol_data,
            pool_data,
            year,
            nb_scenarios
        )
    pol_writer.save()
    pol_writer.close()
    pool_writer.save()
    pool_writer.close()
