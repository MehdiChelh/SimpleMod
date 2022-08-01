from virtual_dataframe import delayed
from utils import save_outputs
from df_types import *
from typing import Tuple
from .formulas import *


@delayed
@pandera.check_types
def one_year(
        pol_writer,
        pool_writer,
        input_data_pol: InputDataPol,
        input_data_pool: InputDataPool,
        input_data_scen_eco_equity: InputDataScenEcoEquityDF,
        pol_data: PolDF,
        pool_data: PoolDF,
        year: int,
        nb_sim: int
) -> Tuple[PoolInfoClosing, PolInfoClosing]:
    # Initialisation
    if year == 0:
        # FIXME need to be changed when simulation is added to pol_data.index
        # FIXME : quickfix, but it would be better if we can broadcast the asignement along the axis (may be easier once we have xarrays)
        pol_data["id_pol"] = input_data_pol.index.to_list() * nb_sim
        # FIXME need to be changed when simulation is added to pool_data.index
        # FIXME : quickfix, but it would be better if we can broadcast the asignement along the axis (may be easier once we have xarrays)
        pool_data["id_pool"] = input_data_pool.index.to_list() * nb_sim

    pol_data: PolInfoOpening = pol_opening(
        input_data_pol, pol_data, year, nb_sim)
    save_outputs(pol_data, pol_writer, f"pol_opening_{year}")
    save_outputs(pool_data, pool_writer, f"pol_opening_{year}")

    pol_data: PolInfoBefPs = pol_bef_ps(pol_data, year)
    save_outputs(pol_data, pol_writer, f"pol_bef_ps_{year}")
    save_outputs(pool_data, pool_writer, f"pol_bef_ps_{year}")

    pool_data: PoolInfoBefPs = pool_bef_ps(pol_data, pool_data)
    save_outputs(pol_data, pol_writer, f"pool_bef_ps_{year}")
    save_outputs(pool_data, pool_writer, f"pool_bef_ps_{year}")

    pool_data: PoolPsRates = pool_ps(input_data_scen_eco_equity,
                                     input_data_pool, pool_data, year, nb_sim)
    save_outputs(pol_data, pol_writer, f"pool_ps_{year}")
    save_outputs(pool_data, pool_writer, f"pool_ps_{year}")

    pol_data: PolInfoClosing = pol_aft_ps(pol_data, pool_data, year)
    save_outputs(pol_data, pol_writer, f"pol_aft_ps_{year}")
    save_outputs(pool_data, pool_writer, f"pol_aft_ps_{year}")

    pool_data: PoolInfoClosing = pool_closing(pol_data, pool_data, year)
    save_outputs(pol_data, pol_writer, f"pool_closing_{year}")
    save_outputs(pool_data, pool_writer, f"pool_closing_{year}")

    pool_data: PoolInfoOpening = pool_opening(pool_data, pol_data, year)
    save_outputs(pol_data, pol_writer, f"pool_opening_{year}")
    save_outputs(pool_data, pool_writer, f"pool_opening_{year}")

    return pol_data, pool_data
