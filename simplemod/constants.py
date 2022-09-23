import os
from enum import Enum


class SimStrategy(Enum):
    loop = "loop"
    default = "default"
    map_partitions = "map_partitions"


SIM_STRATEGY: SimStrategy = SimStrategy[(os.getenv("SIM_STRATEGY") or "default")]
SIM_DEFAULT_COUNT = 10

SIM_ID = int(os.getenv("SIM_ID") or 0)  # FIXME not used anymore
SIM_COUNT = 1 if SIM_STRATEGY == SimStrategy.loop else int(
    os.getenv("SIM_COUNT") or SIM_DEFAULT_COUNT)  # Used to set dataframe's dimension
SIM_TOTAL_COUNT = SIM_COUNT if SIM_STRATEGY != SimStrategy.loop else int(
    os.getenv("SIM_COUNT") or SIM_DEFAULT_COUNT)  # Used to get the number of simulations for the run


class DistributionStrategy(Enum):
    virtual_dataframe = "virtual_dataframe"
    dask_delayed = "dask_delayed"
    dask_submit = "dask_submit"
    ray_remote = "ray_remote"


DISTRIBUTION_STRATEGY: DistributionStrategy = DistributionStrategy[(
    os.getenv("DISTRIBUTION_STRATEGY") or "virtual_dataframe")]

POOL_COUNT = int(os.getenv("POOL_COUNT") or 2)
POL_COUNT = int(os.getenv("POL_COUNT") or 1038 * 1)

POL_PARTITIONS = int(os.getenv("POL_PARTITIONS") or 1)
POOL_PARTITIONS = int(os.getenv("POOL_PARTITIONS") or 1)

NB_YEARS = int(os.getenv("NB_YEARS") or 1)
