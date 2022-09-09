import os


SIM_ID = int(os.getenv("SIM_ID") or 0)
SIM_COUNT = int(os.getenv("SIM_COUNT") or 10)

POOL_COUNT = int(os.getenv("POOL_COUNT") or 2)
POL_COUNT = int(os.getenv("POL_COUNT") or 1038 * 1)

NB_YEARS = int(os.getenv("NB_YEARS") or 100)
