import pandera
import numpy as np


class Pool_schema(pandera.SchemaModel):
    id_sim:  pandera.typing.Series[int]
    id_pool: pandera.typing.Series[int]
    math_res_opening: pandera.typing.Series[np.float64]
    math_res_bef_ps: pandera.typing.Series[np.float64]
    math_res_closing: pandera.typing.Series[np.float64]
    spread: pandera.typing.Series[np.float64]  # FIXME : unify float types


PoolDF = pandera.typing.DataFrame[Pool_schema]
PoolInfoClosing = PoolDF
PoolInfoOpening = PoolDF
PoolInfoBefPs = PoolDF


class PoolPsRatesWithSpread_schema(Pool_schema):
    spread: pandera.typing.Series[np.float64]


PoolPsRatesWithSpread = pandera.typing.DataFrame[PoolPsRatesWithSpread_schema]


class PoolPsRates_schema(Pool_schema):
    ps_rate: pandera.typing.Series[np.float64]


PoolPsRates = pandera.typing.DataFrame[PoolPsRates_schema]
