import pandera
import numpy as np


class Pool_schema(pandera.SchemaModel):
    id_pool: pandera.typing.Index[int]
    math_res_opening: pandera.typing.Series[np.float32]
    math_res_before_ps: pandera.typing.Series[np.float32]
    math_res_closing: pandera.typing.Series[np.float32]


PoolDF = pandera.typing.DataFrame[Pool_schema]
PoolInfoClosing = PoolDF
PoolInfoOpening = PoolDF
PoolInfoBefPs = PoolDF


class PoolPsRatesWithSpread_schema(Pool_schema):
    spread: pandera.typing.Series[np.float32]


PoolPsRatesWithSpread = pandera.typing.DataFrame[PoolPsRatesWithSpread_schema]


class PoolPsRates_schema(Pool_schema):
    ps_rate: pandera.typing.Series[np.float32]


PoolPsRates = pandera.typing.DataFrame[PoolPsRates_schema]
