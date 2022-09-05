from pandera import SchemaModel
from pandera.checks import SeriesCheckObj
from pandera.typing import Series, DataFrame, Index


class _PoolDF_schema(SchemaModel):
    id_sim: Series[int]
    id_pool: Index[int]
    math_res_opening: Series[float]
    math_res_bef_ps: Series[float]
    math_res_closing: Series[float]

    class Config:
        strict = True
        coerce = True


PoolDF = DataFrame[_PoolDF_schema]


class _PoolDFFull_schema(_PoolDF_schema):
    spread: Series[float]
    ps_rate: Series[float]
    tot_return: Series[float]

    class Config:
        strict = True
        coerce = True


PoolDFFull = DataFrame[_PoolDFFull_schema]
PoolInfoClosing = DataFrame[_PoolDFFull_schema]
PoolInfoOpening = DataFrame[_PoolDFFull_schema]
PoolInfoBefPs = DataFrame[_PoolDFFull_schema]
