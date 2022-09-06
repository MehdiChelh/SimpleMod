from pandera import SchemaModel
from pandera.typing import Series, DataFrame, Index


class _PolDF_schema(SchemaModel):
    id_sim: Series[int]
    id_policy: Series[int]
    id_pool: Series[int]
    math_res_opening: Series[float]
    math_res_bef_ps: Series[float]
    math_res_closing: Series[float]

    class Config:
        strict = True
        coerce = True


PolDF = DataFrame[_PolDF_schema]
PolInfoOpening = DataFrame[_PolDF_schema]
PolInfoOpening = DataFrame[_PolDF_schema]
PolInfoClosing = DataFrame[_PolDF_schema]
PolInfoBefPs = DataFrame[_PolDF_schema]
