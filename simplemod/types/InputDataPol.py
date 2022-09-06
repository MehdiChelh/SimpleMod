from pandera import SchemaModel
from pandera.typing import Index, Series, DataFrame


class _InputDataPol_schema(SchemaModel):
    id_policy: Index[int]
    id_pool: Series[int]
    age: Series[int]
    math_res: Series[float]

    class Config:
        strict = True
        coerce = True


InputDataPol = DataFrame[_InputDataPol_schema]
