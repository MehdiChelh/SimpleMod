from pandera import SchemaModel
from pandera.typing import Index, Series, DataFrame


class _InputDataPool_schema(SchemaModel):
    id_pool: Index[int]
    spread: Series[float]

    class Config:
        strict = True
        coerce = True


InputDataPool = DataFrame[_InputDataPool_schema]
