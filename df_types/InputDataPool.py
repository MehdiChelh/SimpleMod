import pandera
import numpy as np


class InputDataPool_schema(pandera.SchemaModel):
    id_pool: pandera.typing.Index[int]
    spread: pandera.typing.Series[np.float64]


InputDataPool = pandera.typing.DataFrame[InputDataPool_schema]
