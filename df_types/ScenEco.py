from pandera.typing import *
import pandera


class ScenEcoEquity_schema(pandera.SchemaModel):
    id_sim: Index[str]
    year_0: Series[int]
    year_1: Series[int]
    year_2: Series[int]
    year_3: Series[int]
    Measure: Series[str]  # FIXME


BEGIN_YEAR_POS = 0  # FIXME
ScenEcoEquityDF = DataFrame[ScenEcoEquity_schema]
