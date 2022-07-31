from pandera.typing import *
import pandera


class InputDataScenEcoEquity_schema(pandera.SchemaModel):
    id_sim: Index[int]
    year_0: Series[float]
    year_1: Series[float]
    year_2: Series[float]
    year_3: Series[float]
    measure: Series[str]  # FIXME


BEGIN_YEAR_POS = 0  # FIXME
InputDataScenEcoEquityDF = DataFrame[InputDataScenEcoEquity_schema]
