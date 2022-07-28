import pandera
import numpy as np


class Pol_schema(pandera.SchemaModel):
    id: pandera.typing.Index[int]
    id_pool: pandera.typing.Series[int]
    math_res_opening: pandera.typing.Series[np.float32]
    math_res_before_ps: pandera.typing.Series[np.float32]
    math_res_closing: pandera.typing.Series[np.float32]


PolDF = pandera.typing.DataFrame[Pol_schema]
PolInfoOpening = PolDF
PolInfoClosing = PolDF
PolInfoBefPs = PolDF
