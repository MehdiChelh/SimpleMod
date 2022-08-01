import pandera
import numpy as np


class Pol_schema(pandera.SchemaModel):
    id_sim:  pandera.typing.Series[int]
    id_pol: pandera.typing.Series[int]
    id_pool: pandera.typing.Series[int]
    math_res_opening: pandera.typing.Series[np.float]
    math_res_bef_ps: pandera.typing.Series[np.float]
    math_res_closing: pandera.typing.Series[np.float]


PolDF = pandera.typing.DataFrame[Pol_schema]
PolInfoOpening = PolDF
PolInfoClosing = PolDF
PolInfoBefPs = PolDF
