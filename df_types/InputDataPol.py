import pandera


class InputDataPol_schema(pandera.SchemaModel):
    id_policy: pandera.typing.Index[int]
    id_pool: pandera.typing.Series[int]
    age: pandera.typing.Series[int]
    math_res: pandera.typing.Series[int]


InputDataPol = pandera.typing.DataFrame[InputDataPol_schema]
