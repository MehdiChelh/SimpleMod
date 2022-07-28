import pandera


class Poldata_schema(pandera.SchemaModel):
    id_policy: pandera.typing.Index[int]
    id_pool: pandera.typing.Series[int]
    Age: pandera.typing.Series[int]
    math_res: pandera.typing.Series[int]


PoldataDF = pandera.typing.DataFrame[Poldata_schema]
