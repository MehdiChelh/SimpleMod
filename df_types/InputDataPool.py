import pandera


class InputDataPool_schema(pandera.SchemaModel):
    id_pool: pandera.typing.Series[int]
    spread: pandera.typing.Series[float]


InputDataPool = pandera.typing.DataFrame[InputDataPool_schema]
