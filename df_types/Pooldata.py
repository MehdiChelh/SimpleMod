import pandera


class Pooldata_schema(pandera.SchemaModel):
    id_pool: pandera.typing.Series[int]
    spread: pandera.typing.Series[float]


PooldataDF = pandera.typing.DataFrame[Pooldata_schema]
