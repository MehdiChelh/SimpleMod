import pandera


class Portfolio_schema(pandera.SchemaModel):
    id: pandera.typing.Index[int]
    id_policy: pandera.typing.Series[int]
    id_pool: pandera.typing.Series[int]
    Age: pandera.typing.Series[int]
    math_res: pandera.typing.Series[int]
    spread: pandera.typing.Series[float]


PortfolioDF = pandera.typing.DataFrame[Portfolio_schema]
