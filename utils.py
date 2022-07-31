from cmath import nan
import pandas as pd
import numpy as np
from itertools import product as it_product
from virtual_dataframe import VDataFrame, VClient, delayed
import pandera as pa


def create_zeros_df(index_shape, cols):
    return VDataFrame(pd.DataFrame(
        data=np.zeros(dtype=np.float32,
                      shape=(np.product(index_shape),
                             len(cols))),
        columns=cols,
        index=pd.MultiIndex.from_tuples(
            [idx for idx in it_product(*[range(l) for l in index_shape])])
    ).reset_index())


def init_vdf_from_schema(
        panderaSchema: pa.model._MetaSchema,
        nrows: int = 0,
        default_data: int = 0, ) -> VDataFrame:
    data = {}
    # for name, field in panderaSchema.__fields__.items():
    for name, field in panderaSchema.to_schema().columns.items():
        # dtype = field[0].arg
        dtype = field.dtype.type
        data[name] = pd.Series(default_data, index=range(
            nrows), name=name, dtype=dtype)
    return VDataFrame(pd.DataFrame(data))
