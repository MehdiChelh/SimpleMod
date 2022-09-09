from itertools import product as it_product
from typing import Dict, Optional, get_args, Union

import numpy as np
import pandas as pd
from numpy import dtype

import pandera
from pandera import SchemaModel
from virtual_dataframe import VDataFrame, VSeries

import subprocess


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
        panderaSchema: Union[pandera.typing.DataFrame, pandera.SchemaModel],
        nrows: int = 0,
        default_data: int = 0, ) -> VDataFrame:
    schema = get_args(panderaSchema)[0]
    if not isinstance(schema, SchemaModel):
        schema = schema.to_schema()

    data = {}
    # for name, field in panderaSchema.__fields__.items():
    for name, field in schema.columns.items():
        # dtype = field[0].arg
        dtype = field.dtype.type
        data[name] = VSeries(
            np.full(nrows, default_data),
            index=range(nrows),
            name=name,
            dtype=dtype).compute()
    return VDataFrame(data)


def schema_to_dtypes(panderaSchema: Union[pandera.typing.DataFrame, pandera.SchemaModel],
                     index: Optional[str] = None) -> Dict[str, dtype]:
    schema = get_args(panderaSchema)[0]
    if not isinstance(schema, SchemaModel):
        schema = schema.to_schema()
    d = {k: t.type for k, t in schema.dtypes.items()}
    if index and schema.index:
        d[index] = schema.index.dtype.type
    return d


def save_outputs(data, writer, sheetname, *args, **kwargs):
    # data.to_excel(writer, sheetname, *args, **kwargs)
    pass  # FIXME


def get_git_revision_hash() -> str:
    return subprocess.check_output(['git', 'rev-parse', '--short', 'HEAD']).decode('ascii').strip()
