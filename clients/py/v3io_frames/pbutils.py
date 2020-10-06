# Copyright 2018 Iguazio
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import warnings
from datetime import datetime

import google.protobuf.pyext._message as message
import numpy as np
import pandas as pd
import pytz
from google.protobuf.message import Message
from pandas.api.types import is_datetime64_any_dtype as is_datetime
from pandas.api.types import is_integer_dtype as is_integer
from pandas.api.types import is_float_dtype as is_float
from pandas.api.types import is_bool_dtype as is_bool
from pandas.api.types import is_string_dtype as is_string
from pandas.core.dtypes.dtypes import CategoricalDtype

from . import frames_pb2 as fpb
from .dtypes import dtype_of
from .errors import MessageError, WriteError

pb_list_types = (
    message.RepeatedCompositeContainer,
    message.RepeatedScalarContainer,
)


def pb_value(v):
    """Convert Python type to frames_pb2.Value"""
    if v is None:
        return None

    dtype = dtype_of(v)
    kw = {dtype.val_key: v}
    return fpb.Value(**kw)


def pb_map(d):
    """Convert map values to frames_pb2.Value"""
    return None if d is None else {k: pb_value(v) for k, v in d.items()}


def SchemaField(name=None, doc=None, default=None, type=None, properties=None):
    """A schema field"""
    # We return a frames_pb2.SchemaField from Python types
    return fpb.SchemaField(
        name=name,
        doc=doc,
        default=pb_value(default),
        type=type,
        properties=pb_map(properties),
    )


def pb2py(obj):
    """Convert protobuf object to Python object"""
    if isinstance(obj, fpb.Value):
        return getattr(obj, obj.WhichOneof('value'))

    if isinstance(obj, Message):
        return {
            field.name: pb2py(value) for field, value in obj.ListFields()
        }

    if isinstance(obj, pb_list_types):
        return [pb2py(v) for v in obj]

    if isinstance(obj, message.MessageMapContainer):
        return {
            key: pb2py(value) for key, value in obj.items()
        }

    return obj


def msg2df(frame, frame_factory, columns=None, do_reorder=True):
    # Treat Null typed index as pandas range-index
    if len(frame.indices) == 1 and frame.indices[0].dtype == fpb.NULL:
        new_index = None
    else:
        indices = [col2series(idx, None) for idx in frame.indices]
        if len(indices) == 1:
            new_index = indices[0]
        elif len(indices) > 1:
            new_index = pd.MultiIndex.from_arrays(indices)
        else:
            new_index = None

    data = {col.name: col2series(col, new_index) for col in frame.columns}

    df = frame_factory(data, new_index)

    # IG-14809 - return an empty dataframe (without columns metadata) in case of empty dataset
    if df.empty:
        return frame_factory({}, new_index)

    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        df.labels = pb2py(frame.labels)

    if do_reorder:
        if columns:
            df = df.reindex(columns=columns)
        else:
            is_range = True
            indices = [False] * len(df.columns)
            for name in df.columns:
                try:
                    if name.startswith('column_'):
                        col_index = int(name[len('column_'):])
                        if col_index < len(indices):
                            indices[col_index] = True
                            continue
                except ValueError:
                    pass
                is_range = False
                break

            if is_range and all(elem for elem in indices):
                renameDict = {}
                for i in range(len(df.columns)):
                    renameDict['column_' + str(i)] = i
                df.rename(columns=renameDict, inplace=True)
                new_index = pd.RangeIndex(start=0, step=1,
                                          stop=len(df.columns))
                df = df.reindex(columns=new_index)
            else:
                df = df.reindex(columns=sorted(df.columns))

    df = insert_nulls_based_on_null_values_map(df, frame.null_values)
    return df


def col2series(col, index):
    current_dtype = ""
    if col.dtype == fpb.BOOLEAN:
        data = col.bools
        current_dtype = "bool"
    elif col.dtype == fpb.FLOAT:
        data = col.floats
        current_dtype = "float"
    elif col.dtype == fpb.INTEGER:
        data = col.ints
        current_dtype = "int"
    elif col.dtype == fpb.STRING:
        data = col.strings
        current_dtype = "object"
    elif col.dtype == fpb.TIME:
        data = [pd.Timestamp(t, unit='ns') for t in col.times]
        current_dtype = "datetime64[ns, UTC]"
    else:
        raise MessageError('unknown dtype - {}'.format(col.dtype))

    if col.kind == col.LABEL:
        data = [data[0]] * col.size
        if col.dtype == fpb.STRING:
            data = pd.Series(data, dtype='category',
                             index=index,
                             name=col.name)
    else:
        data = pd.Series(data, index=index, name=col.name, dtype=current_dtype)

    return data


def idx2series(idx):
    return pd.Series(idx.values, name=idx.name)


def df2msg(df, labels=None, index_cols=None):
    indices = None
    if index_cols is not None:
        # if there is already an index set, we want to preserve it.
        if not (type(df.index) == pd.RangeIndex and df.index.name is None):
            df.reset_index(inplace=True)
        indices = [series2col(df[name], name) for name in index_cols]
        cols = [col for col in df.columns if col not in index_cols]
        df = df[cols]
    elif should_encode_index(df):
        if hasattr(df.index, 'levels'):
            by_name = df.index.get_level_values
            names = df.index.names
            serieses = (idx2series(by_name(name)) for name in names)
        else:
            serieses = [idx2series(df.index)]

        indices = [series2col(s, s.name) for s in serieses]

    schema = get_actual_types(df)
    df, null_values = normalize_df(df.copy(), schema)

    is_range = isinstance(df.columns, pd.RangeIndex)
    columns = []
    for name in df.columns:
        if not is_range and not isinstance(name, str):
            raise Exception('Column names must be strings')
        series = df[name]
        typ = schema[name]
        if isinstance(name, int):
            name = 'column_' + str(name)
        columns.append(series2col_with_dtype(series, name, typ))

    return fpb.Frame(
        columns=columns,
        indices=indices,
        labels=pb_map(labels),
        null_values=null_values,
    )


def series2col_with_dtype(s, name, dtype):
    kw = {
        'name': name,
        'kind': fpb.Column.SLICE,
    }

    if dtype == fpb.INTEGER:
        kw['dtype'] = fpb.INTEGER
        kw['ints'] = s
    elif dtype == fpb.FLOAT:
        kw['dtype'] = fpb.FLOAT
        kw['floats'] = s
    elif dtype == fpb.STRING:  # Pandas dtype for str is object
        kw['strings'] = s
        kw['dtype'] = fpb.STRING
    elif dtype == fpb.BOOLEAN:
        kw['bools'] = s
        kw['dtype'] = fpb.BOOLEAN
    elif dtype == fpb.TIME:
        s = pd.to_datetime(s, utc=True)
        s = s.dt.tz_convert('UTC')
        kw['times'] = s.astype(np.int64)
        kw['dtype'] = fpb.TIME
    elif dtype == fpb.NULL:
        kw['dtype'] = fpb.NULL
    else:
        raise WriteError('{} - unsupported type - {}'.format(s.name, s.dtype))

    return fpb.Column(**kw)


def series2col(s, name):
    kw = {
        'name': name,
        'kind': fpb.Column.SLICE,
    }

    if is_integer(s.dtype):
        kw['dtype'] = fpb.INTEGER
        kw['ints'] = s
    elif is_float(s.dtype):
        kw['dtype'] = fpb.FLOAT
        kw['floats'] = s
    elif s.dtype == np.object:  # Pandas dtype for str is object
        kw['strings'] = s
        kw['dtype'] = fpb.STRING
    elif is_bool(s.dtype):
        kw['bools'] = s
        kw['dtype'] = fpb.BOOLEAN
    elif is_datetime(s.dtype):
        if s.dt.tz:
            try:
                s = s.dt.tz_localize(pytz.UTC)
            except TypeError:
                s = s.dt.tz_convert('UTC')
        kw['times'] = s.astype(np.int64)
        kw['dtype'] = fpb.TIME
    elif is_categorical_dtype(s.dtype):
        # We assume catgorical data is strings
        kw['strings'] = s.astype(str)
        kw['dtype'] = fpb.STRING
    else:
        raise WriteError('{} - unsupported type - {}'.format(s.name, s.dtype))

    return fpb.Column(**kw)


def insert_nulls_based_on_null_values_map(df, null_values):
    # if there are no Null values at all, skip
    if len(null_values) == 0:
        return df
    i = 0
    casted_columns = {}
    for key in df.index:
        for col_name in null_values[i].nullColumns:
            # boolean columns should be converted to `object` to be able to
            # represent None.
            if df[col_name].dtype == np.bool and \
                    col_name not in casted_columns:
                casted_columns[col_name] = True
                df[col_name] = df[col_name].astype(object)
            df.at[key, col_name] = None
        i = i + 1
    return df


def normalize_df(df, schema):
    """
        This function converts all 'Null' values to the according
        default values based on the column type, and creates an indication
        list to specify where are the null values
    :param schema: dictionary specifying the real type of every column
    :param df:
    :return:
    """
    null_values = []
    nulls_exist = False
    for col_pos, col_name in enumerate(df.columns):
        col = df[col_name]
        row_index = 0
        for index, value in col.items():
            if col_pos == 0:
                null_values.append(fpb.NullValuesMap(nullColumns={}))
            if pd.isnull(value):
                nulls_exist = True
                df.at[index, col_name] = get_empty_value_by_type(schema[col_name])
                null_values[row_index].nullColumns[col_name] = True
            row_index = row_index + 1

    if not nulls_exist:
        null_values = []

    return df, null_values


def get_empty_value_by_type(dtype):
    if dtype == fpb.INTEGER:
        return 0
    elif dtype == fpb.FLOAT:
        return 0.0
    elif dtype == fpb.STRING:
        return ''
    elif dtype == fpb.TIME:
        return datetime.fromtimestamp(0, pytz.UTC)
    elif dtype == fpb.BOOLEAN:
        return False
    elif dtype == fpb.NULL:
        return False
    raise Exception('unsupported type {}'.format(dtype))


def get_actual_types(df):
    column_types = {}

    for col_name in df.columns:
        col = df[col_name]
        if is_integer(col.dtype):
            column_types[col.name] = fpb.INTEGER
        elif is_float(col.dtype):
            column_types[col.name] = fpb.FLOAT
        elif is_string(col.dtype):
            has_data = False
            for x in col:
                if pd.isnull(x):
                    continue
                if isinstance(x, str):
                    column_types[col.name] = fpb.STRING
                    has_data = True
                    break
                if isinstance(x, bool):
                    column_types[col.name] = fpb.BOOLEAN
                    has_data = True
                    break
                if isinstance(x, pd.Timestamp):
                    column_types[col.name] = fpb.TIME
                    has_data = True
                    break
                if isinstance(x, datetime):
                    column_types[col.name] = fpb.TIME
                    has_data = True
                    break
                raise WriteError('{} - contains an unsupported value type - {}'
                                 .format(col_name, type(x)))
            # If all items in the column are None
            # it does not matter what type the column will be, set the column as INTEGER
            if not has_data:
                column_types[col.name] = fpb.NULL
        elif is_bool(col.dtype):
            column_types[col.name] = fpb.BOOLEAN
        elif is_datetime(col.dtype):
            column_types[col.name] = fpb.TIME
        elif is_categorical_dtype(col.dtype):
            # We assume catgorical data is strings
            column_types[col.name] = fpb.STRING
        else:
            raise WriteError('{} - unsupported type - {}'
                             .format(col_name, col.dtype))

    return column_types


def should_encode_index(df):
    if df.index.name:
        return True

    return not isinstance(df.index, pd.RangeIndex)


def is_categorical_dtype(dtype):
    return isinstance(dtype, CategoricalDtype)


def _fix_pd():
    # cudf works with older versions of pandas
    import pandas.api.types
    if not hasattr(pandas.api.types, 'is_categorical_dtype'):
        pandas.api.types.is_categorical_dtype = is_categorical_dtype

    import pandas.core.common
    if not hasattr(pandas.core.common, 'is_categorical_dtype'):
        pandas.core.common.is_categorical_dtype = is_categorical_dtype


_fix_pd()
del _fix_pd
