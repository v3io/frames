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

import pandas as pd
import pytest
import v3io_frames as v3f
from conftest import test_backends

try:
    import cudf

    has_cudf = True
except ImportError:
    has_cudf = False


@pytest.mark.skipif(not has_cudf, reason='cudf not found')
def test_concat_categorical():
    df1 = cudf.DataFrame({'a': range(10, 13), 'b': range(50, 53)})
    df1['c'] = pd.Series(['a'] * 3, dtype='category')

    df2 = cudf.DataFrame({'a': range(20, 23), 'b': range(60, 63)})
    df2['c'] = pd.Series(['b'] * 3, dtype='category')

    for backend in test_backends:
        df = v3f.pdutils.concat_dfs([df1, df2], backend, cudf.DataFrame, cudf.concat, False)
        assert len(df) == len(df1) + len(df2), 'bad concat size'
        dtype = df['c'].dtype
        assert v3f.pdutils.is_categorical_dtype(dtype), 'result not categorical'


@pytest.mark.skipif(not has_cudf, reason='cudf not found')
def test_concat_categorical_with_multi_index():
    df1 = cudf.DataFrame({'a': range(10, 13), 'b': range(50, 53)})
    df1['c'] = pd.Series(['a'] * 3, dtype='category')

    df2 = cudf.DataFrame({'a': range(20, 23), 'b': range(60, 63)})
    df2['c'] = pd.Series(['b'] * 3, dtype='category')

    for backend in test_backends:
        df = v3f.pdutils.concat_dfs([df1, df2], backend, cudf.DataFrame, cudf.concat, True)
        assert len(df) == len(df1) + len(df2), 'bad concat size'
        dtype = df['c'].dtype
        assert v3f.pdutils.is_categorical_dtype(dtype), 'result not categorical'
