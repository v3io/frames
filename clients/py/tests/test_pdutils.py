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

import numpy as np
import pandas as pd

from v3io_frames import pdutils


def gen_dfs():
    size = 17
    columns = ['x', 'y', 'z']
    for year in range(2010, 2017):
        index = pd.date_range(str(year), periods=17, name='Date')
        values = np.random.rand(size, len(columns))
        yield pd.DataFrame(values, columns=columns, index=index)


def test_concat_dfs():
    dfs = list(gen_dfs())
    df = pdutils.concat_dfs(dfs)

    assert len(df) == sum(len(d) for d in dfs), 'bad number of rows'
    assert df.index.name == dfs[0].index.name, 'bad index name'
    assert set(df.columns) == set(dfs[0].columns), 'bad columns'


def test_empty_result():
    df = pdutils.concat_dfs([])
    assert df.empty, 'non-empty df'


def gen_cat(value, size):
    series = pd.Series([value])
    series = series.astype('category')
    return series.reindex(pd.RangeIndex(size), method='pad')


def test_concat_dfs_categorical():
    size = 10
    df1 = pd.DataFrame({
        'c': gen_cat('val1', size),
        'v': range(size),
    })

    df2 = pd.DataFrame({
        'c': gen_cat('val2', size),
        'v': range(size, 2*size),
    })

    df = pdutils.concat_dfs([df1, df2])
    assert len(df) == len(df1) + len(df2), 'bad length'
    assert set(df.columns) == set(df1.columns), 'bad columns'
    assert isinstance(df['c'].dtype, pd.CategoricalDtype), 'not categorical'
    assert set(df['c']) == set(df1['c']) | set(df2['c']), 'bad values'
