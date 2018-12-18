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
