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


def concat_dfs(dfs):
    """Concat sequence of DataFrames, can handle MultiIndex frames."""
    dfs = list(dfs)
    names = list(dfs[0].index.names)
    wdf = pd.concat(
        [df.reset_index() for df in dfs],
        ignore_index=True,
        sort=False,
    )

    if len(names) > 1:
        # We need a name for set_index, if we don't have one, take the name
        # pandas assigned to the column
        full_names = [name or wdf.columns[i] for i, name in enumerate(names)]
        wdf.set_index(full_names, inplace=True)
        wdf.index.names = names
    elif names[0]:
        wdf.index = wdf.pop(names[0])

    return wdf
