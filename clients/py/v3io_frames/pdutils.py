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
import warnings

from .pbutils import is_categorical_dtype


def concat_dfs(dfs, frame_factory=pd.DataFrame):
    """Concat sequence of DataFrames, can handle MultiIndex frames."""
    dfs = list(dfs)
    if not dfs:
        return frame_factory()

    if not isinstance(dfs[0], pd.DataFrame):
        import cudf
        return cudf.concat(dfs)

    # Make sure concat keep categorical columns
    # See https://stackoverflow.com/a/44086708/7650
    align_categories(dfs)

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
        wdf.set_index(names[0], inplace=True)
    elif names[0] is None:
        del wdf['index']  # Pandas will add 'index' column

    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        wdf.labels = getattr(dfs[0], 'labels', {})
    return wdf


def align_categories(dfs):
    all_cats = set()
    for df in dfs:
        for col in df.columns:
            if is_categorical_dtype(df[col].dtype):
                all_cats.update(df[col].cat.categories)

    for df in dfs:
        for col in df.columns:
            if is_categorical_dtype(df[col].dtype):
                cats = all_cats - set(df[col].cat.categories)
                df[col].cat.add_categories(cats, inplace=True)
