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

from conftest import here
from v3io_frames import pbutils
import v3io_frames.frames_pb2 as fpb


def test_encode_df():
    labels = {
        'int': 7,
        'str': 'wassup?',
    }

    df = pd.read_csv('{}/weather.csv'.format(here))
    msg = pbutils.df2msg(df, labels)

    names = [col.name for col in msg.columns]
    assert set(names) == set(df.columns), 'columns mismatch'
    assert not msg.indices, 'has index'
    assert pbutils.pb2py(msg.labels) == labels, 'lables mismatch'

    # Now with index
    index_name = 'DATE'
    df.index = df.pop(index_name)
    msg = pbutils.df2msg(df, None)

    names = [col.name for col in msg.columns]
    assert set(names) == set(df.columns), 'columns mismatch'
    assert msg.indices, 'no index'
    assert msg.indices[0].name == index_name, 'bad index name'


def test_multi_index():
    tuples = [
        ('bar', 'one'),
        ('bar', 'two'),
        ('baz', 'one'),
        ('baz', 'two'),
        ('foo', 'one'),
        ('foo', 'two'),
        ('qux', 'one'),
        ('qux', 'two')]
    index = pd.MultiIndex.from_tuples(tuples, names=['first', 'second'])
    df = pd.DataFrame(index=index)
    df['x'] = range(len(df))

    data = pbutils.df2msg(df).SerializeToString()
    msg = fpb.Frame.FromString(data)

    for col in msg.indices:
        values = col.strings
        assert len(values) == len(df), 'bad index length'
