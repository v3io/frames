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

from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import pytest

import v3io_frames as v3f
from conftest import protocols, csv_file, has_go
from time import sleep, monotonic
from random import random


def csv_info():
    with open(csv_file) as fp:
        columns = fp.readline().strip().split(',')
        size = sum(1 for _ in fp)
    return columns, size


columns, size = csv_info()


def reader(id, n, c):
    for i in range(n):
        df = pd.concat(c.read('weather', 'weather.csv'))
        assert set(df.columns) == set(columns), 'columns mismatch'
        assert len(df) == size, 'bad size'
        sleep(random() / 10)


@pytest.mark.parametrize('protocol', protocols)
def test_concurrent(framesd, protocol):
    if not has_go:
        raise AssertionError("Go SDK not found")

    addr = getattr(framesd, '{}_addr'.format(protocol))
    c = v3f.Client(addr)
    start = monotonic()
    with ThreadPoolExecutor() as pool:
        for i in range(7):
            pool.submit(reader, i, 5, c)
    duration = monotonic() - start
    print('duration: {:.3f}sec'.format(duration))
