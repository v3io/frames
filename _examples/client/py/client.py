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

from os.path import abspath, dirname
from datetime import datetime
from getpass import getuser

import pandas as pd

import v3io_frames as v3f

here = abspath(dirname(__file__))
csv_file = '{}/weather.csv'.format(here)

table = '{:%Y-%m-%dT%H:%M:%S}-{}-weather.csv'.format(datetime.now(), getuser())

print('Table: {}'.format(table))

size = 1000
df = pd.read_csv(csv_file, parse_dates=['DATE'])
# Example how we work with iterable of dfs, you can pass the original df
# "as-is" to write
dfs = [df[i*size:i*size+size] for i in range((len(df)//size)+1)]

client = v3f.Client('http://localhost:8080', 's3cr3t')

print('Writing')
out = client.write('weather', table, dfs)
print('Result: {}'.format(out))

print('Reading')
num_dfs = num_rows = 0
for df in client.read(backend='weather', table=table, max_in_message=size):
    print(df)
    num_dfs += 1
    num_rows += len(df)

print('\nnum_dfs = {}, num_rows = {}'.format(num_dfs, num_rows))

# If you'd like to get single big DataFrame use
# df = pd.concat(client.read(table=csv_file, max_in_message=1000))
