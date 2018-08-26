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

import v3io_frames as v3f

here = abspath(dirname(__file__))
csv_file = '{}/weather.csv'.format(here)

client = v3f.Client('http://localhost:8080', 's3cr3t')
for df in client.read(table=csv_file, max_in_message=1000):
    print(df)

# If you'd like to get single big DataFrame use
# df = pd.concat(client.read(table=csv_file, max_in_message=1000))
