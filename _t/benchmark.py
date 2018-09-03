from datetime import datetime
from time import monotonic

import numpy as np
import pandas as pd

import v3io_frames as v3f


def make_df(size):
    return pd.DataFrame({
        'ints': np.arange(size),
        'floats': np.arange(size, dtype=np.float),
        'strings': [f'val{i}' for i in range(size)],
    })


df = make_df(10_000)

table = datetime.now().strftime('%Y%m%dT%H%M%S')
client = v3f.Client(url='http://localhost:8080')
print('>>> write')
wstart = monotonic()
client.write('v3io', table, [df])
print(monotonic() - wstart)

print('>>> read')
rstart = monotonic()
for df in client.read(backend='v3io', table=table):
    pass
print(monotonic() - rstart)
