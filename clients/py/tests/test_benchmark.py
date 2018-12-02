import pytest

import v3io_frames as v3f
from conftest import has_go

from test_integration import integ_params, csv_df

wdf = csv_df(1982)


def read_benchmark(client):
    for df in client.read('csv', 'weather.csv'):
        assert len(df), 'empty df'


def write_benchmark(client, df):
    client.write('csv', 'write-bench.csv', df)


@pytest.mark.skipif(not has_go, reason='Go SDK not found')
@pytest.mark.parametrize('protocol,backend', integ_params)
def test_read(benchmark, framesd, protocol, backend):
    addr = getattr(framesd, '{}_addr'.format(protocol))
    client = v3f.Client(addr)
    benchmark(read_benchmark, client)


@pytest.mark.skipif(not has_go, reason='Go SDK not found')
@pytest.mark.parametrize('protocol,backend', integ_params)
def test_write(benchmark, framesd, protocol, backend):
    addr = getattr(framesd, '{}_addr'.format(protocol))
    client = v3f.Client(addr)
    benchmark(write_benchmark, client, wdf)
