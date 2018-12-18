# v3io_frames - Streaming Data Client for v3io

[![Build Status](https://travis-ci.org/v3io/frames.svg?branch=master)](https://travis-ci.org/v3io/frames)
[![Documentation](https://readthedocs.org/projects/v3io_frames/badge/?version=latest)](https://v3io-frames.readthedocs.io/en/latest/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Streaming data from client [nuclio](http://nuclio.io/) handler to a pandas DataFrame.

You will need a `framesd` server running, see [here](https://github.com/v3io/frames).

```python
import v3io_frames as v3f

client = v3f.Client(address='localhost:8081')
num_dfs = num_rows = 0
size = 1000
for df in client.read(backend='weather', table='table', max_in_message=size):
   print(df)
   num_dfs += 1
   num_rows += len(df)

print('\nnum_dfs = {}, num_rows = {}'.format(num_dfs, num_rows))
```


## License

Apache License Version 2.0, see [LICENSE.txt](LICENSE.txt)
