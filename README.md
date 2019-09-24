# V3IO Frames

[![Build Status](https://travis-ci.org/v3io/frames.svg?branch=master)](https://travis-ci.org/v3io/frames)
[![GoDoc](https://godoc.org/github.com/v3io/frames?status.svg)](https://godoc.org/github.com/v3io/frames)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

V3IO Frames is a high-speed server and client library for accessing time-series (TSDB), NoSQL, and streaming data in the [Iguazio Data Science Platform](https://www.iguazio.com).

## Documentation

Frames currently supports 3 backends and basic CRUD functionality for each.

Supported Backends:
1. TSDB
2. KV
3. Stream
4. CSV - for testing purposes


All of frames operations are executed via the `client` object. To create a client object simply provide the Iguazio web-api endpoint and optional credentials.
```python
import v3io_frames as v3f
client = v3f.Client('framesd:8081', user='user1', password='pass')
```
Note: When running from within the managed jupyter notebook on the iguazio platform there is no need to add credentials as this is handled by the platform.  
Next, for every operation we need to provide a `backend`, and a `table` parameters and optionally other function specific arguments.

### Create
Creates a new table for the desired backend. Not all backends require a table to be created prior to ingestion. For example KV table will be created while ingesting new data, on the other hand since TSDB tables have mandatory fields we need to create a table before ingesting new data.  
```python
client.create(backend=<backend>, table=<table>, attrs=<backend_specefic_attributes>)
```

#### backend specific parameters
##### TSDB
* rate 
* aggregates (optional)
* aggregation-granularity (optional)

For detailed info on these parameters please visit [TSDB](https://github.com/v3io/v3io-tsdb#v3io-tsdb) docs.  
Example:
```python
client.create('tsdb', '/mytable', attrs={'rate': '1/m'})
```

##### Stream
* shards=1 (optional)
* retention_hours=24 (optional)

For detailed info on these parameters please visit [Stream](https://www.iguazio.com/docs/concepts/latest-release/streams) docs.  
Example:
```python
client.create('stream', '/mystream', attrs={'shards': '6'})
```

### Write
Writes a Dataframe into one of the supported backends.  
Common write parameters:
* dfs - list of Dataframes to write
* index_cols=None (optional) - specify specific index columns, by default Dataframe's index columns will be used.
* labels=None (optional)
* max_in_message=0 (optional)
* partition_keys=None (Not yet supported)

Example:
```python
data = [['tom', 10], ['nick', 15], ['juli', 14]] 
df = pd.DataFrame(data, columns = ['name', 'age'])
df.set_index('name')
client.write(backend='kv', table='mytable', dfs=df)
```

#### backend specific parameters
##### KV
<!--
* expression=' ' (optional) - for detailed information on update expressions see [docs](https://www.iguazio.com/docs/reference/latest-release/expressions/update-expression/)
-->
  <!-- [IntInfo] [c-no-update-expression-support] (24.9.19) See Bug IG-12510,
    Requirement IG-5339, & DOC IG-12272. The original write example below had
    an `expression` parameter:
v3c.write(backend='kv', table='mytable', dfs=df, expression='city="NY"', condition='age>14')
    -->
* condition=' ' (optional) - for detailed information on condition expressions see [docs](https://www.iguazio.com/docs/reference/latest-release/expressions/condition-expression/)

Example:
```python
data = [['tom', 10, 'TLV'], ['nick', 15, 'Berlin'], ['juli', 14, 'NY']] 
df = pd.DataFrame(data, columns = ['name', 'age', 'city'])
df.set_index('name')
v3c.write(backend='kv', table='mytable', dfs=df, condition='age>14')
```

### Read
Reads data from a backend.  
Common read parameters:
* iterator: bool - Return iterator of DataFrames or (if False) just one DataFrame
* filter: string - Query filter (can't be used with query)
* columns: []str - List of columns to pass (can't be used with query)
* data_format: string - Data format (Not yet supported)
* marker: string - Query marker (Not yet supported)
* limit: int - Maximal number of rows to return (Not yet supported)
* row_layout: bool - Weather to use row layout (vs the default column layout) (Not yet supported)


#### backend specific parameters
##### TSDB
* start: string
* end: string
* step: string
* aggregators: string
* aggregationWindow: string
* query: string - Query in SQL format
* group_by: string - Query group by (can't be used with query)
* multi_index: bool - Get the results as a multi index data frame where the labels are used as indexes
 in addition to the timestamp, or if `False` (default behavior) only the timestamp will function as the index.

For detailed info on these parameters please visit [TSDB](https://github.com/v3io/v3io-tsdb#v3io-tsdb) docs.  
Example:
```python
df = client.read(backend='tsdb', query="select avg(cpu) as cpu, avg(diskio), avg(network)from mytable", start='now-1d', end='now', step='2h')
```

##### KV
* reset_index: bool - Reset the index. When set to `false` (default), the dataframe will have the key column of the v3io kv as the index column.
When set to `true`, the index will be reset to a range index.
* max_in_message: int - Maximal number of rows per message
* sharding_keys: []string (Experimental)- list of specific sharding keys to query. For range scan formatted tables only.
* segments: []int64 (Not yet supported)
* total_segments: int64 (Not yet supported)
* sort_key_range_start: string (Not yet supported)
* sort_key_range_end: string (Not yet supported)

For detailed info on these parameters please visit KV docs.

Example:
```python
df = client.read(backend='kv', table='mytable', filter='col1>666')
```

##### Stream
* seek: string - excepted values:  time | seq/sequence | latest | earliest.  
if `seq` seek type is requested, need to provide the desired sequence id via `sequence` parameter.  
if `time` seek type is requested, need to provide the desired start time via `start` parameter.
* shard_id: string
* sequence: int64 (optional)

For detailed info on these parameters please visit [Stream](https://www.iguazio.com/docs/concepts/latest-release/streams) docs.

Example:
```python
df = client.read(backend='stream', table='mytable', seek='latest', shard_id='5')
```

### Delete
Deletes a table of a specific backend.

Example:
```python
df = client.delete(backend='<backend>', table='mytable')
```

#### backend specific parameters
##### TSDB
* start: string - delete since start
* end: string - delete since start

Note: if both `start` and `end` are not specified **all** the TSDB table will be deleted.  
For detailed info on these parameters please visit [TSDB](https://github.com/v3io/v3io-tsdb#v3io-tsdb) docs.  
Example:
```python
df = client.delete(backend='tsdb', table='mytable', start='now-1d', end='now-5h')
```
##### KV
* filter: string - Filter for selective delete

Example:
```python
df = client.delete(backend='kv', table='mytable', filter='age>40')
```

### Execute
Provides additional functions that are not covered in the basic CRUD functionality.

##### TSDB
Currently no `execute` commands are available for the TSDB backend.

##### KV
* infer, inferschema - inferring and creating a schema file for a given kv table.
  Example: `client.execute(backend='kv', table='mytable', command='infer')`
<!--
* update - perform an update expression for a specific key.
Example: `client.execute(backend='kv', table='mytable', command='update', args={'key': 'somekey', 'expression': 'col2=30', 'condition': 'col3>15'})`
-->
  <!-- [IntInfo] [c-no-update-expression-support] -->

##### Stream
* put - putting a new object to a stream.
Example: `client.execute(backend='stream', table='mystream', command='put', args={'data': 'this a record', 'clientinfo': 'some_info', 'partition': 'partition_key'})`

## Contributing

### Components

- Go server with support for both the gRPC and HTTP protocols
- Go client
- Python client

### Development

The core is written in [Go](https://golang.org/).
The development is done on the `development` branch and then released to the `master` branch.

- To execute the Go tests, run `make test`.
- To execute the Python tests, run `make test-python`.

#### Adding/Changing Dependencies

- If you add Go dependencies, run `make update-go-deps`.
- If you add Python dependencies, update **clients/py/Pipfile** and run `make
  update-py-deps`.

#### Travis CI

Integration tests are run on [Travis CI](https://travis-ci.org/).
See **.travis.yml** for details.

The following environment variables are defined in the [Travis settings](https://travis-ci.org/v3io/frames/settings):

- Docker Container Registry ([Quay.io](https://quay.io/))
    - `DOCKER_PASSWORD` &mdash; Password for pushing images to Quay.io.
    - `DOCKER_USERNAME` &mdash; Username for pushing images to Quay.io.
- Python Package Index ([PyPI](https://pypi.org/))
    - `V3IO_PYPI_PASSWORD` &mdash; Password for pushing a new release to PyPi.
    - `V3IO_PYPI_USER` &mdash; Username for pushing a new release to PyPi.
- Iguazio Data Science Platform
    - `V3IO_SESSION` &mdash; A JSON encoded map with session information for running tests.
      For example:

      ```
      '{"url":"45.39.128.5:8081","container":"mitzi","user":"daffy","password":"rabbit season"}'
      ```
      > **Note:** Make sure to embed the JSON object within single quotes (`'{...}'`).

### Docker Image

#### Building the Image

Use the following command to build the Docker image:

```sh
make build-docker
```

#### Running the Image

Use the following command to run the Docker image:

```sh
docker run \
	-v /path/to/config.yaml:/etc/framesd.yaml \
	quay.io/v3io/frames:unstable
```

## LICENSE

[Apache 2](LICENSE)

