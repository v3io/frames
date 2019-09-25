# V3IO Frames

[![Build Status](https://travis-ci.org/v3io/frames.svg?branch=master)](https://travis-ci.org/v3io/frames)
[![GoDoc](https://godoc.org/github.com/v3io/frames?status.svg)](https://godoc.org/github.com/v3io/frames)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

V3IO Frames (**"Frames"**) is a multi-model open-source data-access library, developed by Iguazio, which provides a unified high-performance DataFrame API for working with data in the data store of the [Iguazio Data Science Platform](https://www.iguazio.com) (**"the platform"**).

#### In This Document

- [Client Python API Reference](#client-python-api-reference)
- [Contributing](#contributing)
- [LICENSE](#license)

<a id="client-python-api-reference"></a>
## Client Python API Reference

- [Overview](#overview)
- [User Authentication](#user-authentication)
- [Client Constructor](#client-constructor)
- [Common Client Method Parameters](#client-common-method-params)
- [The create Method](#method-create)
- [The write Method](#method-write)
- [The read Method](#method-read)
- [The delete Method](#method-delete)
- [The execute Method](#method-execute)

<a id="api-reference-overview"></a>
### Overview

To use Frames, you first need to import the **v3io_frames** Python library.
For example:
```python
import v3io_frames as v3f
```
Then, you need to create and initialize an instance of the `Client` class; see [Client Constructor](#client-constructor).
You can then use the client methods to perform different data operations on the supported backend types:

<a id="client-methods"></a>
#### Client Methods

The `Client` class features the following methods for supporting basic data operations:

- [`create`](#method-create) &mdash; create a new NoSQL or TSDB table or a stream ("the backend").
- [`delete`](#method-delete) &mdash; delete the backend.
- [`read`](#method-read) &mdash; read data from the backend (as a pandas DataFrame or DataFrame iterator).
- [`write`](#method-write) &mdash; write one or more DataFrames to the backend.
- [`execute`](#method-execute) &mdash; execute a command on the backend. Each backend may support multiple commands.

<a id="backend-types"></a>
#### Backend Types

All Frames client methods receive a [`backend`](#client-method-param-backend) parameter for setting the Frames backend type.
Frames supports the following backend types:

- `kv` &mdash; a platform NoSQL (key/value) table.
- `stream` &mdash; a platform data stream.
- `tsdb` &mdash; a time-series database (TSDB).
- `csv` &mdash; a comma-separated-value (CSV) file.
  This backend type is used only for testing purposes.

> **Note:** Some method parameters are common to all backend types and some are backend-specific, as detailed in this reference.

<a id="user-authentication"></a>
### User Authentication

When creating a Frames client, you must provide valid platform credentials for accessing the backend data, which Frames will use to identify the identity of the user.
This can be done by using any of the following alternative methods (documented in order of precedence):

- <a id="user-auth-client-const-params"></a>Provide the authentication credentials in the [`Client` constructor parameters](#client-constructor-parameters) by using either of the following methods:

  - <a id="user-auth-token"></a>Set the [`token`](#client-param-token) constructor parameter to a valid platform access key with the required data-access permissions.
  - <a id="user-auth-user-password"></a>Set the [`user`](#client-param-user) and [`password`](#client-param-password) constructor parameters to the username and password of a platform user with the required data-access permissions.
  <br/>

  > **Note:** You can't use both methods concurrently: setting both the `token` and `username` and `password` parameters in the same constructor call will produce an error.

- <a id="user-auth-client-envar"></a>Set the authentication credentials in environment variables, by using either of the following methods:

  - <a id="user-auth-client-envar-access-key"></a>Set the `V3IO_ACCESS_KEY` environment variable to a valid platform access key with the required data-access permissions.
  - <a id="user-auth-client-envar-user-pwd"></a>Set the `V3IO_USERNAME` and `V3IO_PASSWORD` environment variables to the username and password of a platform user with the required data-access permissions.
  <br/>

  > **Note:**
  > - When the client constructor is called with [authentication parameters](#user-auth-client-const-params), the authentication-credentials environment variables (if defined) are ignored.
  > - When `V3IO_ACCESS_KEY` is defined, `V3IO_USERNAME` and `V3IO_PASSWORD` are ignored.
  > - The platform's Jupyter Notebook service automatically defines the `V3IO_ACCESS_KEY` environment variable and initializes it to a valid access key for the running user of the service. 

<a id="client-constructor"></a>
### Client Constructor

All Frames operations are executed via an object of the `Client` class.

- [Syntax](#client-constructor-syntax)
- [Parameters and Data Members](#client-constructor-parameters)
- [Example](#client-constructor-example)

<a id="client-constructor-syntax"></a>
#### Syntax

```python
Client(address, user, password, token, container)
```

<a id="client-constructor-parameters"></a>
#### Parameters and Data Members

- <a id="client-param-address"></a>**address** &mdash; the address of the Frames service (`framesdb`).
  <br/>
  When running locally on the platform (for example, from a Jupyter Notebook service), set this parameter to `framesd:8081`.
  <br/>
  When connecting to the platform remotely, set this parameter to the API address of Frames platform service of the parent tenant.
  You can copy this address from the **API** column of the V3IO Frames service on the **Services** platform dashboard page.

  - **Type:** String
  - **Requirement:** Required 

- <a id="client-param-container"></a>**container** &mdash; the name of the platform data container that contains the backend data.
  For example, `"bigdata"` or `"users"`.

  - **Type:** String
  - **Requirement:** Required

- <a id="client-param-user"></a>**user** &mdash; the username of a platform user with permissions to access the backend data.

  - **Type:** String
  - **Requirement:** Required when neither the [`token`](#client-param-token) parameter or the authentication environment variables are set.
    See [User Authentication](#user-authentication).
    <br/>
    When the `user` parameter is set, the [`password`](#client-param-password) parameter must also be set to a matching user password.

- <a id="client-param-password"></a>**password** &mdash; a platform password for the user configured in the [`user`](#client-param-user) parameter.

  - **Type:** String
  - **Requirement:** Required when the [`user`](#client-param-user) parameter is set.
    See [User Authentication](#user-authentication).

- <a id="client-param-token"></a>**token** &mdash; a valid platform access key that allows access to the backend data.
  To get this access key, select the user profile icon on any platform dashboard page, select **Access Tokens**, and copy an existing access key or create a new key.

  - **Type:** String
  - **Requirement:** Required when neither the [`user`](#client-param-user) or [`password`](#client-param-password) parameters or the authentication environment variables are set.
    See [User Authentication](#user-authentication).

<a id="client-constructor-example"></a>
#### Example

The following example, for local platform execution, creates a Frames client for accessing data in the "users" container by using the authentication credentials of the user "iguazio":

```python
import v3io_frames as v3f
client = v3f.Client("framesd:8081", user="iguazio", password="mypass", container="users")
```

<a id="client-common-method-params"></a>
### Common Client Method Parameters

All client methods receive the following common parameters:

- <a id="client-method-param-backend"></a>**backend** &mdash; the backend data type for the operation.
  See the backend-types descriptions in the [overview](#backend-types).

  - **Type:** String
  - **Valid Values:** `"csv"` | `"kv"` | `"stream"` | `"tsdb"`
  - **Requirement:** Required

- <a id="client-method-param-table"></a>**table** &mdash; the relative path to the backend data &mdash; a directory in the target platform data container (as configured for the client object) that represents a platform data collection, such as a TSDB or NoSQL table or a stream.
  For example, `"mytable"` or `"examples/tsdb/my_metrics"`.

  - **Type:** String
  - **Requirement:** Required

Additional method-specific parameters are described for each method.

<a id="method-create"></a>
### create Method

Creates a new data collection (table/stream) in a platform data container according to the configured backend.

The `create` method is supported by the `tsdb` and `stream` backends, but not by the `kv` backend, because NoSQL tables in the platform don't need to be created prior to ingestion; when ingesting data into a table that doesn't exist, the table is automatically created.

- [Syntax](#method-create-syntax)
- [`tsdb` backend `create` parameters](#method-create-params-tsdb)
- [`stream` backend `create` parameters](#method-create-params-stream)

<a id="method-create-syntax"></a>
#### Syntax

```python
create(backend=<backend>, table=<table or stream>, attrs=<backend-specific parameters>)
```

<a id="method-create-params-tsdb"></a>
#### tsdb Backend create Parameters

- <a id="method-create-tsdb-param-rate"></a>**rate** (Required) &mdash; `string` &mdash; the ingestion rate TSDB's metric-samples, as `"[0-9]+/[smh]"` (where `s` = seconds, `m` = minutes, and `h` = hours); for example, `"1/s"` (one sample per minute).
  The rate should be calculated according to the slowest expected ingestion rate.
- <a id="method-create-tsdb-param-aggregates"></a>**aggregates** (Optional)
- <a id="method-create-tsdb-param-aggregation"></a>**aggregation-granularity** (Optional)

For detailed information about these parameters, refer to the [V3IO TSDB documentation](https://github.com/v3io/v3io-tsdb#v3io-tsdb).

Example:
```python
client.create("tsdb", "/mytable", attrs={"rate": "1/m"})
```

<a id="method-create-params-stream"></a>
#### stream Backend create Parameters

- <a id="method-create-stream-param-shards"></a>**shards** (Optional) (default: `1`) &mdash; `int` &mdash; the number of stream shards to create.
- <a id="method-create-stream-param-retention_hours"></a>**retention_hours** (Optional) (default: `24`) &mdash; `int` &mdash; the stream's retention period, in hours.

For detailed information about these parameters, refer to the [platform streams documentation](https://www.iguazio.com/docs/concepts/latest-release/streams).

Example:
```python
client.create("stream", "/mystream", attrs={"shards": 6})
```

<a id="method-write"></a>
### write Method

Writes data from a DataFrame to a data collection (table/stream) in a platform data container according to the configured backend.

- [Syntax](#method-write-syntax)
- [Common parameters](#method-write-backend-common-params)
- [`kv` backend `write` parameters](#method-write-params-kv)

<a id="method-write-syntax"></a>
#### Syntax

```python
write(backend=<backend>, table=<table>, attrs=<backend-specific parameters>)
```

<a id="method-write-backend-common-params"></a>
#### Common write Parameters

All Frames backends that support the `write` method support the following common parameters, which can be set in the `attrs` method parameter:

- <a id="method-write-param-dfs"></a>**dfs** &mdash; list of DataFrames to write.
- <a id="method-write-param-index_cols"></a>**index_cols** (Optional) (default: none) &mdash; specify specific index columns, by default DataFrame's index columns will be used.
- <a id="method-write-param-labels"></a>**labels** (Optional) (default: none)
- <a id="method-write-param-max_in_message"></a>**max_in_message** (Optional) (default: `0`)
- <a id="method-write-param-partition_keys"></a>**partition_keys** (Optional) (default: none) (**Not yet supported**)

Example:
```python
data = [["tom", 10], ["nick", 15], ["juli", 14]]
df = pd.DataFrame(data, columns = ["name", "age"])
df.set_index("name")
client.write(backend="kv", table="mytable", dfs=df)
```

<a id="method-write-params-kv"></a>
#### kv Backend write Parameters

<!--
- <a id="method-write-kv-param-expression"></a>**expression** (Optional) (default: none) &mdash; a platform update expression that determines how to update the table.
  For detailed information about platform update expressions, see the [platform documentation](https://www.iguazio.com/docs/reference/latest-release/expressions/update-expression/).
-->
  <!-- [IntInfo] [c-no-update-expression-support] (24.9.19) See Bug IG-12510,
    Requirement IG-5339, & DOC IG-12272. The original write example below had
    an `expression` parameter:
v3c.write(backend="kv", table="mytable", dfs=df, expression="city='NY'", condition="age>14")
    -->
  <!-- [IntInfo] [c-no-update-expression-support] (24.9.19) See Bug IG-12510,
    Requirement IG-5339, & DOC IG-12272. -->

- <a id="method-write-kv-param-condition"></a>**condition** (Optional) (default: none) &mdash; a platform condition expression that defines conditions for performing the update.
  For detailed information about platform condition expressions, see the [platform documentation](https://www.iguazio.com/docs/reference/latest-release/expressions/condition-expression/).

Example:
```python
data = [["tom", 10, "TLV"], ["nick", 15, "Berlin"], ["juli", 14, "NY"]]
df = pd.DataFrame(data, columns = ["name", "age", "city"])
df.set_index("name")
v3c.write(backend="kv", table="mytable", dfs=df, condition="age>14")
```

<a id="method-read"></a>
### read Method

Reads data from a data collection (table/stream) in a platform data container to a DataFrame according to the configured backend.

- [Syntax](#method-read-syntax)
- [Common parameters](#method-read-backend-common-params)
- [`tsdb` backend `read` parameters](#method-read-params-tsdb)
- [`kv` backend `read` parameters](#method-read-params-kv)
- [`stream` backend `read` parameters](#method-read-params-stream)

Reads data from a backend.

<a id="method-read-syntax"></a>
#### Syntax

```python
read(backend=<backend>, table=<table>, attrs=<backend-specific parameters>)
```

<a id="method-read-backend-common-params"></a>
#### Common read Parameters

All Frames backends that support the `read` method support the following common parameters, which can be set in the `attrs` method parameter:

- <a id="method-read-param-iterator"></a>**iterator** &mdash; `bool` &mdash; return iterator of DataFrames or (if False) just one DataFrame
- <a id="method-read-param-filter"></a>**filter** &mdash; `string` &mdash; query filter (can't be used with query)
- <a id="method-read-param-columns"></a>**columns** &mdash; `[]str` &mdash; list of columns to pass (can't be used with query)
- <a id="method-read-param-data_format"></a>**data_format** &mdash; `string` &mdash; data format (**Not yet supported**)
- <a id="method-read-param-marker"></a>**marker** &mdash; `string` &mdash; query marker (**Not yet supported**)
- <a id="method-read-param-limit"></a>**limit** &mdash; `int` &mdash; maximal number of rows to return (**Not yet supported**)
- <a id="method-read-param-row_layout"></a>**row_layout** &mdash; `bool` &mdash; weather to use row layout (vs the default column layout) (**Not yet supported**)

<a id="method-read-params-tsdb"></a>
#### tsdb Backend read Parameters

- <a id="method-read-tsdb-param-start"></a>**start** &mdash; `string`
- <a id="method-read-tsdb-param-end"></a>**end** &mdash; `string`
- <a id="method-read-tsdb-param-step"></a>**step** &mdash; `string`
- <a id="method-read-tsdb-param-aggregators"></a>**aggregators** &mdash; `string`
- <a id="method-read-tsdb-param-aggregationWindow"></a>**aggregationWindow** &mdash; `string`
- <a id="method-read-tsdb-param-query"></a>**query** &mdash; `string` &mdash; query in SQL format
- <a id="method-read-tsdb-param-group_by"></a>**group_by** &mdash; `string` &mdash; query group by (can't be used with query)
- <a id="method-read-tsdb-param-multi_index"></a>**multi_index** &mdash; `bool` &mdash; get the results as a multi index data frame where the labels are used as indexes in addition to the timestamp, or if `False` (default behavior) only the timestamp will function as the index.

For detailed information about these parameters, refer to the [V3IO TSDB documentation](https://github.com/v3io/v3io-tsdb#v3io-tsdb).

Example:
```python
df = client.read(backend="tsdb", query="select avg(cpu) as cpu, avg(diskio), avg(network)from mytable", start="now-1d", end="now", step="2h")
```

<a id="method-read-params-kv"></a>
#### kv Backend read Parameters

- <a id="method-read-kv-param-reset_index"></a>**reset_index** &mdash; `bool` &mdash; Reset the index. When set to `false` (default), the DataFrame will have the key column of the v3io kv as the index column.
  When set to `true`, the index will be reset to a range index.
- <a id="method-read-kv-param-max_in_message"></a>**max_in_message** &mdash; `int` &mdash; Maximal number of rows per message
- <a id="method-read-kv-param-"></a>**sharding_keys** &mdash; `[]string` (**Experimental**) &mdash; a list of specific sharding keys to query, for range-scan formatted tables only.
- <a id="method-read-kv-param-segments"></a>**segments** &mdash; `[]int64` (**Not yet supported**)
- <a id="method-read-kv-param-total_segments"></a>**total_segments** &mdash; `int64` (**Not yet supported**)
- <a id="method-read-kv-param-sort_key_range_start"></a>**sort_key_range_start** &mdash; `string` (**Not yet supported**)
- <a id="method-read-kv-param-sort_key_range_end"></a>**sort_key_range_end** &mdash; `string` (**Not yet supported**)

For detailed information about these parameters, refer to the platform's NoSQL documentation.

Example:
```python
df = client.read(backend="kv", table="mytable", filter="col1>666")
```

<a id="method-read-params-stream"></a>
#### stream Backend read Parameters

- <a id="method-read-stream-param-seek"></a>**seek** &mdash; `string` &mdash; valid values:  `"time" | "seq"/"sequence" | "latest" | "earliest"`.
  <br/>
  If the `"seq"|"sequence"` seek type is set, you need to provide the desired record sequence ID via the [`sequence`](#method-read-stream-param-sequence) parameter.
  <br/>
  If the `time` seek type is set, you need to provide the desired start time via the `start` parameter.
- <a id="method-read-stream-param-shard_id"></a>**shard_id** &mdash; `string`
- <a id="method-read-stream-param-sequence"></a>**sequence** &mdash; `int64` (Optional)

For detailed information about these parameters, refer to the [platform streams documentation](https://www.iguazio.com/docs/concepts/latest-release/streams).

Example:
```python
df = client.read(backend="stream", table="mytable", seek="latest", shard_id="5")
```

<a id="method-delete"></a>
### delete Method

Deletes a data collection (table/stream) in a platform data container according to the configured backend.
<br/>
The `kb` backend also supports an optional [`filter`](#method-delete-kv-param-filter) parameter that can be used to delete only specific items in a NoSQL tables.

- [Syntax](#method-delete-syntax)
- [`tsdb` backend `delete` parameters](#method-delete-params-tsdb)
- [`kv` backend `delete` parameters](#method-delete-params-kv)

<a id="method-delete-syntax"></a>
#### Syntax

```python
delete(backend=<backend>, table=<table>, attrs=<backend-specific parameters>)
```

<a id="method-delete-params-tsdb"></a>
#### tsdb Backend delete Parameters

- <a id="method-delete-tsdb-param-start"></a>**start** &mdash; `string` &mdash; delete since start
- <a id="method-delete-tsdb-param-end"></a>**end** &mdash; `string` &mdash; delete since start

> **Note:** When neither the `start` or `end` parameters are set, the entire TSDB table is deleted.

For detailed information about these parameters, refer to the [V3IO TSDB](https://github.com/v3io/v3io-tsdb#v3io-tsdb) documentation.

Example:
```python
df = client.delete(backend="tsdb", table="mytable", start="now-1d", end="now-5h")
```

<a id="method-delete-params-kv"></a>
#### kv Backend delete Parameters

- <a id="method-delete-kv-param-filter"></a>**filter** &mdash; `string` &mdash; a platform filter expression that identifies specific items to delete.
  For detailed information about platform filter expressions, see the [platform documentation](https://www.iguazio.com/docs/reference/latest-release/expressions/condition-expression/#filter-expression).

> **Note:** When the `filter` parameter isn't set, the entire table is deleted.

Example:
```python
df = client.delete(backend="kv", table="mytable", filter="age > 40")
```

<a id="method-execute"></a>
### execute Method

Extends the basic CRUD functionality of the other client methods via custom commands:

- [tsdb backend commands](#method-execute-tsdb-cmds)
- [kv backend commands](#method-execute-kv-cmds)
- [stream backend commands](#method-execute-stream-cmds)

<a id="method-execute-tsdb-cmds"></a>
### tsdb Backend execute Commands

Currently, no `execute` commands are available for the `tsdb` backend.

<a id="method-execute-kv-cmds"></a>
### kv Backend execute Commands

- <a id="method-execute-kv-cmd-infer"></a>**infer | inferschema** &mdash; infers the data schema of a given NoSQL table and creates a schema file for the table.

  Example:
  ```python
  client.execute(backend="kv", table="mytable", command="infer")
  ````

<!--
- <a id="method-execute-kv-cmd-update"></a>**update** &mdash; perform an update expression for a specific key.

  Example:
  ```python
  client.execute(backend="kv", table="mytable", command="update", args={"key": "somekey", "expression": "col2=30", "condition": "col3>15"})
  ```
-->
  <!-- [IntInfo] [c-no-update-expression-support] -->

<a id="method-execute-stream-cmds"></a>
### stream Backend execute Commands

- <a id="method-execute-stream-cmd-put"></a>**put** &mdash; adds records to a stream.

  Example:
  ```python
  client.execute(backend="stream", table="mystream", command="put", args={"data": "this a record", "clientinfo": "some_info", "partition": "partition_key"})
  ```

<a id="contributing"></a>
## Contributing

To contribute to V3IO Frames, you need to be aware of the following:

- [Components](#components)
- [Development](#development)
  - [Adding and Changing Dependencies](#adding-and-changing-dependencies)
  - [Travis CI](#travis-ci)
- [Docker Image](#docker-image)
  - [Building the Image](#building-the-image)
  - [Running the Image](#running-the-image)

<a id="components"></a>
### Components

The following components are required for building Frames code:

- Go server with support for both the gRPC and HTTP protocols
- Go client
- Python client

<a id="development"></a>
### Development

The core is written in [Go](https://golang.org/).
The development is done on the `development` branch and then released to the `master` branch.

Before submitting changes, test the code:

- To execute the Go tests, run `make test`.
- To execute the Python tests, run `make test-python`.

<a id="dependencies"></a>
#### Adding and Changing Dependencies

- If you add Go dependencies, run `make update-go-deps`.
- If you add Python dependencies, update **clients/py/Pipfile** and run `make
  update-py-deps`.

<a id="travis-ci"></a>
#### Travis CI

Integration tests are run on [Travis CI](https://travis-ci.org/).
See **.travis.yml** for details.

The following environment variables are defined in the [Travis settings](https://travis-ci.org/v3io/frames/settings):

- Docker Container Registry ([Quay.io](https://quay.io/))
    - `DOCKER_PASSWORD` &mdash; password for pushing images to Quay.io.
    - `DOCKER_USERNAME` &mdash; username for pushing images to Quay.io.
- Python Package Index ([PyPI](https://pypi.org/))
    - `V3IO_PYPI_PASSWORD` &mdash; password for pushing a new release to PyPi.
    - `V3IO_PYPI_USER` &mdash; username for pushing a new release to PyPi.
- Iguazio Data Science Platform
    - `V3IO_SESSION` &mdash; a JSON encoded map with session information for running tests.
      For example:

      ```
      '{"url":"45.39.128.5:8081","container":"mitzi","user":"daffy","password":"rabbit season"}'
      ```
      > **Note:** Make sure to embed the JSON object within single quotes (`'{...}'`).

<a id="docker-image"></a>
### Docker Image

<a id="docker-image-build"></a>
#### Building the Image

Use the following command to build the Docker image:

```sh
make build-docker
```

<a id="docker-image-run"></a>
#### Running the Image

Use the following command to run the Docker image:

```sh
docker run \
	-v /path/to/config.yaml:/etc/framesd.yaml \
	quay.io/v3io/frames:unstable
```

<a id="license"></a>
## LICENSE

[Apache 2](LICENSE)

