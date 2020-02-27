# V3IO Frames

[![GoDoc](https://godoc.org/github.com/v3io/frames?status.svg)](https://godoc.org/github.com/v3io/frames)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

V3IO Frames (**"Frames"**) is a multi-model open-source data-access library that provides a unified high-performance DataFrame API for working with different types of data sources (backends).
The library was developed by Iguazio to simplify working with data in the [Iguazio Data Science Platform](https://www.iguazio.com) (**"the platform"**), but it can be extended to support additional backend types.

> **Note:** For a full API reference of the Frames platform backends, including detailed examples, see the Frames API reference in [the platform documentation](https://www.iguazio.com/docs/reference/latest-release/api-reference/frames/).

#### In This Document

- [Client Python API Reference](#client-python-api-reference)
- [Contributing](#contributing)
- [LICENSE](#license)

<a id="client-python-api-reference"></a>
## Client Python API Reference

- [Overview](#overview)
- [User Authentication](#user-authentication)
- [`Client` Constructor](#client-constructor)
- [Common `Client` Method Parameters](#client-common-method-params)
- [`create` Method](#method-create)
- [`write` Method](#method-write)
- [`read` Method](#method-read)
- [`delete` Method](#method-delete)
- [`execute` Method](#method-execute)

<a id="api-reference-overview"></a>
### Overview

- [Python Version](#python-version)
- [Initialization](#initialization)
- [Backend Types](#backend-types)
- [`Client` Methods](#client-methods)

<a id="python-version"></a>
#### Python Version

The current version of Frames supports Python 3.6 and 3.7.

<a id="initialization"></a>
#### Initialization

To use Frames, you first need to import the **v3io_frames** Python library.
For example:
```python
import v3io_frames as v3f
```
Then, you need to create and initialize an instance of the `Client` class; see [Client Constructor](#client-constructor).
You can then use the client methods to perform different data operations on the supported backend types.

<a id="backend-types"></a>
#### Backend Types

All Frames client methods receive a [`backend`](#client-method-param-backend) parameter for setting the Frames backend type.
Frames currently supports the following backend types:

- `nosql` | `kv` &mdash; a platform NoSQL (key/value) table.
  See the [platform NoSQL backend API reference](https://www.iguazio.com/docs/reference/latest-release/api-reference/frames/nosql/).
  <br/><br/>
  > **Note:** The documentation uses the `"nosql"` alias to the `"kv"` type, which was added in Frames v0.6.10-v0.9.13; `"kv"` is still supported for backwards compatibility with earlier releases.
- `stream` &mdash; a platform data stream **[Tech Preview]**.
  See the [platform TSDB backend API reference](https://www.iguazio.com/docs/reference/latest-release/api-reference/frames/tsdb/).
- `tsdb` &mdash; a time-series database (TSDB).
  See the [platform streaming backend API reference](https://www.iguazio.com/docs/reference/latest-release/api-reference/frames/stream/).
- `csv` &mdash; a comma-separated-value (CSV) file.
  This backend type is used only for testing purposes.

<a id="client-methods"></a>
#### `Client` Methods

The `Client` class features the following methods for supporting operations on a data **collection**, such as a NoSQL or TSDB table or a data stream:

- [`create`](#method-create) &mdash; creates a new collection.
- [`delete`](#method-delete) &mdash; deletes a collection or specific items of the collection.
- [`read`](#method-read) &mdash; reads data from a collection into pandas DataFrames.
- [`write`](#method-write) &mdash; writes data from pandas DataFrames to a collection.
- [`execute`](#method-execute) &mdash; executes a backend-specific command on a collection.
  Each backend may support multiple commands.

> **Note:** Some methods or method parameters are backend-specific, as detailed in this reference.

<a id="user-authentication"></a>
### User Authentication

When creating a Frames client, you must provide valid credentials for accessing the backend data, which Frames will use to identify the identity of the user.
This can be done by using any of the following alternative methods (documented in order of precedence).
For more information about the user authentication for the platform backends, see the [platform documentation](https://www.iguazio.com/docs/reference/latest-release/api-reference/frames/overview/#user-authentication):

- <a id="user-auth-client-const-params"></a>Provide the authentication credentials in the call to the [`Client` constructor](#client-constructor) &mdash; either by setting the [`token`](#client-param-token) parameter to a valid authentication token (access key) or by setting the [`user`](#client-param-user) and [`password`](#client-param-password) parameters to a username and password.
  Note that you cannot set the token parameter concurrently with the username and password parameters.

- <a id="user-auth-client-envar"></a>Provide the authentication credentials in environment variables &mdash; either by setting the `V3IO_ACCESS_KEY` variable to an authentication token or by setting the `V3IO_USERNAME` and `V3IO_PASSWORD` variables to a username and password.

  > **Note:**
  > - When `V3IO_ACCESS_KEY` is defined, `V3IO_USERNAME` and `V3IO_PASSWORD` are ignored.
  > - When the client constructor is called with authentication parameters (option #1), the authentication-credentials environment variables (if defined) are ignored.

<a id="client-constructor"></a>
### `Client` Constructor

All Frames operations are executed via an object of the `Client` class.

- [Syntax](#client-constructor-syntax)
- [Parameters and Data Members](#client-constructor-parameters)
- [Return Value](#client-constructor-return-value)
- [Examples](#client-constructor-examples)

<a id="client-constructor-syntax"></a>
#### Syntax

```python
Client(address=""[, data_url=""], container=""[, user="", password="", token=""])
```

<a id="client-constructor-parameters"></a>
#### Parameters and Data Members

- <a id="client-param-address"></a>**address** &mdash; The address of the Frames service (`framesd`).
  Use the `grpc://` prefix for gRPC (default; recommended) or the `http://` prefix for HTTP.
  When running locally on the platform, set this parameter to `framesd:8081` to use the gRPC (recommended) or to `framesd:8080` to use HTTP; for more information, see the [platform documentation](https://www.iguazio.com/docs/reference/latest-release/api-reference/frames/client-constructor/).

  - **Type:** `str`
  - **Requirement:** Required 

- <a id="client-param-data_url"></a>**data_url** &mdash; A web-API base URL for accessing the backend data.
    By default, the client uses the data URL that's configured for the Frames service; for the platform backends, this is typically the HTTPS URL of the web-APIs service of the parent tenant.

  - **Type:** `str`
  - **Requirement:** Optional

- <a id="client-param-container"></a>**container** &mdash; The name of the data container that contains the backend data.
  For example, `"bigdata"` or `"users"`.

  - **Type:** `str`
  - **Requirement:** Required

- <a id="client-param-user"></a>**user** &mdash; The username of a user with permissions to access the backend data.
  See [User Authentication](#user-authentication).

  - **Type:** `str`
  - **Requirement:** Required when neither the [`token`](#client-param-token) parameter or the authentication environment variables are set.
    <br/>
    When the `user` parameter is set, the [`password`](#client-param-password) parameter must also be set to a matching user password.

- <a id="client-param-password"></a>**password** &mdash; A valid password for the user configured in the [`user`](#client-param-user) parameter.
  See [User Authentication](#user-authentication).

  - **Type:** `str`
  - **Requirement:** Required when the [`user`](#client-param-user) parameter is set.

- <a id="client-param-token"></a>**token** &mdash; A valid token that allows access to the backend data, such as a platform access key for the platform backends.
  See [User Authentication](#user-authentication).

  - **Type:** `str`
  - **Requirement:** Required when neither the [`user`](#client-param-user) or [`password`](#client-param-password) parameters or the authentication environment variables are set.

<a id="client-constructor-return-value"></a>
#### Return Value

Returns a new Frames `Client` data object.

<a id="client-constructor-examples"></a>
#### Examples

The following examples, for local platform execution, both create a Frames client for accessing data in the "users" container by using the authentication credentials of user "iguazio"; the first example uses token (access-key) authentication while the second example uses username and password authentication (see [User Authentication](#user-authentication)):

```python
import v3io_frames as v3f
client = v3f.Client("framesd:8081", token="e8bd4ca2-537b-4175-bf01-8c74963e90bf", container="users")
```

```python
import v3io_frames as v3f
client = v3f.Client("framesd:8081", user="iguazio", password="mypass", container="users")
```

<a id="client-common-method-params"></a>
### Common `Client` Method Parameters

All client methods receive the following common parameters; additional, method-specific parameters are described for each method.

- <a id="client-method-param-backend"></a>**backend** &mdash; The backend data type for the operation.
  See [Backend Types](#backend-types).

  - **Type:** `str`
  - **Requirement:** Required
  - **Valid Values:**  `"nosql"` | `"stream"` | `"tsdb"` | `"csv"` (for testing)

- <a id="client-method-param-table"></a>**table** &mdash; The relative path to a data collection of the specified backend type in the target data container (as configured for the client object).
  For example, `"mytable"` or `"/examples/tsdb/my_metrics"`.

  - **Type:** `str`
  - **Requirement:** Required unless otherwise specified in the method-specific documentation

<a id="method-create"></a>
### `create` Method

Creates a new data collection in the configured client data container, according to the specified backend type.

> **Note:** The `create` method isn't applicable to the `nosql` backend, because NoSQL tables in the platform don't need to be created prior to ingestion; when ingesting data into a table that doesn't exist, the table is automatically created.

- [Syntax](#method-create-syntax)
- [Common parameters](#method-create-common-params)
- [`tsdb` backend `create` parameters](#method-create-params-tsdb)
- [`stream` backend `create` parameters](#method-create-params-stream)
- [Examples](#method-create-examples)

<a id="method-create-syntax"></a>
#### Syntax

```python
create(backend, table, schema=None, if_exists=FAIL, **kw)
```

<a id="method-create-common-params"></a>
#### Common `create` Parameters

All Frames backends that support the `create` method support the following common parameters:

- <a id="method-create-param-if_exists"></a>**if_exists** &mdash; Determines whether to raise an error when the specified collection ([`table`](#client-method-param-table)) already exists.

  - **Type:** `pb.ErrorOptions` enumeration.
    To use the enumeration, import the `frames_pb2 module`; for example:
    <br/><br/>
    ```python
    from v3io_frames import frames_pb2 as fpb
    ```
  - **Requirement:** Optional
  - **Valid Values:** `FAIL` to raise an error when the specified collection already exist; `IGNORE` to ignore this
  - **Default Value:** `FAIL`

- <a id="method-read-param-schema"></a>**schema** &mdash; a schema for describing unstructured collection data.
  This parameter is intended to be used only for testing purposes with the `csv` backend.

  - **Type:** Backend-specific or `None`
  - **Requirement:** Optional
  - **Default Value:** `None`

- <a id="method-read-param-kw"></a>**kw** &mdash; This parameter is used for passing a variable-length list of additional keyword (named) arguments.
  For more information, see the backend-specific method parameters.

  - **Type:** `**` &mdash; variable-length keyword arguments list
  - **Requirement:** Optional

<a id="method-create-params-tsdb"></a>
#### `tsdb` Backend `create` Parameters

The following `create` parameters are specific to the `tsdb` backend and are passed as keyword arguments via the `kw` parameter; for more information and examples, see the platform's [Frames TSDB-backend reference](https://www.iguazio.com/docs/reference/latest-release/api-reference/frames/tsdb/create/):

- <a id="method-create-tsdb-param-rate"></a>**rate** &mdash; metric-samples ingestion rate.

  - **Type:** `str`
  - **Requirement:** Required
  - **Valid Values:** A string of the format `"[0-9]+/[smh]"` &mdash; where '`s`' = seconds, '`m`' = minutes, and '`h`' = hours.
    For example, `"1/s"` (one sample per minute), `"20/m"` (20 samples per minute), or `"50/h"` (50 samples per hour).

- <a id="method-create-tsdb-param-aggregates"></a>**aggregates** &mdash; A list of aggregation functions for real-time aggregation during the samples ingestion ("pre-aggregation").

  - **Type:** `str`
  - **Requirement:** Optional
  - **Valid Values:** A string containing a comma-separated list of supported aggregation functions &mdash; `avg`| `count`| `last`| `max`| `min`| `rate`| `stddev`| `stdvar`| `sum`.
    For example, `"count,avg,min,max"`.

- <a id="method-create-tsdb-param-aggregation_granularity"></a>**aggregation_granularity** &mdash; Aggregation granularity; applicable when the [`aggregates`](#method-create-tsdb-param-aggregates) parameter is set.

  - **Type:** `str`
  - **Requirement:** Optional
  - **Valid Values:** A string of the format `"[0-9]+[mhd]"` &mdash; where '`m`' = minutes, '`h`' = hours, and '`d`' = days.
    For example, `"30m"` (30 minutes), `"2h"` (2 hours), or `"1d"` (1 day).
  - **Default Value:** `"1h"` (1 hour)

<a id="method-create-params-stream"></a>
#### `stream` Backend `create` Parameters

The following `create` parameters are specific to the `stream` backend and are passed as keyword arguments via the `kw` parameter; for more information and examples, see the platform's [Frames streaming-backend reference](https://www.iguazio.com/docs/reference/latest-release/api-reference/frames/stream/create/):

- <a id="method-create-stream-param-shards"></a>**shards** &mdash; The number of stream shards to create.

  - **Type:** `int`
  - **Requirement:** Optional
  - **Default Value:** `1`
  - **Valid Values:** A positive integer (>= 1).
    For example, `100`.

- <a id="method-create-stream-param-retention_hours"></a>**retention_hours** &mdash; The stream's retention period, in hours.

  - **Type:** `int`
  - **Requirement:** Optional
  - **Default Value:** `24`
  - **Valid Values:** A positive integer (>= 1).
    For example, `2` (2 hours).

<a id="method-create-examples"></a>
#### `create` Examples

<a id="method-create-examples-tsdb"></a>
##### `tsdb` Backend

```python
client.create("tsdb", table="mytsdb", rate="10/m")
```

```python
client.create("tsdb", table="/tsdb/my_metrics", rate="1/s", aggregates="count,avg,min,max", aggregation_granularity="1h")
```

<a id="method-create-examples-stream"></a>
##### `stream` Backend

```python
client.create("stream", table="/mystream", shards=3)
```

```python
client.create("stream", table="/my_streams/stream1", retention_hours=2)
```

<a id="method-write"></a>
### `write` Method

Writes data from a DataFrame to a data collection, according to the specified backend type.

- [Syntax](#method-write-syntax)
- [Common parameters](#method-write-common-params)
- [`nosql` backend `write` parameters](#method-write-params-nosql)
- [`tsdb` backend `write` parameters](#method-write-params-tsdb)
- [Examples](#method-write-examples)

<a id="method-write-syntax"></a>
#### Syntax

```python
write(backend, table, dfs, expression='', condition='', labels=None,
    max_rows_in_msg=0, index_cols=None, save_mode='createNewItemsOnly',
    partition_keys=None):
```

> **Note:** The `expression` and `partition_keys` parameters aren't supported in the current release.
<!-- [c-no-update-expression-support] -->

<a id="method-write-common-params"></a>
#### Common `write` Parameters

All Frames backends that support the `write` method support the following common parameters:

- <a id="method-write-param-dfs"></a>**dfs** &mdash; One or more DataFrames containing the data to write.

  - **Type:** A single DataFrame, a list of DataFrames, or a DataFrames iterator
  - **Requirement:** Required

- <a id="method-write-param-index_cols"></a>**index_cols** &mdash; A list of column (attribute) names to be used as index columns for the write operation, regardless of any index-column definitions in the DataFrame.
  By default, the DataFrame's index columns are used.
  <br/>
  > **Note:** The significance and supported number of index columns is backend specific.
  > For example, the `nosql` backend supports only a single index column for the primary-key item attribute, while the `tsdb` backend supports additional index columns for metric labels.

  - **Type:** `[]str`
  - **Requirement:** Optional
  - **Default Value:** `None`

- <a id="method-write-param-labels"></a>**labels** &mdash; This parameter is currently applicable only to the `tsdb` backend (although it's available for all backends) and is therefore documented as part of the `write` method's [`tsdb` backend parameters](#method-write-tsdb-param-labels).

  - **Type:** `dict`
  - **Requirement:** Optional

- <a id="method-write-param-save_mode"></a>**save_mode** &mdash; This parameter is currently applicable only to the `nosql` backend, and is therefore documented as part of the `write` method's [`nosql` backend parameters](#method-write-nosql-param-save_mode).

  - **Type:** `str`
  - **Requirement:** Optional

- <a id="method-write-param-max_rows_in_msg"></a>**max_rows_in_msg** &mdash; Maximum number of rows to write in each message (write chunk size).

  - **Type:** `int`
  - **Requirement:** Optional
  - **Default Value:** `0`

<a id="method-write-params-nosql"></a>
#### `nosql` Backend `write` Parameters

The following `write` parameters are specific to the `nosql` backend; for more information and examples, see the platform's [Frames NoSQL-backend reference](https://www.iguazio.com/docs/reference/latest-release/api-reference/frames/nosql/write/):

<!--
- <a id="method-write-nosql-param-expression"></a>**expression** (Optional) (default: `None`) &mdash; A platform update expression that determines how to update the table for all items in the DataFrame.
  For detailed information about platform update expressions, see the [platform documentation](https://www.iguazio.com/docs/reference/latest-release/expressions/update-expression/).
-->
  <!-- [IntInfo] [c-no-update-expression-support] (24.9.19) See Bug IG-12510,
    Requirement IG-5339, & DOC IG-12272. TODO: When update expressions are
    supported, edit the doc here and for the `write` method's `expression`
    parameter as well as the Frames help text. See v3io/tutorials commit 03c7feb
    (PR #119) for removal of updated documentation of Frames update expressions.
    The original write example below had an `expression` parameter:
client.write(backend="nosql", table="mytable", dfs=df, expression="city='NY'", condition="age>14")
    -->

- <a id="method-write-nosql-param-condition"></a>**condition** &mdash; A platform condition expression that defines conditions for performing the write operation.

  - **Type:** `str`
  - **Requirement:** Optional

- <a id="method-write-nosql-param-save_mode"></a>**save_mode** &mdash; Save mode, which determines in which circumstances to write new item to the table.

  - **Type:** `str`
  - **Requirement:** Optional
  - **Valid Values:**
    - `createNewItemsOnly` &mdash; write only new items; don't replace or update any existing table item with the same name (primary-key attribute value) as a written item.
    - `"updateItem"` &mdash; update items; add new items and update the attributes of existing table items.
    - `"overwriteItem"` &mdash; overwrite items; add new items and replace any existing table item with the same name as a written item.
    - `"errorIfTableExists"` &mdash; create a new table only; only write items if the target table doesn't already exist.
    - `"overwriteTable"` &mdash; overwrite the table; replace all existing table items (if any) with the written items. 
  - **Default Value:** `createNewItemsOnly`

<a id="method-write-params-tsdb"></a>
#### `tsdb` Backend `write` Parameters

The following `write` parameter descriptions are specific to the `tsdb` backend; for more information and examples, see the platform's [Frames TSDB-backend reference](https://www.iguazio.com/docs/reference/latest-release/api-reference/frames/tsdb/write/):

- <a id="method-write-tsdb-param-labels"></a>**labels** &mdash; A dictionary of metric labels of the format `{<label>: <value>[, <label>: <value>, ...]}` to apply to all the DataFrame rows.
  For example, `{"os": "linux", "arch": "x86"}`.

  - **Type:** `dict`
  - **Requirement:** Optional
  - **Default Value:** `None`

<a id="method-write-examples"></a>
#### `write` Examples

<a id="method-write-examples-nosql"></a>
##### `nosql` Backend

```python
data = [["tom", 10, "TLV"], ["nick", 15, "Berlin"], ["juli", 14, "NY"]]
df = pd.DataFrame(data, columns = ["name", "age", "city"])
df.set_index("name", inplace=True)
client.write(backend="nosql", table="mytable", dfs=df, condition="age>14")
```

<a id="method-write-examples-tsdb"></a>
##### `tsdb` Backend

```python
from datetime import datetime
df = pd.DataFrame(data=[[30.1, 12.7]], index=[[datetime.now()], ["1"]],
                  columns=["cpu", "disk"])
df.index.names = ["time", "node"]
client.write(backend="tsdb", table="mytsdb", dfs=df)
```

<a id="method-stream-examples-tsdb"></a>
##### `stream` Backend

```python
import numpy as np
df = pd.DataFrame(np.random.rand(9, 3) * 100,
                  columns=["cpu", "mem", "disk"])
client.write("stream", table="mystream", dfs=df)
```

<a id="method-read"></a>
### `read` Method

Reads data from a data collection to a DataFrame, according to the specified backend type.

- [Syntax](#method-read-syntax)
- [Common parameters](#method-read-common-params)
- [`nosql` backend `read` parameters](#method-read-params-nosql)
- [`tsdb` backend `read` parameters](#method-read-params-tsdb)
- [`stream` backend `read` parameters](#method-read-params-stream)
- [Return Value](#method-read-return-value)
- [Examples](#method-read-examples)

<a id="method-read-syntax"></a>
#### Syntax

```python
read(backend='', table='', query='', columns=None, filter='', group_by='',
    limit=0, data_format='', row_layout=False, max_rows_in_msg=0, marker='',
    iterator=False, get_raw=False, **kw)
```

> **Note:** The `limit`, `data_format`, `row_layout`, and `marker` parameters aren't supported in the current release, and `get_raw` is for internal use only.

<a id="method-read-common-params"></a>
#### Common `read` Parameters

All Frames backends that support the `read` method support the following common parameters:

- <a id="method-read-param-iterator"></a>**iterator** &mdash; set to `True` to to return a pandas DataFrames iterator; `False` (default) returns a single DataFrame.

  - **Type:** `bool`
  - **Requirement:** Optional
  - **Default Value:** `False`

- <a id="method-read-param-filter"></a>**filter** &mdash; A query filter.
  For example, `filter="col1=='my_value'"`.
  <br/>
  This parameter is currently applicable only to the `nosql` and `tsdb` backends, and cannot be used concurrently with the `query` parameter of the `tsdb` backend.

  - **Type:** `str`
  - **Requirement:** Optional

- <a id="method-read-param-columns"></a>**columns** &mdash; A list of attributes (columns) to return.
  <br/>
  This parameter is currently applicable only to the `nosql` and `tsdb` backends, and cannot be used concurrently with the `query` parameter of the `tsdb` backend.

  - **Type:** `[]str`
  - **Requirement:** Optional

- <a id="method-read-param-kw"></a>**kw** &mdash; This parameter is used for passing a variable-length list of additional keyword (named) arguments.
  For more information, see the backend-specific method parameters.

  - **Type:** `**` &mdash; variable-length keyword arguments list
  - **Requirement:** Optional

<a id="method-read-params-nosql"></a>
#### `nosql` Backend `read` Parameters

The following `read` parameters are specific to the `nosql` backend; for more information and examples, see the platform's [Frames NoSQL-backend reference](https://www.iguazio.com/docs/reference/latest-release/api-reference/frames/nosql/read/):

- <a id="method-read-nosql-param-max_rows_in_msg"></a>**max_rows_in_msg** &mdash; The maximum number of rows per message.

  - **Type:** `int`
  - **Requirement:** Optional

The following parameters are passed as keyword arguments via the `kw` parameter:

- <a id="method-read-nosql-param-reset_index"></a>**reset_index** &mdash; Set to `True` to reset the index column of the returned DataFrame and use the auto-generated pandas range-index column; `False` (default) sets the index column to the table's primary-key attribute.

  - **Type:** `bool`
  - **Requirement:** Optional
  - **Default Value:** `False`

- <a id="method-read-nosql-param-sharding_keys"></a>**sharding_keys** **[Tech Preview]** &mdash; A list of specific sharding keys to query, for range-scan formatted tables only.
  <!-- [IntInfo] Tech Preview [TECH-PREVIEW-FRAMES-KV-READ-SHARDING-KEYS-PARAM]
  -->

  - **Type:** `[]str`
  - **Requirement:** Optional

<a id="method-read-params-tsdb"></a>
#### `tsdb` Backend `read` Parameters

The following `read` parameters are specific to the `tsdb` backend; for more information and examples, see the platform's [Frames TSDB-backend reference](https://www.iguazio.com/docs/reference/latest-release/api-reference/frames/tsdb/read/):

- <a id="method-read-tsdb-param-group_by"></a>**group_by** **[Tech Preview]** &mdash; A group-by query string.
  <br/>
  This parameter cannot be used concurrently with the `query` parameter.

  - **Type:** `str`
  - **Requirement:** Optional

- <a id="method-read-tsdb-param-query"></a>**query** **[Tech Preview]** &mdash; A query string in SQL format.
  > **Note:**
  > - When setting the `query` parameter, you must provide the path to the TSDB table as part of the `FROM` caluse in the query string and not in the `read` method's [`table`](#client-method-param-table) parameter.
  > - This parameter cannot be set concurrently with the following parameters: [`aggregators`](#method-read-tsdb-param-aggregators), [`columns`](#method-read-tsdb-param-columns), [`filter`](#method-read-tsdb-param-filter), or [`group_by`](#method-read-tsdb-param-group_by) parameters.

  - **Type:** `str`
  - **Requirement:** Optional

The following parameters are passed as keyword arguments via the `kw` parameter:

- <a id="method-read-tsdb-param-start"></a>**start** &mdash; Start (minimum) time for the read operation.

  - **Type:** `str`
  - **Requirement:** Optional
  - **Valid Values:** A string containing an RFC 3339 time, a Unix timestamp in milliseconds, a relative time of the format `"now"` or `"now-[0-9]+[mhd]"` (where `m` = minutes, `h` = hours, and `'d'` = days), or 0 for the earliest time.
    For example: `"2016-01-02T15:34:26Z"`; `"1451748866"`; `"now-90m"`; `"0"`.
  - **Default Value:** `<end time> - 1h`

- <a id="method-read-tsdb-param-end"></a>**end** &mdash; End (maximum) time for the read operation.

  - **Type:** `str`
  - **Requirement:** Optional
  - **Valid Values:** A string containing an RFC 3339 time, a Unix timestamp in milliseconds, a relative time of the format `"now"` or `"now-[0-9]+[mhd]"` (where `m` = minutes, `h` = hours, and `'d'` = days), or 0 for the earliest time.
    For example: `"2018-09-26T14:10:20Z"`; `"1537971006000"`; `"now-3h"`; `"now-7d"`.
  - **Default Value:** `now`

- <a id="method-read-tsdb-param-step"></a>**step** &mdash; The query aggregation or downsampling step.
  The default step is the query's time range, which can be configured via the [start](#method-read-tsdb-param-start) and [end](#method-read-tsdb-param-end) parameters.

  - **Type:** `str`
  - **Requirement:** Optional

- <a id="method-read-tsdb-param-aggregators"></a>**aggregators** &mdash; Aggregation information to return, as a comma-separated list of supported aggregation functions ("aggregators").
  <br/>
  This parameter cannot be used concurrently with the `query` parameter.

  - **Type:** `str`
  - **Requirement:** Optional
  - **Valid Value:** The following aggregation functions are supported for over-time aggregation (across each unique label set); for cross-series aggregation (across all metric labels), add "`_all`" to the end of the function name:
    <br/>
    `avg` | `count` | `last` | `max` | `min` | `rate` | `stddev` | `stdvar` | `sum`

- <a id="method-read-tsdb-param-aggregation_window"></a>**aggregation_window** **[Tech Preview]** &mdash; Aggregation interval for applying over-time aggregation functions, if set in the [`aggregators`](#method-read-tsdb-param-aggregators) or [`query`](#method-read-tsdb-param-query) parameters.

  - **Type:** `str`
  - **Requirement:** Optional
  - **Valid Values:** A string of the format `"[0-9]+[mhd]"` where '`m`' = minutes, '`h`' = hours, and '`d`' = days.
      For example, `"30m"` (30 minutes), `"2h"` (2 hours), or `"1d"` (1 day).
  - **Default Value:** The query's aggregation [step](#method-read-tsdb-param-step)

- <a id="method-read-tsdb-param-multi_index"></a>**multi_index** &mdash; set to `True` to display labels as index columns in the read results; `False` (default) displays only the metric's sample time as an index column.

  - **Type:** `bool`
  - **Requirement:** Optional
  - **Default Value:** `False`

<a id="method-read-params-stream"></a>
#### `stream` Backend `read` Parameters

The following `read` parameters are specific to the `stream` backend and are passed as keyword arguments via the `kw` parameter; for more information and examples, see the platform's [Frames streaming-backend reference](https://www.iguazio.com/docs/reference/latest-release/api-reference/frames/stream/read/):

- <a id="method-read-stream-param-seek"></a>**seek** &mdash; Seek type.
  <br/>
  When the `"seq"` or `"sequence"` seek type is set, you must set the [`sequence`](#method-read-stream-param-sequence) parameter to the desired record sequence number.
  <br/>
  When the `time` seek type is set, you must set the [`start`](#method-read-stream-param-start) parameter to the desired seek start time.

  - **Type:** `str`
  - **Requirement:** Required
  - **Valid Values:** `"time"` | `"seq"` | `"sequence"` | `"latest"` | `"earliest"`

- <a id="method-read-stream-param-shard_id"></a>**shard_id** &mdash; The ID of the stream shard from which to read.

  - **Type:** `str`
  - **Requirement:** Required
  - **Valid values:** `"0"` ... `"<stream shard count> - 1"`

- <a id="method-read-stream-param-sequence"></a>**sequence** &mdash; The sequence number of the record from which to start reading.

  - **Type:** `int64`
  - **Requirement:** Required

- <a id="method-read-stream-param-start"></a>**start** &mdash; The earliest record ingestion time from which to start reading.

  - **Type:** `str`
  - **Requirement:** Required when [`seek`](#method-read-stream-param-seek) = `"time"`
  - **Valid Values:** A string containing an RFC 3339 time, a Unix timestamp in milliseconds, a relative time of the format `"now"` or `"now-[0-9]+[mhd]"` (where `m` = minutes, `h` = hours, and `'d'` = days), or 0 for the earliest time.
    For example: `"2016-01-02T15:34:26Z"`; `"1451748866"`; `"now-90m"`; `"0"`.

<a id="method-read-return-value"></a>
#### Return Value

- When the value of the [`iterator`](#method-read-param-iterator) parameter is `False` (default) &mdash; returns a single DataFrame.
- When the value of the `iterator` parameter is `True` &mdash; returns a DataFrames iterator.

<!-- [IntInfo] See IG-14065. -->
<!-- 
> **Note:** The returned DataFrames include a `labels` DataFrame attribute with backend-specific data, if applicable.
> For example, for the `stream` backend, this attribute holds the sequence number of the last stream record that was read.
-->

<a id="method-read-examples"></a>
#### `read` Examples

<a id="method-read-examples-nosql"></a>
##### `nosql` Backend

```python
df = client.read(backend="nosql", table="mytable", filter="col1>666")
```

<a id="method-read-examples-tsdb"></a>
##### `tsdb` Backend

```python
df = client.read("tsdb", table="mytsdb" start="0", multi_index=True)
```

```python
df = client.read(backend="tsdb", query="select avg(cpu) as cpu, avg(disk) from 'mytsdb' where node='1'", start="now-1d", end="now", step="2h")
```

<a id="method-stream-examples-tsdb"></a>
##### `stream` Backend

```python
df = client.read(backend="stream", table="mystream", seek="latest", shard_id="5")
```

<a id="method-delete"></a>
### `delete` Method

Deletes a data collection or specific collection items, according to the specified backend type.

- [Syntax](#method-delete-syntax)
- [Common parameters](#method-delete-common-params)
- [`nosql` backend `delete` parameters](#method-delete-params-nosql)
- [`tsdb` backend `delete` parameters](#method-delete-params-tsdb)
- [Examples](#method-delete-examples)

<a id="method-delete-syntax"></a>
#### Syntax

```python
delete(backend, table, filter='', start='', end='', if_missing=FAIL
```

<a id="method-delete-common-params"></a>
#### Common `delete` Parameters

- <a id="method-delete-param-if_missing"></a>**if_missing** &mdash; Determines whether to raise an error when the specified collection ([`table`](#client-method-param-table)) doesn't exist.

  - **Type:** `pb.ErrorOptions` enumeration.
    To use the enumeration, import the `frames_pb2 module`; for example:
    <br/><br/>
    ```python
    from v3io_frames import frames_pb2 as fpb
    ```
  - **Requirement:** Optional
  - **Valid Values:** `FAIL` to raise an error when the specified collection doesn't exist; `IGNORE` to ignore this
  - **Default Value:** `FAIL`

<a id="method-delete-params-nosql"></a>
#### `nosql` Backend `delete` Parameters

The following `delete` parameters are specific to the `nosql` backend; for more information and examples, see the platform's [Frames NoSQL-backend reference](https://www.iguazio.com/docs/reference/latest-release/api-reference/frames/nosql/delete/):

- <a id="method-delete-nosql-param-filter"></a>**filter** &mdash; A filter expression that identifies specific items to delete.

  - **Type:** `str`
  - **Requirement:** Optional
  - **Default Value:** `""` &mdash; delete the entire table and its schema file

<a id="method-delete-params-tsdb"></a>
#### `tsdb` Backend `delete` Parameters

The following `delete` parameters are specific to the `tsdb` backend; for more information and examples, see the platform's [Frames TSDB-backend reference](https://www.iguazio.com/docs/reference/latest-release/api-reference/frames/tsdb/delete/):

- <a id="method-delete-tsdb-param-start"></a>**start** &mdash; Start (minimum) time for the delete operation &mdash; i.e., delete only items whose data sample time is at or after (`>=`) the specified start time.

  - **Type:** `str`
  - **Requirement:** Optional
  - **Valid Values:** A string containing an RFC 3339 time, a Unix timestamp in milliseconds, a relative time of the format `"now"` or `"now-[0-9]+[mhd]"` (where `m` = minutes, `h` = hours, and `'d'` = days), or 0 for the earliest time.
    For example: `"2016-01-02T15:34:26Z"`; `"1451748866"`; `"now-90m"`; `"0"`.
  - **Default Value:** `""` when neither `start` nor [`end`](#method-delete-tsdb-param-end) are set &mdash; delete the entire table and its schema file (**.schema**); `0` when `end` is set

- <a id="method-delete-tsdb-param-end"></a>**end** &mdash; `str` &mdash; End (maximum) time for the delete operation &mdash; i.e., delete only items whose data sample time is before or at (`<=`) the specified end time.

  - **Type:** `str`
  - **Requirement:** Optional
  - **Valid Values:** A string containing an RFC 3339 time, a Unix timestamp in milliseconds, a relative time of the format `"now"` or `"now-[0-9]+[mhd]"` (where `m` = minutes, `h` = hours, and `'d'` = days), or 0 for the earliest time.
    For example: `"2018-09-26T14:10:20Z"`; `"1537971006000"`; `"now-3h"`; `"now-7d"`.
  - **Default Value:** `""` when neither [`start`](#method-delete-tsdb-param-start) nor `end` are set &mdash; delete the entire table and its schema file (**.schema**); `0` when `start` is set

> **Note:**
> - When neither the [`start`](#method-delete-tsdb-param-start) nor [`end`](#method-delete-tsdb-param-end) parameters are set, the entire TSDB table and its schema file are deleted.
> - Only full table partitions within the specified time frame (as determined by the `start` and `end` parameters) are deleted.
>   Items within the specified time frames that reside within partitions that begin before the delete start time or end after the delete end time aren't deleted.
>   The partition interval is calculated automatically based on the table's ingestion rate and is stored in the TSDB's `partitionerInterval` schema field (see the  **.schema** file).

<a id="method-delete-examples"></a>
#### `delete` Examples
<!-- TODO: Add example descriptions. -->

<a id="method-delete-examples-nosql"></a>
##### `nosql` Backend

```python
client.delete(backend="nosql", table="mytable", filter="age > 40")
```

<a id="method-delete-examples-tsdb"></a>
##### `tsdb` Backend

```python
client.delete(backend="tsdb", table="mytsdb", start="now-1d", end="now-5h")
```

<a id="method-delete-examples-stream"></a>
##### `stream` Backend

```python
from v3io_frames import frames_pb2 as fpb
client.delete(backend="stream", table="mystream", if_missing=fpb.IGNORE)
```

<a id="method-execute"></a>
### `execute` Method

Extends the basic CRUD functionality of the other client methods via backend-specific commands for performing operations on a data collection.

- [Syntax](#method-execute-syntax)
- [Common parameters](#method-execute-common-params)
- [nosql backend commands](#method-execute-nosql-cmds)
- [stream backend commands](#method-execute-stream-cmds)

> **Note:** Currently, no `execute` commands are available for the `tsdb` backend.

<a id="method-execute-syntax"></a>
#### Syntax

```python
execute(backend, table, command="", args=None)
```

<a id="method-execute-common-params"></a>
#### Common `execute` Parameters

All Frames backends that support the `execute` method support the following common parameters:

- <a id="method-except-param-command"></a>**command** &mdash; The command to execute.

  - **Type:** `str`
  - **Requirement:** Required
  - **Valid Values:** Backend-specific

- <a id="method-except-param-args"></a>**args** &mdash; A dictionary of `<argument name>: <value>` pairs for passing command-specific parameters (arguments).

  - **Type:** `dict`
  - **Requirement and Valid Values:** Backend-specific
  - **Default Value:** `None`

<a id="method-execute-nosql-cmds"></a>
#### `nosql` Backend `execute` Commands

The following `execute` commands are specific to the `nosql` backend; for more information and examples, see the platform's [Frames NoSQL-backend reference](https://www.iguazio.com/docs/reference/latest-release/api-reference/frames/nosql/execute/):

- <a id="method-execute-nosql-cmd-infer"></a>**infer | infer_schema** &mdash; Infers the data schema of a given NoSQL table and creates a schema file for the table.

  Example:
  ```python
  client.execute(backend="nosql", table="mytable", command="infer")
  ````

<!--
- <a id="method-execute-nosql-cmd-update"></a>**update** &mdash; Updates a specific item in a NoSQL table according to the provided update expression.
  For detailed information about platform update expressions, see the [platform documentation](https://www.iguazio.com/docs/reference/latest-release/expressions/update-expression/).

  Example:
  ```python
  client.execute(backend="nosql", table="mytable", command="update", args={"key": "somekey", "expression": "col2=30", "condition": "col3>15"})
  ```
-->
  <!-- [IntInfo] [c-no-update-expression-support] -->

<a id="method-execute-stream-cmds"></a>
#### `stream` Backend `execute` Commands

The following `execute` commands are specific to the `stream` backend; for more information and examples, see the platform's [Frames streaming-backend reference](https://www.iguazio.com/docs/reference/latest-release/api-reference/frames/stream/execute/):

- <a id="method-execute-stream-cmd-put"></a>**put** &mdash; Adds records to a stream shard.

  Example:
  ```python
  client.execute('stream', table="mystream", command='put',
                 args={'data': '{"cpu": 12.4, "mem": 31.1, "disk": 12.7}',
                       "client_info": "my custom info", "partition": "PK1"})
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
    - `DOCKER_PASSWORD` &mdash; a password for pushing images to Quay.io.
    - `DOCKER_USERNAME` &mdash; a username for pushing images to Quay.io.
- Python Package Index ([PyPI](https://pypi.org/))
    - `V3IO_PYPI_PASSWORD` &mdash; a password for pushing a new release to PyPi.
    - `V3IO_PYPI_USER` &mdash; a username for pushing a new release to PyPi.
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


