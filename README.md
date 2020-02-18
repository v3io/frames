# V3IO Frames

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
- [`Client` Constructor](#client-constructor)
- [Common `Client` Method Parameters](#client-common-method-params)
- [`create` Method](#method-create)
- [`write` Method](#method-write)
- [`read` Method](#method-read)
- [`delete` Method](#method-delete)
- [`execute` Method](#method-execute)

<a id="api-reference-overview"></a>
### Overview

- [Initialization](#initialization)
- [Backend Types](#backend-types)
- [`Client` Methods](#client-methods)

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
Frames supports the following backend types:

- `kv` &mdash; a platform NoSQL (key/value) table.
- `stream` &mdash; a platform data stream.
- `tsdb` &mdash; a time-series database (TSDB).
- `csv` &mdash; a comma-separated-value (CSV) file.
  This backend type is used only for testing purposes.

<a id="client-methods"></a>
#### `Client` Methods

The `Client` class features the following methods for supporting basic data operations:

- [`create`](#method-create) &mdash; creates a new TSDB table or stream ("backend data").
- [`delete`](#method-delete) &mdash; deletes a table or stream or specific table items.
- [`read`](#method-read) &mdash; reads data from a table or stream into pandas DataFrames.
- [`write`](#method-write) &mdash; writes data from pandas DataFrames to a table or stream.
- [`execute`](#method-execute) &mdash; executes a backend-specific command on a table or stream.
  Each backend may support multiple commands.

> **Note:** Some methods or method parameters are backend-specific, as detailed in this reference.

<a id="user-authentication"></a>
### User Authentication

When creating a Frames client, you must provide valid platform credentials for accessing the backend data, which Frames will use to identify the identity of the user.
This can be done by using any of the following alternative methods (documented in order of precedence):

- <a id="user-auth-client-const-params"></a>Provide the authentication credentials in the [`Client` constructor parameters](#client-constructor-parameters) by using either of the following methods:

  - <a id="user-auth-token"></a>Set the [`token`](#client-param-token) constructor parameter to a valid platform access key with the required data-access permissions.
    You can get the access key from the **Access Keys** window that's available from the user-profile menu of the platform dashboard, or by copying the value of the `V3IO_ACCESS_KEY` environment variable in a platform web-shell or Jupyter Notebook service.
  - <a id="user-auth-user-password"></a>Set the [`user`](#client-param-user) and [`password`](#client-param-password) constructor parameters to the username and password of a platform user with the required data-access permissions.
  <br/>

  > **Note:** You cannot use both methods concurrently: setting both the `token` and `user` and `password` parameters in the same constructor call will produce an error.

- <a id="user-auth-client-envar"></a>Set the authentication credentials in environment variables, by using either of the following methods:

  - <a id="user-auth-client-envar-access-key"></a>Set the `V3IO_ACCESS_KEY` environment variable to a valid platform access key with the required data-access permissions.

    > **Note:** The platform's Jupyter Notebook service automatically defines the `V3IO_ACCESS_KEY` environment variable and initializes it to a valid access key for the running user of the service. 
  - <a id="user-auth-client-envar-user-pwd"></a>Set the `V3IO_USERNAME` and `V3IO_PASSWORD` environment variables to the username and password of a platform user with the required data-access permissions.

  > **Note:**
  > - When the client constructor is called with authentication parameters ([option #1](#user-auth-client-const-params)), the authentication-credentials environment variables (if defined) are ignored.
  > - When `V3IO_ACCESS_KEY` is defined, `V3IO_USERNAME` and `V3IO_PASSWORD` are ignored.

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
  <br/>
  When running locally on the platform (for example, from a Jupyter Notebook service), set this parameter to `framesd:8081` to use the gRPC (recommended) or to `framesd:8080` to use HTTP.
  <br/>
  When connecting to the platform remotely, set this parameter to the API address of a Frames platform service in the parent tenant.
  You can copy this address from the **API** column of the V3IO Frames service on the **Services** platform dashboard page.
  <!-- [IntInfo] In platform command-line environments, such as Jupyter
    Notebook, the local-execution Frames port numbers are saved in
    FRAMESD_SERVICE_PORT_GRPC and FRAMESD_SERVICE_PORT environment variables.
  -->

  - **Type:** `str`
  - **Requirement:** Required 

- <a id="client-param-data_url"></a>**data_url** &mdash; A web-API base URL for accessing the backend data.
    By default, the client uses the data URL that's configured for the Frames service, which is typically the HTTPS URL of the web-APIs service of the parent platform tenant.

  - **Type:** `str`
  - **Requirement:** Optional

- <a id="client-param-container"></a>**container** &mdash; The name of the platform data container that contains the backend data.
  For example, `"bigdata"` or `"users"`.

  - **Type:** `str`
  - **Requirement:** Required

- <a id="client-param-user"></a>**user** &mdash; The username of a platform user with permissions to access the backend data.
  See [User Authentication](#user-authentication).

  - **Type:** `str`
  - **Requirement:** Required when neither the [`token`](#client-param-token) parameter or the authentication environment variables are set.
    <br/>
    When the `user` parameter is set, the [`password`](#client-param-password) parameter must also be set to a matching user password.

- <a id="client-param-password"></a>**password** &mdash; A platform password for the user configured in the [`user`](#client-param-user) parameter.
  See [User Authentication](#user-authentication).

  - **Type:** `str`
  - **Requirement:** Required when the [`user`](#client-param-user) parameter is set.

- <a id="client-param-token"></a>**token** &mdash; A valid platform access key that allows access to the backend data.
  See [User Authentication](#user-authentication).

  - **Type:** `str`
  - **Requirement:** Required when neither the [`user`](#client-param-user) or [`password`](#client-param-password) parameters or the authentication environment variables are set.

<a id="client-constructor-return-value"></a>
#### Return Value

Returns a new Frames `Client` data object.

<a id="client-constructor-examples"></a>
#### Examples

The following examples, for local platform execution, both create a Frames client for accessing data in the "users" container by using the authentication credentials of user "iguazio"; the first example uses access-key authentication while the second example uses username and password authentication (see [User Authentication](#user-authentication)):

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
  - **Valid Values:**  `"kv"` | `"stream"` | `"tsdb"` | `"csv"` (for testing)

- <a id="client-method-param-table"></a>**table** &mdash; The relative path to the backend data &mdash; a directory in the target platform data container (as configured for the client object) that represents a TSDB or NoSQL table or a data stream.
  For example, `"mytable"` or `"examples/tsdb/my_metrics"`.

  - **Type:** `str`
  - **Requirement:** Required unless otherwise specified in the method-specific documentation

<a id="method-create"></a>
### `create` Method

Creates a new TSDB table or stream in a platform data container, according to the specified backend type.

The `create` method is supported by the `tsdb` and `stream` backends, but not by the `kv` backend, because NoSQL tables in the platform don't need to be created prior to ingestion; when ingesting data into a table that doesn't exist, the table is automatically created.

- [Syntax](#method-create-syntax)
- [Common parameters](#method-create-common-params)
- [`tsdb` backend `create` parameters](#method-create-params-tsdb)
- [`stream` backend `create` parameters](#method-create-params-stream)
- [Examples](#method-create-examples)

<a id="method-create-syntax"></a>
#### Syntax

```python
create(backend, table, attrs=None, schema=None, if_exists=FAIL)
```
<!-- [IntInfo] (26.9.19) (sharonl) The `schema` parameter is used only with the
  `csv` testing backend. -->

<a id="method-create-common-params"></a>
#### Common `create` Parameters

All Frames backends that support the `create` method support the following common parameters:

- <a id="method-create-param-attrs"></a>**attrs** &mdash; A dictionary of `<argument name>: <value>` pairs for passing additional backend-specific parameters (arguments).

  - **Type:** `dict`
  - **Requirement:** Required for the `tsdb` backend; optional otherwise
  - **Valid Values:** The valid values are backend-specific.
    See [tsdb Backend create Parameters](#method-create-params-tsdb) and [stream Backend create Parameters](#method-create-params-stream).
  - **Default Value:** `None`

<a id="method-create-params-tsdb"></a>
#### `tsdb` Backend `create` Parameters

The following `create` parameters are specific to the `tsdb` backend and are passed via the method's [`attrs`](#method-create-param-attrs) parameter; for more information about these parameters, see the [V3IO TSDB documentation](https://github.com/v3io/v3io-tsdb#v3io-tsdb):

- <a id="method-create-tsdb-param-rate"></a>**rate** &mdash; The ingestion rate of the TSDB metric samples.
  It's recommended that you set the rate to the average expected ingestion rate, and that the ingestion rates for a given TSDB table don't vary significantly; when there's a big difference in the ingestion rates (for example, x10), use separate TSDB tables.

  - **Type:** `str`
  - **Requirement:** Required
  - **Valid Values:** A string of the format `"[0-9]+/[smh]"` &mdash; where '`s`' = seconds, '`m`' = minutes, and '`h`' = hours.
    For example, `"1/s"` (one sample per minute), `"20/m"` (20 samples per minute), or `"50/h"` (50 samples per hour).

- <a id="method-create-tsdb-param-aggregates"></a>**aggregates** &mdash; A list of aggregation functions for executing in real time during the samples ingestion ("pre-aggregation").

  - **Type:** `str`
  - **Requirement:** Optional
  - **Valid Values:** A string containing a comma-separated list of supported aggregation functions &mdash; `avg`| `count`| `last`| `max`| `min`| `rate`| `stddev`| `stdvar`| `sum`.
    For example, `"count,avg,min,max"`.

- <a id="method-create-tsdb-param-aggregation-granularity"></a>**aggregation-granularity** &mdash; Aggregation granularity; i.e., a time interval for applying pre-aggregation functions, if configured in the [`aggregates`](#method-create-tsdb-param-aggregates) parameter.

  - **Type:** `str`
  - **Requirement:** Optional
  - **Valid Values:** A string of the format `"[0-9]+[mhd]"` &mdash; where '`m`' = minutes, '`h`' = hours, and '`d`' = days.
    For example, `"30m"` (30 minutes), `"2h"` (2 hours), or `"1d"` (1 day).
  - **Default Value:** `"1h"` (1 hour)

<a id="method-create-params-stream"></a>
#### `stream` Backend `create` Parameters

The following `create` parameters are specific to the `stream` backend and are passed via the method's [`attrs`](#method-create-param-attrs) parameter; for more information about these parameters, see the [platform's streams documentation](https://www.iguazio.com/docs/concepts/latest-release/streams):

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

- Create a TSDB table named "mytable" in the root directory of the client's data container with an ingestion rate of 10 samples per minute:

  ```python
  client.create("tsdb", "/mytable", attrs={"rate": "10/m"})
  ```

- Create a TSDB table named "my_metrics" in a **tsdb** directory in the client's data container with an ingestion rate of 1 sample per second.
 The table is created with the `count`, `avg`, `min`, and `max` aggregates and an aggregation granularity of 1 hour:

  ```python
  client.create("tsdb", "/tsdb/my_metrics", attrs={"rate": "1/s", "aggregates": "count,avg,min,max", "aggregation-granularity": "1h"})
  ```

<a id="method-create-examples-stream"></a>
##### `stream` Backend

- Create a stream named "mystream" in the root directory of the client's data container.
  The stream has 6 shards and a retention period of 1 hour (default):

  ```python
  client.create("stream", "/mystream", attrs={"shards": 6})
  ```

- Create a stream named "stream1" in a "my_streams" directory in the client's data container.
  The stream has 24 shards (default) and a retention period of 2 hours:

  ```python
  client.create("stream", "my_streams/stream1", attrs={"retention_hours": 2})
  ```

<a id="method-write"></a>
### `write` Method

Writes data from a DataFrame to a table or stream in a platform data container, according to the specified backend type.

- [Syntax](#method-write-syntax)
- [Common parameters](#method-write-common-params)
- [`kv` backend `write` parameters](#method-write-params-kv)
- [`tsdb` backend `write` parameters](#method-write-params-tsdb)
- [Examples](#method-write-examples)

<a id="method-write-syntax"></a>
#### Syntax

```python
write(backend, table, dfs, expression='', condition='', labels=None,
    max_in_message=0, index_cols=None, partition_keys=None)
```

> **Note:** The `expression` and `partition_keys` parameters aren't supported in the current release.
<!-- [c-no-update-expression-support] -->

<a id="method-write-common-params"></a>
#### Common `write` Parameters

All Frames backends that support the `write` method support the following common parameters:

- <a id="method-write-param-dfs"></a>**dfs** (Required) &mdash; A single DataFrame, a list of DataFrames, or a DataFrames iterator &mdash; One or more DataFrames containing the data to write.
  (See the [`tsdb` backend-specific parameters](#method-write-tsdb-param-dfs).)
- <a id="method-write-param-index_cols"></a>**index_cols** (Optional) (default: `None`) &mdash; `[]str` &mdash; A list of column (attribute) names to be used as index columns for the write operation, regardless of any index-column definitions in the DataFrame.
  By default, the DataFrame's index columns are used.
  <br/>
  > **Note:** The significance and supported number of index columns is backend specific.
  > For example, the `kv` backend supports only a single index column for the primary-key item attribute, while the `tsdb` backend supports additional index columns for metric labels.
- <a id="method-write-param-labels"></a>**labels** (Optional) &mdash; This parameter is currently applicable only to the `tsdb` backend (although it's available for all backends) and is therefore documented as part of the `write` method's [`tsdb` backend parameters](#method-write-tsdb-param-labels).
- <a id="method-write-param-max_in_message"></a>**max_in_message** (Optional) (default: `0`)

<a id="method-write-params-kv"></a>
#### `kv` Backend `write` Parameters

The following `write` parameters are specific to the `kv` backend; for more information about these parameters, see the platform's NoSQL documentation:

<!--
- <a id="method-write-kv-param-expression"></a>**expression** (Optional) (default: `None`) &mdash; A platform update expression that determines how to update the table for all items in the DataFrame.
  For detailed information about platform update expressions, see the [platform documentation](https://www.iguazio.com/docs/reference/latest-release/expressions/update-expression/).
-->
  <!-- [IntInfo] [c-no-update-expression-support] (24.9.19) See Bug IG-12510,
    Requirement IG-5339, & DOC IG-12272. TODO: When update expressions are
    supported, edit the doc here and for the `write` method's `expression`
    parameter as well as the Frames help text. See v3io/tutorials commit 03c7feb
    (PR #119) for removal of updated documentation of Frames update expressions.
    The original write example below had an `expression` parameter:
client.write(backend="kv", table="mytable", dfs=df, expression="city='NY'", condition="age>14")
    -->

- <a id="method-write-kv-param-condition"></a>**condition** (Optional) (default: `None`) &mdash; A platform condition expression that defines conditions for performing the write operation.
  For detailed information about platform condition expressions, see the [platform documentation](https://www.iguazio.com/docs/reference/latest-release/expressions/condition-expression/).

<a id="method-write-params-tsdb"></a>
#### `tsdb` Backend `write` Parameters

The following `write` parameter descriptions are specific to the `tsdb` backend; for more information about these parameters, see the [V3IO TSDB documentation](https://github.com/v3io/v3io-tsdb#v3io-tsdb):

- <a id="method-write-tsdb-param-dfs"></a>**dfs** (Required) &mdash; A single DataFrame, a list of DataFrames, or a DataFrames iterator &mdash; One or more DataFrames containing the data to write.
  This is a common `write` parameter, but the following information is specific to the `tsdb` backend:

  - You must define one or more non-index DataFrame columns that represent the sample metrics; the name of the column is the metric name and its values is the sample data (i.e., the ingested metric).
  - You must define a single index column whose value is the sample time of the data.
    This column serves as the table's primary-key attribute.
    Note that a TSDB DataFrame cannot have more than one index column of a time data type.
  - <a id="tsdb-label-index-columns"></a>You can optionally define string index columns that represent metric labels for the current DataFrame row.
    Note that you can also define labels for all DataFrame rows by using the [`labels`](#method-write-tsdb-param-labels) parameter (in addition or instead of using column indexes to apply labels to a specific row).

- <a id="method-write-tsdb-param-labels"></a>**labels** (Optional) (default: `None`) &mdash; `dict` &mdash; A dictionary of metric labels of the format `{<label>: <value>[, <label>: <value>, ...]}`, which will be applied to all the DataFrame rows (i.e., to all the ingested metric samples).
  For example, `{"os": "linux", "arch": "x86"}`.
  Note that you can also define labels for a specific DataFrame row by adding a string index column to the row (in addition or instead of using the `labels` parameter to define labels for all rows), as explained in the description of the [`dfs`](#tsdb-label-index-columns) parameter.

<a id="method-write-examples"></a>
#### `write` Examples

<a id="method-write-examples-kv"></a>
##### `kv` Backend
<!-- TODO: Add example descriptions. -->

```python
data = [["tom", 10, "TLV"], ["nick", 15, "Berlin"], ["juli", 14, "NY"]]
df = pd.DataFrame(data, columns = ["name", "age", "city"])
df.set_index("name", inplace=True)
client.write(backend="kv", table="mytable", dfs=df, condition="age>14")
```

<!-- TODO: Add examples.
<a id="method-write-examples-tsdb"></a>
##### `tsdb` Backend

<a id="method-stream-examples-tsdb"></a>
##### `stream` Backend
-->

<a id="method-read"></a>
### `read` Method

Reads data from a table or stream in a platform data container to a DataFrame, according to the configured backend.

- [Syntax](#method-read-syntax)
- [Common parameters](#method-read-common-params)
- [`kv` backend `read` parameters](#method-read-params-kv)
- [`tsdb` backend `read` parameters](#method-read-params-tsdb)
- [`stream` backend `read` parameters](#method-read-params-stream)
- [Return Value](#method-read-return-value)
- [Examples](#method-read-examples)

Reads data from a backend.

<a id="method-read-syntax"></a>
#### Syntax

```python
read(backend='', table='', query='', columns=None, filter='', group_by='',
    limit=0, data_format='', row_layout=False, max_in_message=0, marker='',
    iterator=False, **kw)
```

> **Note:** The `limit`, `data_format`, `row_layout`, and `marker` parameters aren't supported in the current release.

<a id="method-read-common-params"></a>
#### Common `read` Parameters

All Frames backends that support the `read` method support the following common parameters:

- <a id="method-read-param-iterator"></a>**iterator** &mdash; (Optional) (default: `False`) &mdash; `bool` &mdash; `True` to return a DataFrames iterator; `False` to return a single DataFrame.

- <a id="method-read-param-filter"></a>**filter** (Optional) &mdash; `str` &mdash; A query filter.
  For example, `filter="col1=='my_value'"`.
  <br/>
  This parameter cannot be used concurrently with the `query` parameter of the `tsdb` backend.

- <a id="method-read-param-columns"></a>**columns** &mdash; `[]str` &mdash; A list of attributes (columns) to return.
  <br/>
  This parameter cannot be used concurrently with the `query` parameter of the `tsdb` backend.

<a id="method-read-params-kv"></a>
#### `kv` Backend `read` Parameters

The following `read` parameters are specific to the `kv` backend; for more information about these parameters, see the platform's NoSQL documentation:

- <a id="method-read-kv-param-max_in_message"></a>**max_in_message** &mdash; `int` &mdash; The maximum number of rows per message.

The following parameters are passed as keyword arguments via the `kw` parameter:

- <a id="method-read-kv-param-reset_index"></a>**reset_index** &mdash; `bool` &mdash; Determines whether to reset the index index column of the returned DataFrame: `True` &mdash; reset the index column by setting it to the auto-generated pandas range-index column; `False` (default) &mdash; set the index column to the table's primary-key attribute.

- <a id="method-read-kv-param-sharding_keys"></a>**sharding_keys** &mdash; `[]string` &mdash; A list of specific sharding keys to query, for range-scan formatted tables only.
  <!-- [IntInfo] Tech Preview [TECH-PREVIEW-FRAMES-KV-READ-SHARDING-KEYS-PARAM]
  -->

<a id="method-read-params-tsdb"></a>
#### `tsdb` Backend `read` Parameters

The following `read` parameters are specific to the `tsdb` backend; for more information about these parameters, see the [V3IO TSDB documentation](https://github.com/v3io/v3io-tsdb#v3io-tsdb):

- <a id="method-read-tsdb-param-group_by"></a>**group_by** (Optional) &mdash; `str` &mdash; A group-by query string.
  <br/>
  This parameter cannot be used concurrently with the `query` parameter.

- <a id="method-read-tsdb-param-query"></a>**query** (Optional) &mdash; `str` &mdash; A query string in SQL format.
  > **Note:**
  > - When setting the `query` parameter, you must provide the path to the TSDB table as part of the `FROM` caluse in the query string and not in the `read` method's [`table`](#client-method-param-table) parameter.
  > - This parameter cannot be set concurrently with the following parameters: [`aggregators`](#method-read-tsdb-param-aggregators), [`columns`](#method-read-tsdb-param-columns), [`filter`](#method-read-tsdb-param-filter), or [`group_by`](#method-read-tsdb-param-group_by) parameters.

The following parameters are passed as keyword arguments via the `kw` parameter:

- <a id="method-read-tsdb-param-start"></a>**start** &mdash; `str` &mdash; Start (minimum) time for the read operation, as a string containing an RFC 3339 time, a Unix timestamp in milliseconds, a relative time of the format `"now"` or `"now-[0-9]+[mhd]"` (where `m` = minutes, `h` = hours, and `'d'` = days), or 0 for the earliest time.
  For example: `"2016-01-02T15:34:26Z"`; `"1451748866"`; `"now-90m"`; `"0"`.
  <br/>
  The default start time is `<end time> - 1h`.

- <a id="method-read-tsdb-param-end"></a>**end** &mdash; `str` &mdash; End (maximum) time for the read operation, as a string containing an RFC 3339 time, a Unix timestamp in milliseconds, a relative time of the format `"now"` or `"now-[0-9]+[mhd]"` (where `m` = minutes, `h` = hours, and `'d'` = days), or 0 for the earliest time.
  For example: `"2018-09-26T14:10:20Z"`; `"1537971006000"`; `"now-3h"`; `"now-7d"`.
  <br/>
  The default end time is `"now"`.

- <a id="method-read-tsdb-param-step"></a>**step** (Optional) &mdash; `str` &mdash; The query step (interval), which determines the points over the query's time range at which to perform aggregations (for an aggregation query) or downsample the data (for a query without aggregators).
  The default step is the query's time range, which can be configured via the [start](#method-read-tsdb-param-start) and [end](#method-read-tsdb-param-end) parameters.

- <a id="method-read-tsdb-param-aggregators"></a>**aggregators** (Optional) &mdash; `str` &mdash; Aggregation information to return, as a comma-separated list of supported aggregation functions ("aggregators").
  The following aggregation functions are supported for over-time aggregation (across each unique label set); for cross-series aggregation (across all metric labels), add "`_all`" to the end of the function name:
  <br/>
  `avg` | `count` | `last` | `max` | `min` | `rate` | `stddev` | `stdvar` | `sum`
  <br/>
  This parameter cannot be used concurrently with the `query` parameter.

- <a id="method-read-tsdb-param-aggregationWindow"></a>**aggregationWindow** (Optional) &mdash; `str` &mdash; Aggregation interval for applying over-time aggregation functions, if set in the [`aggregators`](#method-read-tsdb-param-aggregators) or [`query`](#method-read-tsdb-param-query) parameters, as a string of the format `"[0-9]+[mhd]"` where '`m`' = minutes, '`h`' = hours, and '`d`' = days.
  The default aggregation window is the query's aggregation [step](#method-read-tsdb-param-step).
  When using the default aggregation window, the aggregation window starts at the aggregation step; when the `aggregationWindow` parameter is set, the aggregation window ends at the aggregation step. 

- <a id="method-read-tsdb-param-multi_index"></a>**multi_index** (Optional) &mdash; `bool` &mdash; `True` to receive the read results as multi-index DataFrames where the labels are used as index columns in addition to the metric sample-time primary-key attribute; `False` (default) only the timestamp will function as the index.

<a id="method-read-params-stream"></a>
#### `stream` Backend `read` Parameters

The following `read` parameters are specific to the `stream` backend and are passed as keyword arguments via the `kw` parameter; for more information about these parameters, see the [platform's streams documentation](https://www.iguazio.com/docs/concepts/latest-release/streams):

- <a id="method-read-stream-param-seek"></a>**seek** &mdash; `str` (Required) &mdash; Seek type.
  Valid values: `"time"` | `"seq"` | `"sequence"` | `"latest"` | `"earliest"`.
  <br/>
  When the `"seq"` or `"sequence"` seek type is set, you must set the [`sequence`](#method-read-stream-param-sequence) parameter to the desired record sequence number.
  <br/>
  When the `time` seek type is set, you must set the [`start`](#method-read-stream-param-start) parameter to the desired seek start time.

- <a id="method-read-stream-param-shard_id"></a>**shard_id** &mdash; `str` (Required) The ID of the stream shard from which to read.
  Valid values: `"0"` ... `"<stream shard count> - 1"`.

- <a id="method-read-stream-param-sequence"></a>**sequence** &mdash; `int64` (Required when [`seek`](#method-read-stream-param-seek) = `"sequence"`) &mdash; The sequence number of the record from which to start reading.

- <a id="method-read-stream-param-start"></a>**start** &mdash; `str` (Required when [`seek`](#method-read-stream-param-seek) = `"time"`) &mdash; The earliest record ingestion time from which to start reading, as a string containing an RFC 3339 time, a Unix timestamp in milliseconds, a relative time of the format `"now"` or `"now-[0-9]+[mhd]"` (where `m` = minutes, `h` = hours, and `'d'` = days), or 0 for the earliest time.
  For example: `"2016-01-02T15:34:26Z"`; `"1451748866"`; `"now-90m"`; `"0"`.

<a id="method-read-return-value"></a>
#### Return Value

- When the value of the [`iterator`](#method-read-param-iterator) parameter is `False` (default) &mdash; returns a single DataFrame.
- When the value of the `iterator` parameter is `True` &mdash; returns a
  DataFrames iterator.

<!-- [IntInfo] See IG-14065. -->
<!-- 
> **Note:** The returned DataFrames include a `labels` DataFrame attribute with backend-specific data, if applicable.
> For example, for the `stream` backend, this attribute holds the sequence number of the last stream record that was read.
-->

<a id="method-read-examples"></a>
#### `read` Examples
<!-- TODO: Add example descriptions. -->

<a id="method-read-examples-kv"></a>
##### `kv` Backend

```python
df = client.read(backend="kv", table="mytable", filter="col1>666")
```

<a id="method-read-examples-tsdb"></a>
##### `tsdb` Backend

```python
df = client.read(backend="tsdb", query="select avg(cpu) as cpu, avg(diskio), avg(network) from mytable where os='win'", start="now-1d", end="now", step="2h")
```

<a id="method-stream-examples-tsdb"></a>
##### `stream` Backend

```python
df = client.read(backend="stream", table="mytable", seek="latest", shard_id="5")
```

<a id="method-delete"></a>
### `delete` Method

Deletes a table or stream or specific table items from a platform data container, according to the specified backend type.

- [Syntax](#method-delete-syntax)
- [`kv` backend `delete` parameters](#method-delete-params-kv)
- [`tsdb` backend `delete` parameters](#method-delete-params-tsdb)
- [Examples](#method-delete-examples)

<a id="method-delete-syntax"></a>
#### Syntax

```python
delete(backend, table, filter='', start='', end='', if_missing=FAIL
```

<a id="method-delete-params-kv"></a>
#### `kv` Backend `delete` Parameters

- <a id="method-delete-kv-param-filter"></a>**filter** &mdash; A platform filter expression that identifies specific items to delete.
  For detailed information about platform filter expressions, see the [platform documentation](https://www.iguazio.com/docs/reference/latest-release/expressions/condition-expression/#filter-expression).
  > **Note:** When the `filter` parameter isn't set, the entire table and its schema file (**.#schema**) are deleted.

  - **Type:** `str`
  - **Requirement:** Optional
  - **Default Value:** `""`

<a id="method-delete-params-tsdb"></a>
#### `tsdb` Backend `delete` Parameters

The following `delete` parameters are specific to the `tsdb` backend; for more information about these parameters, see the [V3IO TSDB documentation](https://github.com/v3io/v3io-tsdb#v3io-tsdb):

- <a id="method-delete-tsdb-param-start"></a>**start** &mdash; Start (minimum) time for the delete operation &mdash; i.e., delete only items whose data sample time is at or after (`>=`) the specified start time.

  - **Type:** `str`
  - **Requirement:** Optional
  - **Valid Values:** A string containing an RFC 3339 time, a Unix timestamp in milliseconds, a relative time of the format `"now"` or `"now-[0-9]+[mhd]"` (where `m` = minutes, `h` = hours, and `'d'` = days), or 0 for the earliest time.
    For example: `"2016-01-02T15:34:26Z"`; `"1451748866"`; `"now-90m"`; `"0"`.
  - **Default Value:** `""` when neither `start` nor [`end`](#method-delete-tsdb-param-end) are set &mdash; to delete the entire table and its schema file (**.schema**) &mdash; and `0` when `end` is set

- <a id="method-delete-tsdb-param-end"></a>**end** &mdash; `str` &mdash; End (maximum) time for the delete operation &mdash; i.e., delete only items whose data sample time is before or at (`<=`) the specified end time.

  - **Type:** `str`
  - **Requirement:** Optional
  - **Valid Values:** A string containing an RFC 3339 time, a Unix timestamp in milliseconds, a relative time of the format `"now"` or `"now-[0-9]+[mhd]"` (where `m` = minutes, `h` = hours, and `'d'` = days), or 0 for the earliest time.
    For example: `"2018-09-26T14:10:20Z"`; `"1537971006000"`; `"now-3h"`; `"now-7d"`.
  - **Default Value:** `""` when neither [`start`](#method-delete-tsdb-param-start) nor `end` are set &mdash; to delete the entire table and its schema file (**.schema**) &mdash; and `0` when `start` is set

> **Note:**
> - When neither the [`start`](#method-delete-tsdb-param-start) nor [`end`](#method-delete-tsdb-param-end) parameters are set, the entire TSDB table and its schema file are deleted.
> - Only full table partitions within the specified time frame (as determined by the `start` and `end` parameters) are deleted.
>   Items within the specified time frames that reside within partitions that begin before the delete start time or end after the delete end time aren't deleted.
>   The partition interval is calculated automatically based on the table's ingestion rate and is stored in the TSDB's `partitionerInterval` schema field (see the  **.schema** file).

<a id="method-delete-examples"></a>
#### `delete` Examples
<!-- TODO: Add example descriptions. -->

<a id="method-delete-examples-kv"></a>
##### `kv` Backend

```python
client.delete(backend="kv", table="mytable", filter="age > 40")
```

<a id="method-delete-examples-tsdb"></a>
##### `tsdb` Backend

```python
client.delete(backend="tsdb", table="mytable", start="now-1d", end="now-5h")
```

<a id="method-delete-examples-stream"></a>
##### `stream` Backend

```python
client.delete(backend="stream", table="mystream")
```

<a id="method-execute"></a>
### `execute` Method

Extends the basic CRUD functionality of the other client methods via backend-specific commands.

- [Syntax](#method-execute-syntax)
- [Common parameters](#method-execute-common-params)
- [kv backend commands](#method-execute-kv-cmds)
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

- <a id="method-except-param-args"></a>**args** &mdash; A dictionary of `<argument name>: <value>` pairs for passing command-specific parameters (arguments).

  - **Type:** `dict`
  - **Requirement:** Optional
  - **Default Value:** `None`

<a id="method-execute-kv-cmds"></a>
### `kv` Backend `execute` Commands

- <a id="method-execute-kv-cmd-infer"></a>**infer | inferschema** &mdash; Infers the data schema of a given NoSQL table and creates a schema file for the table.

  Example:
  ```python
  client.execute(backend="kv", table="mytable", command="infer")
  ````

<!--
- <a id="method-execute-kv-cmd-update"></a>**update** &mdash; Updates a specific item in a NoSQL table according to the provided update expression.
  For detailed information about platform update expressions, see the [platform documentation](https://www.iguazio.com/docs/reference/latest-release/expressions/update-expression/).

  Example:
  ```python
  client.execute(backend="kv", table="mytable", command="update", args={"key": "somekey", "expression": "col2=30", "condition": "col3>15"})
  ```
-->
  <!-- [IntInfo] [c-no-update-expression-support] -->

<a id="method-execute-stream-cmds"></a>
### `stream` Backend `execute` Commands

- <a id="method-execute-stream-cmd-put"></a>**put** &mdash; Adds records to a stream.

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


