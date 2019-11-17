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
- [Client Constructor](#client-constructor)
- [Common Client Method Parameters](#client-common-method-params)
- [create Method](#method-create)
- [write Method](#method-write)
- [read Method](#method-read)
- [delete Method](#method-delete)
- [execute Method](#method-execute)

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

- [`create`](#method-create) &mdash; creates a new TSDB table or stream ("backend data").
- [`delete`](#method-delete) &mdash; deletes a table or stream or specific table items.
- [`read`](#method-read) &mdash; reads data from a table or stream into pandas DataFrames.
- [`write`](#method-write) &mdash; writes data from pandas DataFrames to a table or stream.
- [`execute`](#method-execute) &mdash; executes a backend-specific command on a table or stream.
  Each backend may support multiple commands.

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
    You can get the access key from the **Access Keys** window that's available from the user-profile menu of the platform dashboard, or by copying the value of the `V3IO_ACCESS_KEY` environment variable in a platform web-shell or Jupyter Notebook service.
  - <a id="user-auth-user-password"></a>Set the [`user`](#client-param-user) and [`password`](#client-param-password) constructor parameters to the username and password of a platform user with the required data-access permissions.
  <br/>

  > **Note:** You can't use both methods concurrently: setting both the `token` and `username` and `password` parameters in the same constructor call will produce an error.

- <a id="user-auth-client-envar"></a>Set the authentication credentials in environment variables, by using either of the following methods:

  - <a id="user-auth-client-envar-access-key"></a>Set the `V3IO_ACCESS_KEY` environment variable to a valid platform access key with the required data-access permissions.

    > **Note:** The platform's Jupyter Notebook service automatically defines the `V3IO_ACCESS_KEY` environment variable and initializes it to a valid access key for the running user of the service. 
  - <a id="user-auth-client-envar-user-pwd"></a>Set the `V3IO_USERNAME` and `V3IO_PASSWORD` environment variables to the username and password of a platform user with the required data-access permissions.

  > **Note:**
  > - When the client constructor is called with authentication parameters ([option #1](#user-auth-client-const-params)), the authentication-credentials environment variables (if defined) are ignored.
  > - When `V3IO_ACCESS_KEY` is defined, `V3IO_USERNAME` and `V3IO_PASSWORD` are ignored.

<a id="client-constructor"></a>
### Client Constructor

All Frames operations are executed via an object of the `Client` class.

- [Syntax](#client-constructor-syntax)
- [Parameters and Data Members](#client-constructor-parameters)
- [Return Value](#client-constructor-return-value)
- [Example](#client-constructor-example)

<a id="client-constructor-syntax"></a>
#### Syntax

```python
Client(address='', container='', user='', password='', token='')
```

<a id="client-constructor-parameters"></a>
#### Parameters and Data Members

- <a id="client-param-address"></a>**address** &mdash; The address of the Frames service (`framesdb`).
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

- <a id="client-param-container"></a>**container** &mdash; The name of the platform data container that contains the backend data.
  For example, `"bigdata"` or `"users"`.

  - **Type:** `str`
  - **Requirement:** Required

- <a id="client-param-user"></a>**user** &mdash; The username of a platform user with permissions to access the backend data.

  - **Type:** `str`
  - **Requirement:** Required when neither the [`token`](#client-param-token) parameter or the authentication environment variables are set.
    See [User Authentication](#user-authentication).
    <br/>
    When the `user` parameter is set, the [`password`](#client-param-password) parameter must also be set to a matching user password.

- <a id="client-param-password"></a>**password** &mdash; A platform password for the user configured in the [`user`](#client-param-user) parameter.

  - **Type:** `str`
  - **Requirement:** Required when the [`user`](#client-param-user) parameter is set.
    See [User Authentication](#user-authentication).

- <a id="client-param-token"></a>**token** &mdash; A valid platform access key that allows access to the backend data.
  To get this access key, select the user profile icon on any platform dashboard page, select **Access Tokens**, and copy an existing access key or create a new key.

  - **Type:** `str`
  - **Requirement:** Required when neither the [`user`](#client-param-user) or [`password`](#client-param-password) parameters or the authentication environment variables are set.
    See [User Authentication](#user-authentication).

<a id="client-constructor-return-value"></a>
#### Return Value

Returns a new Frames `Client` data object.

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

- <a id="client-method-param-backend"></a>**backend** &mdash; The backend data type for the operation.
  See the backend-types descriptions in the [overview](#backend-types).

  - **Type:** `str`
  - **Valid Values:** `"csv"` | `"kv"` | `"stream"` | `"tsdb"`
  - **Requirement:** Required

- <a id="client-method-param-table"></a>**table** &mdash; The relative path to the backend data &mdash; A directory in the target platform data container (as configured for the client object) that represents a TSDB or NoSQL table or a data stream.
  For example, `"mytable"` or `"examples/tsdb/my_metrics"`.

  - **Type:** `str`
  - **Requirement:** Required unless otherwise specified in the method-specific documentation

Additional method-specific parameters are described for each method.

<a id="method-create"></a>
### create Method

Creates a new TSDB table or stream in a platform data container, according to the specified backend type.

The `create` method is supported by the `tsdb` and `stream` backends, but not by the `kv` backend, because NoSQL tables in the platform don't need to be created prior to ingestion; when ingesting data into a table that doesn't exist, the table is automatically created.

- [Syntax](#method-create-syntax)
- [Common parameters](#method-create-common-params)
- [`tsdb` backend `create` parameters](#method-create-params-tsdb)
- [`stream` backend `create` parameters](#method-create-params-stream)

<a id="method-create-syntax"></a>
#### Syntax

```python
create(backend, table, attrs=None)
```
<!--
create(backend, table, attrs=None, schema=None)
-->
<!-- [IntInfo] (26.9.19) The `schema` parameter is used only with the `csv`
 test backend. -->

<a id="method-create-common-params"></a>
#### Common create Parameters

All Frames backends that support the `create` method support the following common parameters:

- <a id="method-create-param-attrs"></a>**attrs** &mdash; A dictionary of `<argument name>: <value>` pairs for passing additional backend-specific parameters (arguments).

  - **Type:** dict
  - **Requirement:** Optional
  - **Default Value:** `None`

<a id="method-create-params-tsdb"></a>
#### tsdb Backend create Parameters

The following `tsdb` backend parameters are passed via the [`attrs`](#method-create-param-attrs) parameter of the  `create` method:

- <a id="method-create-tsdb-param-rate"></a>**rate** &mdash; The ingestion rate TSDB's metric-samples, as `"[0-9]+/[smh]"` (where `s` = seconds, `m` = minutes, and `h` = hours); for example, `"1/s"` (one sample per minute).
  The rate should be calculated according to the slowest expected ingestion rate.

  - **Type:** `str`
  - **Requirement:** Required

- <a id="method-create-tsdb-param-aggregates"></a>**aggregates** &mdash; Default aggregates to calculate in real time during the samples ingestion, as a comma-separated list of supported aggregation functions.

  - **Type:** `str`
  - **Requirement:** Optional

- <a id="method-create-tsdb-param-aggregation"></a>**aggregation-granularity** &mdash; Aggregation granularity; i.e., a time interval for applying the aggregation functions, if configured in the [`aggregates`](#method-create-tsdb-param-aggregates) parameter.

  - **Type:** `str`
  - **Requirement:** Optional

For detailed information about these parameters, refer to the [V3IO TSDB documentation](https://github.com/v3io/v3io-tsdb#v3io-tsdb).

Example:
```python
client.create("tsdb", "/mytable", attrs={"rate": "1/m"})
```

<a id="method-create-params-stream"></a>
#### stream Backend create Parameters

The following `stream` backend parameters are passed via the [`attrs`](#method-create-param-attrs) parameter of the  `create` method:

- <a id="method-create-stream-param-shards"></a>**shards** (Optional) (default: `1`) &mdash; `int` &mdash; The number of stream shards to create.
- <a id="method-create-stream-param-retention_hours"></a>**retention_hours** (Optional) (default: `24`) &mdash; `int` &mdash; The stream's retention period, in hours.

For detailed information about these parameters, refer to the [platform streams documentation](https://www.iguazio.com/docs/concepts/latest-release/streams).

Example:
```python
client.create("stream", "/mystream", attrs={"shards": 6})
```

<a id="method-write"></a>
### write Method

Writes data from a DataFrame to a table or stream in a platform data container, according to the specified backend type.

- [Syntax](#method-write-syntax)
- [Common parameters](#method-write-common-params)
- [`tsdb` backend `write` parameters](#method-write-params-tsdb)
- [`kv` backend `write` parameters](#method-write-params-kv)

<a id="method-write-syntax"></a>
#### Syntax

<!--
```python
write(backend, table, dfs, expression='', condition='', labels=None,
    max_in_message=0, index_cols=None, partition_keys=None)
```
-->
<!-- [c-no-update-expression-support] -->
```python
write(backend, table, dfs, condition='', labels=None, max_in_message=0,
    index_cols=None, partition_keys=None)
```

- When the value of the [`iterator`](#method-read-param-iterator) parameter is `False` (default) &mdash; returns a single DataFrame.
- When the value of the `iterator` parameter is `True` &mdash; returns a
  DataFrames iterator.
  The returned DataFrames include a `"labels"` DataFrame attribute with backend-specific data, if applicable; for example, for the `stream` backend, this attribute holds the sequence number of the last stream record that was read.

<a id="method-write-common-params"></a>
#### Common write Parameters

All Frames backends that support the `write` method support the following common parameters:

- <a id="method-write-param-dfs"></a>**dfs** (Required) &mdash; A single DataFrame, a list of DataFrames, or a DataFrames iterator &mdash; One or more DataFrames containing the data to write.
- <a id="method-write-param-index_cols"></a>**index_cols** (Optional) (default: `None`) &mdash; `[]str` &mdash; A list of column (attribute) names to be used as index columns for the write operation, regardless of any index-column definitions in the DataFrame.
  By default, the DataFrame's index columns are used.
  <br/>
  > **Note:** The significance and supported number of index columns is backend specific.
  > For example, the `kv` backend supports only a single index column for the primary-key item attribute, while the `tsdb` backend supports additional index columns for metric labels.
- <a id="method-write-param-labels"></a>**labels** (Optional) (default: `None`) &mdash; This parameter is currently defined for all backends but is used only for the TSDB backend, therefore it's documented as part of the `write` method's [`tsdb` backend parameters](#method-write-params-tsdb).
- <a id="method-write-param-max_in_message"></a>**max_in_message** (Optional) (default: `0`)
- <a id="method-writse-param-partition_keys"></a>**partition_keys** (Optional) (default: `None`) &mdash; `[]str` &mdash; [**Not supported in this version**]

Example:
```python
data = [["tom", 10], ["nick", 15], ["juli", 14]]
df = pd.DataFrame(data, columns = ["name", "age"])
df.set_index("name", inplace=True)
client.write(backend="kv", table="mytable", dfs=df)
```

<a id="method-write-params-tsdb"></a>
#### tsdb Backend write Parameters

- <a id="method-write-param-labels"></a>**labels** (Optional) (default: `None`) &mdash; `dict` &mdash; A dictionary of `<label name>: <label value>` pairs that define metric labels to add to all written metric-sample table items.
  Note that the values of the metric labels must be of type string.

<a id="method-write-params-kv"></a>
#### kv Backend write Parameters

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
v3c.write(backend="kv", table="mytable", dfs=df, expression="city='NY'", condition="age>14")
    -->

- <a id="method-write-kv-param-condition"></a>**condition** (Optional) (default: `None`) &mdash; A platform condition expression that defines conditions for performing the write operation.
  For detailed information about platform condition expressions, see the [platform documentation](https://www.iguazio.com/docs/reference/latest-release/expressions/condition-expression/).

Example:
```python
data = [["tom", 10, "TLV"], ["nick", 15, "Berlin"], ["juli", 14, "NY"]]
df = pd.DataFrame(data, columns = ["name", "age", "city"])
df.set_index("name", inplace=True)
v3c.write(backend="kv", table="mytable", dfs=df, condition="age>14")
```

<a id="method-read"></a>
### read Method

Reads data from a table or stream in a platform data container to a DataFrame, according to the configured backend.

- [Syntax](#method-read-syntax)
- [Common parameters](#method-read-common-params)
- [`tsdb` backend `read` parameters](#method-read-params-tsdb)
- [`kv` backend `read` parameters](#method-read-params-kv)
- [`stream` backend `read` parameters](#method-read-params-stream)
- [Return Value](#method-read-return-value)

Reads data from a backend.

<a id="method-read-syntax"></a>
#### Syntax

```python
read(backend='', table='', query='', columns=None, filter='', group_by='',
    limit=0, data_format='', row_layout=False, max_in_message=0, marker='',
    iterator=False, **kw)
```

<a id="method-read-common-params"></a>
#### Common read Parameters

All Frames backends that support the `read` method support the following common parameters:

- <a id="method-read-param-iterator"></a>**iterator** &mdash; (Optional) (default: `False`) &mdash; `bool` &mdash; `True` to return a DataFrames iterator; `False` to return a single DataFrame.
- <a id="method-read-param-filter"></a>**filter** (Optional) &mdash; `str` &mdash; A query filter.
  <br/>
  This parameter can't be used concurrently with the `query` parameter. Example: `filter="col1=='my_value'"`
- <a id="method-read-param-columns"></a>**columns** &mdash; `[]str` &mdash; A list of attributes (columns) to return.
  <br/>
  This parameter can't be used concurrently with the `query` parameter.
- <a id="method-read-param-data_format"></a>**data_format** &mdash; `str` &mdash; The data format. [**Not supported in this version**]
- <a id="method-read-param-marker"></a>**marker** &mdash; `str` &mdash; A query marker. [**Not supported in this version**]
- <a id="method-read-param-limit"></a>**limit** &mdash; `int` &mdash; The maximum number of rows to return. [**Not supported in this version**]
- <a id="method-read-param-row_layout"></a>**row_layout** (Optional) (default: `False`) &mdash; `bool` &mdash; `True` to use a row layout; `False` (default) to use a column layout. [**Not supported in this version**]

<a id="method-read-params-tsdb"></a>
#### tsdb Backend read Parameters

- <a id="method-read-tsdb-param-start"></a>**start** &mdash; `str` &mdash; Start (minimum) time for the read operation, as a string containing an RFC 3339 time, a Unix timestamp in milliseconds, a relative time of the format `"now"` or `"now-[0-9]+[mhd]"` (where `m` = minutes, `h` = hours, and `'d'` = days), or 0 for the earliest time.
  For example: `"2016-01-02T15:34:26Z"`; `"1451748866"`; `"now-90m"`; `"0"`.
  <br/>
  The default start time is `<end time> - 1h`.
- <a id="method-read-tsdb-param-end"></a>**end** &mdash; `str` &mdash; End (maximum) time for the read operation, as a string containing an RFC 3339 time, a Unix timestamp in milliseconds, a relative time of the format `"now"` or `"now-[0-9]+[mhd]"` (where `m` = minutes, `h` = hours, and `'d'` = days), or 0 for the earliest time.
  For example: `"2018-09-26T14:10:20Z"`; `"1537971006000"`; `"now-3h"`; `"now-7d"`.
  <br/>
  The default end time is `"now"`.
- <a id="method-read-tsdb-param-step"></a>**step** (Optional) &mdash; `str` &mdash; For an aggregation query, this parameter specifies the aggregation interval for applying the aggregation functions; by default, the aggregation is applied to all sample data within the requested time range.<br/>
  When the query doesn't include aggregates, this parameter specifies an interval for downsampling the raw sample data.
- <a id="method-read-tsdb-param-aggregators"></a>**aggregators** (Optional) &mdash; `str` &mdash; Aggregation information to return, as a comma-separated list of supported aggregation functions.
- <a id="method-read-tsdb-param-aggregationWindow"></a>**aggregationWindow** (Optional) &mdash; `str` &mdash; Aggregation interval for applying the aggregation functions, if set in the [`aggregators`](#"method-read-tsdb-param-aggregators) or [`query`](#method-read-tsdb-param-query) parameters.
- <a id="method-read-tsdb-param-query"></a>**query** (Optional) &mdash; `str` &mdash; A query string in SQL format.
  > **Note:** When the `query` parameter is set, you can either specify the target table within the query string (`FROM <table>`) or by setting the `table` parameter of the `read` method to the table path.
  > When the `query` string specifies the target table, the value of the `table` parameter (if set) is ignored.
- <a id="method-read-tsdb-param-group_by"></a>**group_by** (Optional) &mdash; `str` &mdash; A group-by query  string.
  <br/>
  This parameter can't be used concurrently with the `query` parameter.
- <a id="method-read-tsdb-param-multi_index"></a>**multi_index** (Optional) &mdash; `bool` &mdash; `True` to receive the read results as multi-index DataFrames where the labels are used as index columns in addition to the metric sample-time primary-key attribute; `False` (default) only the timestamp will function as the index.
  <!-- [IntInfo] This parameter is available via the `kw` read parameter. -->

For detailed information about these parameters, refer to the [V3IO TSDB documentation](https://github.com/v3io/v3io-tsdb#v3io-tsdb).

Example:
```python
df = client.read(backend="tsdb", query="select avg(cpu) as cpu, avg(diskio), avg(network) from mytable where os='win'", start="now-1d", end="now", step="2h")
```

<a id="method-read-params-kv"></a>
#### kv Backend read Parameters

- <a id="method-read-kv-param-reset_index"></a>**reset_index** &mdash; `bool` &mdash; Reset the index. When set to `false` (default), the DataFrame will have the key column of the v3io kv as the index column.
  When set to `true`, the index will be reset to a range index.
- <a id="method-read-kv-param-max_in_message"></a>**max_in_message** &mdash; `int` &mdash; The maximum number of rows per message.
- <a id="method-read-kv-param-"></a>**sharding_keys** &mdash; `[]string` (**Experimental**) &mdash; A list of specific sharding keys to query, for range-scan formatted tables only.
- <a id="method-read-kv-param-segments"></a>**segments** &mdash; `[]int64` [**Not supported in this version**]
- <a id="method-read-kv-param-total_segments"></a>**total_segments** &mdash; `int64` [**Not supported in this version**]
- <a id="method-read-kv-param-sort_key_range_start"></a>**sort_key_range_start** &mdash; `str` [**Not supported in this version**]
- <a id="method-read-kv-param-sort_key_range_end"></a>**sort_key_range_end** &mdash; `str` [**Not supported in this version**]

For detailed information about these parameters, refer to the platform's NoSQL documentation.

Example:
```python
df = client.read(backend="kv", table="mytable", filter="col1>666")
```

<a id="method-read-params-stream"></a>
#### stream Backend read Parameters

- <a id="method-read-stream-param-seek"></a>**seek** &mdash; `str` &mdash; Valid values:  `"time" | "seq"/"sequence" | "latest" | "earliest"`.
  <br/>
  If the `"seq"|"sequence"` seek type is set, you need to provide the desired record sequence ID via the [`sequence`](#method-read-stream-param-sequence) parameter.
  <br/>
  If the `time` seek type is set, you need to provide the desired start time via the `start` parameter.
- <a id="method-read-stream-param-shard_id"></a>**shard_id** &mdash; `str`
- <a id="method-read-stream-param-sequence"></a>**sequence** &mdash; `int64` (Optional)

For detailed information about these parameters, refer to the [platform streams documentation](https://www.iguazio.com/docs/concepts/latest-release/streams).

Example:
```python
df = client.read(backend="stream", table="mytable", seek="latest", shard_id="5")
```

<a id="method-read-return-value"></a>
#### Return Value

- When the value of the [`iterator`](#method-read-param-iterator) parameter is `False` (default) &mdash; returns a single DataFrame.
- When the value of the `iterator` parameter is `True` &mdash; returns a
  DataFrames iterator.

> **Note:** The returned DataFrames include a `labels` DataFrame attribute with backend-specific data, if applicable.
> For example, for the `stream` backend, this attribute holds the sequence number of the last stream record that was read.
<!-- [IntInfo] (26.9.19) For the `kv` backend - no relevant "labels" data.
  For the `tsdb` backend - a labels set. When reading a DFs iterator, each DF
  represents a unique label set, as reflected in the `labels` DF attribute, but
  when reading a single DF, it' currently unclear what `labels` should hold. -->

<a id="method-delete"></a>
### delete Method

Deletes a table or stream or specific table items from a platform data container, according to the specified backend type.

- [Syntax](#method-delete-syntax)
- [`tsdb` backend `delete` parameters](#method-delete-params-tsdb)
- [`kv` backend `delete` parameters](#method-delete-params-kv)

<a id="method-delete-syntax"></a>
#### Syntax

```python
delete(backend, table, filter='', start='', end='')
```

<a id="method-delete-params-tsdb"></a>
#### tsdb Backend delete Parameters

- <a id="method-delete-tsdb-param-start"></a>**start** &mdash; `str` &mdash; Start (minimum) time for the delete operation, as a string containing an RFC 3339 time, a Unix timestamp in milliseconds, a relative time of the format `"now"` or `"now-[0-9]+[mhd]"` (where `m` = minutes, `h` = hours, and `'d'` = days), or 0 for the earliest time.
  For example: `"2016-01-02T15:34:26Z"`; `"1451748866"`; `"now-90m"`; `"0"`.
  <br/>
  The default start time is `<end time> - 1h`.
- <a id="method-delete-tsdb-param-end"></a>**end** &mdash; `str` &mdash; End (maximum) time for the delete operation, as a string containing an RFC 3339 time, a Unix timestamp in milliseconds, a relative time of the format `"now"` or `"now-[0-9]+[mhd]"` (where `m` = minutes, `h` = hours, and `'d'` = days), or 0 for the earliest time.
  For example: `"2018-09-26T14:10:20Z"`; `"1537971006000"`; `"now-3h"`; `"now-7d"`.
  <br/>
  The default end time is `"now"`.

> **Note:** When neither the `start` or `end` parameters are set, the entire TSDB table is deleted.

For detailed information about these parameters, refer to the [V3IO TSDB](https://github.com/v3io/v3io-tsdb#v3io-tsdb) documentation.

Example:
```python
df = client.delete(backend="tsdb", table="mytable", start="now-1d", end="now-5h")
```

<a id="method-delete-params-kv"></a>
#### kv Backend delete Parameters

- <a id="method-delete-kv-param-filter"></a>**filter** &mdash; `str` &mdash; A platform filter expression that identifies specific items to delete.
  For detailed information about platform filter expressions, see the [platform documentation](https://www.iguazio.com/docs/reference/latest-release/expressions/condition-expression/#filter-expression).

> **Note:** When the `filter` parameter isn't set, the entire table is deleted.

Example:
```python
df = client.delete(backend="kv", table="mytable", filter="age > 40")
```

<a id="method-execute"></a>
### execute Method

Extends the basic CRUD functionality of the other client methods via backend-specific commands.

- [Syntax](#method-execute-syntax)
- [Common parameters](#method-execute-common-params)
- [tsdb backend commands](#method-execute-tsdb-cmds)
- [kv backend commands](#method-execute-kv-cmds)
- [stream backend commands](#method-execute-stream-cmds)

<a id="method-execute-syntax"></a>
#### Syntax

```python
execute(backend, table, command='', args=None)
```

<a id="method-execute-common-params"></a>
#### Common execute Parameters

All Frames backends that support the `execute` method support the following common parameters:

- <a id="method-except-param-args"></a>**args** &mdash; A dictionary of `<argument name>: <value>` pairs for passing command-specific parameters (arguments).

  - **Type:** dict
  - **Requirement:** Optional
  - **Default Value:** `None`

<a id="method-execute-tsdb-cmds"></a>
### tsdb Backend execute Commands

Currently, no `execute` commands are available for the `tsdb` backend.

<a id="method-execute-kv-cmds"></a>
### kv Backend execute Commands

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
### stream Backend execute Commands

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

