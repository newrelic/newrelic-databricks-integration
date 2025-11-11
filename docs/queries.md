# Query Metrics

The Databricks Integration can collect telemetry about queries executed in
Databricks [SQL warehouses](https://docs.databricks.com/en/compute/sql-warehouse/index.html)
and [serverless compute](https://docs.databricks.com/aws/en/compute/serverless/),
including execution times, query statuses, and query I/O metrics such as the
number of bytes and rows read and the number of bytes spilled to disk. This
feature is enabled by default and can be enabled or disabled using the
[Databricks query metrics `enabled`](./configuration.md#queries--metrics--enabled)
flag in the [integration configuration](./configuration.md).

## Query Metrics `startOffset` Configuration

On each harvest cycle, the integration uses the [Databricks ReST API](https://docs.databricks.com/api/workspace/introduction)
to retrieve query metrics of queries through [SQL warehouses](https://docs.databricks.com/en/compute/sql-warehouse/index.html)
and [serverless compute](https://docs.databricks.com/aws/en/compute/serverless/).
By default, the [query history endpoint](https://docs.databricks.com/api/workspace/queryhistory/list)
of the [Databricks ReST API](https://docs.databricks.com/api/workspace/introduction)
returns a paginated list of _all_ queries through [SQL warehouses](https://docs.databricks.com/en/compute/sql-warehouse/index.html)
and [serverless compute](https://docs.databricks.com/aws/en/compute/serverless/)
sorted in descending order by start time. On some systems, there may be a large
number of queries to retrieve and process, impacting the performance of the
collector. To account for this, the integration provides the [`startOffset`](./configuration.md#queries--metrics--startoffset)
configuration parameter. This parameter is used to tune the performance of the
collection of query metrics by limiting the number of queries to return when
listing queries to queries with start times greater than the current time at
collection minus the offset.

The effect of this behavior is that only queries which have a start time _at_ or
_after_ the calculated start time will be returned on the API call. For example,
using the default [`startOffset`](./configuration.md#queries--metrics--startoffset)
(600 seconds or 10 minutes), only queries which _started_ within the last 10
minutes will be returned. This means that queries that started more than 10
minutes ago will not be returned _even if_ some of those queries are not yet
complete. Therefore, it is important to carefully select a value for the
[`startOffset`](./configuration.md#queries--metrics--startoffset) parameter that
will account for long-running queries without degrading the performance of the
integration.

Another factor to consider when selecting a value for the [`startOffset`](./configuration.md#queries--metrics--startoffset)
parameter is that the integration sets the [`include_metrics`](https://docs.databricks.com/api/workspace/queryhistory/list#include_metrics)
query parameter to `true` when retrieving the query history. Selecting a large
value for the [`startOffset`](./configuration.md#queries--metrics--startoffset)
may cause degradation in the performance of the API call or have other possible
impacts.

## Query Metrics `intervalOffset` Configuration

On each harvest cycle, the integration records any queries that have completed
since the last harvest cycle by comparing the [query end time](https://docs.databricks.com/api/workspace/queryhistory/list#res-query_end_time_ms)
reported by the [query history endpoint](https://docs.databricks.com/api/workspace/queryhistory/list)
against the time when the last harvest cycle occurred. If there is any lag
setting the query end time, it is possible for some queries to be missed. The
[`intervalOffset`](./configuration.md#queries--metrics--intervaloffset)
configuration parameter can be used to delay the processing of queries in order
to account for any potential lag. The default value for the [`intervalOffset`](./configuration.md#queries--metrics--intervaloffset)
configuration parameter is 5 seconds.

To help illustrate, consider a query that ends one second prior to the
current harvest cycle. If the end time of the query takes one second or more to
be reflected in the API, the query will be returned in the query history but the
end time will not be set during the current harvest cycle and therefore can not
be checked. On the subsequent harvest cycle, the query will again be returned
and it's end time will be set but the query will not be recorded because the end
time is less than the time of the last harvest cycle.

## Query Events

Query metric data is sent to New Relic as [event data](https://docs.newrelic.com/docs/data-apis/understand-data/new-relic-data-types/#event-data).
The provided events and attributes are listed in the sections below.

**NOTE:**
* Unlike job metrics, pipeline metrics, and Spark job, stage, and task metrics,
  query metrics are only recorded once a query has completed execution. There
  are no `start` and `complete` events.
* When [serverless compute](https://docs.databricks.com/aws/en/compute/serverless/)
  is enabled, the integration will collect metrics for all SQL and Python
  queries executed on serverless compute for notebooks and jobs.
* Some of the text below is sourced from the documentation on
  [Databricks queries](https://docs.databricks.com/aws/en/sql/user/queries/) and
  the [Databricks SDK Go module documentation](https://pkg.go.dev/github.com/databricks/databricks-sdk-go),
  in particular the file [`model.go`](https://github.com/databricks/databricks-sdk-go/blob/main/service/sql/model.go)

### `DatabricksQuery` events

The integration records an event for each query that completes while the
integration is running. The event is reported using the `DatabricksQuery` event
type.

Each `DatabricksQuery` event includes the following attributes.

| Attribute Name                | Data Type   | Description                                                                 |
|-------------------------------|-------------|---------------------------------------------------------------------------- |
| `id`                          | string      | The query ID                                                                |
| `type`                        | string      | The type of statement for this query (for example, `SELECT`, `INSERT`)      |
| `status`                      | string      | The query status (one of `QUEUED`, `RUNNING`, `CANCELED`, `FAILED`, `FINISHED`) |
| `warehouseId`                 | string      | The ID of the [SQL warehouse](https://docs.databricks.com/en/compute/sql-warehouse/index.html) where the query was executed |
| `warehouseName`               | string      | The name of the [SQL warehouse](https://docs.databricks.com/en/compute/sql-warehouse/index.html) where the query was executed |
| `warehouseCreator`            | string      | The creator of the [SQL warehouse](https://docs.databricks.com/en/compute/sql-warehouse/index.html) where the query was executed (only included  if [`includeIdentityMetadata`](./configuration.md#queries--metrics--includeidentitymetadata) is `true`) |
| `workspaceId`                 | int         | [ID](https://docs.databricks.com/en/workspace/workspace-details.html#workspace-instance-names-urls-and-ids) of the Databricks workspace containing the SQL warehouse or serverless compute where the query was executed |
| `workspaceName`               | string      | [Instance name](https://docs.databricks.com/en/workspace/workspace-details.html#workspace-instance-names-urls-and-ids) of the Databricks workspace containing the SQL warehouse or serverless compute where the query was executed |
| `workspaceUrl`                | string      | [URL](https://docs.databricks.com/en/workspace/workspace-details.html#workspace-instance-names-urls-and-ids) of the Databricks workspace containing the SQL warehouse or serverless compute where the query was executed |
| `duration`                    | int         | The total execution time of the statement (excluding result fetch time), in milliseconds |
| `query`                       | string      | The text of the query, truncated to 4096 characters                         |
| `plansState`                  | string      | Whether plans exist for the execution, or the reason why they are missing   |
| `error`                       | bool        | `true` if the query failed, otherwise `false`                               |
| `errorMessage`                | string      | If the query failed, a message describing why the query could not complete  |
| `userId`                      | int         | The ID of the user who executed the query (only included if [`includeIdentityMetadata`](./configuration.md#queries--metrics--includeidentitymetadata) is `true`) |
| `userName`                    | string      | The email address or username of the user who executed the query (only included if [`includeIdentityMetadata`](./configuration.md#queries--metrics--includeidentitymetadata) is `true`) |
| `runAsUserId`                 | int         | The ID of the user whose credentials were used to execute the query (only included if [`includeIdentityMetadata`](./configuration.md#queries--metrics--includeidentitymetadata) is `true`) |
| `runAsUserName`               | string      | The email address or username of the user whose credentials were used to execute the query (only included if [`includeIdentityMetadata`](./configuration.md#queries--metrics--includeidentitymetadata) is `true`) |
| `executionEndTime`            | int         | The time execution of the query ended, in milliseconds                      |
| `startTime`                   | int         | The time the query started, in milliseconds                                 |
| `endTime`                     | int         | The time the query ended, in milliseconds                                   |
| `final`                       | bool        | `true` if more updates for the query are expected, otherwise `false`        |
| `compilationTime`             | int         | The time spent loading metadata and optimizing the query, in milliseconds   |
| `executionTime`               | int         | The time spent executing the query, in milliseconds                         |
| `fetchTime`                   | int         | The time spent fetching the query results after the execution finished, in milliseconds |
| `totalTime`                   | int         | The total execution time of the query from the client’s point of view, in milliseconds |
| `totalTaskTime`               | int         | The sum of the execution time for all of the query’s tasks, in milliseconds |
| `totalPhotonTime`             | int         | The total execution time for all individual Photon query engine tasks in the query, in milliseconds |
| `overloadingQueueStartTime`   | int         | The timestamp when the query was enqueued waiting while the warehouse was at max load, in milliseconds. This field will be `0` if the query skipped the overloading queue. |
| `provisioningQueueStartTime`  | int         | The timestamp when the query was enqueued waiting for a cluster to be provisioned for the warehouse, in milliseconds. This field will be `0` if the query skipped the provisioning queue. |
| `compilationStartTime`        | int         | The timestamp when the underlying compute started compilation of the query, in milliseconds |
| `fromCache`                   | bool        | `true` if the query result was fetched from cache, otherwise `false`        |
| `bytesRead`                   | int         | The total size of data read by the query, in bytes                          |
| `cacheBytesRead`              | int         | The size of persistent data read from the cache, in bytes                   |
| `filesRead`                   | int         | The number of files read after pruning                                      |
| `partitionsRead`              | int         | The number of partitions read after pruning                                 |
| `remoteBytesRead`             | int         | The size of persistent data read from cloud object storage on your cloud tenant, in bytes |
| `rowsRead`                    | int         | The total number of rows read by the query                                  |
| `rowsReturned`                | int         | The total number of rows returned by the query                              |
| `bytesPruned`                 | int         | The total number of bytes in all tables not read due to pruning             |
| `filesPruned`                 | int         | The total number of files from all tables not read due to pruning           |
| `remoteBytesWritten`          | int         | The size of persistent data written to cloud object storage in your cloud tenant, in bytes |
| `diskBytesSpilled`            | int         | The size of data temporarily written to disk while executing the query, in bytes |
| `networkBytesSent`            | int         | The total amount of data sent over the network between executor nodes during shuffle, in bytes |

## Example Query Metric Queries

**Total completed queries**

```sql
FROM DatabricksQuery
SELECT count(*) AS Queries
WHERE status = 'FINISHED'
```

**Total failed queries**

```sql
FROM DatabricksQuery
SELECT count(*) AS Queries
WHERE status = 'FAILED'
```

**Rate of successful queries per second over time**

```sql
FROM DatabricksQuery
SELECT rate(count(*), 1 second) AS Queries
WHERE status = 'FINISHED'
TIMESERIES
```

**Total failed queries by query text**

```sql
FROM DatabricksQuery
SELECT count(*)
WHERE status = 'FAILED'
FACET query
```

**Average bytes spilled to disk by query text ordered by most bytes spilled**

```sql
FROM DatabricksQuery
SELECT average(diskBytesSpilled)
WHERE diskBytesSpilled > 0
ORDER BY diskBytesSpilled DESC
FACET query
```

**Total duration of all queries run by warehouse over time**

```sql
FROM DatabricksQuery
SELECT sum(duration / 1000) AS Duration
FACET warehouseName
TIMESERIES
```

**Query history**

```sql
WITH
 concat(workspaceUrl, '/sql/warehouses/', warehouseId, '/monitoring?queryId=', id) AS queryLink
FROM DatabricksQuery
SELECT
 status as Status,
 substring(query, 0, 100) as Query,
 startTime as Started,
 duration as Duration,
 userName as User,
 queryLink
```

**Queries with data spilling ordered by most bytes spilled**

```sql
WITH
 concat(workspaceUrl, '/sql/warehouses/', warehouseId, '/monitoring?queryId=', id) AS queryLink
FROM DatabricksQuery
SELECT
 diskBytesSpilled,
 query,
 queryLink
WHERE diskBytesSpilled > 0
ORDER BY diskBytesSpilled DESC
LIMIT 100
```

#### Example Query Metrics Dashboard

A [sample dashboard](../examples/query-metrics-dashboard.json) is included that
shows examples of the types of query metric information that can be displayed
and the NRQL statements to use to visualize the data.

![Sample query metric dashboard image](../examples/query-metrics-dashboard.png)
