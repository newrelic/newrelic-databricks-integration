# Pipeline Update Metrics & Event Logs

## Pipeline Update Metrics

The Databricks Integration can collect telemetry about
[Databricks Lakeflow Declarative Pipeline updates](https://docs.databricks.com/aws/en/dlt/updates), such
as [flow](https://docs.databricks.com/aws/en/ldp/flows) durations,
[flow](https://docs.databricks.com/aws/en/ldp/flows) data quality
metrics, and [update](https://docs.databricks.com/aws/en/dlt/updates)
durations. This feature is enabled by default and can be enabled or disabled
using the [Databricks pipeline update metrics `enabled`](./configuration.md#pipelines--metrics--enabled)
flag in the [integration configuration](./configuration.md).

### Pipeline `startOffset` Configuration

On each harvest cycle, the integration uses the [Databricks ReST API](https://docs.databricks.com/api/workspace/introduction)
to retrieve pipeline events for all [Databricks Lakeflow Declarative Pipelines](https://docs.databricks.com/aws/en/ldp/).
By default, the [Databricks ReST API](https://docs.databricks.com/api/workspace/introduction)
endpoint for [listing pipeline events](https://docs.databricks.com/api/workspace/pipelines/listpipelineevents)
returns a paginated list of _all_ historical pipeline events sorted in
descending order by timestamp. On systems with many pipelines or complex
pipelines with many flows, there may be a large number of pipeline events to
retrieve and process, impacting the performance of the collector. To account for
this, the integration provides the [`startOffset`](./configuration.md#pipelines--metrics--startoffset)
configuration parameter. This parameter is used to tune the performance of the
collection of pipeline [update metrics](./pipelines.md#pipeline-update-metrics)
by limiting the number of pipeline events to return when listing pipeline
events to pipeline events with start times greater than the current time at
collection minus the offset.

The effect of this behavior is that only pipeline events which have a timestamp
_at_ or _after_ the calculated start time will be returned on the API call. For
example, using the default [`startOffset`](./configuration.md#pipelines--metrics--startoffset)
(86400 seconds or 24 hours), only pipeline events which occurred within the last
24 hours will be returned. This means that pipeline events that occurred more
than 24 hours ago will not be returned _even if_ the associated pipeline update
is not yet complete. This can lead to missing flow or update metrics. Therefore,
it is important to carefully select a value for the [`startOffset`](./configuration.md#pipelines--metrics--startoffset)
parameter that will account for long-running pipeline updates without degrading
the performance of the integration.

### Pipeline `intervalOffset` Configuration

The integration uses the [list pipeline events endpoint](https://docs.databricks.com/api/workspace/pipelines/listpipelineevents)
of the [Databricks ReST API](https://docs.databricks.com/api/workspace/introduction)
to retrieve pipeline log events. There can be a slight lag time between when a
pipeline log event is emitted and when it is available in the [API](https://docs.databricks.com/api/workspace/introduction).
The [`intervalOffset`](./configuration.md#pipelines--metrics--intervaloffset)
configuration parameter can be used to delay the processing of pipeline log
events in order to account for this lag. The default value for the
[`intervalOffset`](./configuration.md#pipelines--metrics--intervaloffset)
configuration parameter is 5 seconds.

To help illustrate, here is the processing behavior for the default settings
(60 second [collection interval](./configuration.md#interval) and 5 second
[`intervalOffset`](./configuration.md#pipelines--metrics--intervaloffset)):

* on the first minute, the integration will process all pipeline log events from 60 seconds ago until 55 seconds ago
* on the second interval, the integration will process all pipeline log events from 55 seconds ago until 1:55
* on the third interval, the integration will process all pipeline log events from 1:55 until 2:55
* and so on

### Pipeline Update Events

Pipeline update metrics are sent to New Relic as [event data](https://docs.newrelic.com/docs/data-apis/understand-data/new-relic-data-types/#event-data).
The provided events and attributes are listed in the sections below.

**NOTE:** Some of the text below is sourced from the
[Databricks Lakeflow Declarative Pipelines event log schema documentation](https://docs.databricks.com/aws/en/ldp/monitor-event-log-schema)
and the [Databricks SDK Go module documentation](https://pkg.go.dev/github.com/databricks/databricks-sdk-go).

#### Common pipeline update event attributes

The following attributes are included on _all_ pipeline update events.

| Attribute Name | Data Type | Description |
| --- | --- | --- |
| `databricksWorkspaceId` | string | [ID](https://docs.databricks.com/en/workspace/workspace-details.html#workspace-instance-names-urls-and-ids) of the Databricks workspace of the pipeline associated with the pipeline update |
| `databricksWorkspaceName` | string | [Instance name](https://docs.databricks.com/en/workspace/workspace-details.html#workspace-instance-names-urls-and-ids) of the Databricks workspace of the pipeline associated with the pipeline update |
| `databricksWorkspaceUrl` | string | [URL](https://docs.databricks.com/en/workspace/workspace-details.html#workspace-instance-names-urls-and-ids) of the Databricks workspace of the pipeline associated with the pipeline update |

The following attributes are included on all [`DatabricksPipelineUpdate`](#databrickspipelineupdate-events),
[`DatabricksPipelineFlow`](#databrickspipelineflow-events), and
[`DatabricksPipelineFlowExpectation`](#databrickspipelineflowexpectation-events)
events.

| Attribute Name | Data Type | Description |
| --- | --- | --- |
| `databricksPipelineId` | string | The unique UUID of the pipeline associated with the pipeline update |
| `databricksPipelineUpdateId` | string | The unique UUID of the pipeline update |
| `pipelineName` | string | The name of the pipeline associated with the pipeline update |
| `databricksClusterId` | string | ID of the Databricks [cluster](https://docs.databricks.com/en/getting-started/concepts.html#cluster) where the pipeline update occurred |
| `databricksClusterName` | string | Name of the Databricks [cluster](https://docs.databricks.com/en/getting-started/concepts.html#cluster) where the pipeline update occurred |
| `databricksClusterSource` | string | Source of the Databricks [cluster](https://docs.databricks.com/en/getting-started/concepts.html#cluster) where the pipeline update occurred (one of `API`, `JOB`, `MODELS`, `PIPELINE`, `PIPELINE_MAINTENANCE`, `SQL`, or `UI`) |
| `databricksClusterInstancePoolId` | string | ID of the Databricks cluster [instance pool](https://docs.databricks.com/aws/en/compute/pool-index) used when creating the cluster for the pipeline update |

**NOTE:** Cluster information is not provided by the [Databricks ReST API](https://docs.databricks.com/api/workspace/introduction)
for some pipeline events. For this reason, the availability of cluster
information on [`DatabricksPipelineUpdate`](#databrickspipelineupdate-events),
[`DatabricksPipelineFlow`](#databrickspipelineflow-events), and
[`DatabricksPipelineFlowExpectation`](#databrickspipelineflowexpectation-events)
events is not guaranteed.

The following attributes are included on all [`DatabricksPipelineFlow`](#databrickspipelineflow-events)
and [`DatabricksPipelineFlowExecution`](#databrickspipelineflowexpectation-events)
events.

| Attribute Name | Data Type | Description |
| --- | --- | --- |
| `databricksPipelineFlowId` | string | The unique UUID of the pipeline flow |
| `databricksPipelineFlowName` | string | The unique name of the pipeline flow |

**NOTE:** The flow ID is not provided by the [Databricks ReST API](https://docs.databricks.com/api/workspace/introduction)
in all cases. For this reasons, the availability of the flow ID on the
[`DatabricksPipelineFlow`](#databrickspipelineflow-events) event is not
guaranteed.

#### `DatabricksPipelineUpdate` events

The integration records a start event for each pipeline update that starts while
the integration is running and a complete event for each pipeline update that
completes while the integration is running. Both events are reported using the
`DatabricksPipelineUpdate` event type. The `event` attribute on the
`DatabricksPipelineUpdate` event can be used to differentiate between the start
event (the `event` attribute will be set to `start`) and the complete event (the
`event` attribute will be set to `complete`).

Each `DatabricksPipelineUpdate` start event includes the following attributes.

| Attribute Name | Data Type | Description |
| --- | --- | --- |
| `event` | string | This attribute will always be set to `start` for a start event |
| `creationTime` | number | The time at which the update was created, in milliseconds since the epoch |

Each `DatabricksPipelineUpdate` complete event includes the following
attributes.

| Attribute Name | Data Type | Description |
| --- | --- | --- |
| `event` | string | This attribute will always be set to `complete` for a complete event |
| `status` | string | The completion status of the update, one of `CANCELED`, `COMPLETED`, or `FAILED` |
| `creationTime` | number | The time at which the update was created, in milliseconds since the epoch |
| `completionTime` | number | The time at which the update completed, in millseconds since the epoch |
| `duration` | number | The total duration of the update, in milliseconds |
| `waitStartTime` | number | The time at which the update started waiting for resources to be available, in milliseconds since the epoch. Only included if the update had to wait for resources to be available. |
| `waitDuration` | number | The amount of time the update spent waiting for resources to be available, in milliseconds. Only included if the update had to wait for resources to be available. |
| `startTime` | number | The time at which the update started execution, in milliseconds since the epoch |
| `runDuration` | number | The amount of time between when the update started execution and when the update completed, in milliseconds |

#### `DatabricksPipelineFlow` events

The integration records a start event for each pipeline update flow that starts
while the integration is running and a complete event for each pipeline update
flow that completes while the integration is running. Both events are reported
using the `DatabricksPipelineFlow` event type. The `event` attribute on the
`DatabricksPipelineFlow` event can be used to differentiate between the start
event (the `event` attribute will be set to `start`) and the complete event (the
`event` attribute will be set to `complete`).

Each `DatabricksPipelineFlow` start event includes the following attributes.

| Attribute Name | Data Type | Description |
| --- | --- | --- |
| `event` | string | This attribute will always be set to `start` for a start event |
| `queueStartTime` | number | The time at which the flow was queued, in milliseconds since the epoch |

Each `DatabricksPipelineFlow` complete event includes the following
attributes.

| Attribute Name | Data Type | Description |
| --- | --- | --- |
| `event` | string | This attribute will always be set to `complete` for a complete event |
| `status` | string | The completion status of the flow, one of `COMPLETED`, `EXCLUDED`, `FAILED`, `SKIPPED`, or `STOPPED` |
| `queueStartTime` | number | The time at which the flow was queued, in milliseconds since the epoch |
| `completionTime` | number | The time at which the flow completed, in millseconds since the epoch |
| `queueDuration` | number | The time the flow spent in the queue, in milliseconds |
| `planStartTime` | number | The time at which the flow entered the planning phase, in milliseconds since the epoch. Only included if the flow required planning. |
| `planDuration` | number | The time the flow spent in the planning phase, in milliseconds. Only included if the flow required planning. |
| `startTime` | number | The time at which the flow entered the execution phase, in milliseconds since the epoch. Not included if the flow did not execute. |
| `duration` | number | The duration of the execution phase of the flow, in milliseconds. Not included if the flow did not execute. |
| `backlogBytes` | number | Total backlog across all input sources in the flow, in bytes |
| `backlogFileCount` | number | Total backlog files across all input sources in the flow |
| `outputRowCount` | number | Number of output rows written by the update of the flow |
| `droppedRecordCount` | number | The number of records that were dropped because they failed one or more expectations |

#### `DatabricksPipelineFlowExpectation` events

The integration records a `DatabricksPipelineFlowExpectation` event for each
expectation in each completed update flow that is recorded which includes
the results of evaluating each expectation.

Each `DatabricksPipelineFlowExpectation` event includes the following
attributes.

| Attribute Name | Data Type | Description |
| --- | --- | --- |
| `name` | string | The name of the expectation |
| `dataset` | string | The name of the dataset to which the expectation was added |
| `passedRecordCount` | number | The number of records that passed the expectation |
| `failedRecordCount` | number | The number of records that failed the expectation |

#### `DatabricksPipelineSummary`

On each harvest cycle, the integration records a `DatabricksPipelineSummary`
event with the following attributes.

| Attribute Name | Data Type | Description |
| --- | --- | --- |
| `deletedPipelineCount` | number | The number of deleted pipelines, measured at the time of collection |
| `deployingPipelineCount` | number | The number of pipelines deploying, measured at the time of collection |
| `failedPipelineCount` | number | The number of failed pipelines, measured at the time of collection |
| `idlePipelineCount` | number | The number of idle pipelines, measured at the time of collection |
| `recoveringPipelineCount` | number | The number of pipelines recovering, measured at the time of collection |
| `resettingPipelineCount` | number | The number of pipelines resetting, measured at the time of collection |
| `runningPipelineCount` | number | The number of pipelines running, measured at the time of collection |
| `startingPipelineCount` | number | The number of pipelines starting, measured at the time of collection |
| `stoppingPipelineCount` | number | The number of pipelines stopping, measured at the time of collection |
| `createdUpdateCount` | number | The number of updates created, measured at the time of collection |
| `initializingUpdateCount` | number | The number of updates initializing, measured at the time of collection |
| `queuedUpdateCount` | number | The number of queued updates, measured at the time of collection |
| `resettingUpdateCount` | number | The number of updates resetting, measured at the time of collection |
| `runningUpdateCount` | number | The number of updates running, measured at the time of collection |
| `settingUpTablesUpdateCount` | number | The number of updates setting up tables, measured at the time of collection |
| `stoppingUpdateCount` | number | The number of updates stopping, measured at the time of collection |
| `waitingForResourcesUpdateCount` | number | The number of updates waiting for resources, measured at the time of collection |
| `idleFlowCount` | number | The number of idle flows, measured at the time of collection |
| `planningFlowCount` | number | The number of flows in the planning phase, measured at the time of collection |
| `queuedFlowCount` | number | The number of queued flows, measured at the time of collection |
| `runningFlowCount` | number | The number of flows running, measured at the time of collection |
| `startingFlowCount` | number | The number of flows starting, measured at the time of collection |

**NOTE:**
* The count values in the `DatabricksPipelineSummary` event represent the sum
  total of pipelines, updates, and flows in each state measured at the the time
  of collection. The measurement of pipelines, updates, and flows in a given
  state between two different runs of the integration may or may not include the
  same pipelines, updates, and flows. For example, if the same update is running
  when the integration runs at time `T` and when the integration runs at time
  `T + 1`, it will be counted in the `runningUpdateCount` attribute for the
  `DatabricksPipelineSummary` events generated for each of those runs. For this
  reason, some aggregation functions do not produce meaningful values with these
  counts. For instance, in the previous example, the [`sum()`](https://docs.newrelic.com/docs/nrql/nrql-syntax-clauses-functions/#func-sum)
  function would count the running update twice when aggregated across times `T`
  and `T + 1`, making it seem as though there are a total of two updates
  running in the aggregated time interval.
* The [`latest()`](https://docs.newrelic.com/docs/nrql/nrql-syntax-clauses-functions/#latest)
  aggregator function can be used with the count values in the
  `DatabricksPipelineSummary` event to display the "current" number of
  pipelines, updates, and flows in a given state. In general, the
  [`TIMESERIES`](https://docs.newrelic.com/docs/nrql/nrql-syntax-clauses-functions/#sel-timeseries)
  clause should not be used with the [`latest()`](https://docs.newrelic.com/docs/nrql/nrql-syntax-clauses-functions/#latest)
  aggregator function as it can obscure values when the selected time interval
  includes two or more events. For example, if one update is running at time `T`
  and zero updates are running at time `T + 1`, the [`latest()`](https://docs.newrelic.com/docs/nrql/nrql-syntax-clauses-functions/#latest)
  aggregator function would show that zero updates were running when aggregated
  across times `T` and `T + 1`.
* Count values for completed updates and flows are not included in the
  `DatabricksPipelineSummary` event because completed updates and flows can be
  counted simply by using the `count(*)` aggregator function on
  `DatabricksPipelineUpdate` and `DatabricksPipelineFlow` events, respectively,
   where the `event` attribute is set to `complete`.

#### Pipeline update and flow durations

The Databricks Integration stores the following pipeline update and flow
durations on each [`DatabricksPipelineUpdate`](#databrickspipelineupdate-events)
and [`DatabricksPipelineFlow`](#databrickspipelineflow-events) _complete_ event,
respectively.

**NOTE:** Duration attributes are not set on [`DatabricksPipelineUpdate`](#databrickspipelineupdate-events)
and [`DatabricksPipelineFlow`](#databrickspipelineflow-events) _start_ events.

* `DatabricksPipelineUpdate`

  * `duration`

    The `duration` field represents the total duration of the update from the
    time it is created until the time it completes, including any time spent
    waiting for resources plus the total time the update spent actually
    executing. The value is expressed in milliseconds.

  * `waitDuration`

    The `waitDuration` field represents the amount of time the update spent
    waiting for resources, if any, prior to initiation. The value is expressed
    in milliseconds.

  * `runDuration`

    The `runDuration` field represents the amount of time the update spent
    actually executing, from initiation to completion. The value does not
    include any time spent waiting for resources, if any, and is expressed in
    milliseconds.

* `DatabricksPipelineFlow`

  * `duration`

    The `duration` field represents the amount of time the flow spent actually
    processing a batch of data. The value does not include any time the flow
    spent in the queue or in the planning phase and is expressed in
    milliseconds.

  * `queueDuration`

    The `queueDuration` field represents the time the flow spent in the queue,
    in milliseconds.

  * `planDuration`

    The `planDuration` field represents the time the flow spent in the planning
    phase, in milliseconds.

### Example Pipeline Update Queries

**Current count of running pipelines by workspace**

```sql
WITH
 substring(databricksWorkspaceName, 0, position(databricksWorkspaceName, '.')) AS workspace,
FROM DatabricksPipelineSummary
SELECT latest(runningPipelineCount)
FACET workspace
```

**Current count of pipeline updates waiting for resources by workspace**

```sql
WITH
 substring(databricksWorkspaceName, 0, position(databricksWorkspaceName, '.')) AS workspace,
FROM DatabricksPipelineSummary
SELECT latest(waitingForResourcesUpdateCount)
FACET workspace
```

**Current count of queued flows by workspace**

```sql
WITH
 substring(databricksWorkspaceName, 0, position(databricksWorkspaceName, '.')) AS workspace,
FROM DatabricksPipelineSummary
SELECT latest(queuedFlowCount)
FACET workspace
```

**Running updates by workspace, pipeline name, pipeline update ID, and update creation time**

The following query displays pipeline updates which are currently running by
looking for pipeline updates which have produced a [`DatabricksPipelineUpdate`](#databrickspipelineupdate-events)
start event but no [`DatabricksPipelineUpdate`](#databrickspipelineupdate-events)
complete event. The nested query returns the number of unique values for the
`event` attribute and the value of the `event` attribute of the first
[`DatabricksPipelineUpdate`](#databrickspipelineupdate-events) row returned when
grouping pipeline updates by workspace name, workspace URL, pipeline name,
pipeline update ID, and update creation time (which uniquely identifies a
pipeline update) and ordering by latest timestamp. The outer query counts the
number of results where the unique number of values for the `event` attribute is
equal to 1 and the value of that attribute is equal to `start`.

The query also shows an example of constructing a URL that links directly to the
pipeline update details page within the Databricks workspace UI.

```sql
WITH
 substring(databricksWorkspaceName, 0, position(databricksWorkspaceName, '.')) AS workspace,
 concat(databricksWorkspaceUrl, '/pipelines/', databricksPipelineId, '/updates/', databricksPipelineUpdateId) AS databricksPipelineUpdateUrl
SELECT count(*)
FROM (
 FROM DatabricksPipelineUpdate
 SELECT
  uniqueCount(event) as 'total',
  latest(event) as 'state'
 FACET databricksWorkspaceName, databricksWorkspaceUrl, databricksPipelineId, pipelineName, databricksPipelineUpdateId, toDatetime(creationTime, 'MMMM dd, YYYY HH:mm:ss') AS creationTime
 ORDER BY max(timestamp)
 LIMIT 100)
WHERE total = 1 AND state = 'start'
FACET workspace AS Workspace, pipelineName AS Pipeline, substring(databricksPipelineUpdateId, 0, 6) as Update, creationTime AS 'Update Creation Time', databricksPipelineUpdateUrl AS 'Databricks Link'
LIMIT 100
```

**Recent updates**

```sql
WITH
 substring(databricksWorkspaceName, 0, position(databricksWorkspaceName, '.')) AS workspace,
 substring(databricksPipelineUpdateId, 0, 6) AS update,
 concat(databricksWorkspaceUrl, '/pipelines/', databricksPipelineId, '/updates/', databricksPipelineUpdateId) AS databricksPipelineUpdateUrl
FROM DatabricksPipelineUpdate
SELECT workspace,
 databricksClusterName AS Cluster,
 pipelineName AS Pipeline,
 update,
 creationTime,
 completionTime,
 status,
 duration / 1000 AS Duration,
 databricksPipelineUpdateUrl AS 'Databricks Link'
WHERE event = 'complete'
LIMIT 100
```

**NOTE:** Make sure to use the condition `event = 'complete'` to target only
pipeline update complete events.

**Average update duration by workspace and pipeline name over time**

```sql
WITH
 substring(databricksWorkspaceName, 0, position(databricksWorkspaceName, '.')) AS workspace
FROM DatabricksPipelineUpdate
SELECT average(duration / 1000) AS 'seconds'
WHERE event = 'complete'
FACET workspace, pipelineName
TIMESERIES
```

**NOTE:** Make sure to use the condition `event = 'complete'` to target only
pipeline update complete events.

**Recent flows**

```sql
WITH
 substring(databricksWorkspaceName, 0, position(databricksWorkspaceName, '.')) AS workspace,
 substring(databricksPipelineUpdateId, 0, 6) AS update,
 concat(databricksWorkspaceUrl, '/pipelines/', databricksPipelineId, '/updates/', databricksPipelineUpdateId) AS databricksPipelineUpdateUrl
FROM DatabricksPipelineFlow
SELECT workspace,
 databricksClusterName AS Cluster,
 pipelineName AS Pipeline,
 update,
 databricksPipelineFlowName AS Flow,
 queueStartTime,
 completionTime,
 status,
 duration / 1000 AS Duration,
 databricksPipelineUpdateUrl AS 'Databricks Link'
WHERE event = 'complete'
LIMIT 100
```

**NOTE:** Make sure to use the condition `event = 'complete'` to target only
pipeline flow complete events.

**Average flow queue duration by workspace, pipeline name, and flow name**

```sql
WITH
 substring(databricksWorkspaceName, 0, position(databricksWorkspaceName, '.')) AS workspace
FROM DatabricksPipelineFlow
SELECT average(queueDuration / 1000) AS 'seconds'
WHERE event = 'complete'
FACET workspace, pipelineName, databricksPipelineFlowName
TIMESERIES
```

**NOTE:** Make sure to use the condition `event = 'complete'` to target only
pipeline flow complete events.

**Average output rows written by workspace, pipeline name, and flow name**

```sql
WITH
 substring(databricksWorkspaceName, 0, position(databricksWorkspaceName, '.')) AS workspace
FROM DatabricksPipelineFlow
SELECT average(outputRowCount)
WHERE event = 'complete'
FACET workspace, pipelineName, databricksPipelineFlowName
```

**NOTE:** Make sure to use the condition `event = 'complete'` to target only
pipeline flow complete events.

**Average backlog bytes by workspace, pipeline name, and flow name**

```sql
WITH
 substring(databricksWorkspaceName, 0, position(databricksWorkspaceName, '.')) AS workspace
FROM DatabricksPipelineFlow
SELECT average(backlogBytes)
WHERE event = 'complete'
FACET workspace, pipelineName, databricksPipelineFlowName
```

**NOTE:** Make sure to use the condition `event = 'complete'` to target only
pipeline flow complete events.

**Flow expectation records failed**

```sql
WITH
 substring(databricksWorkspaceName, 0, position(databricksWorkspaceName, '.')) AS workspace,
 substring(databricksPipelineUpdateId, 0, 6) AS update
FROM DatabricksPipelineFlowExpectation
SELECT workspace,
 databricksClusterName AS Cluster,
 pipelineName AS Pipeline,
 update,
 databricksPipelineFlowName AS Flow,
 name AS Expectation,
 dataset,
 failedRecordCount AS Records
WHERE failedRecordCount > 0
LIMIT 100
```

### Example Pipeline Updates Dashboard

A [sample dashboard](../examples/pipeline-updates-dashboard.json) is included
that shows examples of the types of pipeline update information that can be
displayed and the NRQL statements to use to visualize the data.

![Sample pipeline updates dashboard image](../examples/pipeline-updates-dashboard.png)

## Pipeline Event Logs

The Databricks Integration can collect [Databricks Lakeflow Declarative Pipeline event logs](https://docs.databricks.com/aws/en/ldp/monitor-event-logs)
for all [Databricks Lakeflow Declarative Pipelines](https://docs.databricks.com/aws/en/ldp/)
defined in a [workspace](https://docs.databricks.com/en/getting-started/concepts.html#accounts-and-workspaces).
[Databricks Lakeflow Declarative Pipeline event log](https://docs.databricks.com/aws/en/ldp/monitor-event-logs)
entries for every pipeline [update](https://docs.databricks.com/aws/en/dlt/updates)
are collected and sent to [New Relic Logs](https://docs.newrelic.com/docs/logs/get-started/get-started-log-management/).

**NOTE:** Some of the text below is sourced from the
[Databricks Lakeflow Declarative Pipelines event log schema documentation](https://docs.databricks.com/aws/en/ldp/monitor-event-log-schema)
and the [Databricks SDK Go module documentation](https://pkg.go.dev/github.com/databricks/databricks-sdk-go).

### Pipeline Event Log Data

[Databricks Lakeflow Declarative Pipeline event logs](https://docs.databricks.com/aws/en/ldp/monitor-event-logs)
are sent to New Relic as [New Relic Log data](https://docs.newrelic.com/docs/data-apis/understand-data/new-relic-data-types/#log-data).
For each [Databricks Lakeflow Declarative Pipeline event log](https://docs.databricks.com/aws/en/ldp/monitor-event-logs)
entry, the [fields from the event log entry](https://docs.databricks.com/aws/en/ldp/monitor-event-log-schema)
are mapped to attributes on the corresponding [New Relic log](https://docs.newrelic.com/docs/logs/get-started/get-started-log-management/)
entry as follows:

**NOTE:** Due to the way in which the [Databricks ReST API](https://docs.databricks.com/api/workspace/introduction)
returns [Databricks Lakeflow Declarative Pipeline event log](https://docs.databricks.com/aws/en/ldp/monitor-event-logs)
entries (in descending order by timestamp), later event log entries may
sometimes be visible in the [New Relic Logs UI](https://docs.newrelic.com/docs/logs/ui-data/use-logs-ui/)
before older event log entries are visible. This does not affect the temporal
ordering of the log entries. Once older log entries become visible (usually
30-60s longer than the value of the [`interval` configuration parameter](./configuration.md#interval)),
they are properly ordered by timestamp in the [New Relic Logs UI](https://docs.newrelic.com/docs/logs/ui-data/use-logs-ui/).

#### Pipeline event log attributes

| Pipeline Event Log Field Name | New Relic Log Entry Attribute Name | Data Type in New Relic Log Entry | Description |
| --- | --- | --- | --- |
| `message` | `message` | string | A human-readable message describing the event |
| `timestamp` | `timestamp` | integer | The time (in milliseconds since the epoch) the event was recorded |
| `id` | `databricksPipelineEventId` | string | A unique identifier for the event log record |
| `event_type` | `databricksPipelineEventType` | string | The event type. For a list of event types, see [the details object](https://docs.databricks.com/aws/en/ldp/monitor-event-log-schema#the-details-object). |
| `level` | `level`, `databricksPipelineEventLevel` | string | The severity level of the event, for example, `INFO`, `WARN`, `ERROR`, or `METRICS` |
| `maturity_level` | `databricksPipelineEventMaturityLevel` | string | The stability of the event schema, one of `STABLE`, `NULL`, `EVOLVING`, or `DEPRECATED` |
| n/a | `databricksPipelineEventError` | boolean | `true` if an error was captured by the event (see below for additional details), otherwise `false` |
| `origin.batch_id` | `databricksPipelineFlowBatchId` | integer | The id of a microbatch. Unique within a flow. |
| `origin.cloud` | `databricksCloud` | string | The cloud provider, one of `AWS`, `Azure`, or `GCP` |
| `origin.cluster_id` | `databricksClusterId` | string | The id of the cluster where an execution happens. Globally unique. |
| `origin.dataset_name` | `databricksPipelineDatasetName` | string | The fully qualified name of a dataset |
| `origin.flow_id` | `databricksPipelineFlowId` | string | The id of the flow. It tracks the state of the flow being used across multiple updates. |
| `origin.flow_name` | `databricksPipelineFlowName` | string | The name of the flow |
| `origin.host` | `databricksPipelineEventHost` | string | The optional host name where the event was triggered |
| `origin.maintenance_id` | `databricksPipelineMaintenanceId` | string | The id of a maintenance run. Globally unique. |
| `origin.materialization_name` | `databricksPipelineMaterializationName` | string | The materialization name |
| `origin.org_id` | `databricksOrgId` | integer | The org id or workspace ID of the user. Unique within a cloud. |
| `origin.pipeline_id` | `databricksPipelineId` | string | The id of the pipeline. Globally unique. |
| `origin.pipeline_name` | `databricksPipelineName` | string | The name of the pipeline. Not unique. |
| `origin.region` | `databricksCloudRegion` | string | The cloud region |
| `origin.request_id` | `databricksPipelineRequestId` | string | The id of the request that caused an update |
| `origin.table_id` | `databricksDeltaTableId` | string | The id of a (delta) table. Globally unique. |
| `origin.uc_resource_id` | `databricksUcResourceId` | string | The Unity Catalog id of the MV or ST being updated |
| `origin.update_id` | `databricksPipelineUpdateId` | string | The id of a single execution of the pipeline. |

Additionally, if the `error` field is set on the pipeline event log entry,
indicating an error was captured by the event, the following attributes are
added to the New Relic log entry.

| Pipeline Event Log Field Name | New Relic Log Entry Attribute Name | Data Type in New Relic Log Entry | Description |
| --- | --- | --- | --- |
| `error.fatal` | `databricksPipelineEventErrorFatal` | boolean | Whether the error is considered fatal, that is, unrecoverable |
| `error.exceptions[N].class_name` | `databricksPipelineEventErrorExceptionNClassName` | string | Runtime class of the `N`th exception |
| `error.exceptions[N].message` | `databricksPipelineEventErrorExceptionNMessage` | string | Exception message of the `N`th exception |
