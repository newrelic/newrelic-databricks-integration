# Consumption & Cost Data

The Databricks Integration can collect Databricks consumption and cost data from
the Databricks [system tables](https://docs.databricks.com/en/admin/system-tables/index.html).
This data can be used to show Databricks DBU consumption metrics and estimated
Databricks costs directly within New Relic.

This feature is enabled by setting the [Databricks usage `enabled`](./configuration.md#usage--enabled)
flag to `true` in the [integration configuration](./configuration.md). When enabled,
the Databricks collector will collect consumption and cost data once a day at
the time specified in the [`runTime`](./configuration.md#usage--runtime)
configuration parameter. The following information is recorded each time
consumption and cost data is collected:

* [Billable usage records](#billable-usage-data) from the [`system.billing.usage` table](https://docs.databricks.com/en/admin/system-tables/billing.html)
* [List pricing records](#list-pricing-data) from the [`system.billing.list_prices` table](https://docs.databricks.com/en/admin/system-tables/pricing.html)
* [List cost per job run](#list-cost-per-job-run)
* [List cost per job](#list-cost-per-job)
* [List cost of failed job runs for jobs with frequent failures](#list-cost-of-failed-job-runs-for-jobs-with-frequent-failures)
* [List cost of repair job runs for jobs with frequent repairs](#list-cost-of-repaired-job-runs-for-jobs-with-frequent-repairs)

## Consumption and Cost Collection Requirements

In order for the Databricks Integration to collect consumption and cost data
from Databricks, there are several requirements.

1. The SQL [warehouse ID](./configuration.md#usage--warehouseid) of a
   [SQL warehouse](https://docs.databricks.com/en/compute/sql-warehouse/index.html)
   within the workspace specified in the  [`workspaceHost`](./configuration.md#workspacehost)
   configuration parameter must be specified. The Databricks SQL queries used to
   collect consumption and cost data from the Databricks [system tables](https://docs.databricks.com/en/admin/system-tables/index.html)
   will be run against the specified SQL warehouse.

## Billable Usage Data

Billable usage data is collected from the [`system.billing.usage` table](https://docs.databricks.com/en/admin/system-tables/billing.html)
and sent to New Relic as [event data](https://docs.newrelic.com/docs/data-apis/understand-data/new-relic-data-types/#event-data).
The data is always collected _for the previous day_ so that a full day's worth
of data is captured.

### `DatabricksUsage` events

The integration records an event for each usage record in the [`system.billing.usage` table](https://docs.databricks.com/en/admin/system-tables/billing.html)
table. The event is reported using the `DatabricksUsage` event.

Each `DatabricksUsage` event includes the following attributes.

**NOTE:** Descriptions below are sourced from the [billable usage system table reference](https://docs.databricks.com/en/admin/system-tables/billing.html)
and the [jobs system table reference](https://docs.databricks.com/aws/en/admin/system-tables/jobs).

| Name | Description |
|---|---|
| `account_id` | ID of the [account](https://docs.databricks.com/aws/en/getting-started/high-level-architecture#databricks-objects) this usage record was generated for |
| `workspace_id` | [ID](https://docs.databricks.com/en/workspace/workspace-details.html#workspace-instance-names-urls-and-ids) of the workspace this usage record was associated with |
| `workspace_url` | [URL](https://docs.databricks.com/en/workspace/workspace-details.html#workspace-instance-names-urls-and-ids) of the workspace this usage record was associated with |
| `workspace_instance_name` | [Instance name](https://docs.databricks.com/en/workspace/workspace-details.html#workspace-instance-names-urls-and-ids) of the workspace this usage record was associated with |
| `record_id` | Unique ID for this usage record |
| `sku_name` | Name of the SKU associated with the usage record |
| `cloud` | Cloud associated with this usage record. Possible values are `AWS`, `AZURE`, and `GCP`. |
| `usage_start_time` | The start time relevant to this usage record |
| `usage_end_time` | The end time relevant to this usage record |
| `usage_date` | Date of the usage record |
| `usage_unit` | Unit this usage record is measured in |
| `usage_quantity` | Number of units consumed for this usage record |
| `record_type` | Whether the usage record is original, a retraction, or a restatement. See the section ["Record type reference"](https://docs.databricks.com/aws/en/admin/system-tables/billing#record-type) in the Databricks documentation for more details. |
| `ingestion_date` | Date the usage record was ingested into the usage table |
| `billing_origin_product` | The product that originated the usage reocrd |
| `usage_type` | The type of usage attributed to the product or workload for billing purposes |
| `cluster_id` | ID of the [cluster](https://docs.databricks.com/en/getting-started/concepts.html#cluster) associated with this usage record |
| `cluster_creator` | Creator of the [cluster](https://docs.databricks.com/en/getting-started/concepts.html#cluster) associated with this usage record (only included if [`includeIdentityMetadata`](./configuration.md#usage--includeidentitymetadata) is `true`) |
| `cluster_single_user_name` | Single user name of the [cluster](https://docs.databricks.com/en/getting-started/concepts.html#cluster) associated with this usage record if the access mode of the cluster is [dedicated access mode (formerly single-user access mode)](https://docs.databricks.com/en/compute/configure.html#access-mode) (only included if [`includeIdentityMetadata`](./configuration.md#usage--includeidentitymetadata) is `true`) |
| `cluster_source` | Source of the [cluster](https://docs.databricks.com/en/getting-started/concepts.html#cluster) associated with this usage record |
| `cluster_instance_pool_id` | ID of the cluster [instance pool](https://docs.databricks.com/aws/en/compute/pool-index) used when creating the cluster associated with this usage record |
| `warehouse_id` | ID of the [SQL warehouse](https://docs.databricks.com/en/compute/sql-warehouse/index.html) associated with this usage record |
| `warehouse_name` | Name of the [SQL warehouse](https://docs.databricks.com/en/compute/sql-warehouse/index.html) associated with this usage record |
| `warehouse_creator` | Creator of the [SQL warehouse](https://docs.databricks.com/en/compute/sql-warehouse/index.html) associated with this usage record (only included if [`includeIdentityMetadata`](./configuration.md#usage--includeidentitymetadata) is `true`) |
| `instance_pool_id` | ID of the [instance pool](https://docs.databricks.com/aws/en/compute/pool-index) associated with the usage record |
| `node_type` | The instance type of the compute resource associated with the usage record |
| `job_id` | ID of the job associated with this usage record for serverless compute or jobs compute usage. Does not populate for jobs run on all-purpose compute. |
| `job_run_id` | ID of the job run associated with this usage record for serverless compute or jobs compute usage. Does not populate for jobs run on all-purpose compute. |
| `job_name` | **DEPRECATED: Use `serverless_job_name` instead** User-supplied name of the job associated with this usage record for serverless compute or jobs compute usage. **NOTE:** This field will only contain a value for jobs run within a workspace in the same cloud region as the workspace specified in the [`workspaceHost`](./configuration.md#workspacehost) configuration parameter. |
| `serverless_job_name` | User-given name of the job associated with this usage record for jobs run on serverless compute (populated for jobs compute since September 2025). Does not populate for jobs run on all-purpose compute. |
| `notebook_id` | ID of the notebook associated with the usage record for notebooks run on serverless compute  |
| `notebook_path` | Workspace storage path of the notebook associated with this usage record for notebooks run on serverless compute |
| `dlt_pipeline_id` | ID of the declarative pipeline associated with the usage record |
| `dlt_update_id` | ID of the pipeline update associated with the usage record |
| `dlt_maintenance_id` | ID of the pipeline maintenance tasks associated with the usage record |
| `run_name` | Unique user-facing name of the Foundation Model Fine-tuning run associated with the usage record  |
| `endpoint_name` | Name of the model serving endpoint or vector search endpoint associated with the usage record |
| `endpoint_id` | ID of the model serving endpoint or vector search endpoint associated with the usage record |
| `central_clean_room_id` | ID of the central clean room associated with the usage record |
| `run_as` | See the section ["run_as identities"](https://docs.databricks.com/aws/en/admin/system-tables/billing#run_as-identities) in the Databricks documentation for details (only included if [`includeIdentityMetadata`](./configuration.md#usage--includeidentitymetadata) is `true`) |
| `jobs_tier` | Jobs tier product features for this usage record: values include `LIGHT`, `CLASSIC`, or `null` |
| `sql_tier` | SQL tier product features for this usage record: values include `CLASSIC`, `PRO`, or `null` |
| `dlt_tier` | DLT tier product features for this usage record: values include `CORE`, `PRO`, `ADVANCED`, or `null` |
| `is_serverless` | Flag indicating if this usage record is associated with serverless usage: values include `true` or `false`, or `null` (value is `true` or `false` when you can choose between serverless and classic compute, otherwise it's `null`)  |
| `is_photon` | Flag indicating if this usage record is associated with Photon usage: values include `true` or `false`, or `null` |
| `serving_type` | Serving type associated with this usage record: values include `MODEL`, `GPU_MODEL`, `FOUNDATION_MODEL`, `FEATURE`, or `null` |

In addition, all [custom tags](https://docs.databricks.com/aws/en/admin/account-settings/usage-detail-tags#custom-tags)
associated with a usage record are added as event attributes. And, for usage
records associated with jobs within a workspace in the same cloud region as the
workspace specified in the [`workspaceHost`](./configuration.md#workspacehost) configuration
parameter, all user-supplied [custom tags](https://docs.databricks.com/aws/en/admin/account-settings/usage-detail-tags#custom-tags)
associated with the job are also added.

**NOTE:** Not every attribute is included in every event. For example, the
`cluster_*` attributes are only included in events for usage records relevant to
[classic compute](https://docs.databricks.com/aws/en/compute/use-compute).
Similarly, the `warehouse_*` attributes are only included in events for usage
records relevant to [SQL warehouse](https://docs.databricks.com/en/compute/sql-warehouse/index.html)
compute.

## List Pricing Data

List pricing data is collected from the [`system.billing.list_prices` table](https://docs.databricks.com/en/admin/system-tables/pricing.html)
and used to populate a [New Relic lookup table](https://docs.newrelic.com/docs/logs/ui-data/lookup-tables-ui/)
named `DatabricksListPrices`. The entire lookup table is updated each time
consumption and cost data is collected.

### `DatabricksListPrices` lookup table

The integration includes a row in the `DatabricksListPrices` [lookup table](https://docs.newrelic.com/docs/logs/ui-data/lookup-tables-ui/)
for each pricing record in the [`system.billing.list_prices` table](https://docs.databricks.com/en/admin/system-tables/pricing.html)

Each row includes the following columns.

**NOTE:** Descriptions below are sourced from the
[pricing system table reference](https://docs.databricks.com/en/admin/system-tables/pricing.html).

| Name | Description |
|---|---|
| `account_id` | ID of the [account](https://docs.databricks.com/aws/en/getting-started/high-level-architecture#databricks-objects) this pricing record was generated for |
| `price_start_time` | The time this price became effective in UTC |
| `price_end_time` | The time this price stopped being effective in UTC |
| `sku_name` | Name of the SKU associated with the pricing record |
| `cloud` | Name of the Cloud this price is applicable to. Possible values are `AWS`, `AZURE`, and `GCP`. |
| `currency_code` | The currency this price is expressed in |
| `usage_unit` | The unit of measurement that is monetized |
| `list_price` | A single price that can be used for simple long-term estimates |

**NOTE:** The `list_price` field contains a single price suitable for simple
long-term estimates. It does not reflect any promotional pricing or custom price
plans.

## Job Cost Data

Each time consumption and cost data is collected, the Databricks collector runs
a set of queries that leverage data in the [`system.billing.usage` table](https://docs.databricks.com/en/admin/system-tables/billing.html),
the [`system.billing.list_prices` table](https://docs.databricks.com/en/admin/system-tables/pricing.html),
the [`system.lakeflow.jobs` table](https://docs.databricks.com/aws/en/admin/system-tables/jobs#jobs-table-schema),
and the [`system.lakeflow.job_run_timeline` table](https://docs.databricks.com/aws/en/admin/system-tables/jobs#job-run-timeline-table-schema)
to collect job cost data. The data is always collected _for the previous day_ so
that a full day's worth of data is captured. The set of queries that are run
collect the following data.

* [List cost per job run](#list-cost-per-job-run)
* [List cost per job](#list-cost-per-job)
* [List cost of failed job runs for jobs with frequent failures](#list-cost-of-failed-job-runs-for-jobs-with-frequent-failures)
* [List cost of repaired job runs for jobs with frequent repairs](#list-cost-of-repaired-job-runs-for-jobs-with-frequent-repairs)

By default, all queries are run each time consumption and cost data is
collected. This behavior can be configured using the [`optionalQueries`](./configuration.md#usage--optionalqueries)
configuration parameter to selectively enable or disable queries by query ID.

**NOTE:**
* Job cost data is collected only for the workspace specified in the
  [`workspaceHost`](./configuration.md#workspacehost) configuration parameter.
* Job cost data is collected only for jobs run on serverless compute or jobs
  compute. Jobs run on [SQL warehouses](https://docs.databricks.com/en/compute/sql-warehouse/index.html)
  or [all-purpose compute](https://docs.databricks.com/aws/en/compute/use-compute)
  are not billed as jobs \[[*](https://docs.databricks.com/aws/en/admin/system-tables/jobs-cost)\].
* Job costs recorded in the `list_cost` attribute are _estimated_ costs based on
  list price and do not reflect any promotional pricing or custom price plans.

### `DatabricksJobCost` events

For each query, the integration records an event for each row of the result. The
event is reported using the `DatabricksJobCost` event.

#### Common job cost event attributes

The following attributes are included on _all_ job cost events.

| Attribute Name            | Data Type | Description                                                                                                                                                                                     |
|---------------------------|-----------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `query_id`                | string    | The unique ID of the query that produced the data stored in the event (for example, `jobs_cost_list_cost_per_job`)                                                                              |
| `query_title`             | string    | The title of the query that produced the data stored in the event (for example, `List cost per job for prior day`)                                                                              |
| `workspace_id`            | string    | [ID](https://docs.databricks.com/en/workspace/workspace-details.html#workspace-instance-names-urls-and-ids) of the workspace that the associated job belongs to                                 |
| `workspace_url`           | string    | [URL](https://docs.databricks.com/en/workspace/workspace-details.html#workspace-instance-names-urls-and-ids) of the workspace that the associated job belongs to                                |
| `workspace_instance_name` | string    | [Instance name](https://docs.databricks.com/en/workspace/workspace-details.html#workspace-instance-names-urls-and-ids) of the workspace that the associated job belongs to                      |
| `job_id`                  | string    | The ID of the associated job                                                                                                                                                                    |
| `job_name`                | string    | The user-supplied name of the associated job                                                                                                                                                    |

**NOTE:** The `query_id` attribute can be used in [NRQL](https://docs.newrelic.com/docs/nrql/get-started/introduction-nrql-new-relics-query-language/)
queries to scope the query to the appropriate job cost data.

#### List cost per job run

Job cost data on the list cost per job run is collected using the query with the
query id `jobs_cost_list_cost_per_job_run`. This query produces
`DatabricksJobCost` events with the following attributes.

| Name | Description |
|---|---|
| `run_id` | The ID of the job run |
| `run_as` | The ID of the user or service principal used for the job run (only included if [`includeIdentityMetadata`](./configuration.md#queries--metrics--includeidentitymetadata) is `true`) |
| `list_cost` | The estimated list cost of the job run |
| `last_seen_date` | The last time a billable usage record was seen referencing the associated job and the run ID referenced in the `run_id` attribute, in UTC |

#### List cost per job

Job cost data on the list cost per job is collected using the query with the
query id `jobs_cost_list_cost_per_job`. This query produces `DatabricksJobCost`
events with the following attributes.

| Name | Description |
|---|---|
| `runs` | The number of job runs seen for the day this result was collected for the associated job |
| `run_as` | The ID of the user or service principal used for job runs for the associated job (only included if [`includeIdentityMetadata`](./configuration.md#queries--metrics--includeidentitymetadata) is `true`) |
| `list_cost` | The estimated list cost of all runs for the associated job for the day this result was collected |
| `last_seen_date` | The last time a billable usage record was seen referencing the associated job, in UTC |

#### List cost of failed job runs for jobs with frequent failures

Job cost data on the list cost of failed job runs for jobs with frequent
failures for the prior day is collected using the query with the query id
`jobs_cost_frequent_failures`. This query produces `DatabricksJobCost` events
with the following attributes.

| Name | Description |
|---|---|
| `runs` | The number of job runs seen for the day this result was collected for the associated job |
| `run_as` | The ID of the user or service principal used for job runs for the associated job (only included if [`includeIdentityMetadata`](./configuration.md#queries--metrics--includeidentitymetadata) is `true`) |
| `failures` | The number of job runs seen with the result state `ERROR`, `FAILED`, or `TIMED_OUT` for the day this result was collected for the associated job |
| `list_cost` | The estimated list cost of all failed job runs seen for the day this result was collected for the associated job |
| `last_seen_date` | The time of the last job run for the associated job, in UTC |

#### List cost of repaired job runs for jobs with frequent repairs

Job cost data on the list cost of repaired job runs for jobs with frequent
repairs for the prior day is collected using the query with the query id
`jobs_cost_most_retries`. This query produces `DatabricksJobCost` events with
the following attributes.

| Name | Description |
|---|---|
| `run_id` | The ID of the job run |
| `run_as` | The ID of the user or service principal used for the job run (only included if [`includeIdentityMetadata`](./configuration.md#queries--metrics--includeidentitymetadata) is `true`) |
| `repairs` | The number of repair runs seen for the day this result was collected for the associated job |
| `list_cost` | The estimated list cost of the repair runs seen for the day this result was collected for the associated job |
| `repair_time_seconds` | The cumulative duration of the repair runs seen for the day this result was collected for the associated job. This attribute will only be set for job runs that were successfully repaired. |

## Example Consumption & Cost Dashboard

A [sample dashboard](../examples/consumption-cost-dashboard.json) is included
that shows examples of the types of consumption and cost information that can be
displayed and the NRQL statements to use to visualize the data.

![Sample billable usage dashboard image](../examples/consumption-cost-dashboard-billable-usage.png)
![Sample job cost dashboard image](../examples/consumption-cost-dashboard-job-cost.png)
