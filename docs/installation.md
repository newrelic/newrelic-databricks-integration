# Installation

The Databricks Integration supports two deployment scenarios:

1. [Deploy the integration to a Databricks cluster (recommended)](#deploy-the-integration-to-a-databricks-cluster)
1. [Deploy the integration remotely](#deploy-the-integration-remotely)

**NOTE:** [Apache Spark data](./spark.md) for a Databricks [cluster](https://docs.databricks.com/en/getting-started/concepts.html#cluster)
cannot be collected remotely. In order to collect [Apache Spark data](./spark.md)
from a Databricks cluster, the Databricks Integration _must_ be deployed to the
Databricks cluster.

## Deploy the integration to a Databricks cluster

The Databricks Integration is intended be deployed on the driver node of a
Databricks all-purpose, job, or pipeline [cluster](https://docs.databricks.com/en/getting-started/concepts.html#cluster)
using a [cluster-scoped init script](https://docs.databricks.com/en/init-scripts/cluster-scoped.html).
The [init script](../init/cluster_init_integration.sh) is designed to use custom
[environment variables](https://docs.databricks.com/aws/en/compute/configure#environment-variables)
to specify configuration parameters necessary for the integration [configuration](./configuration.md)
so that it does **not need to be modified**.

To install the [init script](../init/cluster_init_integration.sh) on an
all-purpose cluster, perform the following steps.

1. Login to your Databricks account and navigate to the desired
   [workspace](https://docs.databricks.com/en/getting-started/concepts.html#accounts-and-workspaces).
1. Follow the [recommendations for init scripts](https://docs.databricks.com/en/init-scripts/index.html#recommendations-for-init-scripts)
   to store the [`cluster_init_integration.sh`](../init/cluster_init_integration.sh)
   script within your workspace in the recommended manner. For example, if your
   workspace is [enabled for Unity Catalog](https://docs.databricks.com/en/data-governance/unity-catalog/get-started.html#step-1-confirm-that-your-workspace-is-enabled-for-unity-catalog),
   you should store the init script in a [Unity Catalog volume](https://docs.databricks.com/en/ingestion/file-upload/upload-to-volume.html).
1. Navigate to the [`Compute`](https://docs.databricks.com/en/compute/clusters-manage.html#view-compute)
   tab and select the desired all-purpose compute to open the compute details
   UI.
1. Click the button labeled `Edit` to [edit the compute's configuration](https://docs.databricks.com/en/compute/clusters-manage.html#edit-a-compute).
1. Follow the steps to [use the UI to configure a cluster-scoped init script](https://docs.databricks.com/en/init-scripts/cluster-scoped.html#configure-a-cluster-scoped-init-script-using-the-ui)
   and point to the location where you stored the init script in step 2 above.
1. Follow the steps to [set compute environment variables](https://docs.databricks.com/en/compute/configure.html#environment-variables)
   to add the environment variables [listed below](#supported-init-script-environment-variables).
1. If your cluster is not running, click on the button labeled `Confirm` to
   save your changes. Then, restart the cluster. If your cluster is already
   running, click on the button labeled `Confirm and restart` to save your
   changes and restart the cluster.

### Supported Init Script Environment Variables

* `NEW_RELIC_API_KEY` - Your [New Relic User API Key](https://docs.newrelic.com/docs/apis/intro-apis/new-relic-api-keys/#user-key)
* `NEW_RELIC_LICENSE_KEY` - Your [New Relic License Key](https://docs.newrelic.com/docs/apis/intro-apis/new-relic-api-keys/#license-key)
* `NEW_RELIC_ACCOUNT_ID` - Your [New Relic Account ID](https://docs.newrelic.com/docs/accounts/accounts-billing/account-structure/account-id/)
* `NEW_RELIC_REGION` - The [region](https://docs.newrelic.com/docs/accounts/accounts-billing/account-setup/choose-your-data-center/#regions-availability)
  of your New Relic account; one of `US` or `EU`. Defaults to `US`.
* `NEW_RELIC_DATABRICKS_INTERVAL` - The integration collection [interval](./configuration.md#interval),
  in seconds. Defaults to `30`.
* `NEW_RELIC_DATABRICKS_LOG_LEVEL` - The integration [log level](./configuration.md#level).
  Defaults to `warn`.
* `NEW_RELIC_DATABRICKS_WORKSPACE_HOST` - The [instance name](https://docs.databricks.com/en/workspace/workspace-details.html#workspace-instance-names-urls-and-ids)
  of the target Databricks instance
* `NEW_RELIC_DATABRICKS_ACCESS_TOKEN` - To [authenticate](./authentication.md) with
  a [personal access token](https://docs.databricks.com/en/dev-tools/auth/pat.html#databricks-personal-access-tokens-for-workspace-users),
  your personal access token
* `NEW_RELIC_DATABRICKS_OAUTH_CLIENT_ID` - To [use a service principal to authenticate with Databricks (OAuth M2M)](https://docs.databricks.com/en/dev-tools/auth/oauth-m2m.html),
  the OAuth client ID for the service principal
* `NEW_RELIC_DATABRICKS_OAUTH_CLIENT_SECRET` - To [use a service principal to authenticate with Databricks (OAuth M2M)](https://docs.databricks.com/en/dev-tools/auth/oauth-m2m.html),
  an OAuth client secret associated with the service principal
* `NEW_RELIC_DATABRICKS_JOB_RUNS_ENABLED` - Set to `true` to enable collection
  of [Databricks Lakeflow Job run metrics](./jobs.md) for this workspace or
  `false` to disable collection. Defaults to `true`.
* `NEW_RELIC_DATABRICKS_JOB_RUNS_START_OFFSET` - The [start offset](./configuration.md#jobs--runs--startoffset)
  to use when collecting [Databricks Lakeflow Job run metrics](./jobs.md), in
  seconds. Defaults to 86400 (1 day).
* `NEW_RELIC_DATABRICKS_PIPELINE_METRICS_ENABLED` - Set to `true` to enable
  collection of [Databricks Lakeflow Declarative Pipeline update metrics](./pipelines.md#pipeline-update-metrics)
  for this workspace or `false` to disable collection. Defaults to `true`.
* `NEW_RELIC_DATABRICKS_PIPELINE_METRICS_START_OFFSET` - The [start offset](./configuration.md#pipelines--metrics--startoffset)
  to use when collecting [Databricks Lakeflow Declarative Pipeline update metrics](./pipelines.md#pipeline-update-metrics),
  in seconds. Defaults to 86400 (1 day).
* `NEW_RELIC_DATABRICKS_PIPELINE_EVENT_LOGS_ENABLED` - Set to `true` to enable
  collection of [Databricks Lakeflow Declarative Pipeline event logs](./pipelines.md#pipeline-event-logs)
  for this workspace or `false` to disable collection. Defaults to `true`.
* `NEW_RELIC_DATABRICKS_QUERY_METRICS_ENABLED` - Set to `true` to enable
  collection of Databricks [query metrics](./queries.md) for this workspace
  or `false` to disable collection. Defaults to `true`.
* `NEW_RELIC_DATABRICKS_QUERY_METRICS_INCLUDE_IDENTITY_METADATA` - Set to `true`
  to enable inclusion of [identity related metadata](./configuration.md#queries--metrics--includeidentitymetadata)
  with Databricks [query metrics](./queries.md). Defaults to `false`.
* `NEW_RELIC_DATABRICKS_QUERY_METRICS_START_OFFSET` - The [start offset](./configuration.md#queries--metrics--startoffset)
  to use when collecting [query metrics](./queries.md), in seconds. Defaults to
  600 (10 minutes).
* `NEW_RELIC_DATABRICKS_QUERY_METRICS_MAX_RESULTS` - The [maximum number of results](./configuration.md#queries--metrics--maxresults)
  to return per page on query history API calls when collecting [query metrics](./queries.md).
  Defaults to 100.
* `NEW_RELIC_DATABRICKS_USAGE_ENABLED` - Set to `true` to enable collection of
  [consumption and cost data](./billable-usage.md) or `false` to disable
  collection. Defaults to `false`.
* `NEW_RELIC_DATABRICKS_USAGE_INCLUDE_IDENTITY_METADATA` - Set to `true` to
  enable inclusion of [identity related metadata](./configuration.md#usage--includeidentitymetadata)
  with Databricks consumption and cost data. Defaults to `false`.
* `NEW_RELIC_DATABRICKS_USAGE_RUN_TIME` - The time of day to collect
  [consumption and cost data](./billable-usage.md). See the documentation of the
  [`runTime` configuration parameter](./configuration.md#usage--runtime) for details on
  the format of this value.
* `NEW_RELIC_DATABRICKS_SQL_WAREHOUSE` - The ID of a [SQL warehouse](https://docs.databricks.com/en/compute/sql-warehouse/index.html)
   where usage queries should be run
* `NEW_RELIC_INFRASTRUCTURE_ENABLED` - Set to `true` to install the
  [New Relic Infrastructure agent](https://docs.newrelic.com/docs/infrastructure/introduction-infra-monitoring/)
  on the driver and worker nodes of the [cluster](https://docs.databricks.com/en/getting-started/concepts.html#cluster).
  Defaults to `false`.
* `NEW_RELIC_INFRASTRUCTURE_LOG_LEVEL` - The [New Relic Infrastructure agent](https://docs.newrelic.com/docs/infrastructure/introduction-infra-monitoring/)
  [log level](https://docs.newrelic.com/docs/infrastructure/infrastructure-agent/configuration/infrastructure-agent-configuration-settings/#level).
  Defaults to `info`.
* `NEW_RELIC_INFRASTRUCTURE_LOGS_ENABLED` - Set to `true` to enable collection
  of the Spark driver and executor logs, the Spark driver event log, and the
  driver and worker init script logs. Logs will be [forwarded](https://docs.newrelic.com/docs/logs/forward-logs/forward-your-logs-using-infrastructure-agent/)
  to [New Relic Logs](https://docs.newrelic.com/docs/logs/get-started/get-started-log-management/)
  via the [New Relic Infrastructure agent](https://docs.newrelic.com/docs/infrastructure/introduction-infra-monitoring/)
  and therefore, setting this environment variable to `true` requires that the
  `NEW_RELIC_INFRASTRUCTURE_ENABLED` environment variable also be set to `true`.
  Defaults to `false`.
* `NEW_RELIC_DATABRICKS_STARTUP_RETRIES` - The number of attempts the startup
  script will make to read the driver environment script. Defaults to `5`.
* `NEW_RELIC_DATABRICKS_SEND_STARTUP_LOGS_ENABLED` - Set to `true` to capture
  the startup logs and
  [send them to New Relic Logs](./troubleshooting.md#sending-startup-logs-to-new-relic-logs).
* `NEW_RELIC_DATABRICKS_COPY_STARTUP_LOGS_ENABLED` - Set to `true` to capture
  the startup logs and
  [copy them to DBFS](./troubleshooting.md#copying-startup-logs-to-dbfs).
* `NEW_RELIC_DATABRICKS_AZURE_MSI_ENABLED` - Set to `true` to authenticate using
  [Azure managed identities](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/auth/azure-mi)
* `NEW_RELIC_DATABRICKS_AZURE_TENANT_ID` - Set to your Microsoft Entra tenant
  ID to authenticate using a
  [Microsoft Entra service principal](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/auth/azure-sp)
* `NEW_RELIC_DATABRICKS_AZURE_CLIENT_ID` - Set to the client ID of your managed
  identity or service principal to authenticate using
  [Azure managed identities](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/auth/azure-mi)
  or a
  [Microsoft Entra service principal](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/auth/azure-sp),
  respectively
* `NEW_RELIC_DATABRICKS_AZURE_CLIENT_SECRET` - Set to the client secret of your
  service principal to authenticate using a
  [Microsoft Entra service principal](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/auth/azure-sp)
* `NEW_RELIC_DATABRICKS_AZURE_RESOURCE_ID` - Set to the Azure resource ID for
  your Azure Databricks workspace. Required to authenticate using
  [Azure managed identities](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/auth/azure-mi).
  Optional to authenticate using a
  [Microsoft Entra service principal](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/auth/azure-sp).

**NOTE:**
* In most cases, the [init script](../init/cluster_init_integration.sh) should
  *not* need to be modified. Use the environment variables [listed above](#supported-init-script-environment-variables)
  to customize the installation and configuration of the integration.
* The `NEW_RELIC_API_KEY` is currently unused but is required by the
  [new-relic-client-go](https://github.com/newrelic/newrelic-client-go) module
  used by the integration.
* Only the personal access token _or_ OAuth credentials need to be specified but
  not both. If both are specified, the OAuth credentials take precedence.
* Sensitive data like credentials and API keys should never be specified
  directly in custom [environment variables](https://docs.databricks.com/aws/en/compute/configure#environment-variables).
  Instead, it is recommended to create a [secret](https://docs.databricks.com/en/security/secrets/secrets.html)
  using the [Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/index.html)
  and [reference the secret in the environment variable](https://docs.databricks.com/aws/en/security/secrets/secrets-spark-conf-env-var#reference-a-secret-in-an-environment-variable).
  See the [additional information](./additional-information.md) documentation
  for an [example](./additional-information.md#example-creating-and-using-a-secret-for-your-new-relic-license-key)
  of creating a [secret](https://docs.databricks.com/en/security/secrets/secrets.html)
  and referencing it in a custom [environment variable](https://docs.databricks.com/aws/en/compute/configure#environment-variables).
* When `NEW_RELIC_DATABRICKS_USAGE_ENABLED` is set to `true`, a [SQL warehouse](https://docs.databricks.com/en/compute/sql-warehouse/index.html)
  ID must be specified using `NEW_RELIC_DATABRICKS_SQL_WAREHOUSE` or the
  integration will _fail_ to start.
* To install the [init script](../init/cluster_init_integration.sh) on a job
  cluster, see the section [configure compute for jobs](https://docs.databricks.com/aws/en/jobs/compute)
  in the Databricks documentation.
* To install the [init script](../init/cluster_init_integration.sh) on a pipeline
  cluster, see the section [configure classic compute for Lakeflow Declarative Pipelines](https://docs.databricks.com/aws/en/dlt/configure-compute)
  in the Databricks documentation.
* When installing the integration on a cluster with standard access mode
  (formerly shared access mode), make sure to add the [init script](../init/cluster_init_integration.sh)
  to the [allowlist](https://docs.databricks.com/aws/en/data-governance/unity-catalog/manage-privileges/allowlist).
* Make sure to restart the cluster following the configuration of the
  init script and environment variables.
* Log messages for the integration are reported to New Relic using the [Go APM agent's](https://docs.newrelic.com/docs/apm/agents/go-agent/get-started/introduction-new-relic-go/)
  [logs in context](https://docs.newrelic.com/docs/logs/logs-context/configure-logs-context-go/)
  feature and can be accessed via the APM UI for the entity named
  "New Relic Databricks Integration" or via the [New Relic Logs UI](https://docs.newrelic.com/docs/logs/ui-data/use-logs-ui/)
  using the query `entity.name:"New Relic Databricks Integration"`.
* If `NEW_RELIC_DATABRICKS_COPY_STARTUP_LOGS_ENABLED` is set to `true` but the
  [DBFS root](https://docs.databricks.com/aws/en/dbfs/#what-is-the-dbfs-root) is
  not enabled for your account and workspace, the integration may fail to start.

## Deploy the integration remotely

The Databricks Integration can be deployed remotely on a supported host
environment outside Databricks.

The following operating systems and architectures are supported.

* Linux / amd64
* Windows / amd64

To install the Databricks Integration remotely on a supported host environment,
perform the following steps.

1. Download the appropriate archive for your platform from the [latest release](https://github.com/newrelic/newrelic-databricks-integration/releases/latest).
1. Extract the archive to a new or existing directory.
1. Create a directory named `configs` in the same directory.
1. Create a file named `config.yml` in the `configs` directory and copy the
   contents of the file [`config.template.yml`](../configs/config.template.yml)
   in this repository into it.
1. Edit the `config.yml` file and [configure](./configuration.md) the integration
   appropriately for your host environment and Databricks deployment.
1. From the directory where the archive was extracted, execute the integration
   binary using the command `./newrelic-databricks-integration` (or
   `.\newrelic-databricks-integration.exe` on Windows) with the appropriate
   [command line options](./cli.md).
