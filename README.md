[![Community Project header](https://github.com/newrelic/open-source-office/raw/master/examples/categories/images/Community_Project.png)](https://github.com/newrelic/open-source-office/blob/master/examples/categories/index.md#category-community-project)

![GitHub forks](https://img.shields.io/github/forks/newrelic/newrelic-databricks-integration?style=social)
![GitHub stars](https://img.shields.io/github/stars/newrelic/newrelic-databricks-integration?style=social)
![GitHub watchers](https://img.shields.io/github/watchers/newrelic/newrelic-databricks-integration?style=social)

![GitHub all releases](https://img.shields.io/github/downloads/newrelic/newrelic-databricks-integration/total)
![GitHub release (latest by date)](https://img.shields.io/github/v/release/newrelic/newrelic-databricks-integration)
![GitHub last commit](https://img.shields.io/github/last-commit/newrelic/newrelic-databricks-integration)
![GitHub Release Date](https://img.shields.io/github/release-date/newrelic/newrelic-databricks-integration)

![GitHub issues](https://img.shields.io/github/issues/newrelic/newrelic-databricks-integration)
![GitHub issues closed](https://img.shields.io/github/issues-closed/newrelic/newrelic-databricks-integration)
![GitHub pull requests](https://img.shields.io/github/issues-pr/newrelic/newrelic-databricks-integration)
![GitHub pull requests closed](https://img.shields.io/github/issues-pr-closed/newrelic/newrelic-databricks-integration)

# Databricks Integration

The Databricks Integration is a standalone application that collects telemetry from the Databricks Data
Intelligence Platform, to be used in troubleshooting and optimizing Databricks workloads.

The integration collects the following types of telemetry:

-   Apache Spark application metrics, such as Spark executor memory and cpu metrics, durations and I/O metrics of Spark jobs, stages, and tasks, and Spark RDD memory and disk metrics
-   Databricks Lakeflow job run metrics, such as durations, start and end times, and termination codes and types for job and task runs.
-   Databricks Lakeflow Declarative Pipeline update metrics, such as durations, start and end times, and completion status for updates and flows.
-   Databricks Lakeflow Declarative Pipeline event logs
-   Databricks query metrics, including execution times, query statuses, and query I/O metrics.
-   Databricks cluster health metrics and logs, such as driver and worker memory and cpu metrics and driver and executor logs.
-   Databricks consumption and cost data, DBU consumption metrics and estimated Databricks costs.

## Usage Guide

To get up and running quickly, refer to the [Getting Started](#getting-started) section; for comprehensive usage details, review the additional sections linked below.

-   [Getting Started](#getting-started)
-   [Installation](./docs/installation.md)
-   [Configuration](./docs/configuration.md)
-   [Authentication](./docs/authentication.md)
-   [Apache Spark Application Metrics](./docs/spark.md)
-   [Job Run Metrics](./docs/jobs.md)
-   [Pipeline Update Metrics](./docs/pipelines.md#pipeline-update-metrics)
-   [Pipeline Event Logs](./docs/pipelines.md#pipeline-event-logs)
-   [Query Metrics](./docs/queries.md)
-   [Cluster Health](./docs/cluster-health.md)
-   [Consumption & Cost Data](./docs/consumption-cost.md)
-   [Using the CLI](./docs/cli.md)
-   [Import the Example Dashboards](./docs/example-dashboards.md)
-   [Additional Information](./docs/additional-information.md)
-   [Contribute to the Integration](./docs/contribute.md)

## Getting Started

Follow the steps below to get started with the Databricks Integration quickly.

**1. Install the integration**

Follow the steps to [deploy the integration to a Databricks cluster](./docs/installation.md#deploy-the-integration-to-a-databricks-cluster).

**2. Verify the installation**

Once the Databricks Integration has run for a few minutes, use the [query builder](https://one.newrelic.com/data-exploration/query-builder) in New Relic to run the following query (replace `[YOUR_CLUSTER_NAME]` with the _name_ of the Databricks cluster _where the integration was installed_):

`SELECT uniqueCount(executorId) AS Executors FROM SparkExecutorSample WHERE databricksclustername = '[YOUR_CLUSTER_NAME]'`

The result of the query should be **a number greater than zero**.

[TODO: troubleshooting if no data]

**3. Import the example dashboards (optional)**

To help you get started using the collected telemetry, [example dashboards](./examples/) have been provided that can be imported into New Relic.

To use these dashboards, follow the instructions found in [Import the Example Dashboards](./docs/example-dashboards.md).

## Support

New Relic has open-sourced this project. This project is provided AS-IS WITHOUT
WARRANTY OR DEDICATED SUPPORT. Issues and contributions should be reported to
the project here on GitHub.

We encourage you to bring your experiences and questions to the
[Explorers Hub](https://discuss.newrelic.com/) where our community members
collaborate on solutions and new ideas.

### Privacy

At New Relic we take your privacy and the security of your information
seriously, and are committed to protecting your information. We must emphasize
the importance of not sharing personal data in public forums, and ask all users
to scrub logs and diagnostic information for sensitive information, whether
personal, proprietary, or otherwise.

We define “Personal Data” as any information relating to an identified or
identifiable individual, including, for example, your name, phone number, post
code or zip code, Device ID, IP address, and email address.

For more information, review [New Relic’s General Data Privacy Notice](https://newrelic.com/termsandconditions/privacy).

### Contribute

We encourage your contributions to improve this project! Keep in mind that
when you submit your pull request, you'll need to sign the CLA via the
click-through using CLA-Assistant. You only have to sign the CLA one time per
project.

If you have any questions, or to execute our corporate CLA (which is required
if your contribution is on behalf of a company), drop us an email at
opensource@newrelic.com.

If you would like to contribute to this project, please review the standards outlined in [Contribute to the Integration](./docs/contribute.md), as well as [these guidelines](./CONTRIBUTING.md).

**A note about vulnerabilities**

As noted in our [security policy](../../security/policy), New Relic is committed
to the privacy and security of our customers and their data. We believe that
providing coordinated disclosure by security researchers and engaging with the
security community are important means to achieve our security goals.

If you believe you have found a security vulnerability in this project or any of New Relic's products or websites, we welcome and greatly appreciate you reporting it to New Relic through [our bug bounty program](https://docs.newrelic.com/docs/security/security-privacy/information-security/report-security-vulnerabilities/).

### License

The Databricks Integration project is licensed under the
[Apache 2.0](http://apache.org/licenses/LICENSE-2.0.txt) License.
