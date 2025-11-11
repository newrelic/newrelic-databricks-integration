# Using the CLI

The Databricks Integration is a [Go](https://go.dev/) application that is
packaged as a binary executable.

When [deployed to a Databricks cluster](./installation.md#deploy-the-integration-to-a-databricks-cluster),
the [init script](../init/cluster_init_integration.sh) automatically downloads
the [latest release](https://github.com/newrelic/newrelic-databricks-integration/releases/latest)
on the driver node, installs it at the path `/databricks/driver/newrelic`, and
runs the binary executable each time the cluster starts. Generally there is no
need to execute the integration directly in this scenario.

When [deployed remotely](./installation.md#deploy-the-integration-remotely), the
integration can be run directly from the command line by executing the
integration binary named `newrelic-databricks-integration` (or
`newrelic-databricks-integration.exe` on Windows) that is included in the
[release](https://github.com/newrelic/newrelic-databricks-integration/releases/latest)
archive.

## CLI Options

The integration binary supports the following command line options.

| Option        | Description                                                           | Default               |
|---------------|-----------------------------------------------------------------------|-----------------------|
| --config_path | path to the [configuration file](./configuration.md#configyml) to use | `configs/config.yml`  |
| --dry_run     | flag to enable "dry run" mode                                         | `false`               |
| --env_prefix  | prefix to use for environment variable lookups                        | `''`                  |
| --verbose     | flag to enable verbose logging                                        | `false`               |
| --version     | display version information only                                      | N/a                   |

### `--config-path`

The `--config-path` option specifies the path to the [configuration file](./configuration.md#configyml)
used to [configure](./configuration.md) the integration. By default, the
configuration file will be loaded from the path `configs/config.yml` relative to
the current working directory.

The configuration path can also be specified using the environment variable
`CONFIG_PATH`.

**NOTE:** When [deployed to a Databricks cluster](./installation.md#deploy-the-integration-to-a-databricks-cluster),
the [configuration file](./configuration.md#configyml) is automatically created
at the location `/databricks/driver/newrelic/configs/config.yml` and the
integration is run with the working directory `/databricks/driver/newrelic`.
There is no need to specify the configuration path using the `--config_path`
option or the `CONFIG_PATH` environment variable.

### `--dry_run`

The `--dry_run` option is used to enable "dry run" mode. In dry run mode, the
integration will collect all telemetry as it normally would but will not send
any telemetry to New Relic. This option is generally used with the [`--verbose`](#--verbose)
option to inspect the payloads that would be sent to New Relic for
troubleshooting purposes.

Dry run mode can also be enabled using the environment variable `DRY_RUN`.

### `--env_prefix`

The `--env_prefix` option is used to specify a prefix that will be prepended to
environment variable names on _all_ environment variable lookups.

For example, if the environment variable prefix is set to `foo`, the integration
will look for the configuration path in the environment variable
`FOO_CONFIG_PATH` instead of `CONFIG_PATH`.

### `--verbose`

The `--verbose` option is used to enable verbose (debug) logging by setting the
[log level](./configuration.md#level) to `debug`. Verbose logging should be used
for troubleshooting only as it generates a significant amount of logs. It should
only be used in a production environment while a problem is being reproduced. It
should not be enabled continuously. It is often used with the [`--dry_run`](#--dry_run)
option to inspect the payloads that would be sent to New Relic.

Logs are output to the standard error stream (`stderr`) by default. Use the log
[`fileName`](./configuration.md#filename) configuration parameter to output logs
to a file.

Verbose logging can also be specified using the environment variable `VERBOSE`.

### `--version`

The `--version` option is used to display the version information of the
integration. The integration will exit immediately after displaying the version
information.
