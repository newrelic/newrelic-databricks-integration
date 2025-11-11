# Authentication

The Databricks integration uses the [Databricks SDK for Go](https://docs.databricks.com/en/dev-tools/sdk-go.html)
to access the [Databricks ReST API](https://docs.databricks.com/api/workspace/introduction).
The SDK performs authentication on behalf of the integration. Authentication
options are specified using Databricks [configuration profiles](https://docs.databricks.com/en/dev-tools/auth/config-profiles.html)
and/or [environment variables](https://docs.databricks.com/aws/en/dev-tools/auth#environment-variables).
See the [SDK documentation](https://github.com/databricks/databricks-sdk-go?tab=readme-ov-file#authentication)
and the [Databricks unified authentication documentation](https://docs.databricks.com/en/dev-tools/auth/unified-auth.html)
for more details.

For convenience purposes, the following parameters can be used in the
[Databricks configuration](./configuration.md#databricks) section of the [`config.yml`](./configuration.md#configyml)
file instead of using a Databricks [configuration profile](https://docs.databricks.com/en/dev-tools/auth/config-profiles.html)
and/or [environment variables](https://docs.databricks.com/aws/en/dev-tools/auth#environment-variables).

- [`accessToken`](./configuration.md#accesstoken) - When set, the integration
  will instruct the SDK to explicitly use [Databricks personal access token authentication](https://docs.databricks.com/en/dev-tools/auth/pat.html).
  If [personal access token authentication](https://docs.databricks.com/en/dev-tools/auth/pat.html)
  fails, the SDK will _not_ attempt to try other authentication mechanisms as it
  it does when using a [configuration profile](https://docs.databricks.com/en/dev-tools/auth/config-profiles.html)
  and/or [environment variables](https://docs.databricks.com/aws/en/dev-tools/auth#environment-variables).
  Instead the integration will either fail to start or will report errors in the
  [integration log](./troubleshooting.md#viewing-integration-logs) when making
  [Databricks ReST API](https://docs.databricks.com/api/workspace/introduction)
  calls.
- [`oauthClientId`](./configuration.md#oauthclientid) - When set, the
  integration will instruct the SDK to explicitly [use service principal authentication via OAuth](https://docs.databricks.com/en/dev-tools/auth/oauth-m2m.html).
  If [service principal authentication](https://docs.databricks.com/en/dev-tools/auth/oauth-m2m.html)
  fails, The SDK will _not_ attempt to try other authentication mechanisms as it
  does when using a [configuration profile](https://docs.databricks.com/en/dev-tools/auth/config-profiles.html)
  and/or [environment variables](https://docs.databricks.com/aws/en/dev-tools/auth#environment-variables).
  Instead the integration will either fail to start or will report errors in the
  [integration log](./troubleshooting.md#viewing-integration-logs) when making
  [Databricks ReST API](https://docs.databricks.com/api/workspace/introduction)
  calls.

  The OAuth Client secret can be set using the [`oauthClientSecret`](./configuration.md#oauthclientsecret)
  configuration parameter or any of the other mechanisms supported by the SDK
  (e.g. the `client_secret` field in a Databricks [configuration profile](https://docs.databricks.com/en/dev-tools/auth/config-profiles.html)
  or the `DATABRICKS_CLIENT_SECRET` [environment variable](https://docs.databricks.com/aws/en/dev-tools/auth#environment-variables)).
- [`oauthClientSecret`](./configuration.md#oauthclientsecret) - The OAuth client
  secret to use when [using service principal authentication via OAuth](https://docs.databricks.com/en/dev-tools/auth/oauth-m2m.html).
  This value is _only_ used when [`oauthClientId`](./configuration.md#oauthclientid)
  is set in the [`config.yml`](./configuration.md#configyml). The OAuth client
  secret can also be set using any of the other mechanisms supported by the SDK
  (e.g. the `client_secret` field in a Databricks [configuration profile](https://docs.databricks.com/en/dev-tools/auth/config-profiles.html)
  or the `DATABRICKS_CLIENT_SECRET` [environment variable](https://docs.databricks.com/aws/en/dev-tools/auth#environment-variables)).

**NOTE:**
* If both the `accessToken` and an `oauthClientId` are specified in the [`config.yml`](./configuration.md#configyml),
  OAuth authentication will take precedence.
* Authentication applies only to calls made to collect Databricks
  telemetry via the [Databricks SDK for Go](https://docs.databricks.com/en/dev-tools/sdk-go.html).
  It does not apply to [Spark ReST API](https://spark.apache.org/docs/3.5.2/monitoring.html#rest-api)
  calls because authentication is not supported by the Spark collector.
