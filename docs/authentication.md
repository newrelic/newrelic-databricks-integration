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
  [integration log](./troubleshooting.md#log-data) when making
  [Databricks ReST API](https://docs.databricks.com/api/workspace/introduction)
  calls.
- [`oauthClientId`](./configuration.md#oauthclientid) - When set, the
  integration will instruct the SDK to explicitly [use service principal authentication via OAuth](https://docs.databricks.com/en/dev-tools/auth/oauth-m2m.html).
  If [service principal authentication](https://docs.databricks.com/en/dev-tools/auth/oauth-m2m.html)
  fails, The SDK will _not_ attempt to try other authentication mechanisms as it
  does when using a [configuration profile](https://docs.databricks.com/en/dev-tools/auth/config-profiles.html)
  and/or [environment variables](https://docs.databricks.com/aws/en/dev-tools/auth#environment-variables).
  Instead the integration will either fail to start or will report errors in the
  [integration log](./troubleshooting.md#log-data) when making
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
- [`azureClientId`](./configuration.md#azureclientid) - When set, the
  integration will instruct the SDK to explicitly
  [use Azure managed identity authentication](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/auth/azure-mi)
  if [`azureMsiEnabled`](./configuration.md#azuremsienabled) is set to `true`,
  or
  [use Azure Entra service principal authentication](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/auth/azure-sp).
  if [`azureMsiEnabled`](./configuration.md#azuremsienabled) is missing or set
  to `false`. If authentication fails, The SDK will _not_ attempt to try other
  authentication mechanisms as it does when using a
  [configuration profile](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/auth/config-profiles)
  and/or
  [environment variables](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/auth/azure-mi#environment).
  Instead the integration will either fail to start or will report errors in the
  [integration log](./troubleshooting.md#log-data) when making
  [Databricks ReST API](https://docs.databricks.com/api/workspace/introduction)
  calls.
- [`azureMsiEnabled`](./configuration.md#azuremsienabled) - When
  [`azureClientId`](./configuration.md#azureclientid) is set, set this
  configuration parameter to `true` to
  [use Azure managed identity authentication](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/auth/azure-mi)
  or omit the parameter ot set to `false` to
  [use Azure Entra service principal authentication](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/auth/azure-sp).
  This value is _only_ used when
  [`azureClientId`](./configuration.md#azureClientId) is set. This value can
  also be set using any of the other mechanisms supported by the SDK
  (e.g. the `azure_use_msi` field in a Databricks [configuration profile](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/auth/config-profiles)
  or the `ARM_USE_MSI` [environment variable](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/auth/azure-mi#environment)).
- [`azureClientSecret`](./configuration.md#azureclientsecret) - The Azure Entra
  service principal client secret to use when
  [using Azure Entra service principal authentication](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/auth/azure-sp).
  This value is _only_ used when [`azureClientId`](./configuration.md#azureclientid)
  is set and [`azureMsiEnabled`](./configuration.md#azuremsienabled) is missing
  or set to `false`. The Azure Entra service principal client secret can also be
  set using any of the other mechanisms supported by the SDK
  (e.g. the `azure_client_secret` field in a Databricks [configuration profile](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/auth/config-profiles)
  or the `ARM_CLIENT_SECRET` [environment variable](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/auth/azure-sp#environment-variables)).
- [`azureTenantId`](./configuration.md#azuretenantid) - The Azure Entra
  service principal tenant ID to use when
  [using Azure Entra service principal authentication](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/auth/azure-sp).
  This value is _only_ used when [`azureClientId`](./configuration.md#azureclientid)
  is set and [`azureMsiEnabled`](./configuration.md#azuremsienabled) is missing
  or set to `false`. The Azure Entra service principal tenant ID can also be set
  using any of the other mechanisms supported by the SDK
  (e.g. the `azure_tenant_id` field in a Databricks [configuration profile](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/auth/config-profiles)
  or the `ARM_TENANT_ID` [environment variable](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/auth/azure-sp#environment-variables)).
- [`azureResourceId`](./configuration.md#azureresourceid) - The Azure resource
  ID of your Azure Databricks workspace. This value can be used for either
  [Azure managed identity authentication](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/auth/azure-mi)
  or
  [use Azure Entra service principal authentication](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/auth/azure-sp).
  It can also be set using any of the other mechanisms supported by the SDK
  (e.g. the `azure_workspace_resource_id` field in a Databricks [configuration profile](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/auth/config-profiles)
  or the `DATABRICKS_AZURE_RESOURCE_ID` [environment variable](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/auth/azure-sp#environment-variables)).

**NOTE:**
* If configuration parameters are set for multiple types of authentication, the
  order of precedence is as follows.

  * Databricks OAuth authentication (`oauthClientId` is set)
  * Databricks Personal Access Token authentication (`accessToken` is set)
  * Azure managed identity authentication (`azureClientId` is set and
    `azureMsiEnabled` is set to `true`)
  * Azure Entra service principal authentication (`azureClientId` is set and
    `azureMsiEnabled` is missing or set to `false`)

* Authentication applies only to calls made to collect Databricks
  telemetry via the [Databricks SDK for Go](https://docs.databricks.com/en/dev-tools/sdk-go.html).
  It does not apply to [Spark ReST API](https://spark.apache.org/docs/3.5.2/monitoring.html#rest-api)
  calls because authentication is not supported by the Spark collector.
* Before using the [`azureResourceId`](./configuration.md#azureresourceid)
  configuration parameter with
  [Azure managed identity authentication](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/auth/azure-mi)
  or
  [Azure Entra service principal authentication](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/auth/azure-sp),
  note that the Databricks Azure
  [documentation](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/auth/azure-mi#environment)
  "recommends [specifying the workspace host (for example using the
  [`workspaceHost`](./configuration.md#workspacehost) configuration parameter)]
  and explicitly assigning the identity to the workspace." Using the
  [`azureResourceId`](./configuration.md#azureresourceid) configuration
  parameter instead "requires Contributor or Owner permissions on the Azure
  resource, or a custom role with specific Azure Databricks permissions."
