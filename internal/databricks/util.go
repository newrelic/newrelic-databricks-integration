package databricks

import (
	"time"

	databricksSdk "github.com/databricks/databricks-sdk-go"
	databricksSdkConfig "github.com/databricks/databricks-sdk-go/config"
	"github.com/spf13/viper"
)

const (
	RFC3339Milli = "2006-01-02T15:04:05.000GMT"
)

var (
	// Now is exposed like this for any uses of time.Now() to be mocked in
	// tests.
    Now = time.Now
)

func makeAttributesMap(
	tags map[string]string,
) map[string]interface{} {
	attrs := make(map[string]interface{})

	for k, v := range tags {
		attrs[k] = v
	}

	return attrs
}

func newDatabricksSdkConfig() *databricksSdk.Config {
	databricksConfig := &databricksSdk.Config{}

	/*
	 * If the user explicitly specifies a host in the config, use that.
	 * Otherwise the user can specify using an SDK-supported mechanism.
	 */
	databricksWorkspaceHost := viper.GetString("databricks.workspaceHost")
	if databricksWorkspaceHost != "" {
		databricksConfig.Host = databricksWorkspaceHost
	}

	// Configure authentication
	configureWorkspaceAuth(databricksConfig)

	return databricksConfig
}

func configureWorkspaceAuth(config *databricksSdk.Config) {
	/*
	 * Any of the variables below can be specified in any of the ways that
	 * are supported by the Databricks SDK so if we don't explicitly find one
	 * in the config file, it's not an error.  We assume the user has used one
	 * of the SDK mechanisms and if they haven't the SDK will return an error at
	 * config time or when a request fails.
	 */

	// Prefer OAuth by looking for client ID in our config first
	databricksOAuthClientId := viper.GetString("databricks.oauthClientId")
	if databricksOAuthClientId != "" {
		/*
		 * If an OAuth client ID was in our config we will at this point tell
		 * the SDK to use OAuth M2M authentication. The secret may come from our
		 * config but can still come from any of the supported SDK mechanisms.
		 * So if we don't find the secret in our config file, it's not an error.
		 * Note that because we are forcing OAuth M2M authentication now, the
		 * SDK will not try other mechanisms if OAuth M2M authentication is
		 * unsuccessful.
		 */
		config.ClientID = databricksOAuthClientId
		config.Credentials = databricksSdkConfig.M2mCredentials{}

		databricksOAuthClientSecret := viper.GetString(
			"databricks.oauthClientSecret",
		)
		if databricksOAuthClientSecret != "" {
			config.ClientSecret = databricksOAuthClientSecret
		}

		return
	}

	// Check for a PAT in our config next
	databricksAccessToken := viper.GetString("databricks.accessToken")
	if databricksAccessToken != "" {
		/*
		* If the user didn't specify an OAuth client ID but does specify a PAT,
		* we will at this point tell the SDK to use PAT authentication. Note
		* that because we are forcing PAT authentication now, the SDK will not
		* try other mechanisms if PAT authentication is unsuccessful.
		*/
		config.Token = databricksAccessToken
		config.Credentials = databricksSdkConfig.PatCredentials{}

		return
	}

    // Check for Azure client ID in auth settings in our config next
    databricksAzureClientId := viper.GetString("databricks.azureClientId")
    if databricksAzureClientId != "" {
        /*
		 * If an Azure client ID was in our config we will at this point tell
		 * the SDK to use either Azure MSI (managed identity) or Azure Entra
         * service principal authentication. We use the
		 * databricks.azureMsiEnabled configuration parameter to control which
		 * of the authentication types to use. The remaining settings (client
		 * secret and tenant ID for Azure Entra service principal and the Azure
		 * Databricks resource ID for either) may come from our config but can
		 * still come from any of the supported SDK mechanisms. So if we don't
		 * find these values in our config file, it's not an error. Note that
		 * because we are forcing Azure MSI or Azure Entra service principal
		 * authentication now, the SDK will not try other mechanisms if Azure
		 * authentication is unsuccessful.
		 */
        config.AzureClientID = databricksAzureClientId

        databricksAzureMsiEnabled := viper.GetBool("databricks.azureMsiEnabled")
        if databricksAzureMsiEnabled {
            config.Credentials = databricksSdkConfig.AzureMsiCredentials{}
			config.AzureUseMSI = true
        } else {
            config.Credentials =
                databricksSdkConfig.AzureClientSecretCredentials{}
			config.AzureUseMSI = false

            databricksAzureTenantId := viper.GetString("databricks.azureTenantId")
            if databricksAzureTenantId != "" {
                config.AzureTenantID = databricksAzureTenantId
            }

            databricksAzureClientSecret := viper.GetString("databricks.azureClientSecret")
            if databricksAzureClientSecret != "" {
                config.AzureClientSecret = databricksAzureClientSecret
            }
        }

        databricksAzureResourceId := viper.GetString("databricks.azureResourceId")
        if databricksAzureResourceId != "" {
            config.AzureResourceID = databricksAzureResourceId
        }

		return
    }

	/*
	 * At this point, it's up to the user to specify authentication via an
	 * SDK-supported mechanism. This does not preclude the user from using OAuth
	 * M2M authentication, PAT authentication, or Azure authentication. The user
	 * can still use these authentication types via SDK-supported mechanisms or
	 * any other SDK-supported authentication types via the corresponding
	 * SDK-supported mechanisms.
	 */
}
