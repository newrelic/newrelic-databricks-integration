package databricks

import (
	"testing"

	databricksSdk "github.com/databricks/databricks-sdk-go"
	databricksSdkConfig "github.com/databricks/databricks-sdk-go/config"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

func TestMakeAttributesMap(t *testing.T) {
	// Setup test tag map
	tags := map[string]string{
		"key1": "value1",
		"key2": "value2",
	}

	// Setup expected attributes map
	expected := map[string]interface{}{
		"key1": "value1",
		"key2": "value2",
	}

	// Execute the function under test
	result := makeAttributesMap(tags)

	// Verify result
	assert.Equal(t, expected, result)
}

func TestMakeAttributesMap_EmptyTags(t *testing.T) {
	// Setup empty tag map
	tags := map[string]string{}

	// Setup expected attributes map
	expected := map[string]interface{}{}

	// Execute the function under test
	result := makeAttributesMap(tags)

	// Verify result
	assert.Equal(t, expected, result)
}

func TestNewDatabricksSdkConfig_HostNotInConfig(t *testing.T) {
	// Reset viper config to ensure clean test state
	viper.Reset()

	// Execute the function under test
	config := newDatabricksSdkConfig()

	// Verify result
	assert.Empty(t, config.Host, "host should be empty")
}

func TestNewDatabricksSdkConfig_HostInConfig(t *testing.T) {
	// Reset viper config to ensure clean test state
	viper.Reset()

	// Set the host in the config
	viper.Set("databricks.workspaceHost", "fake-workspace-host.fake-domain.com")

	// Execute the function under test
	config := newDatabricksSdkConfig()

	// Verify result
	assert.Equal(t, "fake-workspace-host.fake-domain.com", config.Host)
}

func TestNewDatabricksSdkConfig_AuthConfigured(t *testing.T) {
	// Reset viper config to ensure clean test state
	viper.Reset()

	// Set the OAuth client ID in the config to validate configureWorkspaceAuth
	// is being called from newDatabricksSdkConfig
	viper.Set("databricks.oauthClientId", "fake-oauth-client-id")

	// Execute the function under test
	config := newDatabricksSdkConfig()

	// Verify result
	assert.NotNil(t, config.Credentials, "credentials should not be nil")
	assert.IsType(
		t,
		databricksSdkConfig.M2mCredentials{},
		config.Credentials,
		"credential strategy should be M2M credentials",
	)
}

func TestConfigureWorkspaceAuth_NoConfig(t *testing.T) {
	// Reset viper config to ensure clean test state
	viper.Reset()

	// Setup mock config
	config := &databricksSdk.Config{}

	// Execute the function under test
	configureWorkspaceAuth(config)

	// Verify result
	assert.Nil(t, config.Credentials, "credential strategy should be nil")
	assert.Empty(t, config.ClientID, "client ID should be empty")
	assert.Empty(t, config.ClientSecret, "client secret should be empty")
	assert.Empty(t, config.Token, "token should be empty")
	assert.False(t, config.AzureUseMSI, "Azure use MSI should be false")
	assert.Empty(t, config.AzureClientID, "Azure client ID should be empty")
	assert.Empty(
		t,
		config.AzureClientSecret,
		"Azure client secret should be empty",
	)
	assert.Empty(t, config.AzureTenantID, "Azure tenant ID should be empty")
	assert.Empty(t, config.AzureResourceID, "Azure resource ID should be empty")
}

func TestConfigureWorkspaceAuth_OAuthClientId(t *testing.T) {
	// Reset viper config to ensure clean test state
	viper.Reset()

	// Set the OAuth client ID
	viper.Set("databricks.oauthClientId", "fake-oauth-client-id")

	// Setup mock config
	config := &databricksSdk.Config{}

	// Execute the function under test
	configureWorkspaceAuth(config)

	// Verify result
	assert.NotNil(t, config.Credentials, "credentials should not be nil")
	assert.IsType(
		t,
		databricksSdkConfig.M2mCredentials{},
		config.Credentials,
		"credential strategy should be M2M credentials",
	)
	assert.Equal(t, "fake-oauth-client-id", config.ClientID)
	assert.Empty(t, config.ClientSecret, "client secret should be empty")
	assert.Empty(t, config.Token, "token should be empty")
	assert.False(t, config.AzureUseMSI, "Azure use MSI should be false")
	assert.Empty(t, config.AzureClientID, "Azure client ID should be empty")
	assert.Empty(
		t,
		config.AzureClientSecret,
		"Azure client secret should be empty",
	)
	assert.Empty(t, config.AzureTenantID, "Azure tenant ID should be empty")
	assert.Empty(t, config.AzureResourceID, "Azure resource ID should be empty")
}

func TestConfigureWorkspaceAuth_OAuthClientIdAndSecret(t *testing.T) {
	// Reset viper config to ensure clean test state
	viper.Reset()

	// Set the OAuth client ID and secret
	viper.Set("databricks.oauthClientId", "fake-oauth-client-id")
	viper.Set("databricks.oauthClientSecret", "fake-oauth-client-secret")

	// Setup mock config
	config := &databricksSdk.Config{}

	// Execute the function under test
	configureWorkspaceAuth(config)

	// Verify result
	assert.NotNil(t, config.Credentials, "credentials should not be nil")
	assert.IsType(
		t,
		databricksSdkConfig.M2mCredentials{},
		config.Credentials,
		"credential strategy should be M2M credentials",
	)
	assert.Equal(t, "fake-oauth-client-id", config.ClientID)
	assert.Equal(t, "fake-oauth-client-secret", config.ClientSecret)
	assert.Empty(t, config.Token, "token should be empty")
	assert.False(t, config.AzureUseMSI, "Azure use MSI should be false")
	assert.Empty(t, config.AzureClientID, "Azure client ID should be empty")
	assert.Empty(
		t,
		config.AzureClientSecret,
		"Azure client secret should be empty",
	)
	assert.Empty(t, config.AzureTenantID, "Azure tenant ID should be empty")
	assert.Empty(t, config.AzureResourceID, "Azure resource ID should be empty")
}

func TestConfigureWorkspaceAuth_AccessToken(t *testing.T) {
	// Reset viper config to ensure clean test state
	viper.Reset()

	// Set the PAT
	viper.Set("databricks.accessToken", "fake-pat")

	// Setup mock config
	config := &databricksSdk.Config{}

	// Execute the function under test
	configureWorkspaceAuth(config)

	// Verify result
	assert.NotNil(t, config.Credentials, "credentials should not be nil")
	assert.IsType(
		t,
		databricksSdkConfig.PatCredentials{},
		config.Credentials,
		"credential strategy should be PAT credentials",
	)
	assert.Empty(t, config.ClientID, "client ID should be empty")
	assert.Empty(t, config.ClientSecret, "client secret should be empty")
	assert.Equal(t, "fake-pat", config.Token)
	assert.False(t, config.AzureUseMSI, "Azure use MSI should be false")
	assert.Empty(t, config.AzureClientID, "Azure client ID should be empty")
	assert.Empty(
		t,
		config.AzureClientSecret,
		"Azure client secret should be empty",
	)
	assert.Empty(t, config.AzureTenantID, "Azure tenant ID should be empty")
	assert.Empty(t, config.AzureResourceID, "Azure resource ID should be empty")
}

func TestConfigureWorkspaceAuth_AzureClientId(t *testing.T) {
	// Reset viper config to ensure clean test state
	viper.Reset()

	// Set the Azure client ID
	viper.Set("databricks.azureClientId", "fake-azure-client-id")

	// Setup mock config
	config := &databricksSdk.Config{}

	// Execute the function under test
	configureWorkspaceAuth(config)

	// Verify result
	assert.NotNil(t, config.Credentials, "credentials should not be nil")
	assert.IsType(
		t,
		databricksSdkConfig.AzureClientSecretCredentials{},
		config.Credentials,
		"credential strategy should be Azure client secret credentials",
	)
	assert.Empty(t, config.ClientID, "client id should be empty")
	assert.Empty(t, config.ClientSecret, "client secret should be empty")
	assert.Empty(t, config.Token, "token should be empty")
	assert.False(t, config.AzureUseMSI, "Azure use MSI should be false")
	assert.Equal(t, "fake-azure-client-id", config.AzureClientID)
	assert.Empty(
		t,
		config.AzureClientSecret,
		"Azure client secret should be empty",
	)
	assert.Empty(t, config.AzureTenantID, "Azure tenant ID should be empty")
	assert.Empty(t, config.AzureResourceID, "Azure resource ID should be empty")
}

func TestConfigureWorkspaceAuth_AzureClientIdAndMsiEnabled(t *testing.T) {
	// Reset viper config to ensure clean test state
	viper.Reset()

	// Set the Azure client ID and MSI enabled
	viper.Set("databricks.azureClientId", "fake-azure-client-id")
	viper.Set("databricks.azureMsiEnabled", "true")

	// Setup mock config
	config := &databricksSdk.Config{}

	// Execute the function under test
	configureWorkspaceAuth(config)

	// Verify result
	assert.NotNil(t, config.Credentials, "credentials should not be nil")
	assert.IsType(
		t,
		databricksSdkConfig.AzureMsiCredentials{},
		config.Credentials,
		"credential strategy should be Azure MSI credentials",
	)
	assert.Empty(t, config.ClientID, "client id should be empty")
	assert.Empty(t, config.ClientSecret, "client secret should be empty")
	assert.Empty(t, config.Token, "token should be empty")
	assert.True(t, config.AzureUseMSI, "Azure use MSI should be true")
	assert.Equal(t, "fake-azure-client-id", config.AzureClientID)
	assert.Empty(
		t,
		config.AzureClientSecret,
		"Azure client secret should be empty",
	)
	assert.Empty(t, config.AzureTenantID, "Azure tenant ID should be empty")
	assert.Empty(t, config.AzureResourceID, "Azure resource ID should be empty")
}

func TestConfigureWorkspaceAuth_AzureClientIdAndTenantId(t *testing.T) {
	// Reset viper config to ensure clean test state
	viper.Reset()

	// Set the Azure client ID and tenant ID
	viper.Set("databricks.azureClientId", "fake-azure-client-id")
	viper.Set("databricks.azureTenantId", "fake-azure-tenant-id")

	// Setup mock config
	config := &databricksSdk.Config{}

	// Execute the function under test
	configureWorkspaceAuth(config)

	// Verify result
	assert.NotNil(t, config.Credentials, "credentials should not be nil")
	assert.IsType(
		t,
		databricksSdkConfig.AzureClientSecretCredentials{},
		config.Credentials,
		"credential strategy should be Azure client secret credentials",
	)
	assert.Empty(t, config.ClientID, "client id should be empty")
	assert.Empty(t, config.ClientSecret, "client secret should be empty")
	assert.Empty(t, config.Token, "token should be empty")
	assert.False(t, config.AzureUseMSI, "Azure use MSI should be false")
	assert.Equal(t, "fake-azure-client-id", config.AzureClientID)
	assert.Empty(
		t,
		config.AzureClientSecret,
		"Azure client secret should be empty",
	)
	assert.Equal(t, "fake-azure-tenant-id", config.AzureTenantID)
	assert.Empty(t, config.AzureResourceID, "Azure resource ID should be empty")
}

func TestConfigureWorkspaceAuth_AzureClientIdAndTenantIdAndClientSecret(
	t *testing.T,
) {
	// Reset viper config to ensure clean test state
	viper.Reset()

	// Set the Azure client ID and tenant ID and client secret
	viper.Set("databricks.azureClientId", "fake-azure-client-id")
	viper.Set("databricks.azureClientSecret", "fake-azure-client-secret")
	viper.Set("databricks.azureTenantId", "fake-azure-tenant-id")

	// Setup mock config
	config := &databricksSdk.Config{}

	// Execute the function under test
	configureWorkspaceAuth(config)

	// Verify result
	assert.NotNil(t, config.Credentials, "credentials should not be nil")
	assert.IsType(
		t,
		databricksSdkConfig.AzureClientSecretCredentials{},
		config.Credentials,
		"credential strategy should be Azure client secret credentials",
	)
	assert.Empty(t, config.ClientID, "client id should be empty")
	assert.Empty(t, config.ClientSecret, "client secret should be empty")
	assert.Empty(t, config.Token, "token should be empty")
	assert.False(t, config.AzureUseMSI, "Azure use MSI should be false")
	assert.Equal(t, "fake-azure-client-id", config.AzureClientID)
	assert.Equal(t, "fake-azure-client-secret", config.AzureClientSecret)
	assert.Equal(t, "fake-azure-tenant-id", config.AzureTenantID)
	assert.Empty(t, config.AzureResourceID, "Azure resource ID should be empty")
}

func TestConfigureWorkspaceAuth_AzureClientIdAndTenantIdAndClientSecretAndResourceId(
	t *testing.T,
) {
	// Reset viper config to ensure clean test state
	viper.Reset()

	// Set the Azure client ID and tenant ID and client secret and resource ID
	viper.Set("databricks.azureClientId", "fake-azure-client-id")
	viper.Set("databricks.azureClientSecret", "fake-azure-client-secret")
	viper.Set("databricks.azureTenantId", "fake-azure-tenant-id")
	viper.Set("databricks.azureResourceId", "fake-azure-resource-id")

	// Setup mock config
	config := &databricksSdk.Config{}

	// Execute the function under test
	configureWorkspaceAuth(config)

	// Verify result
	assert.NotNil(t, config.Credentials, "credentials should not be nil")
	assert.IsType(
		t,
		databricksSdkConfig.AzureClientSecretCredentials{},
		config.Credentials,
		"credential strategy should be Azure client secret credentials",
	)
	assert.Empty(t, config.ClientID, "client id should be empty")
	assert.Empty(t, config.ClientSecret, "client secret should be empty")
	assert.Empty(t, config.Token, "token should be empty")
	assert.False(t, config.AzureUseMSI, "Azure use MSI should be false")
	assert.Equal(t, "fake-azure-client-id", config.AzureClientID)
	assert.Equal(t, "fake-azure-client-secret", config.AzureClientSecret)
	assert.Equal(t, "fake-azure-tenant-id", config.AzureTenantID)
	assert.Equal(t, "fake-azure-resource-id", config.AzureResourceID)
}
