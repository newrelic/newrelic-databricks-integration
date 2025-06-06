package spark

import (
	"net/http"

	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/connectors"
)

// MockHttpAuthenticator implements the HttpAuthenticator interface for testing
type MockHttpAuthenticator struct {
	// AuthenticateFunc allows tests to customize the authentication behavior
	AuthenticateFunc func(
		connector *connectors.HttpConnector,
		req *http.Request,
	) error
}

// Authenticate implements the HttpAuthenticator interface
func (m *MockHttpAuthenticator) Authenticate(
	connector *connectors.HttpConnector,
	req *http.Request,
) error {
	if m.AuthenticateFunc != nil {
		return m.AuthenticateFunc(connector, req)
	}

	// Default implementation - just return nil
	return nil
}
