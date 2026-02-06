package spark

import (
	"io"
	"strings"

	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/connectors"
)

type mockHttpGetConnector struct {
	url string
	authenticator connectors.HttpAuthenticator
	headers map[string]string
	response string
	requestError error
}

func newMockHttpGetConnector(
	url string,
	response string,
) *mockHttpGetConnector {
	return &mockHttpGetConnector{ url: url, response: response }
}

func newMockHttpGetConnectorWithRequestError(
	url string,
	response string,
	requestError error,
) *mockHttpGetConnector {
	return &mockHttpGetConnector{
		url: url,
		response: response,
		requestError: requestError,
	}
}

func (c *mockHttpGetConnector) SetAuthenticator(
	authenticator connectors.HttpAuthenticator,
) {
	c.authenticator = authenticator
}

func (c *mockHttpGetConnector) SetHeaders(headers map[string]string) {
	c.headers = headers
}

func (c *mockHttpGetConnector) Request() (io.ReadCloser, error) {
	if c.requestError != nil {
		return nil, c.requestError
	}

	return io.NopCloser(strings.NewReader(c.response)), nil
}
