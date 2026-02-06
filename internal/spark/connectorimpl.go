package spark

import (
	"io"

	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/connectors"
)

type httpGetConnectorImpl struct {
	connector *connectors.HttpConnector
}

func newHttpGetConnectorImpl(url string) httpGetConnector {
	return &httpGetConnectorImpl{
		connectors.NewHttpGetConnector(url),
	}
}

func (c *httpGetConnectorImpl) SetAuthenticator(
	authenticator connectors.HttpAuthenticator,
) {
	c.connector.SetAuthenticator(authenticator)
}

func (c *httpGetConnectorImpl) SetHeaders(headers map[string]string) {
	c.connector.SetHeaders(headers)
}

func (c *httpGetConnectorImpl) Request() (io.ReadCloser, error) {
	return c.connector.Request()
}
