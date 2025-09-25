package databricks

import (
	"time"
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
