package spark

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
