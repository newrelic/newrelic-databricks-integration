package main

import (
	"context"

	"github.com/newrelic/newrelic-databricks-integration/internal/databricks"
	"github.com/newrelic/newrelic-databricks-integration/internal/spark"
	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration"
	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/log"
	"github.com/spf13/viper"
)

const (
	INTEGRATION_ID = "com.newrelic.labs.newrelic-databricks-integration"
	INTEGRATION_NAME = "New Relic Databricks Integration"
)

func main() {
	// Create a new background context to use
	ctx := context.Background()

	// Create the integration with options
	i, err := integration.NewStandaloneIntegration(
		INTEGRATION_NAME,
		INTEGRATION_ID,
		INTEGRATION_NAME,
		integration.WithInterval(60),
		integration.WithLicenseKey(),
		integration.WithApiKey(),
		integration.WithEvents(ctx),
		integration.WithLastUpdate(),
		integration.WithLogs(ctx),
	)
	fatalIfErr(err)

	tags := viper.GetStringMapString("tags")

	if viper.IsSet("databricks") {
		err = databricks.InitPipelines(ctx, i, tags)
		fatalIfErr(err)
	}

	if viper.IsSet("spark") {
		err = spark.InitPipelines(ctx, i, tags)
		fatalIfErr(err)
	}

	// Run the integration
	defer i.Shutdown(ctx)
 	err = i.Run(ctx)
	fatalIfErr(err)
}

func fatalIfErr(err error) {
	if err != nil {
		log.Fatalf(err)
	}
}
