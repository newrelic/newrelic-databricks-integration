package databricks

import (
	"context"
	"fmt"
	"time"

	databricksSdk "github.com/databricks/databricks-sdk-go"
	databricksSdkConfig "github.com/databricks/databricks-sdk-go/config"

	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration"
	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/exporters"
	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/log"
	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/pipeline"
	"github.com/spf13/viper"
)

func InitPipelines(
	ctx context.Context,
	i *integration.LabsIntegration,
	tags map[string]string,
) error {
	// Create a workspace client
	w, err := getWorkspaceClient()
	if err != nil {
		return err
	}

	// Create the newrelic exporter
	newRelicExporter := exporters.NewNewRelicExporter(
		"newrelic-api",
		i.Name,
		i.Id,
		i.NrClient,
		i.GetLicenseKey(),
		i.GetRegion(),
		i.DryRun,
	)

	collectUsageData := true
	if viper.IsSet("databricks.usage.enabled") {
		collectUsageData = viper.GetBool("databricks.usage.enabled")
	}

	if collectUsageData {
		// Initialize caches
		initInfoByIdCaches(w)

		// We need a sql warehouse ID to run the SQL queries
		warehouseId := viper.GetString("databricks.usage.warehouseId")
		if warehouseId == "" {
			return fmt.Errorf("warehouse ID required for querying usage")
		}

		// Get time of day to run
		runTime := viper.GetString("databricks.usage.runTime")
		if runTime == "" {
			runTime = "02:00:00"
		}

		timeOfDay, err := time.Parse(time.TimeOnly, runTime)
		if err != nil {
			return fmt.Errorf("invalid runTime value \"%s\"", runTime)
		}

		crontab := fmt.Sprintf(
			"TZ=UTC %d %d * * *",
			timeOfDay.Minute(),
			timeOfDay.Hour(),
		)

		includeIdentityMetadata := viper.GetBool(
			"databricks.usage.includeIdentityMetadata",
		)

		queries := []*query{
			&gBillingUsageQuery,
		}

		for i := 0; i < len(gOptionalUsageQueries); i += 1 {
			query := gOptionalUsageQueries[i]
			addQuery := true
			key := "databricks.usage.optionalQueries." + query.id

			if viper.IsSet(key) {
				addQuery = viper.GetBool(key)
			}

			if addQuery {
				queries = append(queries, &query)
			}
		}

		usageReceiver := NewDatabricksQueryReceiver(
			"databricks-usage-receiver",
			w,
			warehouseId,
			"system",
			"billing",
			includeIdentityMetadata,
			queries,
		)

		ep := pipeline.NewEventsPipeline("databricks-usage-pipeline")
		ep.AddReceiver(usageReceiver)
		ep.AddExporter(newRelicExporter)

		lpc := NewDatabricksListPricesCollector(
			i,
			w,
			warehouseId,
		)

		log.Debugf("adding usage components with schedule %s", crontab)

		i.AddSchedule(
			crontab,
			[]integration.Component{
				ep,
				lpc,
			},
		)
	}

	collectJobRunData := true
	if viper.IsSet("databricks.jobs.runs.enabled") {
		collectJobRunData = viper.GetBool("databricks.jobs.runs.enabled")
	}

	if collectJobRunData {
		startOffset := int64(24 * 60 * 60)
		if viper.IsSet("databricks.jobs.runs.startOffset") {
			startOffset = viper.GetInt64("databricks.jobs.runs.startOffset")
		}

		includeRunId := viper.GetBool(
			"databricks.jobs.runs.includeRunId",
		)

		// Create a metrics pipeline
		mp := pipeline.NewMetricsPipeline("databricks-job-run-pipeline")
		mp.AddExporter(newRelicExporter)

		// Create the receiver
		databricksJobsReceiver := NewDatabricksJobRunReceiver(
			i,
			w,
			viper.GetString("databricks.jobs.runs.metricPrefix"),
			time.Duration(startOffset) * time.Second,
			includeRunId,
			tags,
		)
		mp.AddReceiver(databricksJobsReceiver)

		log.Debugf("initializing Databricks job run pipeline")

		i.AddComponent(mp)
	}

	collectPipelineEventLogs := true
	if viper.IsSet("databricks.pipelines.logs.enabled") {
		collectPipelineEventLogs = viper.GetBool(
			"databricks.pipelines.logs.enabled",
		)
	}

	if collectPipelineEventLogs {
		databricksPipelineEventsReceiver :=
			NewDatabricksPipelineEventsReceiver(i, w, tags)

		// Create a logs pipeline for the event logs
		lp := pipeline.NewLogsPipeline(
			"databricks-pipeline-event-logs-pipeline",
		)
		lp.AddReceiver(databricksPipelineEventsReceiver)
		lp.AddExporter(newRelicExporter)

		log.Debugf("initializing Databricks pipeline event logs pipeline")

		i.AddComponent(lp)
	}

	collectPipelineMetrics := true
	if viper.IsSet("databricks.pipelines.metrics.enabled") {
		collectPipelineMetrics = viper.GetBool(
			"databricks.pipelines.metrics.enabled",
		)
	}

	if collectPipelineMetrics {
		startOffset := int64(24 * 60 * 60)
		if viper.IsSet("databricks.pipelines.metrics.startOffset") {
			startOffset = viper.GetInt64(
				"databricks.pipelines.metrics.startOffset",
			)
		}

		intervalOffset := int64(5)
		if viper.IsSet("databricks.pipelines.metrics.intervalOffset") {
			intervalOffset = viper.GetInt64(
				"databricks.pipelines.metrics.intervalOffset",
			)
		}

		databricksPipelineMetricsReceiver, err :=
			NewDatabricksPipelineMetricsReceiver(
				i,
				w,
				viper.GetString("databricks.pipelines.metrics.metricPrefix"),
				time.Duration(startOffset) * time.Second,
				time.Duration(intervalOffset) * time.Second,
				viper.GetBool("databricks.pipelines.metrics.includeUpdateId"),
				tags,
			)
		if err != nil {
			return err
		}

		// Create a metrics pipeline for the pipeline metrics
		mp := pipeline.NewMetricsPipeline(
			"databricks-pipeline-metrics-pipeline",
		)
		mp.AddReceiver(databricksPipelineMetricsReceiver)
		mp.AddExporter(newRelicExporter)

		log.Debugf("initializing Databricks pipeline metrics pipeline")

		i.AddComponent(mp)
	}

	collectQueryMetrics := true
	if viper.IsSet("databricks.queries.metrics.enabled") {
		collectQueryMetrics = viper.GetBool(
			"databricks.queries.metrics.enabled",
		)
	}

	if collectQueryMetrics {
		// Initialize caches so we can lookup warehouse names for queries
		initInfoByIdCaches(w)

		// @todo: at some point, all the checks like this should be converted
		// to use viper.SetDefault(). Then the IsSet() is not necessary. For
		// now, we will leave it as is as we don't gain much by changing. Also,
		// then the difference between using GetInt() (below) and GetInt64()
		// (above) can be changed to use GetInt64() everywhere.
		startOffset := DEFAULT_QUERY_HISTORY_START_OFFSET
		if viper.IsSet("databricks.queries.metrics.startOffset") {
			startOffset = viper.GetInt(
				"databricks.queries.metrics.startOffset",
			)
		}

		intervalOffset := DEFAULT_QUERY_HISTORY_INTERVAL_OFFSET
		if viper.IsSet("databricks.queries.metrics.intervalOffset") {
			intervalOffset = viper.GetInt(
				"databricks.queries.metrics.intervalOffset",
			)
		}

		maxResults := DEFAULT_QUERY_HISTORY_MAX_RESULTS
		if viper.IsSet("databricks.queries.metrics.maxResults") {
			maxResults = viper.GetInt(
				"databricks.queries.metrics.maxResults",
			)
		}

		databricksQueryMetricsReceiver, err :=
			NewDatabricksQueryMetricsReceiver(
				i,
				w,
				time.Duration(startOffset) * time.Second,
				time.Duration(intervalOffset) * time.Second,
				maxResults,
				viper.GetBool(
					"databricks.queries.metrics.includeIdentityMetadata",
				),
				tags,
			)
		if err != nil {
			return err
		}

		// Create an events pipeline for the query metrics
		ep := pipeline.NewEventsPipeline(
			"databricks-query-metrics-pipeline",
		)
		ep.AddReceiver(databricksQueryMetricsReceiver)
		ep.AddExporter(newRelicExporter)

		log.Debugf("initializing Databricks query metrics pipeline")

		i.AddComponent(ep)
	}

	return nil
}

func getWorkspaceClient() (*databricksSdk.WorkspaceClient, error) {
	// Databricks config
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
	err := configureAuth(databricksConfig)
	if err != nil {
		return nil, err
	}

	// Create the workspace client
	w, err := databricksSdk.NewWorkspaceClient(databricksConfig)
	if err != nil {
		return nil, err
	}

	return w, nil
}

func configureAuth(config *databricksSdk.Config) error {
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

		return nil
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

		return nil
	}

	/*
	 * At this point, it's up to the user to specify authentication via an
	 * SDK-supported mechanism. This does not preclude the user from using OAuth
	 * M2M authentication or PAT authentication. The user can still use these
	 * authentication types via SDK-supported mechanisms or any other
	 * SDK-supported authentication types via the corresponding SDK-supported
	 * mechanisms.
	 */

	return nil
}
