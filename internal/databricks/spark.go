package databricks

import (
	"context"
	"fmt"
	"maps"
	"strings"

	databricksSdk "github.com/databricks/databricks-sdk-go"
	databricksSdkCompute "github.com/databricks/databricks-sdk-go/service/compute"
	"github.com/newrelic/newrelic-databricks-integration/internal/spark"
	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/log"

	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/model"
	"github.com/spf13/viper"
)

type DatabricksSparkReceiver struct {
	w			*databricksSdk.WorkspaceClient
	tags		map[string]string
}

func NewDatabricksSparkReceiver(
	w *databricksSdk.WorkspaceClient,
	tags map[string]string,
) *DatabricksSparkReceiver {
	return &DatabricksSparkReceiver{ w, tags }
}

func (d *DatabricksSparkReceiver) GetId() string {
	return "databricks-spark-receiver"
}

func (d *DatabricksSparkReceiver) PollMetrics(
	ctx context.Context,
	writer chan <- model.Metric,
) error {
	log.Debugf("polling for Spark metrics")

	log.Debugf("listing all clusters")
	all, err := d.w.Clusters.ListAll(
		ctx,
		databricksSdkCompute.ListClustersRequest{},
	)
	if err != nil {
	  return fmt.Errorf("failed to list clusters: %w", err)
	}

	uiEnabled := true
	if viper.IsSet("databricks.sparkClusterSources.ui") {
		uiEnabled = viper.GetBool("databricks.sparkClusterSources.ui")
	} else if viper.IsSet("databricks.spark.clusterSources.ui")  {
		uiEnabled = viper.GetBool("databricks.spark.clusterSources.ui")
	}

	jobEnabled := true
	if viper.IsSet("databricks.sparkClusterSources.job") {
		jobEnabled = viper.GetBool("databricks.sparkClusterSources.job")
	} else if viper.IsSet("databricks.spark.clusterSources.job")  {
		jobEnabled = viper.GetBool("databricks.spark.clusterSources.job")
	}

	apiEnabled := true
	if viper.IsSet("databricks.sparkClusterSources.api") {
		apiEnabled = viper.GetBool("databricks.sparkClusterSources.api")
	} else if viper.IsSet("databricks.spark.clusterSources.api")  {
		apiEnabled = viper.GetBool("databricks.spark.clusterSources.api")
	}

	for _, c := range all {
		if c.ClusterSource != databricksSdkCompute.ClusterSourceUi &&
			c.ClusterSource != databricksSdkCompute.ClusterSourceApi &&
			c.ClusterSource != databricksSdkCompute.ClusterSourceJob {
			log.Debugf(
				"skipping cluster %s because cluster source %s is unsupported",
				c.ClusterName,
				c.ClusterSource,
			)
			continue
		}

		if (
			c.ClusterSource == databricksSdkCompute.ClusterSourceUi &&
			!uiEnabled ) ||
			(
				c.ClusterSource == databricksSdkCompute.ClusterSourceApi &&
				!apiEnabled ) ||
			(
				c.ClusterSource == databricksSdkCompute.ClusterSourceJob &&
				!jobEnabled ) {
			log.Debugf(
				"skipping cluster %s because cluster source %s is disabled",
				c.ClusterName,
				c.ClusterSource,
			)
			continue
		}

		if c.State != databricksSdkCompute.StateRunning {
			log.Debugf(
				"skipping cluster %s with source %s because it is not running",
				c.ClusterName,
				c.ClusterSource,
			)
			continue
		}

		/// resolve the spark context UI URL for the cluster
		log.Debugf(
			"resolving Spark context UI URL for cluster %s with source %s",
			c.ClusterName,
			c.ClusterSource,
		)
		sparkContextUiPath, err := getSparkContextUiPathForCluster(
			ctx,
			d.w,
			&c,
		)
		if err != nil {
			return err
		}

		databricksSparkApiClient, err := NewDatabricksSparkApiClient(
			sparkContextUiPath,
			d.w,
		)
		if err != nil {
			return err
		}

		// Initialize spark pipelines
		log.Debugf(
			"polling metrics for cluster %s with spark context UI URL %s",
			c.ClusterName,
			sparkContextUiPath,
		)

		newTags := maps.Clone(d.tags)
		newTags["clusterProvider"] = "databricks"
		newTags["databricksClusterId"] = c.ClusterId
		newTags["databricksClusterName"] = c.ClusterName

		metricPrefix := ""
		if viper.IsSet("databricks.sparkMetricPrefix") {
			metricPrefix = viper.GetString("databricks.sparkMetricPrefix")
		} else if viper.IsSet("databricks.spark.metricPrefix") {
			metricPrefix = viper.GetString("databricks.spark.metricPrefix")
		}

		err = spark.PollMetrics(
			ctx,
			databricksSparkApiClient,
			metricPrefix,
			newTags,
			writer,
		)
		if err != nil {
			return err
		}
	}

	return nil
}

func getSparkContextUiPathForCluster(
	ctx context.Context,
	w *databricksSdk.WorkspaceClient,
	c *databricksSdkCompute.ClusterDetails,
) (string, error) {
	// @see https://databrickslabs.github.io/overwatch/assets/_index/realtime_helpers.html

	clusterId := c.ClusterId

	log.Debugf("creating execution context for cluster %s", clusterId)
	waitContextStatus, err := w.CommandExecution.Create(
		ctx,
		databricksSdkCompute.CreateContext{
			ClusterId: clusterId,
			Language: databricksSdkCompute.LanguagePython,
		},
	)
	if err != nil {
		return "", err
	}

	execContext, err := waitContextStatus.Get()
	if err != nil {
		return "", err
	}

	cmd := databricksSdkCompute.Command{
		ClusterId: clusterId,
		Command: `
		print(f'{spark.conf.get("spark.databricks.clusterUsageTags.clusterOwnerOrgId")}')
		print(f'{spark.conf.get("spark.ui.port")}')
		`,
		ContextId: execContext.Id,
		Language: databricksSdkCompute.LanguagePython,
	}

	log.Debugf(
		"executing context UI discovery script for cluster %s",
		clusterId,
	)
	waitCommandStatus, err := w.CommandExecution.Execute(
		ctx,
		cmd,
	)
	if err != nil {
		return "", err
	}

	resp, err := waitCommandStatus.Get()
	if err != nil {
		return "", err
	}

	data, ok := resp.Results.Data.(string);
	if !ok {
		return "", fmt.Errorf("command result is not a string value")
	}

	vals := strings.Split(data, "\n")
	if len(vals) != 2 {
		return "", fmt.Errorf("invalid command result")
	}

	if vals[0] == "" || vals[1] == "" {
		return "", fmt.Errorf("empty command results")
	}

	// @TODO: I think this URL pattern only works for multi-tenant accounts.
	// We may need a flag for single tenant accounts and use the o/0 form
	// shown on the overwatch site.

	url := fmt.Sprintf(
		"/driver-proxy-api/o/%s/%s/%s",
		vals[0],
		clusterId,
		vals[1],
	)

	return url, nil
}
