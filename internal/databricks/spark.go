package databricks

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"time"

	databricksSdk "github.com/databricks/databricks-sdk-go"
	databricksSdkCompute "github.com/databricks/databricks-sdk-go/service/compute"
	"github.com/newrelic/newrelic-databricks-integration/internal/spark"
	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration"
	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/log"

	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/model"
	"github.com/spf13/viper"
)

type DatabricksSparkReceiver struct {
	w			*databricksSdk.WorkspaceClient
	uiEnabled			bool
	apiEnabled			bool
	jobEnabled			bool
	pipelineEnabled		bool
	pollClusterTimeout	time.Duration
	pollClusterTasks	int
	tags		map[string]string
}

func NewDatabricksSparkReceiver(
	i *integration.LabsIntegration,
	w *databricksSdk.WorkspaceClient,
	tags map[string]string,
) *DatabricksSparkReceiver {
	viper.SetDefault("databricks.spark.clusterSources.ui", true)
	viper.SetDefault("databricks.spark.clusterSources.api", true)
	viper.SetDefault("databricks.spark.clusterSources.job", true)
	viper.SetDefault("databricks.spark.clusterSources.pipeline", true)
	// the `interval` config parameter is mistakenly cast directly to a
	// time.Duration with no units in the SDK so here we need to cast it back to
	// match the expected configuration type (an integer representing number of
	// seconds)
	viper.SetDefault("databricks.spark.pollClusterTimeout", int64(i.Interval))
	viper.SetDefault("databricks.spark.pollClusterTasks", 5)

	uiEnabled := viper.GetBool("databricks.spark.clusterSources.ui")
	if viper.IsSet("databricks.sparkClusterSources.ui") {
		uiEnabled = viper.GetBool("databricks.sparkClusterSources.ui")
	}

	apiEnabled := viper.GetBool("databricks.spark.clusterSources.api")
	if viper.IsSet("databricks.sparkClusterSources.api") {
		apiEnabled = viper.GetBool("databricks.sparkClusterSources.api")
	}

	jobEnabled := viper.GetBool("databricks.spark.clusterSources.job")
	if viper.IsSet("databricks.sparkClusterSources.job") {
		jobEnabled = viper.GetBool("databricks.sparkClusterSources.job")
	}

	pipelineEnabled := viper.GetBool("databricks.spark.clusterSources.pipeline")
	if viper.IsSet("databricks.sparkClusterSources.pipeline") {
		pipelineEnabled =
			viper.GetBool("databricks.sparkClusterSources.pipeline")
	}

	pollClusterTimeout := time.Duration(viper.GetInt64(
		"databricks.spark.pollClusterTimeout",
	)) * time.Second
	pollClusterTasks := viper.GetInt("databricks.spark.pollClusterTasks")

	return &DatabricksSparkReceiver{
		w,
		uiEnabled,
		apiEnabled,
		jobEnabled,
		pipelineEnabled,
		pollClusterTimeout,
		pollClusterTasks,
		tags,
	 }
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
	all := d.w.Clusters.List(
		ctx,
		databricksSdkCompute.ListClustersRequest{},
	)

	clusters := []*databricksSdkCompute.ClusterDetails{}

	for ; all.HasNext(ctx); {
		c, err := all.Next(ctx)
		if err != nil {
			return fmt.Errorf(
				"error while retrieving cluster results: %w",
				err,
			)
		}

		if !isSupportedClusterSource(c.ClusterSource) {
			log.Debugf(
				"skipping cluster %s (%s) because cluster source %s is unsupported",
				c.ClusterName,
				c.ClusterId,
				c.ClusterSource,
			)
			continue
		}

		if !d.isClusterSourceEnabled(c.ClusterSource) {
			log.Debugf(
				"skipping cluster %s (%s) because cluster source %s is disabled",
				c.ClusterName,
				c.ClusterId,
				c.ClusterSource,
			)
			continue
		}

		if c.State != databricksSdkCompute.StateRunning {
			log.Debugf(
				"skipping cluster %s (%s) with source %s because it is not running",
				c.ClusterName,
				c.ClusterId,
				c.ClusterSource,
			)
			continue
		}

		log.Debugf(
			"adding cluster %s (%s) with source %s to cluster list",
			c.ClusterName,
			c.ClusterId,
			c.ClusterSource,
		)

		clusters = append(clusters, &c)
	}

	start := 0
	count := len(clusters)

	for ;start < count; {
		end := min(start + d.pollClusterTasks, count)

		err := pollClusters(
			ctx,
			d.w,
			d.pollClusterTimeout,
			clusters[start:end],
			d.tags,
			writer,
		)
		if err != nil {
			return err
		}

		start = end
	}

	return nil
}

func (d *DatabricksSparkReceiver) isClusterSourceEnabled(
	source databricksSdkCompute.ClusterSource,
) bool {
	return (source == databricksSdkCompute.ClusterSourceUi && d.uiEnabled) ||
		(
			source == databricksSdkCompute.ClusterSourceApi &&
			d.apiEnabled ) ||
		(
			source == databricksSdkCompute.ClusterSourceJob &&
			d.jobEnabled ) ||
		(
			(source == databricksSdkCompute.ClusterSourcePipeline ||
				source == databricksSdkCompute.ClusterSourcePipelineMaintenance) &&
			d.pipelineEnabled )
}

func isSupportedClusterSource(source databricksSdkCompute.ClusterSource) bool {
	return source == databricksSdkCompute.ClusterSourceUi ||
		source == databricksSdkCompute.ClusterSourceApi ||
		source == databricksSdkCompute.ClusterSourceJob ||
		source == databricksSdkCompute.ClusterSourcePipeline ||
		source == databricksSdkCompute.ClusterSourcePipelineMaintenance
}

func pollClusters(
	ctx context.Context,
	w *databricksSdk.WorkspaceClient,
	pollClusterTimeout time.Duration,
	clusters []*databricksSdkCompute.ClusterDetails,
	tags map[string]string,
	writer chan <- model.Metric,
) error {
	errChan := make(chan error)
	count := len(clusters)
	resultCount := 0
	childCtx, cancel := context.WithCancel(ctx)

	log.Debugf("processing %d clusters", count)

	for _, cluster := range clusters {
		go pollCluster(
			childCtx,
			w,
			cluster,
			errChan,
			tags,
			writer,
		)
	}

	errs := []error{}
	ticker := time.NewTicker(pollClusterTimeout)
	defer ticker.Stop()

	for {
		select {
		case err := <- errChan:
			// OK to append nil if result is nil because errors.Join() will
			// discard them
			errs = append(errs, err)

			resultCount += 1

			if resultCount == count {
				// all poll goroutines have completed, call cancel to prevent
				// child context leak
				cancel()

				// if all errors are nil, this returns nil, else an error which
				// is the behavior we want
				return errors.Join(errs...)
			}

		case <-ticker.C:
			log.Debugf("poll cluster timeout ticked; calling cancel func")

			cancel()

			return fmt.Errorf(
				"polling clusters failed to complete in a timely manner",
			)

		case <-ctx.Done():
			// cancel received on parent context, call cancel to prevent child
			// context leak
			cancel()
			return nil
		}
	}
}

func pollCluster(
	ctx context.Context,
	w *databricksSdk.WorkspaceClient,
	c *databricksSdkCompute.ClusterDetails,
	errChan chan error,
	tags map[string]string,
	writer chan <- model.Metric,
) {
	log.Debugf(
		"processing cluster %s (%s) with source %s",
		c.ClusterName,
		c.ClusterId,
		c.ClusterSource,
	)

	sparkContextUiPath, err := getSparkContextUiPathForCluster(
		ctx,
		w,
		c,
	)
	if err != nil {
		errChan <- err
		return
	}

	databricksSparkApiClient, err := NewDatabricksSparkApiClient(
		sparkContextUiPath,
		w,
	)
	if err != nil {
		errChan <- err
		return
	}

	// Initialize spark pipelines
	log.Debugf(
		"polling metrics for cluster %s (%s) with spark context UI URL %s",
		c.ClusterName,
		c.ClusterId,
		sparkContextUiPath,
	)

	newTags := maps.Clone(tags)
	newTags["clusterProvider"] = "databricks"
	newTags["databricksClusterId"] = c.ClusterId
	newTags["databricksClusterName"] = c.ClusterName
	newTags["databricksClusterSource"] = string(c.ClusterSource)

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
		errChan <- err
		return
	}

	errChan <- nil
}
