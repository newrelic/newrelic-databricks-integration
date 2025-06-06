package spark

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/newrelic/newrelic-databricks-integration/internal/databricks"
	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration"
	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/model"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

func TestGetClusterManager_DefaultConfig(t *testing.T) {
	// Reset viper config to ensure clean test state
	viper.Reset()

	// Execute the function under test
	clusterManager, err := getClusterManager()

	// Verify results
	assert.NoError(t, err)
	assert.Equal(t, ClusterManagerTypeStandalone, clusterManager)
}

func TestGetClusterManager_DatabricksConfig(t *testing.T) {
	// Reset viper config to ensure clean test state
	viper.Reset()

	// Set the cluster manager type to databricks
	viper.Set("spark.clusterManager", "databricks")

	// Execute the function under test
	clusterManager, err := getClusterManager()

	// Verify results
	assert.NoError(t, err)
	assert.IsType(
		t,
		ClusterManagerType("databricks"),
		clusterManager,
		"Should return a ClusterManagerType",
	)
	assert.Equal(t, ClusterManagerTypeDatabricks, clusterManager)
}

func TestGetClusterManager_InvalidType(t *testing.T) {
	// Reset viper config to ensure clean test state
	viper.Reset()

	// Set the cluster manager type to an invalid value
	viper.Set("spark.clusterManager", "invalid_manager")

	// Execute the function under test
	clusterManager, err := getClusterManager()

	// Verify results
	assert.Error(t, err)
	assert.IsType(
		t,
		ClusterManagerType(""),
		clusterManager,
		"Should return a ClusterManagerType",
	)
	assert.Equal(t, ClusterManagerType(""), clusterManager)
	assert.Equal(t, "invalid clusterManager type", err.Error())
}

func TestGetClusterManager_EmptyString(t *testing.T) {
	// Reset viper config to ensure clean test state
	viper.Reset()

	// Set the cluster manager type explicitly to an empty string
	viper.Set("spark.clusterManager", "")

	// Execute the function under test
	clusterManager, err := getClusterManager()

	// Verify results
	assert.Error(t, err)
	assert.IsType(
		t,
		ClusterManagerType(""),
		clusterManager,
		"Should return a ClusterManagerType",
	)
	assert.Equal(t, ClusterManagerType(""), clusterManager)
	assert.Equal(t, "invalid clusterManager type", err.Error())
}

func TestGetClusterManager_CaseInsensitive(t *testing.T) {
	// Reset viper config to ensure clean test state
	viper.Reset()

	// Set the cluster manager type to "DATABRICKS" (uppercase)
	viper.Set("spark.clusterManager", "DATABRICKS")

	// Execute the function under test
	clusterManager, err := getClusterManager()

	// Verify results
	assert.NoError(t, err)
	assert.IsType(
		t,
		ClusterManagerType("databricks"),
		clusterManager,
		"Should return a ClusterManagerType",
	)
	assert.Equal(t, ClusterManagerTypeDatabricks, clusterManager)
}

func TestNewSparkMetricsReceiver_ValidParams(t *testing.T) {
	// Reset viper config to ensure clean test state
	viper.Reset()

	// Set the cluster manager type to databricks
	viper.Set("spark.clusterManager", "databricks")

	// Setup up mock workspace
	_, teardown := setupMockWorkspace()
	defer teardown()

	// Setup mock integration
	mockIntegration := &integration.LabsIntegration{
		Interval: 60,
	}

	// Setup mock Spark API client
	mockClient := &sparkApiClientImpl{}

	// Setup mock tag map
	tags := map[string]string{"env": "test"}

	// Setup metric prefix
	metricPrefix := "test.prefix."

	// Execute the function under test
	receiver, err := newSparkMetricsReceiver(
		context.Background(),
		mockIntegration,
		mockClient,
		metricPrefix,
		tags,
	)

	// Verify results
	assert.NoError(t, err)
	assert.NotNil(t, receiver)

	sparkReceiver, ok := receiver.(*SparkMetricsReceiver)
	assert.True(t, ok, "Should return a *SparkMetricsReceiver")
	assert.Equal(t, mockIntegration, sparkReceiver.i)
	assert.Equal(t, mockClient, sparkReceiver.client)
	assert.Equal(t, metricPrefix, sparkReceiver.metricPrefix)
	assert.Equal(t, ClusterManagerTypeDatabricks, sparkReceiver.clusterManager)
	assert.NotNil(t, sparkReceiver.metricDecorator)
	assert.IsType(
		t,
		&DatabricksMetricDecorator{},
		sparkReceiver.metricDecorator,
		"Should return a DatabricksMetricDecorator",
	)
	assert.Equal(t, tags, sparkReceiver.tags)
}

func TestNewSparkMetricsReceiver_ClusterManagerError(t *testing.T) {
	// Reset viper config to ensure clean test state
	viper.Reset()

	// Set the cluster manager type to an invalid value that will cause error
	viper.Set("spark.clusterManager", "invalid_manager")

	// Setup up mock workspace
	_, teardown := setupMockWorkspace()
	defer teardown()

	// Setup mock integration
	mockIntegration := &integration.LabsIntegration{
		Interval: 60,
	}

	// Setup mock Spark API client
	mockClient := &sparkApiClientImpl{}

	// Setup mock tag map
	tags := map[string]string{"env": "test"}

	// Setup metric prefix
	metricPrefix := "test.prefix."

	// Execute the function under test
	receiver, err := newSparkMetricsReceiver(
		context.Background(),
		mockIntegration,
		mockClient,
		metricPrefix,
		tags,
	)

	// Verify results
	assert.Error(t, err)
	assert.Nil(t, receiver)
	assert.Equal(t, "invalid clusterManager type", err.Error())
}

func TestNewSparkMetricsReceiver_NewDatabricksWorkspaceError(t *testing.T) {
	// Reset viper config to ensure clean test state
	viper.Reset()

	// Set the cluster manager type to databricks
	viper.Set("spark.clusterManager", "databricks")

	// Setup expected error message
	expectedError := "error creating Databricks workspace"

	// Save original NewDatabricksWorkspace function and setup a defer function
	// to restore after the test
	originalNewDatabricksWorkspace := databricks.NewDatabricksWorkspace
	defer func() {
		databricks.NewDatabricksWorkspace = originalNewDatabricksWorkspace
	}()

	// Setup NewDatabricksWorkspace to return an error
	databricks.NewDatabricksWorkspace = func() (
		databricks.DatabricksWorkspace,
		error,
	) {
		return nil, errors.New(expectedError)
	}

	// Setup mock integration
	mockIntegration := &integration.LabsIntegration{
		Interval: 60,
	}

	// Setup mock Spark API client
	mockClient := &sparkApiClientImpl{}

	// Setup mock tag map
	tags := map[string]string{"env": "test"}

	// Setup metric prefix
	metricPrefix := "test.prefix."

	// Execute the function under test
	receiver, err := newSparkMetricsReceiver(
		context.Background(),
		mockIntegration,
		mockClient,
		metricPrefix,
		tags,
	)

	// Verify results
	assert.Error(t, err)
	assert.Nil(t, receiver)
	assert.Equal(t, expectedError, err.Error())
}

func TestNewSparkMetricsReceiver_StandaloneClusterManager(t *testing.T) {
	// Reset viper config to ensure clean test state
	viper.Reset()

	// Set the cluster manager type to standalone
	viper.Set("spark.clusterManager", "standalone")

	// Setup up mock workspace
	_, teardown := setupMockWorkspace()
	defer teardown()

	// Setup mock integration
	mockIntegration := &integration.LabsIntegration{
		Interval: 60,
	}

	// Setup mock Spark API client
	mockClient := &sparkApiClientImpl{}

	// Setup mock tag map
	tags := map[string]string{"env": "test"}

	// Setup metric prefix
	metricPrefix := "test.prefix."

	// Execute the function under test
	receiver, err := newSparkMetricsReceiver(
		context.Background(),
		mockIntegration,
		mockClient,
		metricPrefix,
		tags,
	)

	// Verify results
	assert.NoError(t, err)
	assert.NotNil(t, receiver)

	sparkReceiver, ok := receiver.(*SparkMetricsReceiver)
	assert.True(t, ok, "Should return a *SparkMetricsReceiver")
	assert.Equal(t, mockIntegration, sparkReceiver.i)
	assert.Equal(t, mockClient, sparkReceiver.client)
	assert.Equal(t, metricPrefix, sparkReceiver.metricPrefix)
	assert.Equal(t, ClusterManagerTypeStandalone, sparkReceiver.clusterManager)
	assert.Nil(t, sparkReceiver.metricDecorator)
	assert.Equal(t, tags, sparkReceiver.tags)
}

func TestNewSparkMetricsReceiver_CustomTags(t *testing.T) {
	// Reset viper config to ensure clean test state
	viper.Reset()

	// Set the cluster manager type to standalone
	viper.Set("spark.clusterManager", "standalone")

	// Setup up mock workspace
	_, teardown := setupMockWorkspace()
	defer teardown()

	// Setup mock integration
	mockIntegration := &integration.LabsIntegration{
		Interval: 60,
	}

	// Setup mock Spark API client
	mockClient := &sparkApiClientImpl{}

	// Setup mock tag map
	tags := map[string]string{
		"environment": "production",
		"region":      "us-west",
		"datacenter":  "dc1",
		"team":        "platform",
	}

	// Setup metric prefix
	metricPrefix := "test.prefix."

	// Execute the function under test
	receiver, err := newSparkMetricsReceiver(
		context.Background(),
		mockIntegration,
		mockClient,
		metricPrefix,
		tags,
	)

	// Verify results
	assert.NoError(t, err)
	assert.NotNil(t, receiver)

	sparkReceiver, ok := receiver.(*SparkMetricsReceiver)
	assert.True(t, ok, "Should return a *SparkMetricsReceiver")
	assert.Equal(t, mockIntegration, sparkReceiver.i)
	assert.Equal(t, mockClient, sparkReceiver.client)
	assert.Equal(t, metricPrefix, sparkReceiver.metricPrefix)
	assert.Equal(t, ClusterManagerTypeStandalone, sparkReceiver.clusterManager)
	assert.Nil(t, sparkReceiver.metricDecorator)
	assert.Equal(t, tags, sparkReceiver.tags)
	assert.Equal(t, 4, len(sparkReceiver.tags), "Should have exactly 4 tags")
	assert.Equal(t, "production", sparkReceiver.tags["environment"])
	assert.Equal(t, "us-west", sparkReceiver.tags["region"])
	assert.Equal(t, "dc1", sparkReceiver.tags["datacenter"])
	assert.Equal(t, "platform", sparkReceiver.tags["team"])
}

func TestSparkMetricsReceiver_GetId(t *testing.T) {
	// Setup mock receiver instance
	// It doesn't matter what values we use here since the GetId method doesn't
	// rely on these values.
	receiver := &SparkMetricsReceiver{
		i:              &integration.LabsIntegration{},
		client:         &sparkApiClientImpl{},
		metricPrefix:   "test.prefix.",
		clusterManager: ClusterManagerTypeStandalone,
		tags:           map[string]string{"env": "test"},
	}

	// Execute the function under test
	id := receiver.GetId()

	// Verify result
	assert.Equal(t, "spark-metrics-receiver", id)
}

func TestPollMetrics_GetApplicationsError(t *testing.T) {
	// Setup mock Spark API client
	mockClient := &MockSparkApiClient{}

	// Track if GetApplications was called
	getApplicationsCalled := false

	// Setup expected error message
	expectedError := "error getting Spark applications"

	// Set up the mock client to return an error when GetApplications is called
	mockClient.GetApplicationsFunc = func(
		ctx context.Context,
	) ([]SparkApplication, error) {
		getApplicationsCalled = true

		return nil, errors.New(expectedError)
	}

	// Setup a metrics channel to receive metrics
	metricsChan := make(chan model.Metric, 100) // Buffer to prevent blocking

	// Create the metrics receiver instance
	receiver := &SparkMetricsReceiver{
		nil,
		mockClient,
		"spark.",
		ClusterManagerTypeStandalone,
		nil,
		map[string]string{},
	}

	// Execute the function under test
	err := receiver.PollMetrics(context.Background(), metricsChan)

	// Close the channel to iterate through it
	close(metricsChan)

	// Collect all metrics to make sure the channel is empty
	metrics := make([]model.Metric, 0)
	for metric := range metricsChan {
		metrics = append(metrics, metric)
	}

	// Verify result
	assert.Error(t, err)
	assert.Equal(t, expectedError, err.Error())
	assert.True(t, getApplicationsCalled, "GetApplications should be called")
	assert.Empty(t, metrics, "No metrics should be received")
}

func TestPollMetrics_CollectExecutorMetricsError(t *testing.T) {
	// Setup mock Spark API client
	mockClient := &MockSparkApiClient{}

	// Setup mock SparkApplications
	sparkApps := []SparkApplication{
		{
			Id:   "app-123456",
			Name: "Test Spark App",
		},
	}

	// Setup expected error message
	expectedError := "error getting Spark executor metrics"

	// Setup function call trackers
	getApplicationsCalled := false
	getExecutorsCalled := false

	// Setup the client to return our mock applications and set the tracker
	mockClient.GetApplicationsFunc = func(
		ctx context.Context,
	) ([]SparkApplication, error) {
		getApplicationsCalled = true
		return sparkApps, nil
	}

	// Setup the client to return our verify the app, and set the tracker, and
	// return an error
	mockClient.GetApplicationExecutorsFunc = func(
		ctx context.Context,
		app *SparkApplication,
	) ([]SparkExecutor, error) {
		assert.Equal(
			t,
			&sparkApps[0],
			app,
			"Should pass the correct application",
		)
		getExecutorsCalled = true

		return nil, errors.New(expectedError)
	}

	// Setup mock tag map
	tags := map[string]string{
		"environment": "test",
		"cluster":     "spark-test",
	}

	// Setup metrics channel to receive metrics
	metricsChan := make(chan model.Metric, 500) // Buffer to prevent blocking

	// Create metrics receiver instance using the mock client and tags
	receiver := &SparkMetricsReceiver{
		nil,
		mockClient,
		"spark.",
		ClusterManagerTypeStandalone,
		nil,
		tags,
	}

	// Execute the function under test
	err := receiver.PollMetrics(context.Background(), metricsChan)

	// Close the channel
	close(metricsChan)

	// Verify results
	assert.Error(t, err)
    assert.Equal(t, expectedError, err.Error())
    assert.True(t, getApplicationsCalled, "GetApplications should be called")
    assert.True(t, getExecutorsCalled, "GetApplicationExecutors should be called")
}

func TestPollMetrics_CollectJobMetricsError(t *testing.T) {
	// Setup mock Spark API client
	mockClient := &MockSparkApiClient{}

	// Setup mock SparkApplications
	sparkApps := []SparkApplication{
		{
			Id:   "app-123456",
			Name: "Test Spark App",
		},
	}

	// Setup expected error message
	expectedError := "error getting Spark job metrics"

	// Setup function call trackers
	getApplicationsCalled := false
	getJobsCalled := false

	// Setup the client to return our mock applications and set the tracker
	mockClient.GetApplicationsFunc = func(
		ctx context.Context,
	) ([]SparkApplication, error) {
		getApplicationsCalled = true
		return sparkApps, nil
	}

	// Setup the client to return our verify the app, and set the tracker, and
	// return an error
	mockClient.GetApplicationJobsFunc = func(
		ctx context.Context,
		app *SparkApplication,
	) ([]SparkJob, error) {
		assert.Equal(
			t,
			&sparkApps[0],
			app,
			"Should pass the correct application",
		)
		getJobsCalled = true

		return nil, errors.New(expectedError)
	}

	// Setup mock tag map
	tags := map[string]string{
		"environment": "test",
		"cluster":     "spark-test",
	}

	// Setup metrics channel to receive metrics
	metricsChan := make(chan model.Metric, 500) // Buffer to prevent blocking

	// Create metrics receiver instance using the mock client and tags
	receiver := &SparkMetricsReceiver{
		nil,
		mockClient,
		"spark.",
		ClusterManagerTypeStandalone,
		nil,
		tags,
	}

	// Execute the function under test
	err := receiver.PollMetrics(context.Background(), metricsChan)

	// Close the channel
	close(metricsChan)

	// Verify results
	assert.Error(t, err)
    assert.Equal(t, expectedError, err.Error())
    assert.True(t, getApplicationsCalled, "GetApplications should be called")
    assert.True(t, getJobsCalled, "GetApplicationJobs should be called")
}

func TestPollMetrics_CollectStageMetricsError(t *testing.T) {
	// Setup mock Spark API client
	mockClient := &MockSparkApiClient{}

	// Setup mock SparkApplications
	sparkApps := []SparkApplication{
		{
			Id:   "app-123456",
			Name: "Test Spark App",
		},
	}

	// Setup expected error message
	expectedError := "error getting Spark stage metrics"

	// Setup function call trackers
	getApplicationsCalled := false
	getStagesCalled := false

	// Setup the client to return our mock applications and set the tracker
	mockClient.GetApplicationsFunc = func(
		ctx context.Context,
	) ([]SparkApplication, error) {
		getApplicationsCalled = true
		return sparkApps, nil
	}

	// Setup the client to return our verify the app, and set the tracker, and
	// return an error
	mockClient.GetApplicationStagesFunc = func(
		ctx context.Context,
		app *SparkApplication,
	) ([]SparkStage, error) {
		assert.Equal(
			t,
			&sparkApps[0],
			app,
			"Should pass the correct application",
		)
		getStagesCalled = true

		return nil, errors.New(expectedError)
	}

	// Setup mock tag map
	tags := map[string]string{
		"environment": "test",
		"cluster":     "spark-test",
	}

	// Setup metrics channel to receive metrics
	metricsChan := make(chan model.Metric, 500) // Buffer to prevent blocking

	// Create metrics receiver instance using the mock client and tags
	receiver := &SparkMetricsReceiver{
		nil,
		mockClient,
		"spark.",
		ClusterManagerTypeStandalone,
		nil,
		tags,
	}

	// Execute the function under test
	err := receiver.PollMetrics(context.Background(), metricsChan)

	// Close the channel
	close(metricsChan)

	// Verify results
	assert.Error(t, err)
    assert.Equal(t, expectedError, err.Error())
    assert.True(t, getApplicationsCalled, "GetApplications should be called")
    assert.True(t, getStagesCalled, "GetApplicationStages should be called")
}

func TestPollMetrics_CollectRDDMetricsError(t *testing.T) {
	// Setup mock Spark API client
	mockClient := &MockSparkApiClient{}

	// Setup mock SparkApplications
	sparkApps := []SparkApplication{
		{
			Id:   "app-123456",
			Name: "Test Spark App",
		},
	}

	// Setup expected error message
	expectedError := "error getting Spark RDD metrics"

	// Setup function call trackers
	getApplicationsCalled := false
	getRDDsCalled := false

	// Setup the client to return our mock applications and set the tracker
	mockClient.GetApplicationsFunc = func(
		ctx context.Context,
	) ([]SparkApplication, error) {
		getApplicationsCalled = true
		return sparkApps, nil
	}

	// Setup the client to return our verify the app, and set the tracker, and
	// return an error
	mockClient.GetApplicationRDDsFunc = func(
		ctx context.Context,
		app *SparkApplication,
	) ([]SparkRDD, error) {
		assert.Equal(
			t,
			&sparkApps[0],
			app,
			"Should pass the correct application",
		)
		getRDDsCalled = true

		return nil, errors.New(expectedError)
	}

	// Setup mock tag map
	tags := map[string]string{
		"environment": "test",
		"cluster":     "spark-test",
	}

	// Setup metrics channel to receive metrics
	metricsChan := make(chan model.Metric, 500) // Buffer to prevent blocking

	// Create metrics receiver instance using the mock client and tags
	receiver := &SparkMetricsReceiver{
		nil,
		mockClient,
		"spark.",
		ClusterManagerTypeStandalone,
		nil,
		tags,
	}

	// Execute the function under test
	err := receiver.PollMetrics(context.Background(), metricsChan)

	// Close the channel
	close(metricsChan)

	// Verify results
	assert.Error(t, err)
    assert.Equal(t, expectedError, err.Error())
    assert.True(t, getApplicationsCalled, "GetApplications should be called")
    assert.True(t, getRDDsCalled, "GetApplicationRDDs should be called")
}

func TestPollMetrics(t *testing.T) {
	// Setup mock Spark API client
	mockClient := &MockSparkApiClient{}

	// Setup mock SparkApplications
	sparkApps := []SparkApplication{
		{
			Id:   "app-123456",
			Name: "Test Spark App",
		},
	}

	// Setup mock SparkExecutor
	sparkExecutor := SparkExecutor{Id: "exec-1", MemoryUsed: 1024}

	// Setup mock SparkJob
	sparkJob := SparkJob{JobId: 1, NumActiveTasks: 20}

	// Setup mock SparkStage
	sparkStage := SparkStage{StageId: 1, NumTasks: 5}

	// Setup mock SparkRDD
	sparkRDD := SparkRDD{Id: 1, NumPartitions: 3}

	// Setup function call trackers
	getApplicationsCalled := false
	getExecutorsCalled := false
	getJobsCalled := false
	getStagesCalled := false
	getRDDsCalled := false

	// Setup the client to return our mock applications and set the tracker
	mockClient.GetApplicationsFunc = func(
		ctx context.Context,
	) ([]SparkApplication, error) {
		getApplicationsCalled = true
		return sparkApps, nil
	}

	// Setup the client to verify the app, set the tracker, and return our mock
	// executors
	mockClient.GetApplicationExecutorsFunc = func(
		ctx context.Context,
		app *SparkApplication,
	) ([]SparkExecutor, error) {
		assert.Equal(
			t,
			&sparkApps[0],
			app,
			"Should pass the correct application",
		)
		getExecutorsCalled = true
		return []SparkExecutor{sparkExecutor}, nil
	}

	// Setup the client to verify the app, set the tracker, and return our mock
	// jobs
	mockClient.GetApplicationJobsFunc = func(
		ctx context.Context,
		app *SparkApplication,
	) ([]SparkJob, error) {
		assert.Equal(
			t,
			&sparkApps[0],
			app,
			"Should pass the correct application",
		)
		getJobsCalled = true
		return []SparkJob{sparkJob}, nil
	}

	// Setup the client to verify the app, set the tracker, and return our mock
	// stages
	mockClient.GetApplicationStagesFunc = func(
		ctx context.Context,
		app *SparkApplication,
	) ([]SparkStage, error) {
		assert.Equal(
			t,
			&sparkApps[0],
			app,
			"Should pass the correct application",
		)
		getStagesCalled = true
		return []SparkStage{sparkStage}, nil
	}

	// Setup the client to verify the app, set the tracker, and return our mock
	// RDDs
	mockClient.GetApplicationRDDsFunc = func(
		ctx context.Context,
		app *SparkApplication,
	) ([]SparkRDD, error) {
		assert.Equal(
			t,
			&sparkApps[0],
			app,
			"Should pass the correct application",
		)
		getRDDsCalled = true
		return []SparkRDD{sparkRDD}, nil
	}

	// Setup mock tag map
	tags := map[string]string{
		"environment": "test",
		"cluster":     "spark-test",
	}

	// Setup metrics channel to receive metrics
	metricsChan := make(chan model.Metric, 500) // Buffer to prevent blocking

	// Create metrics receiver instance using the mock client and tags
	receiver := &SparkMetricsReceiver{
		nil,
		mockClient,
		"spark.",
		ClusterManagerTypeStandalone,
		nil,
		tags,
	}

	// Execute the function under test
	err := receiver.PollMetrics(context.Background(), metricsChan)

	// Close the channel to iterate through it
	close(metricsChan)

	// Collect all metrics for verification
	metrics := make([]model.Metric, 0)
	for metric := range metricsChan {
		metrics = append(metrics, metric)
	}

	// Verify results
	assert.NoError(t, err)
	assert.NotEmpty(t, metrics, "Should have received metrics")
	assert.True(t, getApplicationsCalled, "GetApplications should be called")
	assert.True(
		t,
		getExecutorsCalled,
		"GetApplicationExecutors should be called",
	)
	assert.True(t, getJobsCalled, "GetApplicationJobs should be called")
	assert.True(t, getStagesCalled, "GetApplicationStages should be called")
	assert.True(t, getRDDsCalled, "GetApplicationRDDs should be called")

	// Verify one metric from each collect* call
	foundExecutorMemoryUsed := false
	foundJobNumActiveTasks := false
	foundStageNumTasksTotal := false
	foundRDDNumPartitions := false

	for _, metric := range metrics {
		assert.NotNil(t, metric.Attributes)

		// Verify basic attributes
		assert.Contains(t, metric.Attributes, "sparkAppId")
		assert.Equal(t, "app-123456", metric.Attributes["sparkAppId"])
		assert.Contains(t, metric.Attributes, "sparkAppName")
		assert.Equal(t, "Test Spark App", metric.Attributes["sparkAppName"])
		assert.Contains(t, metric.Attributes, "environment")
		assert.Equal(t, "test", metric.Attributes["environment"])
		assert.Contains(t, metric.Attributes, "cluster")
		assert.Equal(t, "spark-test", metric.Attributes["cluster"])

		if metric.Name == "spark.app.executor.memoryUsed" {
			foundExecutorMemoryUsed = true
			assert.Equal(t, float64(1024), metric.Value.Float())
		} else if metric.Name == "spark.app.job.tasks" {
			status, ok := metric.Attributes["sparkAppTaskStatus"]
			if ok && status == "active" {
				foundJobNumActiveTasks = true
				assert.Equal(t, float64(20), metric.Value.Float())
			}
		} else if metric.Name == "spark.app.stage.tasks.total" {
			foundStageNumTasksTotal = true
			assert.Equal(t, float64(5), metric.Value.Float())
		} else if metric.Name == "spark.app.storage.rdd.partitions" {
			foundRDDNumPartitions = true
			assert.Equal(t, float64(3), metric.Value.Float())
		}
	}

	assert.True(
		t,
		foundExecutorMemoryUsed,
		"Should have found executor MemoryUsed metric",
	)
	assert.True(
		t,
		foundJobNumActiveTasks,
		"Should have found job NumActiveTasks metric",
	)
	assert.True(
		t,
		foundStageNumTasksTotal,
		"Should have found stage NumTasks metric",
	)
	assert.True(
		t,
		foundRDDNumPartitions,
		"Should have found RDD NumPartitions metric",
	)
}

func TestCollectSparkAppExecutorMetrics_Error(t *testing.T) {
	// Setup mock Spark API client
	mockClient := &MockSparkApiClient{}

	// Setup mock metrics decorator
	mockDecorator := &MockSparkMetricDecorator{}

	// Setup mock SparkApplication
	sparkApp := &SparkApplication{
		Id:   "app-123456",
		Name: "Test Spark App",
	}

	// Track if GetApplications was called
	getApplicationsCalled := false

	// Setup expected error message
	expectedError := "mock GetApplicationExecutors error"

	// Setup the client to verify the app, set the tracker and return an error
	// from GetApplicationExecutors
	mockClient.GetApplicationExecutorsFunc = func(
		ctx context.Context,
		app *SparkApplication,
	) ([]SparkExecutor, error) {
		assert.Equal(t, sparkApp, app, "Should pass the correct application")
		getApplicationsCalled = true

		return nil, errors.New(expectedError)
	}

	// Setup metrics channel to receive metrics
	metricsChan := make(chan model.Metric, 100) // Buffer to prevent blocking

	// Setup mock tag map
	tags := map[string]string{
		"environment": "test",
		"cluster":     "spark-test",
	}

	// Execute the function under test
	err := collectSparkAppExecutorMetrics(
		context.Background(),
		mockClient,
		sparkApp,
		"spark.",
		mockDecorator,
		tags,
		metricsChan,
	)

	// Close the channel to iterate through it
	close(metricsChan)

	// Collect all metrics to make sure the channel is empty
	metrics := make([]model.Metric, 0)
	for metric := range metricsChan {
		metrics = append(metrics, metric)
	}

	// Verify results
	assert.Error(t, err)
	assert.Equal(t, expectedError, err.Error())
	assert.True(t, getApplicationsCalled, "GetApplications should be called")
	assert.Empty(t, metrics, "No metrics should be received")
}

func TestCollectSparkAppExecutorMetrics(t *testing.T) {
	// Setup mock Spark API client
	mockClient := &MockSparkApiClient{}

	// Setup mock metrics decorator
	mockDecorator := &MockSparkMetricDecorator{}

	// Setup mock SparkApplication
	sparkApp := &SparkApplication{
		Id:   "app-123456",
		Name: "Test Spark App",
	}

	// Setup mock executors
	mockExecutors := []SparkExecutor{
		{
			Id:                "executor-1",
			RddBlocks:         10,
			MemoryUsed:        1024,
			DiskUsed:          2048,
			TotalCores:        4,
			MaxTasks:          8,
			ActiveTasks:       2,
			FailedTasks:       1,
			CompletedTasks:    5,
			TotalTasks:        8,
			TotalDuration:     5000,
			TotalGCTime:       500,
			TotalInputBytes:   10240,
			TotalShuffleRead:  2048,
			TotalShuffleWrite: 1024,
			MaxMemory:         4096,
			MemoryMetrics: SparkExecutorMemoryMetrics{
				UsedOnHeapStorageMemory:   512,
				UsedOffHeapStorageMemory:  256,
				TotalOnHeapStorageMemory:  1024,
				TotalOffHeapStorageMemory: 1024,
			},
			PeakMemoryMetrics: SparkExecutorPeakMemoryMetrics{
				JVMHeapMemory:          2048,
				JVMOffHeapMemory:       1024,
				OnHeapExecutionMemory:  512,
				OffHeapExecutionMemory: 256,
				OnHeapStorageMemory:    512,
				OffHeapStorageMemory:   256,
				DirectPoolMemory:       128,
				MappedPoolMemory:       64,
				TotalOffHeapMemory:     2048,
			},
		},
	}

	// Setup the client to return our mock executors
	mockClient.GetApplicationExecutorsFunc = func(
		ctx context.Context,
		app *SparkApplication,
	) ([]SparkExecutor, error) {
		assert.Equal(t, sparkApp, app, "Should pass the correct application")

		return mockExecutors, nil
	}

	// Setup a metrics channel to receive metrics
	metricsChan := make(chan model.Metric, 1000) // Buffer to prevent blocking

	// Setup mock tag map
	tags := map[string]string{
		"environment": "test",
		"cluster":     "spark-test",
	}

	// Execute the function under test
	err := collectSparkAppExecutorMetrics(
		context.Background(),
		mockClient,
		sparkApp,
		"spark.",
		mockDecorator,
		tags,
		metricsChan,
	)

	// Close the channel to iterate through it
	close(metricsChan)

	// Collect all metrics for verification
	metrics := make([]model.Metric, 0)
	for metric := range metricsChan {
		metrics = append(metrics, metric)
	}

	// Verify results
	assert.NoError(t, err)
	assert.NotEmpty(t, metrics, "Should have received metrics")

	// Verify that all metrics have the expected attributes
	for _, metric := range metrics {
		assert.NotNil(t, metric.Attributes)

		// Verify basic attributes
		assert.Contains(t, metric.Attributes, "sparkAppId")
		assert.Equal(t, "app-123456", metric.Attributes["sparkAppId"])
		assert.Contains(t, metric.Attributes, "sparkAppName")
		assert.Equal(t, "Test Spark App", metric.Attributes["sparkAppName"])
		assert.Contains(t, metric.Attributes, "environment")
		assert.Equal(t, "test", metric.Attributes["environment"])
		assert.Contains(t, metric.Attributes, "cluster")
		assert.Equal(t, "spark-test", metric.Attributes["cluster"])

		// Verify workspace attributes
		assert.Contains(t, metric.Attributes, "databricksWorkspaceId")
		assert.Equal(t, int64(12345), metric.Attributes["databricksWorkspaceId"])
		assert.Contains(t, metric.Attributes, "databricksWorkspaceName")
		assert.Equal(
			t,
			"foo.fakedomain.local",
			metric.Attributes["databricksWorkspaceName"],
		)
		assert.Contains(t, metric.Attributes, "databricksWorkspaceUrl")
		assert.Equal(
			t,
			"https://foo.fakedomain.local",
			metric.Attributes["databricksWorkspaceUrl"],
		)

		// Verify executor attributes
		assert.Contains(t, metric.Attributes, "sparkAppExecutorId")
		assert.Equal(t, "executor-1", metric.Attributes["sparkAppExecutorId"])

		// Verify metric name format
		assert.True(
			t,
			strings.HasPrefix(metric.Name, "spark.app.executor."),
			"Metric name should start with 'spark.app.executor.' but found: %s",
			metric.Name,
		)
	}

	// Verify a few metrics to ensure values are correctly passed
	foundRddBlocks := false
	foundUsedOnHeapStorageMemory := false
	foundPeakJVMHeapMemory := false

	for _, metric := range metrics {
		if metric.Name == "spark.app.executor.rddBlocks" {
			foundRddBlocks = true
			assert.Equal(t, float64(10), metric.Value.Float())
		} else if metric.Name == "spark.app.executor.memory.usedOnHeapStorage" {
			foundUsedOnHeapStorageMemory = true
			assert.Equal(t, float64(512), metric.Value.Float())
		} else if metric.Name == "spark.app.executor.memory.peak.jvmHeap" {
			foundPeakJVMHeapMemory = true
			assert.Equal(t, float64(2048), metric.Value.Float())
		}
	}

	assert.True(
		t,
		foundRddBlocks,
		"Should have found rddBlocks metric",
	)

	assert.True(
		t,
		foundUsedOnHeapStorageMemory,
		"Should have found usedOnHeapStorageMemory metric",
	)

	assert.True(
		t,
		foundPeakJVMHeapMemory,
		"Should have found jvmHeapMemory metric",
	)
}

func TestUpdateJobCounters_RunningJob(t *testing.T) {
	// Setup mock SparkJob with running status
	job := &SparkJob{
		JobId:  123,
		Name:   "Test Running Job",
		Status: "running", // Status is "running"
	}

	// Setup jobCounters struct
	counters := &jobCounters{
		running:   0,
		unknown:   0,
		succeeded: 0,
		failed:    0,
	}

	// Execute the function under test
	updateJobCounters(job, counters)

	// Verify results
	assert.Equal(
		t,
		int64(1), counters.running,
		"Should increment running counter",
	)

	assert.Equal(
		t,
		int64(0),
		counters.unknown,
		"Should not change unknown counter",
	)

	assert.Equal(
		t,
		int64(0),
		counters.succeeded,
		"Should not change succeeded counter",
	)

	assert.Equal(
		t,
		int64(0),
		counters.failed,
		"Should not change failed counter",
	)
}

func TestUpdateJobCounters_UnknownJob(t *testing.T) {
	// Setup mock SparkJob with unknown status
	job := &SparkJob{
		JobId:  124,
		Name:   "Test Unknown Job",
		Status: "unknown", // Status is "unknown"
	}

	// Setup jobCounters struct
	counters := &jobCounters{
		running:   0,
		unknown:   0,
		succeeded: 0,
		failed:    0,
	}

	// Execute the function under test
	updateJobCounters(job, counters)

	// Verify results
	assert.Equal(
		t,
		int64(1),
		counters.unknown,
		"Should increment unknown counter",
	)

	assert.Equal(
		t,
		int64(0),
		counters.running,
		"Should not change running counter",
	)

	assert.Equal(
		t,
		int64(0),
		counters.succeeded,
		"Should not change succeeded counter",
	)

	assert.Equal(
		t,
		int64(0),
		counters.failed,
		"Should not change failed counter",
	)
}

func TestUpdateJobCounters_SucceededJob(t *testing.T) {
	// Setup mock SparkJob with succeeded status
	job := &SparkJob{
		JobId:  124,
		Name:   "Test Succeeded Job",
		Status: "succeeded", // Status is "succeeded"
	}

	// Setup jobCounters struct
	counters := &jobCounters{
		running:   0,
		unknown:   0,
		succeeded: 0,
		failed:    0,
	}

	// Execute the function under test
	updateJobCounters(job, counters)

	// Verify results
	assert.Equal(
		t,
		int64(1),
		counters.succeeded,
		"Should increment succeeded counter",
	)

	assert.Equal(
		t,
		int64(0),
		counters.running,
		"Should not change running counter",
	)

	assert.Equal(
		t,
		int64(0),
		counters.unknown,
		"Should not change unknown counter",
	)

	assert.Equal(
		t,
		int64(0),
		counters.failed,
		"Should not change failed counter",
	)
}

func TestUpdateJobCounters_FailedJob(t *testing.T) {
	// Setup mock SparkJob with failed status
	job := &SparkJob{
		JobId:  124,
		Name:   "Test Failed Job",
		Status: "failed", // Status is "failed"
	}

	// Setup jobCounters struct
	counters := &jobCounters{
		running:   0,
		unknown:   0,
		succeeded: 0,
		failed:    0,
	}

	// Execute the function under test
	updateJobCounters(job, counters)

	// Verify results
	assert.Equal(
		t,
		int64(1),
		counters.failed,
		"Should increment failed counter",
	)

	assert.Equal(
		t,
		int64(0),
		counters.running,
		"Should not change running counter",
	)

	assert.Equal(
		t,
		int64(0),
		counters.unknown,
		"Should not change unknown counter",
	)

	assert.Equal(
		t,
		int64(0),
		counters.succeeded,
		"Should not change succeeded counter",
	)
}

func TestUpdateJobCounters_OtherStatus(t *testing.T) {
	// Setup mock SparkJob with a status that isn't explicitly handled
	job := &SparkJob{
		JobId:  125,
		Name:   "Test Unrecognized Job",
		Status: "pending", // Status is not one of the recognized values
	}

	// Setup jobCounters struct
	counters := &jobCounters{
		running:   0,
		unknown:   0,
		succeeded: 0,
		failed:    0,
	}

	// Execute the function under test
	updateJobCounters(job, counters)

	// Verify results
	assert.Equal(
		t,
		int64(0),
		counters.running,
		"Should not change running counter",
	)

	assert.Equal(
		t,
		int64(0),
		counters.unknown, "Should not change unknown counter",
	)

	assert.Equal(
		t,
		int64(0),
		counters.succeeded,
		"Should not change succeeded counter",
	)

	assert.Equal(
		t,
		int64(0),
		counters.failed,
		"Should not change failed counter",
	)
}

func TestUpdateJobCounters_CaseInsensitivity(t *testing.T) {
	// Setup various SparkJob instances with different case formats for status
	jobRunning := &SparkJob{
		JobId:  126,
		Name:   "Test Case Insensitivity - Running",
		Status: "RUNNING", // Uppercase status
	}

	jobUnknown := &SparkJob{
		JobId:  127,
		Name:   "Test Case Insensitivity - Unknown",
		Status: "Unknown", // Title case status
	}

	jobSucceeded := &SparkJob{
		JobId:  128,
		Name:   "Test Case Insensitivity - Succeeded",
		Status: "Succeeded", // Title case status
	}

	jobFailed := &SparkJob{
		JobId:  129,
		Name:   "Test Case Insensitivity - Failed",
		Status: "FAiLed", // Mixed case status
	}

	// Setup jobCounters struct
	counters := &jobCounters{
		running:   0,
		unknown:   0,
		succeeded: 0,
		failed:    0,
	}

	// Execute the function under test for each job
	updateJobCounters(jobRunning, counters)
	updateJobCounters(jobUnknown, counters)
	updateJobCounters(jobSucceeded, counters)
	updateJobCounters(jobFailed, counters)

	// Verify results
	assert.Equal(
		t,
		int64(1),
		counters.running,
		"Should increment running counter with uppercase RUNNING",
	)

	assert.Equal(
		t,
		int64(1),
		counters.unknown,
		"Should increment unknown counter with title case Unknown",
	)

	assert.Equal(
		t,
		int64(1),
		counters.succeeded,
		"Should increment succeeded counter with title case Succeeded",
	)

	assert.Equal(
		t,
		int64(1),
		counters.failed,
		"Should increment failed counter with mixed case FaiLed",
	)
}

func TestCollectSparkAppJobMetrics_GetApplicationJobsError(t *testing.T) {
	// Setup mock client
	mockClient := &MockSparkApiClient{}

	// Setup mock metrics decorator
	mockDecorator := &MockSparkMetricDecorator{}

	// Setup mock SparkApplication
	sparkApp := &SparkApplication{
		Id:   "app-123456",
		Name: "Test Spark App",
	}

	// Track if GetApplicationJobs was called
	getApplicationJobsCalled := false

	// Setup expected error message
	expectedError := "mock GetApplicationJobs error"

	// Setup the client to verify the app, set the tracker, and return an error
	// from GetApplicationJobs
	mockClient.GetApplicationJobsFunc = func(
		ctx context.Context,
		app *SparkApplication,
	) ([]SparkJob, error) {
		assert.Equal(t, sparkApp, app, "Should pass the correct application")
		getApplicationJobsCalled = true

		return nil, errors.New(expectedError)
	}

	// Setup a metrics channel to receive metrics
	metricsChan := make(chan model.Metric, 100) // Buffer to prevent blocking

	// Setup mock tag map
	tags := map[string]string{
		"environment": "test",
		"cluster":     "spark-test",
	}

	// Execute the function under test
	err := collectSparkAppJobMetrics(
		context.Background(),
		mockClient,
		sparkApp,
		"spark.",
		mockDecorator,
		tags,
		metricsChan,
	)

	// Close the channel to iterate through it
	close(metricsChan)

	// Collect all metrics to make sure the channel is empty
	metrics := make([]model.Metric, 0)
	for metric := range metricsChan {
		metrics = append(metrics, metric)
	}

	// Verify results
	assert.Error(t, err)
	assert.Equal(t, expectedError, err.Error())
	assert.True(
		t,
		getApplicationJobsCalled,
		"GetApplicationJobs should be called",
	)
	assert.Empty(t, metrics, "No metrics should be received")
}

func TestCollectSparkAppJobMetrics(t *testing.T) {
	// Setup mock client
	mockClient := &MockSparkApiClient{}

	// Setup mock metrics decorator
	mockDecorator := &MockSparkMetricDecorator{}

	// Setup mock SparkApplication
	sparkApp := &SparkApplication{
		Id:   "app-123456",
		Name: "Test Spark App",
	}

	// Setup mock jobs
	mockJobs := []SparkJob{
		{
			JobId:               123,
			Name:                "Test Job",
			SubmissionTime:      "2023-01-01T00:00:00.000GMT",
			CompletionTime:      "2023-01-01T00:05:30.000GMT",
			JobGroup:            "test-group",
			StageIds:            []int{1, 2, 3},
			Status:              "succeeded",
			NumTasks:            100,
			NumActiveTasks:      0,
			NumCompletedTasks:   95,
			NumSkippedTasks:     2,
			NumFailedTasks:      3,
			NumKilledTasks:      0,
			NumCompletedIndices: 95,
			NumActiveStages:     0,
			NumCompletedStages:  3,
			NumSkippedStages:    0,
			NumFailedStages:     0,
		},
		{
			JobId:               124,
			Name:                "Running Job",
			SubmissionTime:      "2023-01-01T00:10:00.000GMT",
			CompletionTime:      "",
			JobGroup:            "test-group",
			StageIds:            []int{4, 5},
			Status:              "running",
			NumTasks:            80,
			NumActiveTasks:      30,
			NumCompletedTasks:   50,
			NumSkippedTasks:     0,
			NumFailedTasks:      0,
			NumKilledTasks:      0,
			NumCompletedIndices: 50,
			NumActiveStages:     1,
			NumCompletedStages:  1,
			NumSkippedStages:    0,
			NumFailedStages:     0,
		},
		{
			JobId:               125,
			Name:                "Failed Job",
			SubmissionTime:      "2023-01-01T00:15:00.000GMT",
			CompletionTime:      "2023-01-01T00:16:45.000GMT",
			JobGroup:            "test-group",
			StageIds:            []int{6},
			Status:              "failed",
			NumTasks:            50,
			NumActiveTasks:      0,
			NumCompletedTasks:   30,
			NumSkippedTasks:     0,
			NumFailedTasks:      20,
			NumKilledTasks:      0,
			NumCompletedIndices: 30,
			NumActiveStages:     0,
			NumCompletedStages:  0,
			NumSkippedStages:    0,
			NumFailedStages:     1,
		},
		{
			JobId:               126,
			Name:                "Test Job Invalid Completion Time",
			SubmissionTime:      "2023-01-01T00:00:00.000GMT",
			CompletionTime:      "invalid_completion_time",
			JobGroup:            "test-group",
			StageIds:            []int{7, 8},
			Status:              "succeeded",
			NumTasks:            100,
			NumActiveTasks:      0,
			NumCompletedTasks:   95,
			NumSkippedTasks:     2,
			NumFailedTasks:      3,
			NumKilledTasks:      0,
			NumCompletedIndices: 95,
			NumActiveStages:     0,
			NumCompletedStages:  3,
			NumSkippedStages:    0,
			NumFailedStages:     0,
		},
	}

	// Setup the client to return our mock jobs
	mockClient.GetApplicationJobsFunc = func(
		ctx context.Context,
		app *SparkApplication,
	) ([]SparkJob, error) {
		assert.Equal(t, sparkApp, app, "Should pass the correct application")

		return mockJobs, nil
	}

	// Setup a metrics channel to receive metrics
	metricsChan := make(chan model.Metric, 1000) // Buffer to prevent blocking

	// Setup mock tag map
	tags := map[string]string{
		"environment": "test",
		"cluster":     "spark-test",
	}

	// Execute the function under test
	err := collectSparkAppJobMetrics(
		context.Background(),
		mockClient,
		sparkApp,
		"spark.",
		mockDecorator,
		tags,
		metricsChan,
	)

	// Close the channel to iterate through it
	close(metricsChan)

	// Collect all metrics for verification
	metrics := make([]model.Metric, 0)
	for metric := range metricsChan {
		metrics = append(metrics, metric)
	}

	// Verify results
	assert.NoError(t, err)
	assert.NotEmpty(t, metrics, "Should have received metrics")

	jobStatuses := map[int]string{
		123: "succeeded",
		124: "running",
		125: "failed",
	}

	// Verify that all metrics have the expected attributes
	for _, metric := range metrics {
		assert.NotNil(t, metric.Attributes)

		// Verify basic attributes
		assert.Contains(t, metric.Attributes, "sparkAppId")
		assert.Equal(t, "app-123456", metric.Attributes["sparkAppId"])
		assert.Contains(t, metric.Attributes, "sparkAppName")
		assert.Equal(t, "Test Spark App", metric.Attributes["sparkAppName"])
		assert.Contains(t, metric.Attributes, "environment")
		assert.Equal(t, "test", metric.Attributes["environment"])
		assert.Contains(t, metric.Attributes, "cluster")
		assert.Equal(t, "spark-test", metric.Attributes["cluster"])

		// Verify workspace attributes
		assert.Contains(t, metric.Attributes, "databricksWorkspaceId")
		assert.Equal(t, int64(12345), metric.Attributes["databricksWorkspaceId"])
		assert.Contains(t, metric.Attributes, "databricksWorkspaceName")
		assert.Equal(
			t,
			"foo.fakedomain.local",
			metric.Attributes["databricksWorkspaceName"],
		)
		assert.Contains(t, metric.Attributes, "databricksWorkspaceUrl")
		assert.Equal(
			t,
			"https://foo.fakedomain.local",
			metric.Attributes["databricksWorkspaceUrl"],
		)

		if metric.Name != "spark.app.jobs" {
			// Verify job attributes
			assert.Contains(t, metric.Attributes, "sparkAppJobId")
			assert.Contains(
				t,
				[]int{123, 124, 125},
				metric.Attributes["sparkAppJobId"],
			)
			assert.Contains(t, metric.Attributes, "sparkAppJobStatus")
			assert.Equal(
				t,
				jobStatuses[metric.Attributes["sparkAppJobId"].(int)],
				metric.Attributes["sparkAppJobStatus"],
			)

			// Verify metric name format
			assert.True(
				t,
				strings.HasPrefix(metric.Name, "spark.app.job."),
				"Metric name should start with 'spark.app.job.' but found: %s",
				metric.Name,
			)
		} else {
			// Verify job status attribute
			assert.Contains(t, metric.Attributes, "sparkAppJobStatus")
			assert.Contains(
				t,
				[]string{"running", "lost", "succeeded", "failed"},
				metric.Attributes["sparkAppJobStatus"],
			)

			if metric.Attributes["sparkAppJobStatus"] == "running" {
				assert.Equal(t, int64(1), metric.Value.Int())
			} else if metric.Attributes["sparkAppJobStatus"] == "lost" {
				assert.Equal(t, int64(0), metric.Value.Int())
			} else if metric.Attributes["sparkAppJobStatus"] == "succeeded" {
				assert.Equal(t, int64(2), metric.Value.Int())
			} else if metric.Attributes["sparkAppJobStatus"] == "failed" {
				assert.Equal(t, int64(1), metric.Value.Int())
			}
		}
	}

	// Verify a few metrics to ensure values are correctly passed
	jobCompletedStages := map[int]int64{
		123: 3,
		124: 1,
		125: 0,
	}

	jobCompletedTasks := map[int]int64{
		123: 95,
		124: 50,
		125: 30,
	}

	jobCompletedIndices := map[int]int64{
		123: 95,
		124: 50,
		125: 30,
	}

	stageCompletedMetricsFound := 0
	taskCompletedMetricsFound := 0
	jobIndicesCompletedMetricsFound := 0
	jobsRunningFound := false
	jobsLostFound := false
	jobsSucceededFound := false
	jobsFailedFound := false

	for _, metric := range metrics {
		if metric.Name == "spark.app.job.stages" &&
			metric.Attributes["sparkAppStageStatus"] == "complete" {
			// Verify the spark.app.job.stages metric with
			// sparkAppStageStatus == "complete" for each job. There should be
			// one for each job because we only run the collection once.
			assert.Equal(
				t,
				jobCompletedStages[metric.Attributes["sparkAppJobId"].(int)],
				metric.Value.Int(),
			)

			stageCompletedMetricsFound += 1
		} else if metric.Name == "spark.app.job.tasks" &&
			metric.Attributes["sparkAppTaskStatus"] == "complete" {
			// Verify the spark.app.job.tasks metric with
			// sparkAppTaskStatus == "complete" for each job. There should be
			// one for each job because we only run the collection once.
			assert.Equal(
				t,
				jobCompletedTasks[metric.Attributes["sparkAppJobId"].(int)],
				metric.Value.Int(),
			)

			taskCompletedMetricsFound += 1
		} else if metric.Name == "spark.app.job.indices.completed" {
			// Verify the spark.app.job.indices.completed metric for
			// each job. There should be one for each job because we only run
			// the collection once.
			assert.Equal(
				t,
				jobCompletedIndices[metric.Attributes["sparkAppJobId"].(int)],
				metric.Value.Int(),
			)

			jobIndicesCompletedMetricsFound += 1
		} else if metric.Name == "spark.app.jobs" {
			// Verify the spark.app.jobs metric for each job status. There
			// should be one for each job status.
			if metric.Attributes["sparkAppJobStatus"] == "running" {
				assert.Equal(t, int64(1), metric.Value.Int())
				jobsRunningFound = true
			} else if metric.Attributes["sparkAppJobStatus"] == "lost" {
				assert.Equal(t, int64(0), metric.Value.Int())
				jobsLostFound = true
			} else if metric.Attributes["sparkAppJobStatus"] == "succeeded" {
				assert.Equal(t, int64(2), metric.Value.Int())
				jobsSucceededFound = true
			} else if metric.Attributes["sparkAppJobStatus"] == "failed" {
				assert.Equal(t, int64(1), metric.Value.Int())
				jobsFailedFound = true
			}
		}
	}

	// Verify we saw everything we expected
	assert.Equal(
		t,
		3,
		stageCompletedMetricsFound,
		"Expected 3 spark.app.job.stages metrics",
	)

    assert.Equal(
    	t,
    	3,
    	taskCompletedMetricsFound,
    	"Expected 3 spark.app.job.tasks metrics",
    )

    assert.Equal(
    	t,
    	3,
    	jobIndicesCompletedMetricsFound,
    	"Expected 3 spark.app.job.indices.completed metrics",
    )

    assert.True(
    	t,
    	jobsRunningFound,
    	"Expected 1 spark.app.jobs metric with status 'running'",
    )

    assert.True(
    	t,
    	jobsLostFound,
    	"Expected 0 spark.app.jobs metric with status 'lost'",
    )

    assert.True(
    	t,
    	jobsSucceededFound,
    	"Expected 1 spark.app.jobs metric with status 'success'",
    )

	assert.True(
		t,
		jobsFailedFound,
		"Expected 1 spark.app.jobs metric with status 'failed'",
	)
}

func TestUpdateStageCounters_ActiveStage(t *testing.T) {
	// Setup mock SparkStage with active status
	stage := &SparkStage{
		StageId: 123,
		Name:    "Test Active Stage",
		Status:  "active", // Status is "active"
	}

	// Setup stageCounters struct
	counters := &stageCounters{
		active:   0,
		pending:  0,
		complete: 0,
		failed:   0,
		skipped:  0,
	}

	// Execute the function under test
	updateStageCounters(stage, counters)

	// Verify results
	assert.Equal(
		t,
		int64(1),
		counters.active,
		"Should increment active counter",
	)

	assert.Equal(
		t,
		int64(0),
		counters.pending,
		"Should not change pending counter",
	)

	assert.Equal(
		t,
		int64(0),
		counters.complete,
		"Should not change complete counter",
	)

	assert.Equal(
		t,
		int64(0),
		counters.failed,
		"Should not change failed counter",
	)

	assert.Equal(
		t,
		int64(0),
		counters.skipped,
		"Should not change skipped counter",
	)
}

func TestUpdateStageCounters_PendingStage(t *testing.T) {
	// Setup mock SparkStage with pending status
	stage := &SparkStage{
		StageId: 123,
		Name:    "Test Pending Stage",
		Status:  "pending", // Status is "pending"
	}

	// Setup stageCounters struct
	counters := &stageCounters{
		active:   0,
		pending:  0,
		complete: 0,
		failed:   0,
		skipped:  0,
	}

	// Execute the function under test
	updateStageCounters(stage, counters)

	// Verify results
	assert.Equal(
		t,
		int64(1),
		counters.pending,
		"Should increment pending counter",
	)

	assert.Equal(
		t,
		int64(0),
		counters.active,
		"Should not change active counter",
	)

	assert.Equal(
		t,
		int64(0),
		counters.complete,
		"Should not change complete counter",
	)

	assert.Equal(
		t,
		int64(0),
		counters.failed,
		"Should not change failed counter",
	)

	assert.Equal(
		t,
		int64(0),
		counters.skipped,
		"Should not change skipped counter",
	)
}

func TestUpdateStageCounters_CompleteStage(t *testing.T) {
	// Setup mock SparkStage with complete status
	stage := &SparkStage{
		StageId: 123,
		Name:    "Test Complete Stage",
		Status:  "complete", // Status is "complete"
	}

	// Setup stageCounters struct
	counters := &stageCounters{
		active:   0,
		pending:  0,
		complete: 0,
		failed:   0,
		skipped:  0,
	}

	// Execute the function under test
	updateStageCounters(stage, counters)

	// Verify results
	assert.Equal(
		t,
		int64(1),
		counters.complete,
		"Should increment complete counter",
	)

	assert.Equal(
		t,
		int64(0),
		counters.active,
		"Should not change active counter",
	)

	assert.Equal(
		t,
		int64(0),
		counters.pending,
		"Should not change pending counter",
	)

	assert.Equal(
		t,
		int64(0),
		counters.failed,
		"Should not change failed counter",
	)

	assert.Equal(
		t,
		int64(0),
		counters.skipped,
		"Should not change skipped counter",
	)
}

func TestUpdateStageCounters_FailedStage(t *testing.T) {
	// Setup mock SparkStage with failed status
	stage := &SparkStage{
		StageId: 101,
		Name:    "Test Failed Stage",
		Status:  "failed", // Status is "failed"
	}

	// Setup stageCounters struct
	counters := &stageCounters{
		active:   0,
		pending:  0,
		complete: 0,
		failed:   0,
		skipped:  0,
	}

	// Execute the function under test
	updateStageCounters(stage, counters)

	// Verify results
	assert.Equal(
		t,
		int64(1),
		counters.failed,
		"Should increment failed counter",
	)

	assert.Equal(
		t,
		int64(0),
		counters.active,
		"Should not change active counter",
	)

	assert.Equal(
		t,
		int64(0),
		counters.pending,
		"Should not change pending counter",
	)

	assert.Equal(
		t,
		int64(0),
		counters.complete,
		"Should not change complete counter",
	)

	assert.Equal(
		t,
		int64(0),
		counters.skipped,
		"Should not change skipped counter",
	)
}

func TestUpdateStageCounters_SkippedStage(t *testing.T) {
	// Setup mock SparkStage with skipped status
	stage := &SparkStage{
		StageId: 130,
		Name:    "Test Skipped Stage",
		Status:  "skipped", // Status is "skipped"
	}

	// Seupt stageCounters struct
	counters := &stageCounters{
		active:   0,
		pending:  0,
		complete: 0,
		failed:   0,
		skipped:  0,
	}

	// Execute the function under test
	updateStageCounters(stage, counters)

	// Verify results
	assert.Equal(
		t,
		int64(1),
		counters.skipped,
		"Should increment skipped counter",
	)

	assert.Equal(
		t,
		int64(0),
		counters.active,
		"Should not change active counter",
	)

	assert.Equal(
		t,
		int64(0),
		counters.pending,
		"Should not change pending counter",
	)

	assert.Equal(
		t,
		int64(0),
		counters.complete,
		"Should not change complete counter",
	)

	assert.Equal(
		t,
		int64(0),
		counters.failed,
		"Should not change failed counter",
	)
}

func TestUpdateStageCounters_UnknownStatus(t *testing.T) {
	// Setup mock SparkStage with an unrecognized status
	stage := &SparkStage{
		StageId: 130,
		Name:    "Test Unknown Status Stage",
		Status:  "unknown_status", // Status is not one of the recognized values
	}

	// Setup stageCounters struct
	counters := &stageCounters{
		active:   0,
		pending:  0,
		complete: 0,
		failed:   0,
		skipped:  0,
	}

	// Execute the function under test
	updateStageCounters(stage, counters)

	// Verify results
	assert.Equal(
		t,
		int64(0),
		counters.active,
		"Should not change active counter",
	)

	assert.Equal(
		t,
		int64(0),
		counters.pending,
		"Should not change pending counter",
	)

	assert.Equal(
		t,
		int64(0),
		counters.complete,
		"Should not change complete counter",
	)

	assert.Equal(
		t,
		int64(0),
		counters.failed,
		"Should not change failed counter",
	)

	assert.Equal(
		t,
		int64(0),
		counters.skipped,
		"Should not change skipped counter",
	)
}

func TestUpdateStageCounters_CaseInsensitivity(t *testing.T) {
	// Setup various SparkStage instances with different case formats for
	// status
	stageActive := &SparkStage{
		StageId: 126,
		Name:    "Test Case Insensitivity - Active",
		Status:  "ACTIVE", // Uppercase status
	}

	stagePending := &SparkStage{
		StageId: 127,
		Name:    "Test Case Insensitivity - Pending",
		Status:  "Pending", // Title case status
	}

	stageComplete := &SparkStage{
		StageId: 128,
		Name:    "Test Case Insensitivity - Complete",
		Status:  "cOmpLete", // Mixed case status
	}

	stageFailed := &SparkStage{
		StageId: 129,
		Name:    "Test Case Insensitivity - Failed",
		Status:  "fAiLeD", // Mixed case status
	}

	stageSkipped := &SparkStage{
		StageId: 130,
		Name:    "Test Case Insensitivity - Skipped",
		Status:  "SKIPPED", // Uppercase status
	}

	// Initialize stageCounters struct
	counters := &stageCounters{
		active:   0,
		pending:  0,
		complete: 0,
		failed:   0,
		skipped:  0,
	}

	// Execute the function under test for each stage
	updateStageCounters(stageActive, counters)
	updateStageCounters(stagePending, counters)
	updateStageCounters(stageComplete, counters)
	updateStageCounters(stageFailed, counters)
	updateStageCounters(stageSkipped, counters)

	// Verify results
	assert.Equal(
		t,
		int64(1),
		counters.active,
		"Should increment active counter with uppercase ACTIVE",
	)

	assert.Equal(
		t,
		int64(1),
		counters.pending,
		"Should increment pending counter with title case Pending",
	)

	assert.Equal(
		t,
		int64(1),
		counters.complete,
		"Should increment complete counter with mixed case cOmpLete",
	)

	assert.Equal(
		t,
		int64(1),
		counters.failed,
		"Should increment failed counter with mixed case fAiLeD",
	)

	assert.Equal(
		t,
		int64(1),
		counters.skipped,
		"Should increment skipped counter with uppercase SKIPPED",
	)
}

func TestCollectSparkAppStageMetrics_GetApplicationStagesError(t *testing.T) {
	// Setup mock client
	mockClient := &MockSparkApiClient{}

	// Setup mock metrics decorator
	mockDecorator := &MockSparkMetricDecorator{}

	// Setup mock SparkApplication
	sparkApp := &SparkApplication{
		Id:   "app-123456",
		Name: "Test Spark App",
	}

	// Track if GetApplicationStages was called
	getApplicationStagesCalled := false

	// Setup expected error message
	expectedError := "mock GetApplicationStages error"

	// Setup the client to verify the app, set the tracker, and return an error
	// from GetApplicationStages
	mockClient.GetApplicationStagesFunc = func(
		ctx context.Context,
		app *SparkApplication,
	) ([]SparkStage, error) {
		assert.Equal(t, sparkApp, app, "Should pass the correct application")
		getApplicationStagesCalled = true

		return nil, errors.New(expectedError)
	}

	// Setup a metrics channel to receive metrics
	metricsChan := make(chan model.Metric, 100) // Buffer to prevent blocking

	// Setup mock tag map
	tags := map[string]string{
		"environment": "test",
		"cluster":     "spark-test",
	}

	// Execute the function under test
	err := collectSparkAppStageMetrics(
		context.Background(),
		mockClient,
		sparkApp,
		"spark.",
		mockDecorator,
		tags,
		metricsChan,
	)

	// Close the channel to iterate through it
	close(metricsChan)

	// Collect all metrics to make sure the channel is empty
	metrics := make([]model.Metric, 0)
	for metric := range metricsChan {
		metrics = append(metrics, metric)
	}

	assert.Error(t, err)
	assert.Equal(t, expectedError, err.Error())
	assert.True(
		t,
		getApplicationStagesCalled,
		"GetApplicationStages should be called",
	)
	assert.Empty(t, metrics, "No metrics should be received")
}

func TestCollectSparkAppStageMetrics(t *testing.T) {
	// Setup mock client
	mockClient := &MockSparkApiClient{}

	// Setup mock metrics decorator
	mockDecorator := &MockSparkMetricDecorator{}

	// Create a mock SparkApplication
	sparkApp := &SparkApplication{
		Id:   "app-123456",
		Name: "Test Spark App",
	}

	// Create mock stages with different statuses and mock values
	mockStages := []SparkStage{
		{
			Status:                           "active",
			StageId:                          1,
			AttemptId:                        0,
			NumTasks:                         100,
			NumActiveTasks:                   75,
			NumCompleteTasks:                 20,
			NumFailedTasks:                   5,
			NumKilledTasks:                   0,
			NumCompletedIndices:              20,
			PeakNettyDirectMemory:            1048576, // 1MB
			PeakJvmDirectMemory:              2097152, // 2MB
			PeakSparkDirectMemoryOverLimit:   0,
			PeakTotalOffHeapMemory:           3145728, // 3MB
			SubmissionTime:                   "2023-05-15T10:30:00.000GMT",
			FirstTaskLaunchedTime:            "2023-05-15T10:30:05.000GMT",
			CompletionTime:                   "", // Still active, no completion time
			ExecutorDeserializeTime:          2500,
			ExecutorDeserializeCpuTime:       2000,
			ExecutorRunTime:                  15000,
			ExecutorCpuTime:                  12000,
			ResultSize:                       307200, // 300KB
			JvmGcTime:                        1200,
			ResultSerializationTime:          500,
			MemoryBytesSpilled:               524288,   // 512KB
			DiskBytesSpilled:                 1048576,  // 1MB
			PeakExecutionMemory:              4194304,  // 4MB
			InputBytes:                       10485760, // 10MB
			InputRecords:                     50000,
			OutputBytes:                      5242880, // 5MB
			OutputRecords:                    25000,
			ShuffleRemoteBlocksFetched:       500,
			ShuffleLocalBlocksFetched:        1500,
			ShuffleFetchWaitTime:             3000,
			ShuffleRemoteBytesRead:           2097152, // 2MB
			ShuffleRemoteBytesReadToDisk:     1048576, // 1MB
			ShuffleLocalBytesRead:            3145728, // 3MB
			ShuffleReadBytes:                 5242880, // 5MB
			ShuffleReadRecords:               30000,
			ShuffleCorruptMergedBlockChunks:  0,
			ShuffleMergedFetchFallbackCount:  5,
			ShuffleMergedRemoteBlocksFetched: 200,
			ShuffleMergedLocalBlocksFetched:  800,
			ShuffleMergedRemoteChunksFetched: 150,
			ShuffleMergedLocalChunksFetched:  600,
			ShuffleMergedRemoteBytesRead:     1048576, // 1MB
			ShuffleMergedLocalBytesRead:      2097152, // 2MB
			ShuffleRemoteReqsDuration:        4500,
			ShuffleMergedRemoteReqsDuration:  2200,
			ShuffleWriteBytes:                4194304, // 4MB
			ShuffleWriteTime:                 3500,
			ShuffleWriteRecords:              20000,
			Name:                             "Active Stage",
			SchedulingPool:                   "default",
			Tasks: map[string]SparkTask{
				"0": {
					TaskId:       1001,
					Index:        42,
					Attempt:      0,
					PartitionId:  42,
					LaunchTime:   "2023-05-15T10:30:10.000GMT",
					Duration:     5000,
					ExecutorId:   "executor-123",
					Status:       "SUCCESS",
					TaskLocality: "NODE_LOCAL",
					Speculative:  false,
					TaskMetrics: SparkTaskMetrics{
						ExecutorDeserializeTime:    200,
						ExecutorDeserializeCpuTime: 150,
						ExecutorRunTime:            4500,
						ExecutorCpuTime:            4000,
						ResultSize:                 102400, // 100KB
						JvmGcTime:                  300,
						ResultSerializationTime:    100,
						MemoryBytesSpilled:         51200,  // 50KB
						DiskBytesSpilled:           102400, // 100KB
						PeakExecutionMemory:        524288, // 512KB
						InputMetrics: struct {
							BytesRead   int `json:"bytesRead"`
							RecordsRead int `json:"recordsRead"`
						}{
							BytesRead:   1048576, // 1MB
							RecordsRead: 50000,
						},
						OutputMetrics: struct {
							BytesWritten   int `json:"bytesWritten"`
							RecordsWritten int `json:"recordsWritten"`
						}{
							BytesWritten:   5242880, // 5MB
							RecordsWritten: 25000,
						},
						ShuffleReadMetrics: struct {
							RemoteBlocksFetched   int `json:"remoteBlocksFetched"`
							LocalBlocksFetched    int `json:"localBlocksFetched"`
							FetchWaitTime         int `json:"fetchWaitTime"`
							RemoteBytesRead       int `json:"remoteBytesRead"`
							RemoteBytesReadToDisk int `json:"remoteBytesReadToDisk"`
							LocalBytesRead        int `json:"localBytesRead"`
							RecordsRead           int `json:"recordsRead"`
							RemoteReqsDuration    int `json:"remoteReqsDuration"`
							SufflePushReadMetrics struct {
								CorruptMergedBlockChunks  int `json:"corruptMergedBlockChunks"`
								MergedFetchFallbackCount  int `json:"mergedFetchFallbackCount"`
								RemoteMergedBlocksFetched int `json:"remoteMergedBlocksFetched"`
								LocalMergedBlocksFetched  int `json:"localMergedBlocksFetched"`
								RemoteMergedChunksFetched int `json:"remoteMergedChunksFetched"`
								LocalMergedChunksFetched  int `json:"localMergedChunksFetched"`
								RemoteMergedBytesRead     int `json:"remoteMergedBytesRead"`
								LocalMergedBytesRead      int `json:"localMergedBytesRead"`
								RemoteMergedReqsDuration  int `json:"remoteMergedReqsDuration"`
							} `json:"shufflePushReadMetrics"`
						}{
							RemoteBlocksFetched:   50,
							LocalBlocksFetched:    150,
							FetchWaitTime:         400,
							RemoteBytesRead:       102400, // 100KB
							RemoteBytesReadToDisk: 51200,  // 50KB
							LocalBytesRead:        153600, // 150KB
							RecordsRead:           5000,
							RemoteReqsDuration:    600,
							SufflePushReadMetrics: struct {
								CorruptMergedBlockChunks  int `json:"corruptMergedBlockChunks"`
								MergedFetchFallbackCount  int `json:"mergedFetchFallbackCount"`
								RemoteMergedBlocksFetched int `json:"remoteMergedBlocksFetched"`
								LocalMergedBlocksFetched  int `json:"localMergedBlocksFetched"`
								RemoteMergedChunksFetched int `json:"remoteMergedChunksFetched"`
								LocalMergedChunksFetched  int `json:"localMergedChunksFetched"`
								RemoteMergedBytesRead     int `json:"remoteMergedBytesRead"`
								LocalMergedBytesRead      int `json:"localMergedBytesRead"`
								RemoteMergedReqsDuration  int `json:"remoteMergedReqsDuration"`
							}{
								CorruptMergedBlockChunks:  0,
								MergedFetchFallbackCount:  2,
								RemoteMergedBlocksFetched: 20,
								LocalMergedBlocksFetched:  80,
								RemoteMergedChunksFetched: 15,
								LocalMergedChunksFetched:  60,
								RemoteMergedBytesRead:     51200,  // 50KB
								LocalMergedBytesRead:      102400, // 100KB
								RemoteMergedReqsDuration:  300,
							},
						},
						ShuffleWriteMetrics: struct {
							BytesWritten   int `json:"bytesWritten"`
							WriteTime      int `json:"writeTime"`
							RecordsWritten int `json:"recordsWritten"`
						}{
							BytesWritten:   5242880, // 5MB
							WriteTime:      3500,
							RecordsWritten: 25000,
						},
						PhotonMemoryMetrics: struct {
							OffHeapMinMemorySize          int `json:"offHeapMinMemorySize"`
							OffHeapMaxMemorySize          int `json:"offHeapMaxMemorySize"`
							PhotonBufferPoolMinMemorySize int `json:"photonBufferPoolMinMemorySize"`
							PhotonBufferPoolMaxMemorySize int `json:"photonBufferPoolMaxMemorySize"`
						}{
							OffHeapMinMemorySize:          4194304, // 4MB
							OffHeapMaxMemorySize:          8388608, // 8MB
							PhotonBufferPoolMinMemorySize: 262144,  // 256KB
							PhotonBufferPoolMaxMemorySize: 524288,  // 512KB
						},
						PhotonizedTaskTimeNs: 4000000000, // 4 seconds in nanoseconds
					},
					SchedulerDelay:    300,
					GettingResultTime: 200,
				},
			},
			ExecutorSummary:   map[string]SparkExecutorSummary{},
			ResourceProfileId: 0,
			PeakExecutorMetrics: SparkExecutorPeakMemoryMetrics{
				JVMHeapMemory:          8388608, // 8MB
				JVMOffHeapMemory:       4194304, // 4MB
				OnHeapExecutionMemory:  2097152, // 2MB
				OffHeapExecutionMemory: 1048576, // 1MB
				OnHeapStorageMemory:    3145728, // 3MB
				OffHeapStorageMemory:   1572864, // 1.5MB
				DirectPoolMemory:       524288,  // 512KB
				MappedPoolMemory:       262144,  // 256KB
				TotalOffHeapMemory:     6291456, // 6MB
			},
			ShuffleMergersCount: 2,
		},
		{
			Status:                           "pending",
			StageId:                          2,
			AttemptId:                        0,
			NumTasks:                         80,
			NumActiveTasks:                   0,
			NumCompleteTasks:                 0,
			NumFailedTasks:                   0,
			NumKilledTasks:                   0,
			NumCompletedIndices:              0,
			PeakNettyDirectMemory:            0,
			PeakJvmDirectMemory:              0,
			PeakSparkDirectMemoryOverLimit:   0,
			PeakTotalOffHeapMemory:           0,
			SubmissionTime:                   "2023-05-15T10:35:00.000GMT",
			FirstTaskLaunchedTime:            "", // Not launched yet
			CompletionTime:                   "", // Not completed
			ExecutorDeserializeTime:          0,
			ExecutorDeserializeCpuTime:       0,
			ExecutorRunTime:                  0,
			ExecutorCpuTime:                  0,
			ResultSize:                       0,
			JvmGcTime:                        0,
			ResultSerializationTime:          0,
			MemoryBytesSpilled:               0,
			DiskBytesSpilled:                 0,
			PeakExecutionMemory:              0,
			InputBytes:                       0,
			InputRecords:                     0,
			OutputBytes:                      0,
			OutputRecords:                    0,
			ShuffleRemoteBlocksFetched:       0,
			ShuffleLocalBlocksFetched:        0,
			ShuffleFetchWaitTime:             0,
			ShuffleRemoteBytesRead:           0,
			ShuffleRemoteBytesReadToDisk:     0,
			ShuffleLocalBytesRead:            0,
			ShuffleReadBytes:                 0,
			ShuffleReadRecords:               0,
			ShuffleCorruptMergedBlockChunks:  0,
			ShuffleMergedFetchFallbackCount:  0,
			ShuffleMergedRemoteBlocksFetched: 0,
			ShuffleMergedLocalBlocksFetched:  0,
			ShuffleMergedRemoteChunksFetched: 0,
			ShuffleMergedLocalChunksFetched:  0,
			ShuffleMergedRemoteBytesRead:     0,
			ShuffleMergedLocalBytesRead:      0,
			ShuffleRemoteReqsDuration:        0,
			ShuffleMergedRemoteReqsDuration:  0,
			ShuffleWriteBytes:                0,
			ShuffleWriteTime:                 0,
			ShuffleWriteRecords:              0,
			Name:                             "Pending Stage",
			SchedulingPool:                   "default",
			Tasks:                            map[string]SparkTask{},
			ExecutorSummary:                  map[string]SparkExecutorSummary{},
			ResourceProfileId:                0,
			PeakExecutorMetrics:              SparkExecutorPeakMemoryMetrics{},
			ShuffleMergersCount:              0,
		},
		{
			Status:                           "complete",
			StageId:                          3,
			AttemptId:                        0,
			NumTasks:                         120,
			NumActiveTasks:                   0,
			NumCompleteTasks:                 115,
			NumFailedTasks:                   3,
			NumKilledTasks:                   2,
			NumCompletedIndices:              115,
			PeakNettyDirectMemory:            2097152, // 2MB
			PeakJvmDirectMemory:              3145728, // 3MB
			PeakSparkDirectMemoryOverLimit:   0,
			PeakTotalOffHeapMemory:           5242880, // 5MB
			SubmissionTime:                   "2023-05-15T10:20:00.000GMT",
			FirstTaskLaunchedTime:            "2023-05-15T10:20:05.000GMT",
			CompletionTime:                   "2023-05-15T10:25:30.000GMT",
			ExecutorDeserializeTime:          3000,
			ExecutorDeserializeCpuTime:       2500,
			ExecutorRunTime:                  320000,
			ExecutorCpuTime:                  280000,
			ResultSize:                       1048576, // 1MB
			JvmGcTime:                        5000,
			ResultSerializationTime:          1500,
			MemoryBytesSpilled:               2097152,  // 2MB
			DiskBytesSpilled:                 4194304,  // 4MB
			PeakExecutionMemory:              8388608,  // 8MB
			InputBytes:                       20971520, // 20MB
			InputRecords:                     100000,
			OutputBytes:                      10485760, // 10MB
			OutputRecords:                    50000,
			ShuffleRemoteBlocksFetched:       1200,
			ShuffleLocalBlocksFetched:        3000,
			ShuffleFetchWaitTime:             8000,
			ShuffleRemoteBytesRead:           5242880,  // 5MB
			ShuffleRemoteBytesReadToDisk:     2097152,  // 2MB
			ShuffleLocalBytesRead:            8388608,  // 8MB
			ShuffleReadBytes:                 13631488, // 13MB
			ShuffleReadRecords:               75000,
			ShuffleCorruptMergedBlockChunks:  0,
			ShuffleMergedFetchFallbackCount:  10,
			ShuffleMergedRemoteBlocksFetched: 500,
			ShuffleMergedLocalBlocksFetched:  1500,
			ShuffleMergedRemoteChunksFetched: 400,
			ShuffleMergedLocalChunksFetched:  1200,
			ShuffleMergedRemoteBytesRead:     3145728, // 3MB
			ShuffleMergedLocalBytesRead:      6291456, // 6MB
			ShuffleRemoteReqsDuration:        12000,
			ShuffleMergedRemoteReqsDuration:  6000,
			ShuffleWriteBytes:                9437184, // 9MB
			ShuffleWriteTime:                 7500,
			ShuffleWriteRecords:              45000,
			Name:                             "Complete Stage",
			SchedulingPool:                   "default",
			Tasks: map[string]SparkTask{
				"0": {
					TaskId:       1002,
					Index:        43,
					Attempt:      1, // This is a retry attempt
					PartitionId:  43,
					LaunchTime:   "2023-05-15T10:30:15.000GMT",
					Duration:     7500, // Longer duration
					ExecutorId:   "executor-456",
					Status:       "RUNNING",    // Different status
					TaskLocality: "RACK_LOCAL", // Different locality
					Speculative:  true,         // Speculative execution
					TaskMetrics: SparkTaskMetrics{
						ExecutorDeserializeTime:    250,
						ExecutorDeserializeCpuTime: 200,
						ExecutorRunTime:            7000,
						ExecutorCpuTime:            6500,
						ResultSize:                 204800, // 200KB
						JvmGcTime:                  450,
						ResultSerializationTime:    180,
						MemoryBytesSpilled:         102400,  // 100KB
						DiskBytesSpilled:           204800,  // 200KB
						PeakExecutionMemory:        1048576, // 1MB
						InputMetrics: struct {
							BytesRead   int `json:"bytesRead"`
							RecordsRead int `json:"recordsRead"`
						}{
							BytesRead:   2097152, // 2MB
							RecordsRead: 75000,
						},
						OutputMetrics: struct {
							BytesWritten   int `json:"bytesWritten"`
							RecordsWritten int `json:"recordsWritten"`
						}{
							BytesWritten:   3145728, // 3MB
							RecordsWritten: 15000,
						},
						ShuffleReadMetrics: struct {
							RemoteBlocksFetched   int `json:"remoteBlocksFetched"`
							LocalBlocksFetched    int `json:"localBlocksFetched"`
							FetchWaitTime         int `json:"fetchWaitTime"`
							RemoteBytesRead       int `json:"remoteBytesRead"`
							RemoteBytesReadToDisk int `json:"remoteBytesReadToDisk"`
							LocalBytesRead        int `json:"localBytesRead"`
							RecordsRead           int `json:"recordsRead"`
							RemoteReqsDuration    int `json:"remoteReqsDuration"`
							SufflePushReadMetrics struct {
								CorruptMergedBlockChunks  int `json:"corruptMergedBlockChunks"`
								MergedFetchFallbackCount  int `json:"mergedFetchFallbackCount"`
								RemoteMergedBlocksFetched int `json:"remoteMergedBlocksFetched"`
								LocalMergedBlocksFetched  int `json:"localMergedBlocksFetched"`
								RemoteMergedChunksFetched int `json:"remoteMergedChunksFetched"`
								LocalMergedChunksFetched  int `json:"localMergedChunksFetched"`
								RemoteMergedBytesRead     int `json:"remoteMergedBytesRead"`
								LocalMergedBytesRead      int `json:"localMergedBytesRead"`
								RemoteMergedReqsDuration  int `json:"remoteMergedReqsDuration"`
							} `json:"shufflePushReadMetrics"`
						}{
							RemoteBlocksFetched:   120,
							LocalBlocksFetched:    80,
							FetchWaitTime:         750,
							RemoteBytesRead:       524288, // 512KB
							RemoteBytesReadToDisk: 262144, // 256KB
							LocalBytesRead:        307200, // 300KB
							RecordsRead:           8000,
							RemoteReqsDuration:    900,
							SufflePushReadMetrics: struct {
								CorruptMergedBlockChunks  int `json:"corruptMergedBlockChunks"`
								MergedFetchFallbackCount  int `json:"mergedFetchFallbackCount"`
								RemoteMergedBlocksFetched int `json:"remoteMergedBlocksFetched"`
								LocalMergedBlocksFetched  int `json:"localMergedBlocksFetched"`
								RemoteMergedChunksFetched int `json:"remoteMergedChunksFetched"`
								LocalMergedChunksFetched  int `json:"localMergedChunksFetched"`
								RemoteMergedBytesRead     int `json:"remoteMergedBytesRead"`
								LocalMergedBytesRead      int `json:"localMergedBytesRead"`
								RemoteMergedReqsDuration  int `json:"remoteMergedReqsDuration"`
							}{
								CorruptMergedBlockChunks:  1, // Has a corruption
								MergedFetchFallbackCount:  5,
								RemoteMergedBlocksFetched: 40,
								LocalMergedBlocksFetched:  60,
								RemoteMergedChunksFetched: 35,
								LocalMergedChunksFetched:  45,
								RemoteMergedBytesRead:     153600, // 150KB
								LocalMergedBytesRead:      204800, // 200KB
								RemoteMergedReqsDuration:  550,
							},
						},
						ShuffleWriteMetrics: struct {
							BytesWritten   int `json:"bytesWritten"`
							WriteTime      int `json:"writeTime"`
							RecordsWritten int `json:"recordsWritten"`
						}{
							BytesWritten:   8388608, // 8MB
							WriteTime:      5000,
							RecordsWritten: 40000,
						},
						PhotonMemoryMetrics: struct {
							OffHeapMinMemorySize          int `json:"offHeapMinMemorySize"`
							OffHeapMaxMemorySize          int `json:"offHeapMaxMemorySize"`
							PhotonBufferPoolMinMemorySize int `json:"photonBufferPoolMinMemorySize"`
							PhotonBufferPoolMaxMemorySize int `json:"photonBufferPoolMaxMemorySize"`
						}{
							OffHeapMinMemorySize:          6291456,  // 6MB
							OffHeapMaxMemorySize:          12582912, // 12MB
							PhotonBufferPoolMinMemorySize: 524288,   // 512KB
							PhotonBufferPoolMaxMemorySize: 1048576,  // 1MB
						},
						PhotonizedTaskTimeNs: 6500000000, // 6.5 seconds in nanoseconds
					},
					SchedulerDelay:    450,
					GettingResultTime: 350,
				},
			},
			ExecutorSummary:   map[string]SparkExecutorSummary{},
			ResourceProfileId: 0,
			PeakExecutorMetrics: SparkExecutorPeakMemoryMetrics{
				JVMHeapMemory:          4194304, // 4MB
				JVMOffHeapMemory:       4194304, // 4MB
				OnHeapExecutionMemory:  2097152, // 2MB
				OffHeapExecutionMemory: 1048576, // 1MB
				OnHeapStorageMemory:    3145728, // 3MB
				OffHeapStorageMemory:   1572864, // 1.5MB
				DirectPoolMemory:       524288,  // 512KB
				MappedPoolMemory:       262144,  // 256KB
				TotalOffHeapMemory:     6291456, // 6MB
			},
			ShuffleMergersCount: 3,
		},
		{
			Status:                           "complete",
			StageId:                          4,
			AttemptId:                        0,
			NumTasks:                         120,
			NumActiveTasks:                   0,
			NumCompleteTasks:                 115,
			NumFailedTasks:                   3,
			NumKilledTasks:                   2,
			NumCompletedIndices:              115,
			PeakNettyDirectMemory:            2097152, // 2MB
			PeakJvmDirectMemory:              3145728, // 3MB
			PeakSparkDirectMemoryOverLimit:   0,
			PeakTotalOffHeapMemory:           5242880, // 5MB
			SubmissionTime:                   "2023-05-15T10:20:00.000GMT",
			FirstTaskLaunchedTime:            "2023-05-15T10:20:05.000GMT",
			CompletionTime:                   "invalid_completion_time",
			ExecutorDeserializeTime:          3000,
			ExecutorDeserializeCpuTime:       2500,
			ExecutorRunTime:                  320000,
			ExecutorCpuTime:                  280000,
			ResultSize:                       1048576, // 1MB
			JvmGcTime:                        5000,
			ResultSerializationTime:          1500,
			MemoryBytesSpilled:               2097152,  // 2MB
			DiskBytesSpilled:                 4194304,  // 4MB
			PeakExecutionMemory:              8388608,  // 8MB
			InputBytes:                       20971520, // 20MB
			InputRecords:                     100000,
			OutputBytes:                      10485760, // 10MB
			OutputRecords:                    50000,
			ShuffleRemoteBlocksFetched:       1200,
			ShuffleLocalBlocksFetched:        3000,
			ShuffleFetchWaitTime:             8000,
			ShuffleRemoteBytesRead:           5242880,  // 5MB
			ShuffleRemoteBytesReadToDisk:     2097152,  // 2MB
			ShuffleLocalBytesRead:            8388608,  // 8MB
			ShuffleReadBytes:                 13631488, // 13MB
			ShuffleReadRecords:               75000,
			ShuffleCorruptMergedBlockChunks:  0,
			ShuffleMergedFetchFallbackCount:  10,
			ShuffleMergedRemoteBlocksFetched: 500,
			ShuffleMergedLocalBlocksFetched:  1500,
			ShuffleMergedRemoteChunksFetched: 400,
			ShuffleMergedLocalChunksFetched:  1200,
			ShuffleMergedRemoteBytesRead:     3145728, // 3MB
			ShuffleMergedLocalBytesRead:      6291456, // 6MB
			ShuffleRemoteReqsDuration:        12000,
			ShuffleMergedRemoteReqsDuration:  6000,
			ShuffleWriteBytes:                9437184, // 9MB
			ShuffleWriteTime:                 7500,
			ShuffleWriteRecords:              45000,
			Name:                             "Complete Stage Invalid Completion Time",
			SchedulingPool:                   "default",
			Tasks:                            map[string]SparkTask{},
			ExecutorSummary:                  map[string]SparkExecutorSummary{},
			ResourceProfileId:                0,
			PeakExecutorMetrics: SparkExecutorPeakMemoryMetrics{
				JVMHeapMemory:          8388608, // 8MB
				JVMOffHeapMemory:       4194304, // 4MB
				OnHeapExecutionMemory:  2097152, // 2MB
				OffHeapExecutionMemory: 1048576, // 1MB
				OnHeapStorageMemory:    3145728, // 3MB
				OffHeapStorageMemory:   1572864, // 1.5MB
				DirectPoolMemory:       524288,  // 512KB
				MappedPoolMemory:       262144,  // 256KB
				TotalOffHeapMemory:     6291456, // 6MB
			},
			ShuffleMergersCount: 3,
		},
	}

	// Setup the client to return our mock stages
	mockClient.GetApplicationStagesFunc = func(
		ctx context.Context,
		app *SparkApplication,
	) ([]SparkStage, error) {
		assert.Equal(t, sparkApp, app, "Should pass the correct application")

		return mockStages, nil
	}

	// Setup a metrics channel to receive metrics
	metricsChan := make(chan model.Metric, 1000) // Buffer to prevent blocking

	// Setup mock tag map
	tags := map[string]string{
		"environment": "test",
		"cluster":     "spark-test",
	}

	// Execute the function under test
	err := collectSparkAppStageMetrics(
		context.Background(),
		mockClient,
		sparkApp,
		"spark.",
		mockDecorator,
		tags,
		metricsChan,
	)

	// Close the channel to iterate through it
	close(metricsChan)

	// Collect all metrics for verification
	metrics := make([]model.Metric, 0)
	for metric := range metricsChan {
		metrics = append(metrics, metric)
	}

	// Verify results
	assert.NoError(t, err)
	assert.NotEmpty(t, metrics, "Should have received metrics")

	stageNames := map[int]string{
		1: "Active Stage",
        2: "Pending Stage",
		3: "Complete Stage",
    }

	stageStatuses := map[int]string{
		1: "active",
		2: "pending",
		3: "complete",
	}

	stageTaskExecutorIds := map[int]string{
		1001: "executor-123",
		1002: "executor-456",
    }

	stageTaskStatuses := map[int]string{
		1001: "success",
        1002: "running",
    }

	stageTaskLocalities := map[int]string{
		1001: "NODE_LOCAL",
        1002: "RACK_LOCAL",
    }

	stageTaskSpeculativeFlags := map[int]bool{
		1001: false,
        1002: true,
    }

	stageTaskAttempts := map[int]int{
		1001: 0,
		1002: 1,
	}

	// Verify that all metrics have the expected attributes
	for _, metric := range metrics {
		assert.NotNil(t, metric.Attributes)

		// Verify basic attributes
		assert.Contains(t, metric.Attributes, "sparkAppId")
		assert.Equal(t, "app-123456", metric.Attributes["sparkAppId"])
		assert.Contains(t, metric.Attributes, "sparkAppName")
		assert.Equal(t, "Test Spark App", metric.Attributes["sparkAppName"])
		assert.Contains(t, metric.Attributes, "environment")
		assert.Equal(t, "test", metric.Attributes["environment"])
		assert.Contains(t, metric.Attributes, "cluster")
		assert.Equal(t, "spark-test", metric.Attributes["cluster"])

		// Verify workspace attributes
		assert.Contains(t, metric.Attributes, "databricksWorkspaceId")
		assert.Equal(t, int64(12345), metric.Attributes["databricksWorkspaceId"])
		assert.Contains(t, metric.Attributes, "databricksWorkspaceName")
		assert.Equal(
			t,
			"foo.fakedomain.local",
			metric.Attributes["databricksWorkspaceName"],
		)
		assert.Contains(t, metric.Attributes, "databricksWorkspaceUrl")
		assert.Equal(
			t,
			"https://foo.fakedomain.local",
			metric.Attributes["databricksWorkspaceUrl"],
		)

		if metric.Name != "spark.app.stages" {
			// Verify stage attributes
			assert.Contains(t, metric.Attributes, "sparkAppStageId")
			assert.Contains(
				t,
				[]int{1, 2, 3},
				metric.Attributes["sparkAppStageId"],
			)
			assert.Contains(t, metric.Attributes, "sparkAppStageName")
			assert.Equal(
                t,
                stageNames[metric.Attributes["sparkAppStageId"].(int)],
                metric.Attributes["sparkAppStageName"],
            )
			assert.Contains(t, metric.Attributes, "sparkAppStageStatus")
			assert.Equal(
				t,
				stageStatuses[metric.Attributes["sparkAppStageId"].(int)],
				metric.Attributes["sparkAppStageStatus"],
			)

			// Verify metric name format
			assert.True(
				t,
				strings.HasPrefix(metric.Name, "spark.app.stage."),
				"Metric name should start with 'spark.app.stage.' but found: %s",
				metric.Name,
			)

			if strings.HasPrefix(metric.Name, "spark.app.stage.task.") {
				// Verify task attributes
				assert.Contains(t, metric.Attributes, "sparkAppTaskId")
				assert.Contains(
					t,
					[]int{1001, 1002},
					metric.Attributes["sparkAppTaskId"],
				)
				assert.Contains(t, metric.Attributes, "sparkAppTaskExecutorId")
				assert.Equal(
					t,
					stageTaskExecutorIds[
						metric.Attributes["sparkAppTaskId"].(int),
					],
					metric.Attributes["sparkAppTaskExecutorId"],
				)
				assert.Contains(t, metric.Attributes, "sparkAppTaskStatus")
				assert.Equal(
					t,
					stageTaskStatuses[
						metric.Attributes["sparkAppTaskId"].(int),
					],
					metric.Attributes["sparkAppTaskStatus"],
				)
				assert.Contains(t, metric.Attributes, "sparkAppTaskLocality")
				assert.Equal(
					t,
					stageTaskLocalities[
						metric.Attributes["sparkAppTaskId"].(int),
					],
					metric.Attributes["sparkAppTaskLocality"],
				)
				assert.Contains(t, metric.Attributes, "sparkAppTaskSpeculative")
				assert.Equal(
					t,
					stageTaskSpeculativeFlags[
						metric.Attributes["sparkAppTaskId"].(int),
					],
                    metric.Attributes["sparkAppTaskSpeculative"],
				)
				assert.Contains(t, metric.Attributes, "sparkAppTaskAttempt")
				assert.Equal(
					t,
					stageTaskAttempts[
						metric.Attributes["sparkAppTaskId"].(int),
					],
					metric.Attributes["sparkAppTaskAttempt"],
				)
			}
		} else {
			// Verify stage status attribute
			assert.Contains(t, metric.Attributes, "sparkAppStageStatus")
			assert.Contains(
				t,
				[]string{"active", "pending", "complete", "failed", "skipped"},
				metric.Attributes["sparkAppStageStatus"],
			)

			if metric.Attributes["sparkAppStageStatus"] == "active" {
				assert.Equal(t, int64(1), metric.Value.Int())
			} else if metric.Attributes["sparkAppStageStatus"] == "pending" {
				assert.Equal(t, int64(1), metric.Value.Int())
			} else if metric.Attributes["sparkAppStageStatus"] == "complete" {
				assert.Equal(t, int64(2), metric.Value.Int())
			} else if metric.Attributes["sparkAppStageStatus"] == "failed" {
				assert.Equal(t, int64(0), metric.Value.Int())
			} else if metric.Attributes["sparkAppStageStatus"] == "skipped" {
				assert.Equal(t, int64(0), metric.Value.Int())
			}
		}
	}

	// Verify a few metrics to ensure values are correctly passed

	stageCompletedTasks := map[int]int64{
		1: 20,
		2: 0,
		3: 115,
	}

	stageShuffleMergersCount := map[int]int64{
		1: 2,
		2: 0,
		3: 3,
	}

	stageJvmHeapPeakMemory := map[int]int64{
		1: 8388608, // 8MB
		2: 0,
		3: 4194304, // 4MB
	}

	stageCompletedIndices := map[int]int64{
		1: 20,
		2: 0,
		3: 115,
	}

	stageTaskDurations := map[int]int64{
		1001: 5000,
		1002: 7500,
	}

	stageTaskInputBytesRead := map[int]int64{
		1001: 1048576, // 1MB
		1002: 2097152, // 2MB
	}

	stageTaskShufflePushReadCorruptMergedBlockChunks := map[int]int64{
		1001: 0,
		1002: 1,
	}

	for _, metric := range metrics {
		if metric.Name == "spark.app.stage.tasks" &&
			metric.Attributes["sparkAppTaskStatus"] == "complete" {
			assert.Equal(
				t,
				stageCompletedTasks[
					metric.Attributes["sparkAppStageId"].(int),
				],
				metric.Value.Int(),
			)
		} else if metric.Name == "spark.app.stage.shuffle.mergersCount" {
			assert.Equal(
				t,
				stageShuffleMergersCount[
					metric.Attributes["sparkAppStageId"].(int),
				],
				metric.Value.Int(),
			)
		} else if metric.Name ==
			"spark.app.stage.executor.memory.peak.jvmHeap" {
			assert.Equal(
				t,
				stageJvmHeapPeakMemory[
					metric.Attributes["sparkAppStageId"].(int),
				],
				metric.Value.Int(),
			)
		} else if metric.Name == "spark.app.stage.indices.completed" {
			assert.Equal(
				t,
				stageCompletedIndices[
					metric.Attributes["sparkAppStageId"].(int),
				],
				metric.Value.Int(),
			)
		} else if metric.Name == "spark.app.stage.task.duration" {
			assert.Equal(
				t,
				stageTaskDurations[metric.Attributes["sparkAppTaskId"].(int)],
				metric.Value.Int(),
			)
		} else if metric.Name == "spark.app.stage.task.input.bytesRead" {
			assert.Equal(
				t,
				stageTaskInputBytesRead[
					metric.Attributes["sparkAppTaskId"].(int),
				],
				metric.Value.Int(),
			)
		} else if metric.Name ==
			"spark.app.stage.task.shuffle.read.push.corruptMergedBlockChunks" {
			assert.Equal(
				t,
				stageTaskShufflePushReadCorruptMergedBlockChunks[
					metric.Attributes["sparkAppTaskId"].(int),
				],
				metric.Value.Int(),
			)
		}
	}
}

func TestCollectSparkAppRDDMetrics_GetApplicationRDDsError(t *testing.T) {
	// Setup mock client
	mockClient := &MockSparkApiClient{}

	// Setup mock metrics decorator
	mockDecorator := &MockSparkMetricDecorator{}

	// Setup mock SparkApplication
	sparkApp := &SparkApplication{
		Id:   "app-123456",
		Name: "Test Spark App",
	}

	// Track if GetApplicationRDDs was called
	getApplicationRDDsCalled := false

	// Setup expected error message
	expectedError := "mock GetApplicationRDDs error"

	// Setup the client to verify the app, set the tracker, and return an error
	// from GetApplicationRDDs
	mockClient.GetApplicationRDDsFunc = func(
		ctx context.Context,
		app *SparkApplication,
	) ([]SparkRDD, error) {
		assert.Equal(t, sparkApp, app, "Should pass the correct application")
		getApplicationRDDsCalled = true

		return nil, errors.New(expectedError)
	}

	// Setup a metrics channel to receive metrics
	metricsChan := make(chan model.Metric, 100) // Buffer to prevent blocking

	// Setup mock tag map
	tags := map[string]string{
		"environment": "test",
		"cluster":     "spark-test",
	}

	// Execute the function under test
	err := collectSparkAppRDDMetrics(
		context.Background(),
		mockClient,
		sparkApp,
		"spark.",
		mockDecorator,
		tags,
		metricsChan,
	)

	// Close the channel to iterate through it
	close(metricsChan)

	// Collect all metrics to make sure the channel is empty
	metrics := make([]model.Metric, 0)
	for metric := range metricsChan {
		metrics = append(metrics, metric)
	}

	// Verify results
	assert.Error(t, err)
	assert.Equal(t, expectedError, err.Error())
	assert.True(
		t,
		getApplicationRDDsCalled,
		"GetApplicationRDDs should be called",
	)
	assert.Empty(t, metrics, "No metrics should be received")
}

func TestCollectSparkAppRDDMetrics(t *testing.T) {
	// Setup mock client
	mockClient := &MockSparkApiClient{}

	// Setup mock metrics decorator
	mockDecorator := &MockSparkMetricDecorator{}

	// Setup mock SparkApplication
	sparkApp := &SparkApplication{
		Id:   "app-123456",
		Name: "Test Spark App",
	}

	// Create mock rdds with mock values
	mockRDDs := []SparkRDD{
		{
			Id:                  1,
			Name:                "Test RDD 1",
			NumPartitions:       10,
			NumCachedPartitions: 8,
			MemoryUsed:          1048576, // 1MB
			DiskUsed:            524288,  // 512KB
			DataDistribution: []struct {
				MemoryUsed             int `json:"memoryUsed"`
				MemoryRemaining        int `json:"memoryRemaining"`
				DiskUsed               int `json:"diskUsed"`
				OnHeapMemoryUsed       int `json:"onHeapMemoryUsed"`
				OffHeapMemoryUsed      int `json:"offHeapMemoryUsed"`
				OnHeapMemoryRemaining  int `json:"onHeapMemoryRemaining"`
				OffHeapMemoryRemaining int `json:"offHeapMemoryRemaining"`
			}{
				{
					MemoryUsed:             262144,  // 256KB
					MemoryRemaining:        1048576, // 1MB
					DiskUsed:               262144,  // 256KB
					OnHeapMemoryUsed:       393216,  // 384KB
					OffHeapMemoryUsed:      131072,  // 128KB
					OnHeapMemoryRemaining:  1048576, // 1MB
					OffHeapMemoryRemaining: 524288,  // 512KB
				},
				{
					MemoryUsed:             524288,  // 512KB
					MemoryRemaining:        2097152, // 2MB
					DiskUsed:               262144,  // 256KB
					OnHeapMemoryUsed:       393216,  // 384KB
					OffHeapMemoryUsed:      131072,  // 128KB
					OnHeapMemoryRemaining:  2097152, // 2MB
					OffHeapMemoryRemaining: 1048576, // 1MB
				},
			},
			Partitions: []struct {
				BlockName  string   `json:"blockName"`
				MemoryUsed int      `json:"memoryUsed"`
				DiskUsed   int      `json:"diskUsed"`
				Executors  []string `json:"executors"`
			}{
				{
					BlockName:  "rdd_1_0",
					MemoryUsed: 131072, // 128KB
					DiskUsed:   65536,  // 64KB
					Executors:  []string{"executor-1"},
				},
				{
					BlockName:  "rdd_1_1",
					MemoryUsed: 65536, // 64KB
					DiskUsed:   65536, // 64KB
					Executors:  []string{"executor-1"},
				},
			},
		},
		{
			Id:                  2,
			Name:                "Test RDD 2",
			NumPartitions:       20,
			NumCachedPartitions: 15,
			MemoryUsed:          2097152, // 2MB
			DiskUsed:            1048576, // 1MB
			DataDistribution: []struct {
				MemoryUsed             int `json:"memoryUsed"`
				MemoryRemaining        int `json:"memoryRemaining"`
				DiskUsed               int `json:"diskUsed"`
				OnHeapMemoryUsed       int `json:"onHeapMemoryUsed"`
				OffHeapMemoryUsed      int `json:"offHeapMemoryUsed"`
				OnHeapMemoryRemaining  int `json:"onHeapMemoryRemaining"`
				OffHeapMemoryRemaining int `json:"offHeapMemoryRemaining"`
			}{
				{
					MemoryUsed:             1048576, // 1MB
					MemoryRemaining:        1048576, // 1MB
					DiskUsed:               524288,  // 512KB
					OnHeapMemoryUsed:       786432,  // 768KB
					OffHeapMemoryUsed:      262144,  // 256KB
					OnHeapMemoryRemaining:  1572864, // 1.5MB
					OffHeapMemoryRemaining: 524288,  // 512KB
				},
				{
					MemoryUsed:             3145728, // 3MB
					MemoryRemaining:        3145728, // 3MB
					DiskUsed:               524288,  // 512KB
					OnHeapMemoryUsed:       786432,  // 768KB
					OffHeapMemoryUsed:      262144,  // 256KB
					OnHeapMemoryRemaining:  3145728, // 3MB
					OffHeapMemoryRemaining: 1048576, // 1MB
				},
			},
			Partitions: []struct {
				BlockName  string   `json:"blockName"`
				MemoryUsed int      `json:"memoryUsed"`
				DiskUsed   int      `json:"diskUsed"`
				Executors  []string `json:"executors"`
			}{
				{
					BlockName:  "rdd_2_0",
					MemoryUsed: 1572864, // 1.5MB
					DiskUsed:   0,
					Executors:  []string{"executor-1"},
				},
				{
					BlockName:  "rdd_2_1",
					MemoryUsed: 786432, // 768KB
					DiskUsed:   0,
					Executors:  []string{"executor-1"},
				},
			},
		},
	}

	// Setup the client to return our mock RDDs
	mockClient.GetApplicationRDDsFunc = func(
		ctx context.Context,
		app *SparkApplication,
	) ([]SparkRDD, error) {
		assert.Equal(t, sparkApp, app, "Should pass the correct application")

		return mockRDDs, nil
	}

	// Setup a metrics channel to receive metrics
	metricsChan := make(chan model.Metric, 1000) // Buffer to prevent blocking

	// Setup mock tag map
	tags := map[string]string{
		"environment": "test",
		"cluster":     "spark-test",
	}

	// Execute the function under test
	err := collectSparkAppRDDMetrics(
		context.Background(),
		mockClient,
		sparkApp,
		"spark.",
		mockDecorator,
		tags,
		metricsChan,
	)

	// Close the channel to iterate through it
	close(metricsChan)

	// Collect all metrics for verification
	metrics := make([]model.Metric, 0)
	for metric := range metricsChan {
		metrics = append(metrics, metric)
	}

	// Verify results
	assert.NoError(t, err)
	assert.NotEmpty(t, metrics, "Should have received metrics")

	rddNames := map[int]string{
		1: "Test RDD 1",
        2: "Test RDD 2",
	}

	// Verify that all metrics have the expected attributes
	for _, metric := range metrics {
		assert.NotNil(t, metric.Attributes)

		// Verify basic attributes
		assert.Contains(t, metric.Attributes, "sparkAppId")
		assert.Equal(t, "app-123456", metric.Attributes["sparkAppId"])
		assert.Contains(t, metric.Attributes, "sparkAppName")
		assert.Equal(t, "Test Spark App", metric.Attributes["sparkAppName"])
		assert.Contains(t, metric.Attributes, "environment")
		assert.Equal(t, "test", metric.Attributes["environment"])
		assert.Contains(t, metric.Attributes, "cluster")
		assert.Equal(t, "spark-test", metric.Attributes["cluster"])

		// Verify workspace attributes
		assert.Contains(t, metric.Attributes, "databricksWorkspaceId")
		assert.Equal(t, int64(12345), metric.Attributes["databricksWorkspaceId"])
		assert.Contains(t, metric.Attributes, "databricksWorkspaceName")
		assert.Equal(
			t,
			"foo.fakedomain.local",
			metric.Attributes["databricksWorkspaceName"],
		)
		assert.Contains(t, metric.Attributes, "databricksWorkspaceUrl")
		assert.Equal(
			t,
			"https://foo.fakedomain.local",
			metric.Attributes["databricksWorkspaceUrl"],
		)

		// Verify RDD attributes
		assert.Contains(t, metric.Attributes, "sparkAppRDDId")
		assert.Contains(
			t,
			[]int{1, 2},
			metric.Attributes["sparkAppRDDId"],
		)
		assert.Contains(t, metric.Attributes, "sparkAppRDDName")
		assert.Equal(
            t,
            rddNames[metric.Attributes["sparkAppRDDId"].(int)],
            metric.Attributes["sparkAppRDDName"],
        )

		// Verify metric name format
		assert.True(
			t,
			strings.HasPrefix(metric.Name, "spark.app.storage.rdd."),
			"Metric name should start with 'spark.app.storage.rdd.' but found: %s",
			metric.Name,
		)
	}

	// Verify a few metrics to ensure values are correctly passed

	rddMemoryUsed := map[int]int64{
		1: 1048576, // 1MB
		2: 2097152, // 2MB
	}

	rddDistributionMemoryUsed := map[int]map[int]int64{
		1: {0: 262144, 1: 524288},
		2: {0: 1048576, 1: 3145728},
	}

	rddPartitionMemoryUsed := map[int]map[string]int64{
		1: {"rdd_1_0": 131072, "rdd_1_1": 65536},
		2: {"rdd_2_0": 1572864, "rdd_2_1": 786432},
	}

	// Verify a few metrics to ensure values are correctly passed
	for _, metric := range metrics {
		if metric.Name == "spark.app.storage.rdd.memory.used" {
			assert.Equal(
				t,
				rddMemoryUsed[metric.Attributes["sparkAppRDDId"].(int)],
				metric.Value.Int(),
			)
		} else if strings.HasPrefix(
			metric.Name,
			"spark.app.storage.rdd.distribution.",
		) {
			// Verify RDD distribution attributes
			assert.Contains(
				t,
				metric.Attributes,
				"sparkAppRddDistributionIndex",
			)

			if metric.Name == "spark.app.storage.rdd.distribution.memory.used" {
				assert.Equal(
					t,
					rddDistributionMemoryUsed[
						metric.Attributes["sparkAppRDDId"].(int),
					][metric.Attributes["sparkAppRddDistributionIndex"].(int)],
					metric.Value.Int(),
				)
			}
		} else if strings.HasPrefix(
			metric.Name,
			"spark.app.storage.rdd.partition.",
		) {
			// Verify RDD partition attributes
			assert.Contains(
				t,
				metric.Attributes,
				"sparkAppRddPartitionBlockName",
			)

			if metric.Name == "spark.app.storage.rdd.partition.memory.used" {
				assert.Equal(
					t,
					rddPartitionMemoryUsed[
						metric.Attributes["sparkAppRDDId"].(int),
					][
						metric.Attributes["sparkAppRddPartitionBlockName"].(string),
					],
					metric.Value.Int(),
				)
			}
		}
	}
}
