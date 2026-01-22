package spark

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

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

	// Set the databricks cluster ID
	viper.Set("spark.databricks.clusterId", "fake-cluster-id")

	// Setup up mock workspace
	_, teardown := setupMockWorkspace()
	defer teardown()

	// Setup mock cluster info
	teardown2 := setupMockClusterInfo()
	defer teardown2()

	// Setup mock integration
	mockIntegration := &integration.LabsIntegration{
		Interval: 60,
	}

	// Setup mock Spark API client
	mockClient := &MockSparkApiClient{}

	// Setup mock tag map
	tags := map[string]string{"env": "test"}

	// Mock Now
	now := time.Now()
	Now = func () time.Time { return now }

	// Execute the function under test
	receiver, err := newSparkMetricsReceiver(
		context.Background(),
		mockIntegration,
		mockClient,
		tags,
	)

	// Verify results
	assert.NoError(t, err)
	assert.NotNil(t, receiver)

	sparkReceiver, ok := receiver.(*SparkMetricsReceiver)
	assert.True(t, ok, "Should return a *SparkMetricsReceiver")
	assert.Equal(t, mockIntegration, sparkReceiver.i)
	assert.Equal(t, mockClient, sparkReceiver.client)
	assert.Equal(t, ClusterManagerTypeDatabricks, sparkReceiver.clusterManager)
	assert.NotNil(t, sparkReceiver.eventDecorator)
	assert.Equal(t, now.UTC(), sparkReceiver.lastRun)
	assert.IsType(
		t,
		&DatabricksSparkEventDecorator{},
		sparkReceiver.eventDecorator,
		"Should return a DatabricksSparkEventDecorator",
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
	mockClient := &MockSparkApiClient{}

	// Setup mock tag map
	tags := map[string]string{"env": "test"}

	// Execute the function under test
	receiver, err := newSparkMetricsReceiver(
		context.Background(),
		mockIntegration,
		mockClient,
		tags,
	)

	// Verify results
	assert.Error(t, err)
	assert.Nil(t, receiver)
	assert.Equal(t, "invalid clusterManager type", err.Error())
}

func TestNewSparkMetricsReceiver_MissingDatabricksClusterIdError(t *testing.T) {
	// Reset viper config to ensure clean test state
	viper.Reset()

	// Set the cluster manager type to databricks
	viper.Set("spark.clusterManager", "databricks")

	// Do not set the databricks cluster ID to simulate missing the ID from the
	// config

	// Setup mock integration
	mockIntegration := &integration.LabsIntegration{
		Interval: 60,
	}

	// Setup mock Spark API client
	mockClient := &MockSparkApiClient{}

	// Setup mock tag map
	tags := map[string]string{"env": "test"}

	// Execute the function under test
	receiver, err := newSparkMetricsReceiver(
		context.Background(),
		mockIntegration,
		mockClient,
		tags,
	)

	// Verify results
	assert.Error(t, err)
	assert.Nil(t, receiver)
	assert.Equal(t, "missing databricks cluster ID", err.Error())
}

func TestNewSparkMetricsReceiver_NewDatabricksWorkspaceError(t *testing.T) {
	// Reset viper config to ensure clean test state
	viper.Reset()

	// Set the cluster manager type to databricks
	viper.Set("spark.clusterManager", "databricks")

	// Set the databricks cluster ID
	viper.Set("spark.databricks.clusterId", "fake-cluster-id")

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
	mockClient := &MockSparkApiClient{}

	// Setup mock tag map
	tags := map[string]string{"env": "test"}

	// Execute the function under test
	receiver, err := newSparkMetricsReceiver(
		context.Background(),
		mockIntegration,
		mockClient,
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
	mockClient := &MockSparkApiClient{}

	// Setup mock tag map
	tags := map[string]string{"env": "test"}

	// Mock Now
	now := time.Now()
	Now = func () time.Time { return now }

	// Execute the function under test
	receiver, err := newSparkMetricsReceiver(
		context.Background(),
		mockIntegration,
		mockClient,
		tags,
	)

	// Verify results
	assert.NoError(t, err)
	assert.NotNil(t, receiver)

	sparkReceiver, ok := receiver.(*SparkMetricsReceiver)
	assert.True(t, ok, "Should return a *SparkMetricsReceiver")
	assert.Equal(t, mockIntegration, sparkReceiver.i)
	assert.Equal(t, mockClient, sparkReceiver.client)
	assert.Equal(t, ClusterManagerTypeStandalone, sparkReceiver.clusterManager)
	assert.Nil(t, sparkReceiver.eventDecorator)
	assert.Equal(t, now.UTC(), sparkReceiver.lastRun)
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
	mockClient := &MockSparkApiClient{}

	// Setup mock tag map
	tags := map[string]string{
		"environment": "production",
		"region":      "us-west",
		"datacenter":  "dc1",
		"team":        "platform",
	}

	// Mock Now
	now := time.Now()
	Now = func () time.Time { return now }

	// Execute the function under test
	receiver, err := newSparkMetricsReceiver(
		context.Background(),
		mockIntegration,
		mockClient,
		tags,
	)

	// Verify results
	assert.NoError(t, err)
	assert.NotNil(t, receiver)

	sparkReceiver, ok := receiver.(*SparkMetricsReceiver)
	assert.True(t, ok, "Should return a *SparkMetricsReceiver")
	assert.Equal(t, mockIntegration, sparkReceiver.i)
	assert.Equal(t, mockClient, sparkReceiver.client)
	assert.Equal(t, ClusterManagerTypeStandalone, sparkReceiver.clusterManager)
	assert.Nil(t, sparkReceiver.eventDecorator)
	assert.Equal(t, now.UTC(), sparkReceiver.lastRun)
	assert.Equal(t, tags, sparkReceiver.tags)
	assert.Equal(t, 4, len(sparkReceiver.tags), "Should have exactly 4 tags")
	assert.Equal(t, "production", sparkReceiver.tags["environment"])
	assert.Equal(t, "us-west", sparkReceiver.tags["region"])
	assert.Equal(t, "dc1", sparkReceiver.tags["datacenter"])
	assert.Equal(t, "platform", sparkReceiver.tags["team"])
}

func TestSparkMetricsReceiver_GetId(t *testing.T) {
	// Setup mock integration
	mockIntegration := &integration.LabsIntegration{
		Interval: 60,
	}

	// Setup mock Spark API client
	mockClient := &MockSparkApiClient{}

	// Setup mock tag map
	tags := map[string]string{"env": "test"}

	// Create metrics receiver instance using the mock integration, client, and
	// tags
	receiver, err := newSparkMetricsReceiver(
		context.Background(),
		mockIntegration,
		mockClient,
		tags,
	)

	// There should not be any error since we are defaulting the clusterManager
	// and using a mock workspace, but verify just to be safe.
	assert.NoError(t, err)

	// Execute the function under test
	id := receiver.GetId()

	// Verify result
	assert.Equal(t, "spark-metrics-receiver", id)
}

func TestPollEvents_GetApplicationsError(t *testing.T) {
	// Setup mock integration
	mockIntegration := &integration.LabsIntegration{
		Interval: 60,
	}

	// Setup mock Spark API client
	mockClient := &MockSparkApiClient{}

	// Setup mock tag map
	tags := map[string]string{"env": "test"}

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

	// Setup an event channel to receive metrics
	eventsChan := make(chan model.Event, 100) // Buffer to prevent blocking

	// Create the metrics receiver instance using the mock integration, client,
	// and tags
	receiver, err := newSparkMetricsReceiver(
		context.Background(),
        mockIntegration,
        mockClient,
        tags,
	)

	// There should not be any error since we are defaulting the clusterManager
	// and using a mock workspace, but verify just to be safe
	assert.NoError(t, err)

	// Execute the function under test
	err = receiver.PollEvents(context.Background(), eventsChan)

	// Close the channel to iterate through it
	close(eventsChan)

	// Collect all events to make sure the channel is empty
	events := make([]model.Event, 0)
	for event := range eventsChan {
		events = append(events, event)
	}

	// Verify result
	assert.Error(t, err)
	assert.Equal(t, expectedError, err.Error())
	assert.True(t, getApplicationsCalled, "GetApplications should be called")
	assert.Empty(t, events, "No events should be received")
}

func TestPollEvents_CollectExecutorMetricsError(t *testing.T) {
	// Setup mock integration
	mockIntegration := &integration.LabsIntegration{
		Interval: 60,
	}

	// Setup mock Spark API client
	mockClient := &MockSparkApiClient{}

	// Setup mock tag map
	tags := map[string]string{"env": "test"}

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

	// Setup metrics channel to receive metrics
	eventsChan := make(chan model.Event, 500) // Buffer to prevent blocking

	// Create the metrics receiver instance using the mock integration, client,
	// and tags
	receiver, err := newSparkMetricsReceiver(
		context.Background(),
        mockIntegration,
        mockClient,
        tags,
	)

	// There should not be any error since we are defaulting the clusterManager,
	// but verify just to be safe
	assert.NoError(t, err)

	// Execute the function under test
	err = receiver.PollEvents(context.Background(), eventsChan)

	// Close the channel
	close(eventsChan)

	// Verify results
	assert.Error(t, err)
    assert.Equal(t, expectedError, err.Error())
    assert.True(t, getApplicationsCalled, "GetApplications should be called")
    assert.True(t, getExecutorsCalled, "GetApplicationExecutors should be called")
}

func TestPollEvents_CollectJobMetricsError(t *testing.T) {
	// Setup mock integration
	mockIntegration := &integration.LabsIntegration{
		Interval: 60,
	}

	// Setup mock Spark API client
	mockClient := &MockSparkApiClient{}

	// Setup mock tag map
	tags := map[string]string{"env": "test"}

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

	// Setup metrics channel to receive metrics
	eventsChan := make(chan model.Event, 500) // Buffer to prevent blocking

	// Create the metrics receiver instance using the mock integration, client,
	// and tags
	receiver, err := newSparkMetricsReceiver(
		context.Background(),
        mockIntegration,
        mockClient,
        tags,
	)

	// There should not be any error since we are defaulting the clusterManager,
	// but verify just to be safe
	assert.NoError(t, err)

	// Execute the function under test
	err = receiver.PollEvents(context.Background(), eventsChan)

	// Close the channel
	close(eventsChan)

	// Verify results
	assert.Error(t, err)
    assert.Equal(t, expectedError, err.Error())
    assert.True(t, getApplicationsCalled, "GetApplications should be called")
    assert.True(t, getJobsCalled, "GetApplicationJobs should be called")
}

func TestPollEvents_CollectStageMetricsError(t *testing.T) {
	// Setup mock integration
	mockIntegration := &integration.LabsIntegration{
		Interval: 60,
	}

	// Setup mock Spark API client
	mockClient := &MockSparkApiClient{}

	// Setup mock tag map
	tags := map[string]string{"env": "test"}

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

	// Setup metrics channel to receive metrics
	eventsChan := make(chan model.Event, 500) // Buffer to prevent blocking

	// Create the metrics receiver instance using the mock integration, client,
	// and tags
	receiver, err := newSparkMetricsReceiver(
		context.Background(),
        mockIntegration,
        mockClient,
        tags,
	)

	// There should not be any error since we are defaulting the clusterManager,
	// but verify just to be safe
	assert.NoError(t, err)

	// Execute the function under test
	err = receiver.PollEvents(context.Background(), eventsChan)

	// Close the channel
	close(eventsChan)

	// Verify results
	assert.Error(t, err)
    assert.Equal(t, expectedError, err.Error())
    assert.True(t, getApplicationsCalled, "GetApplications should be called")
    assert.True(t, getStagesCalled, "GetApplicationStages should be called")
}

func TestPollEvents_CollectRDDMetricsError(t *testing.T) {
	// Setup mock integration
	mockIntegration := &integration.LabsIntegration{
		Interval: 60,
	}

	// Setup mock Spark API client
	mockClient := &MockSparkApiClient{}

	// Setup mock tag map
	tags := map[string]string{"env": "test"}

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

	// Setup metrics channel to receive metrics
	eventsChan := make(chan model.Event, 500) // Buffer to prevent blocking

	// Create the metrics receiver instance using the mock integration, client,
	// and tags
	receiver, err := newSparkMetricsReceiver(
		context.Background(),
        mockIntegration,
        mockClient,
        tags,
	)

	// There should not be any error since we are defaulting the clusterManager,
	// but verify just to be safe
	assert.NoError(t, err)

	// Execute the function under test
	err = receiver.PollEvents(context.Background(), eventsChan)

	// Close the channel
	close(eventsChan)

	// Verify results
	assert.Error(t, err)
    assert.Equal(t, expectedError, err.Error())
    assert.True(t, getApplicationsCalled, "GetApplications should be called")
    assert.True(t, getRDDsCalled, "GetApplicationRDDs should be called")
}

func TestPollEvents(t *testing.T) {
	// Reset viper config to ensure clean test state
	viper.Reset()

	// Set the cluster manager type to databricks so we use the
	// DatabricksSparkEventDecorator
	viper.Set("spark.clusterManager", "databricks")

	// Set the databricks cluster ID
	viper.Set("spark.databricks.clusterId", "fake-cluster-id")

	// Setup up mock workspace
	_, teardown := setupMockWorkspace()
	defer teardown()

	// Setup mock cluster info
	teardown2 := setupMockClusterInfo()
	defer teardown2()

	// Setup mock integration
	mockIntegration := &integration.LabsIntegration{
		Interval: 60,
	}

	// Setup mock Spark API client
	mockClient := &MockSparkApiClient{}

	// Setup mock tag map
	tags := map[string]string{
		"environment": "test",
	}

	// Mock Now
	now := time.Now()
	Now = func () time.Time { return now }

	// Setup mock SparkApplications
	sparkApps := []SparkApplication{
		{
			Id:   "app-123456",
			Name: "Test Spark App",
		},
	}

	// Setup mock SparkExecutor
	sparkExecutor := SparkExecutor{Id: "exec-1", MemoryUsed: 1024}

	// Setup job/stage/task times
	jobSubmissionTime := now.UTC().Add(10 * time.Second)
	stageFirstTaskLaunchedTime := now.UTC().Add(30 * time.Second)
	taskLaunchTime := now.UTC().Add(30 * time.Second)

	// Setup mock SparkJob
	sparkJob := SparkJob{
		JobId: 1,
		SubmissionTime: jobSubmissionTime.Format(RFC3339Milli),
	}

	// Setup mock SparkStage
	sparkStage := SparkStage{
		StageId: 1,
		FirstTaskLaunchedTime: stageFirstTaskLaunchedTime.Format(RFC3339Milli),
		Tasks: map[string]SparkTask{
			"0": {
				TaskId: 0,
				// RUNNING is used here to ensure a completion event is not
				// generated
				Status: "RUNNING",
				LaunchTime: taskLaunchTime.Format(RFC3339Milli),
			},
			"1": {
				TaskId: 1,
				// RUNNING is used here to ensure a completion event is not
				// generated
				Status: "RUNNING",
				LaunchTime: taskLaunchTime.Format(RFC3339Milli),
			},
		},
	}

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

	// Setup metrics channel to receive metrics
	eventsChan := make(chan model.Event, 500) // Buffer to prevent blocking

	// Create the metrics receiver instance using the mock integration, client,
	// and tags
	receiver, err := newSparkMetricsReceiver(
		context.Background(),
        mockIntegration,
        mockClient,
        tags,
	)

	// There should not be any error since we are using a mock workspace, but
	// verify just to be safe
	assert.NoError(t, err)

	// Verify that the initial lastRun timestamp is set correctly
	sparkReceiver, ok := receiver.(*SparkMetricsReceiver)
	assert.True(t, ok, "Should return a *SparkMetricsReceiver")
	assert.Equal(t, now.UTC(), sparkReceiver.lastRun)

	// Now reset the mock Now function to simulate time progression
	now2 := now.Add(time.Minute)
	Now = func () time.Time { return now2 }

	// Execute the function under test
	err = receiver.PollEvents(context.Background(), eventsChan)

	// Close the channel to iterate through it
	close(eventsChan)

	// Collect all metrics for verification
	events := make([]model.Event, 0)
	for event := range eventsChan {
		events = append(events, event)
	}

	// Verify results
	assert.NoError(t, err)
	assert.NotEmpty(t, events, "Should have received events")
	assert.Equal(t, 6, len(events), "Should have received 6 events")
	assert.True(t, getApplicationsCalled, "GetApplications should be called")
	assert.True(
		t,
		getExecutorsCalled,
		"GetApplicationExecutors should be called",
	)
	assert.True(t, getJobsCalled, "GetApplicationJobs should be called")
	assert.True(t, getStagesCalled, "GetApplicationStages should be called")
	assert.True(t, getRDDsCalled, "GetApplicationRDDs should be called")

	// Verify that the lastRun timestamp is updated correctly
	assert.Equal(t, now2.UTC(), sparkReceiver.lastRun)

	validEventTypes := []string{
		"SparkExecutorSample",
		"SparkJob",
		"SparkStage",
		"SparkTask",
		"SparkRDDSample",
	}

	// Verify one metric from each collect* call
	foundSparkExecutorSample := false
	foundSparkJob := false
	foundSparkStage := false
	foundSparkTask0 := false
	foundSparkTask1 := false
	foundSparkTaskOther := false
	foundSparkRDD := false

	// Verify that all events have the expected attributes
	for _, event := range events {
		assert.NotNil(t, event.Attributes)

		// Verify basic attributes
		assert.Contains(t, event.Attributes, "sparkAppId")
		assert.Equal(t, "app-123456", event.Attributes["sparkAppId"])
		assert.Contains(t, event.Attributes, "sparkAppName")
		assert.Equal(t, "Test Spark App", event.Attributes["sparkAppName"])
		assert.Contains(t, event.Attributes, "environment")
		assert.Equal(t, "test", event.Attributes["environment"])

		// Verify workspace attributes
		assert.Contains(t, event.Attributes, "databricksWorkspaceId")
		assert.Equal(t, int64(12345), event.Attributes["databricksWorkspaceId"])
		assert.Contains(t, event.Attributes, "databricksWorkspaceName")
		assert.Equal(
			t,
			"foo.fakedomain.local",
			event.Attributes["databricksWorkspaceName"],
		)
		assert.Contains(t, event.Attributes, "databricksWorkspaceUrl")
		assert.Equal(
			t,
			"https://foo.fakedomain.local",
			event.Attributes["databricksWorkspaceUrl"],
		)

		// Verify cluster attributes
		assert.Contains(t, event.Attributes, "databricksClusterId")
		assert.Equal(
			t,
			"fake-cluster-id",
			event.Attributes["databricksClusterId"],
		)
		assert.Contains(t, event.Attributes, "databricksClusterName")
		assert.Equal(
			t,
			"fake-cluster-name",
			event.Attributes["databricksClusterName"],
		)
		assert.Contains(t, event.Attributes, "databricksclustername")
		assert.Equal(
			t,
			"fake-cluster-name",
			event.Attributes["databricksclustername"],
		)
		assert.Contains(t, event.Attributes, "databricksClusterSource")
		assert.Equal(
			t,
			"fake-cluster-source",
			event.Attributes["databricksClusterSource"],
		)
		assert.Contains(t, event.Attributes, "databricksClusterInstancePoolId")
		assert.Equal(
			t,
			"fake-cluster-instance-pool-id",
			event.Attributes["databricksClusterInstancePoolId"],
		)

		// Verify timestamp
		assert.Equal(
			t,
			now2.UnixMilli(),
			event.Timestamp,
			"Should have the correct event timestamp",
		)

		// Verify event type
		assert.Contains(t, validEventTypes, event.Type)

		if event.Type == "SparkExecutorSample" {
			// Verify SparkExecutorSample events have expected attributes
			foundSparkExecutorSample = true

			assert.Contains(t, event.Attributes, "executorId")
			assert.Equal(t, "exec-1", event.Attributes["executorId"])
			assert.Contains(t, event.Attributes, "memoryUsedBytes")
			assert.Equal(
				t,
				1024,
				event.Attributes["memoryUsedBytes"],
			)
		} else if event.Type == "SparkJob" {
			// Verify SparkJob events have expected attributes
			foundSparkJob = true

			assert.Contains(t, event.Attributes, "jobId")
			assert.Equal(
				t,
				1,
				event.Attributes["jobId"],
			)
			assert.Contains(t, event.Attributes, "submissionTime")
			assert.Equal(
                t,
                jobSubmissionTime.UnixMilli(),
                event.Attributes["submissionTime"],
            )
		} else if event.Type == "SparkStage" {
			// Verify SparkStage events have expected attributes
			foundSparkStage = true

			assert.Contains(t, event.Attributes, "stageId")
			assert.Equal(
                t,
                1,
                event.Attributes["stageId"],
            )
			assert.Contains(t, event.Attributes, "firstTaskLaunchedTime")
			assert.Equal(
                t,
                stageFirstTaskLaunchedTime.UnixMilli(),
                event.Attributes["firstTaskLaunchedTime"],
            )
		} else if event.Type == "SparkTask" {
			// Verify SparkTask events have expected attributes
			assert.Contains(t, event.Attributes, "taskId")

			if event.Attributes["taskId"] == 0 {
				foundSparkTask0 = true
			} else if event.Attributes["taskId"] == 1 {
				foundSparkTask1 = true
			} else {
				foundSparkTaskOther = true
			}

			assert.Contains(t, event.Attributes, "stageId")
			assert.Equal(
                t,
                1,
                event.Attributes["stageId"],
            )
		} else if event.Type == "SparkRDDSample" {
			// Verify SparkRDD events have expected attributes
			foundSparkRDD = true

			assert.Contains(t, event.Attributes, "rddId")
			assert.Equal(t, 1, event.Attributes["rddId"])
			assert.Contains(t, event.Attributes, "partitionCount")
			assert.Equal(
                t,
                3,
                event.Attributes["partitionCount"],
            )
		}
	}

	assert.True(
		t,
		foundSparkExecutorSample,
		"Should have found executor SparkExecutorSample event",
	)
	assert.True(
		t,
		foundSparkJob,
		"Should have found job SparkJob event",
	)
	assert.True(
		t,
		foundSparkStage,
		"Should have found stage SparkStage event",
	)
	assert.True(
		t,
		foundSparkTask0,
		"Should have found SparkTask for task 0",
	)
	assert.True(
		t,
		foundSparkTask1,
		"Should have found SparkTask for task 1",
	)
	assert.False(
		t,
		foundSparkTaskOther,
		"Should not have found other SparkTask events",
	)
	assert.True(
		t,
		foundSparkRDD,
		"Should have found SparkRDDSample event",
	)
}

func TestCollectSparkAppExecutorMetrics_GetApplicationExecutorsError(
	t *testing.T,
) {
	// Setup mock Spark API client
	mockClient := &MockSparkApiClient{}

	// Setup mock metrics decorator
	mockDecorator := &MockSparkEventDecorator{}

	// Setup mock tag map
	tags := map[string]string{
		"environment": "test",
		"cluster":     "spark-test",
	}

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
	eventsChan := make(chan model.Event, 100) // Buffer to prevent blocking

	// Execute the function under test
	err := collectSparkAppExecutorMetrics(
		context.Background(),
		mockClient,
		sparkApp,
		mockDecorator,
		tags,
		eventsChan,
	)

	// Close the channel to iterate through it
	close(eventsChan)

	// Collect all events to make sure the channel is empty
	events := make([]model.Event, 0)
	for event := range eventsChan {
		events = append(events, event)
	}

	// Verify results
	assert.Error(t, err)
	assert.Equal(t, expectedError, err.Error())
	assert.True(t, getApplicationsCalled, "GetApplications should be called")
	assert.Empty(t, events, "No events should be received")
}

func TestCollectSparkAppExecutorMetrics(t *testing.T) {
	// Setup mock Spark API client
	mockClient := &MockSparkApiClient{}

	// Setup mock metrics decorator
	mockDecorator := &MockSparkEventDecorator{}

	// Setup mock tag map
	tags := map[string]string{
		"environment": "test",
	}

	// Mock Now
	now := time.Now()
	Now = func () time.Time { return now }

	// Setup mock SparkApplication
	sparkApp := &SparkApplication{
		Id:   "app-123456",
		Name: "Test Spark App",
	}

	// Setup add time
	addTime := now.UTC()
	addTimeFormatted := addTime.Format(RFC3339Milli)

	// Setup mock executors
	mockExecutors := []SparkExecutor{
		{
			Id:                "executor-1",
			IsActive:          true,
			IsExcluded:        false,
			IsBlacklisted:     false,
			AddTime:           addTimeFormatted,
			RddBlocks:         10,
			MemoryMetrics: SparkExecutorMemoryMetrics{
				UsedOnHeapStorageMemory:   512,
			},
			PeakMemoryMetrics: SparkExecutorPeakMemoryMetrics{
				JVMHeapMemory:          2048,
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

	// Setup an events channel to receive events
	eventsChan := make(chan model.Event, 1000) // Buffer to prevent blocking

	// Execute the function under test
	err := collectSparkAppExecutorMetrics(
		context.Background(),
		mockClient,
		sparkApp,
		mockDecorator,
		tags,
		eventsChan,
	)

	// Close the channel to iterate through it
	close(eventsChan)

	// Collect all events for verification
	events := make([]model.Event, 0)
	for event := range eventsChan {
		events = append(events, event)
	}

	// Verify results
	assert.NoError(t, err)
	assert.NotEmpty(t, events, "Should have received events")

	// Verify that all events have the expected attributes
	for _, event := range events {
		// Verify timestamp
		assert.Equal(
			t,
			now.UnixMilli(),
			event.Timestamp,
			"Should have the correct event timestamp",
		)

		// Verify event type
		assert.Equal(t, "SparkExecutorSample", event.Type)

		// Verify attributes
		assert.NotNil(t, event.Attributes)

		// Verify basic attributes
		assert.Contains(t, event.Attributes, "sparkAppId")
		assert.Equal(t, "app-123456", event.Attributes["sparkAppId"])
		assert.Contains(t, event.Attributes, "sparkAppName")
		assert.Equal(t, "Test Spark App", event.Attributes["sparkAppName"])
		assert.Contains(t, event.Attributes, "environment")
		assert.Equal(t, "test", event.Attributes["environment"])

		// Verify workspace attributes
		assert.Contains(t, event.Attributes, "databricksWorkspaceId")
		assert.Equal(t, int64(12345), event.Attributes["databricksWorkspaceId"])
		assert.Contains(t, event.Attributes, "databricksWorkspaceName")
		assert.Equal(
			t,
			"foo.fakedomain.local",
			event.Attributes["databricksWorkspaceName"],
		)
		assert.Contains(t, event.Attributes, "databricksWorkspaceUrl")
		assert.Equal(
			t,
			"https://foo.fakedomain.local",
			event.Attributes["databricksWorkspaceUrl"],
		)

		// Verify cluster attributes
		assert.Contains(t, event.Attributes, "databricksClusterId")
		assert.Equal(
			t,
			"fake-cluster-id",
			event.Attributes["databricksClusterId"],
		)
		assert.Contains(t, event.Attributes, "databricksClusterName")
		assert.Equal(
			t,
			"fake-cluster-name",
			event.Attributes["databricksClusterName"],
		)
		assert.Contains(t, event.Attributes, "databricksclustername")
		assert.Equal(
			t,
			"fake-cluster-name",
			event.Attributes["databricksclustername"],
		)
		assert.Contains(t, event.Attributes, "databricksClusterSource")
		assert.Equal(
			t,
			"fake-cluster-source",
			event.Attributes["databricksClusterSource"],
		)
		assert.Contains(t, event.Attributes, "databricksClusterInstancePoolId")
		assert.Equal(
			t,
			"fake-cluster-instance-pool-id",
			event.Attributes["databricksClusterInstancePoolId"],
		)

		// Verify a few executor attributes to ensure values are correctly
		// passed

		assert.Contains(t, event.Attributes, "executorId")
		assert.Equal(t, "executor-1", event.Attributes["executorId"])

		assert.Contains(t, event.Attributes, "rddBlockCount")
		assert.Equal(t, 10, event.Attributes["rddBlockCount"])

		assert.Contains(t, event.Attributes, "onHeapMemoryUsedBytes")
		assert.Equal(
			t,
			512,
			event.Attributes["onHeapMemoryUsedBytes"],
		)

		assert.Contains(t, event.Attributes, "peakJvmHeapMemoryUsedBytes")
		assert.Equal(
			t,
			2048,
			event.Attributes["peakJvmHeapMemoryUsedBytes"],
		)

		assert.Contains(t, event.Attributes, "isActive")
		assert.Equal(
			t,
			true,
			event.Attributes["isActive"],
		)

		assert.Contains(t, event.Attributes, "isExcluded")
		assert.Equal(
			t,
			false,
			event.Attributes["isExcluded"],
		)

		assert.Contains(t, event.Attributes, "isBlacklisted")
		assert.Equal(
			t,
			false,
			event.Attributes["isBlacklisted"],
		)

		assert.Contains(t, event.Attributes, "addTime")
		assert.Equal(
			t,
			addTime.UnixMilli(),
			event.Attributes["addTime"],
		)
	}
}

func TestCollectSparkAppJobMetrics_GetApplicationJobsError(t *testing.T) {
	// Setup mock client
	mockClient := &MockSparkApiClient{}

	// Setup mock metrics decorator
	mockDecorator := &MockSparkEventDecorator{}

	// Setup mock tag map
	tags := map[string]string{
		"environment": "test",
		"cluster":     "spark-test",
	}

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
	eventsChan := make(chan model.Event, 100) // Buffer to prevent blocking

	// Execute the function under test
	_, err := collectSparkAppJobMetrics(
		context.Background(),
		mockClient,
		sparkApp,
		mockDecorator,
		time.Now().UTC(),
		tags,
		eventsChan,
	)

	// Close the channel to iterate through it
	close(eventsChan)

	// Collect all events to make sure the channel is empty
	events := make([]model.Event, 0)
	for event := range eventsChan {
		events = append(events, event)
	}

	// Verify results
	assert.Error(t, err)
	assert.Equal(t, expectedError, err.Error())
	assert.True(
		t,
		getApplicationJobsCalled,
		"GetApplicationJobs should be called",
	)
	assert.Empty(t, events, "No events should be received")
}

func TestCollectSparkAppJobMetrics(t *testing.T) {
	// Setup mock client
	mockClient := &MockSparkApiClient{}

	// Setup mock metrics decorator
	mockDecorator := &MockSparkEventDecorator{}

	// Setup mock tag map
	tags := map[string]string{
		"environment": "test",
	}

	// Mock Now
	now := time.Now()
	Now = func () time.Time { return now }

	// Setup times to be used to test behavior of event production based on
	// lastRun time.
	lastRun := now.UTC()
	pastTime := lastRun.Add(-15 * time.Second)
	pastTimeFormatted := pastTime.Format(RFC3339Milli)
	futureTime := lastRun.Add(15 * time.Second)
	futureTimeFormatted := futureTime.Format(RFC3339Milli)

	// Setup mock SparkApplication
	sparkApp := &SparkApplication{
		Id:   "app-123456",
		Name: "Test Spark App",
	}

	// Setup mock jobs
	mockJobs := []SparkJob{
		{
			JobId:               120,
			Name:                "Test Job Submit Before / Complete Before",
			Description:         "",
			// This job should produce no events
			SubmissionTime:      pastTimeFormatted,
			CompletionTime:      pastTimeFormatted,
			Status:              "succeeded",
			NumCompletedTasks:   95,
			NumCompletedIndices: 95,
			NumCompletedStages:  3,
		},
		{
			JobId:               121,
			Name:                "Test Job Submit Before / Complete Empty",
			Description:         "",
			// This job should produce no events
			SubmissionTime:      pastTimeFormatted,
			CompletionTime:      "",
			Status:              "running",
			NumCompletedTasks:   95,
			NumCompletedIndices: 95,
			NumCompletedStages:  3,
		},
		{
			JobId:               122,
			Name:                "Test Job Submit Before / Complete After",
			Description:         "This is test job 122",
			// This job should produce one complete event
			SubmissionTime:      pastTimeFormatted,
			CompletionTime:      futureTimeFormatted,
			Status:              "succeeded",
			JobGroup:			 "group1",
			JobTags: 		     []string{"tag1", "tag2"},
			NumCompletedTasks:   95,
			NumCompletedIndices: 95,
			NumCompletedStages:  3,
		},
		{
			JobId:               123,
			Name:                "Test Job Submit After / Complete Empty",
			Description:         "This is test job 123",
			// This job should produce one submit event
			SubmissionTime:      futureTimeFormatted,
			CompletionTime:      "",
			Status:              "running",
			JobGroup:			 "group2",
			JobTags: 		     []string{"tag3", "tag4"},
			NumCompletedTasks:   95,
			NumCompletedIndices: 95,
			NumCompletedStages:  3,
		},
		{
			JobId:               124,
			Name:                "Test Job Submit After / Complete After",
			Description:         "This is test job 124",
			// This job should produce one submit event and one complete event
			SubmissionTime:      futureTimeFormatted,
			CompletionTime:      futureTimeFormatted,
			Status:              "failed",
			JobGroup:			 "group3",
			JobTags: 		     []string{"tag5", "tag6"},
			NumCompletedTasks:   5,
			NumCompletedIndices: 10,
			NumCompletedStages:  5,
		},
		{
			JobId:               125,
			Name:                "Test Job Invalid Submission Time",
			// This job should produce no events
			SubmissionTime:      "invalid_submission_time",
			CompletionTime:      futureTimeFormatted,
			Status:              "succeeded",
			NumCompletedTasks:   100,
			NumCompletedIndices: 45,
			NumCompletedStages:  7,
		},
		{
			JobId:               126,
			Name:                "Test Job Invalid Completion Time",
			// This job should produce no events
			SubmissionTime:      pastTimeFormatted,
			CompletionTime:      "invalid_completion_time",
			Status:              "succeeded",
			NumCompletedTasks:   100,
			NumCompletedIndices: 45,
			NumCompletedStages:  7,
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

	// Setup an events channel to receive events
	eventsChan := make(chan model.Event, 1000) // Buffer to prevent blocking

	// Execute the function under test
	completedJobs, err := collectSparkAppJobMetrics(
		context.Background(),
		mockClient,
		sparkApp,
		mockDecorator,
		lastRun,
		tags,
		eventsChan,
	)

	// Close the channel to iterate through it
	close(eventsChan)

	// Collect all events for verification
	events := make([]model.Event, 0)
	for event := range eventsChan {
		events = append(events, event)
	}

	// Verify results
	assert.NoError(t, err)
	assert.NotNil(t, completedJobs, "Should return completed jobs")
	assert.Equal(t, 2, len(completedJobs), "Should have two completed jobs")
	assert.NotEmpty(t, events, "Should have received events")
	assert.Equal(t, 4, len(events), "Should have received four events")

	// Verify completed jobs
	assert.Equal(
		t,
		mockJobs[2],
		completedJobs[0],
		"First completed job does not match expected completed job",
	)
	assert.Equal(
		t,
		mockJobs[4],
		completedJobs[1],
		"Second completed job does not match expected completed job",
	)

	validEvents := []string{"start", "complete"}

	jobIds := []int{122, 123, 124}

	jobNames := map[int]string{
		122: "Test Job Submit Before / Complete After",
		123: "Test Job Submit After / Complete Empty",
		124: "Test Job Submit After / Complete After",
	}

	jobDescriptions := map[int]string{
		122: "This is test job 122",
		123: "This is test job 123",
		124: "This is test job 124",
	}

	jobGroups := map[int]string{
		122: "group1",
		123: "group2",
		124: "group3",
	}

	jobTags := map[int]string{
		122: "tag1,tag2",
		123: "tag3,tag4",
		124: "tag5,tag6",
	}

	jobStatuses := map[int]string{
		122: "succeeded",
		124: "failed",
	}

	jobSubmissionTimes := map[int]int64{
		122: pastTime.UnixMilli(),
		124: futureTime.UnixMilli(),
	}

	jobCompletedStageCount := map[int]int{
		122: 3,
		124: 5,
	}

	jobCompletedTaskCount := map[int]int{
		122: 95,
		124: 5,
	}

	jobCompletedIndexCount := map[int]int{
		122: 95,
		124: 10,
	}

	found122Complete := false
	found123Start := false
	found124Start := false
	found124Complete := false
	foundOther := false

	// Verify that all events have the expected attributes
	for _, event := range events {
		// Verify timestamp
		assert.Equal(
			t,
			now.UnixMilli(),
			event.Timestamp,
			"Should have the correct event timestamp",
		)

		// Verify event type
		assert.Equal(t, "SparkJob", event.Type)

		// Verify attributes
		assert.NotNil(t, event.Attributes)

		// Verify basic attributes
		assert.Contains(t, event.Attributes, "sparkAppId")
		assert.Equal(t, "app-123456", event.Attributes["sparkAppId"])
		assert.Contains(t, event.Attributes, "sparkAppName")
		assert.Equal(t, "Test Spark App", event.Attributes["sparkAppName"])
		assert.Contains(t, event.Attributes, "environment")
		assert.Equal(t, "test", event.Attributes["environment"])

		// Verify workspace attributes
		assert.Contains(t, event.Attributes, "databricksWorkspaceId")
		assert.Equal(t, int64(12345), event.Attributes["databricksWorkspaceId"])
		assert.Contains(t, event.Attributes, "databricksWorkspaceName")
		assert.Equal(
			t,
			"foo.fakedomain.local",
			event.Attributes["databricksWorkspaceName"],
		)
		assert.Contains(t, event.Attributes, "databricksWorkspaceUrl")
		assert.Equal(
			t,
			"https://foo.fakedomain.local",
			event.Attributes["databricksWorkspaceUrl"],
		)

		// Verify cluster attributes
		assert.Contains(t, event.Attributes, "databricksClusterId")
		assert.Equal(
			t,
			"fake-cluster-id",
			event.Attributes["databricksClusterId"],
		)
		assert.Contains(t, event.Attributes, "databricksClusterName")
		assert.Equal(
			t,
			"fake-cluster-name",
			event.Attributes["databricksClusterName"],
		)
		assert.Contains(t, event.Attributes, "databricksclustername")
		assert.Equal(
			t,
			"fake-cluster-name",
			event.Attributes["databricksclustername"],
		)
		assert.Contains(t, event.Attributes, "databricksClusterSource")
		assert.Equal(
			t,
			"fake-cluster-source",
			event.Attributes["databricksClusterSource"],
		)
		assert.Contains(t, event.Attributes, "databricksClusterInstancePoolId")
		assert.Equal(
			t,
			"fake-cluster-instance-pool-id",
			event.Attributes["databricksClusterInstancePoolId"],
		)

		// Verify event
		assert.Contains(t, event.Attributes, "event")
		assert.Contains(t, validEvents, event.Attributes["event"])

		evt := event.Attributes["event"].(string)

		// Verify job attributes
		assert.Contains(t, event.Attributes, "jobId")
		assert.Contains(t, jobIds, event.Attributes["jobId"])

		jobId := event.Attributes["jobId"].(int)

		assert.Contains(t, event.Attributes, "jobName")
		assert.Equal(t, jobNames[jobId], event.Attributes["jobName"])

		assert.Contains(t, event.Attributes, "description")
		assert.Equal(
			t,
			jobDescriptions[jobId],
			event.Attributes["description"],
		)

		assert.Contains(t, event.Attributes, "jobGroup")
		assert.Equal(t, jobGroups[jobId], event.Attributes["jobGroup"])

		assert.Contains(t, event.Attributes, "jobTags")
		assert.Equal(
			t,
			jobTags[jobId],
			event.Attributes["jobTags"],
		)

		if evt == "start" {
            if jobId == 123 {
                found123Start = true
            } else if jobId == 124 {
                found124Start = true
            } else {
                foundOther = true
				continue
            }

			assert.Contains(t, event.Attributes, "submissionTime")
            assert.Equal(
            	t,
            	futureTime.UnixMilli(),
            	event.Attributes["submissionTime"],
            )
        } else if evt == "complete" {
			if jobId == 122 {
				found122Complete = true
			} else if jobId == 124 {
				found124Complete = true
			} else {
				foundOther = true
				continue
			}

			assert.Contains(t, event.Attributes, "status")
			status := event.Attributes["status"].(string)

			assert.Equal(t, jobStatuses[jobId], status)

			assert.Contains(t, event.Attributes, "submissionTime")
            assert.Equal(
            	t,
            	jobSubmissionTimes[jobId],
            	event.Attributes["submissionTime"],
            )

			assert.Contains(t, event.Attributes, "completionTime")
            assert.Equal(
            	t,
            	futureTime.UnixMilli(),
            	event.Attributes["completionTime"],
            )

			assert.Contains(t, event.Attributes, "completedStageCount")
			assert.Equal(
				t,
				jobCompletedStageCount[jobId],
				event.Attributes["completedStageCount"],
			)

			assert.Contains(t, event.Attributes, "completedTaskCount")
			assert.Equal(
				t,
				jobCompletedTaskCount[jobId],
				event.Attributes["completedTaskCount"],
			)

			assert.Contains(t, event.Attributes, "completedIndexCount")
			assert.Equal(
				t,
				jobCompletedIndexCount[jobId],
				event.Attributes["completedIndexCount"],
			)
		} else {
			foundOther = true
		}
	}

	// Verify we saw exactly the events we expected
	assert.True(t, found122Complete)
	assert.True(t, found123Start)
	assert.True(t, found124Start)
	assert.True(t, found124Complete)
	assert.False(t, foundOther)
}

func TestCollectSparkAppStageMetrics_GetApplicationStagesError(t *testing.T) {
	// Setup mock client
	mockClient := &MockSparkApiClient{}

	// Setup mock metrics decorator
	mockDecorator := &MockSparkEventDecorator{}

	// Setup mock tag map
	tags := map[string]string{
		"environment": "test",
		"cluster":     "spark-test",
	}

	// Setup mock SparkApplication
	sparkApp := &SparkApplication{
		Id:   "app-123456",
		Name: "Test Spark App",
	}

	// Setup mock completedJobs
	completedJobs := []SparkJob{}

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

	// Setup an events channel to receive events
	eventsChan := make(chan model.Event, 100) // Buffer to prevent blocking

	// Execute the function under test
	err := collectSparkAppStageMetrics(
		context.Background(),
		mockClient,
		sparkApp,
		completedJobs,
		mockDecorator,
		time.Now().UTC(),
		tags,
		eventsChan,
	)

	// Close the channel to iterate through it
	close(eventsChan)

	// Collect all events to make sure the channel is empty
	events := make([]model.Event, 0)
	for event := range eventsChan {
		events = append(events, event)
	}

	assert.Error(t, err)
	assert.Equal(t, expectedError, err.Error())
	assert.True(
		t,
		getApplicationStagesCalled,
		"GetApplicationStages should be called",
	)
	assert.Empty(t, events, "No events should be received")
}

func TestCollectSparkAppStageMetrics(t *testing.T) {
	// Setup mock client
	mockClient := &MockSparkApiClient{}

	// Setup mock metrics decorator
	mockDecorator := &MockSparkEventDecorator{}

	// Setup mock tag map
	tags := map[string]string{
		"environment": "test",
	}

	// Mock Now
	now := time.Now()
	Now = func () time.Time { return now }

	// Setup times to be used to test behavior of event production based on
	// lastRun time.
	lastRun := now.UTC()
	pastTime := lastRun.Add(-15 * time.Second)
	pastTimeFormatted := pastTime.Format(RFC3339Milli)
	futureTime := lastRun.Add(15 * time.Second)
	futureTimeFormatted := futureTime.Format(RFC3339Milli)
	launchTime := lastRun.Add(5 * time.Second)
	launchTimeFormatted := launchTime.Format(RFC3339Milli)

	// Create a mock SparkApplication
	sparkApp := &SparkApplication{
		Id:   "app-123456",
		Name: "Test Spark App",
	}

	// Setup mock completedJobs for testing skipped stage functionality
	completedJobs := []SparkJob{
		{
			JobId:               1,
			Name:                "Test Job",
			StageIds:            []int{11},
		},
	}

	// Create mock stages with different statuses and mock values
	mockStages := []SparkStage{
		// Test stage start before / complete before
		// This stage should produce no stage events since the stage was
		// started before the last run and completed before the last run.
		{
			Status:                           "complete",
            StageId:                          1,
            AttemptId:                        0,
            NumTasks:                         100,
            NumActiveTasks:                   0,
            NumCompleteTasks:                 100,
            NumFailedTasks:                   0,
            NumKilledTasks:                   0,
            NumCompletedIndices:              50,
			FirstTaskLaunchedTime:            pastTimeFormatted,
			CompletionTime:                   pastTimeFormatted,
			Name:                             "Test Stage Start Before / Complete Before",
			Tasks: map[string]SparkTask{
				// This task is designed to ensure the task processing logic
				// does not run when it's parent stage completed before the last
				// run. We do this by crafting a task with a launch time after
				// the last run. Under normal circumstances, this task
				// would produce a task launch event. By asserting that no task
				// launch event with this task ID is produced, we can confirm
				// that the task processing logic is skipped for this stage.
				//
				// NOTE: Practically this situation should not occur since we
				// should never get a task that launched after the parent stage
				// completed.
				"1000": {
					TaskId:       1000,
					Index:        40,
					Attempt:      0,
					PartitionId:  40,
					LaunchTime:   launchTimeFormatted,
					Duration:     5000,
					ExecutorId:   "executor-123",
					Status:       "RUNNING",
					TaskLocality: "NODE_LOCAL",
					Speculative:  false,
					TaskMetrics: SparkTaskMetrics{
						ExecutorRunTime:            4500,
						InputMetrics: struct {
							BytesRead   int `json:"bytesRead"`
							RecordsRead int `json:"recordsRead"`
						}{
							BytesRead:   1048576,
						},
					},
				},
			},
			PeakExecutorMetrics: SparkExecutorPeakMemoryMetrics{
				JVMHeapMemory:          8388608,
			},
			ShuffleMergersCount: 			  0,
		},
		// Test stage start before / complete empty
		// This stage should produce no stage events since the stage was
		// started before the last run and has not completed yet (because the
		// completion time is empty).
		{
            Status:                           "active",
            StageId:                          2,
            AttemptId:                        0,
            NumTasks:                         100,
            NumActiveTasks:                   0,
            NumCompleteTasks:                 0,
            NumFailedTasks:                   100,
            NumKilledTasks:                   0,
            NumCompletedIndices:              0,
			FirstTaskLaunchedTime:            pastTimeFormatted,
            CompletionTime:                   "",
			Name:                             "Test Stage Start Before / Complete Empty",
			Tasks: map[string]SparkTask{
				// Test task launch before / complete before
				// This task should produce no task events since the task was
				// launched before the last run and completed before the last
				// run (because the task status is "SUCCESS" and the calculated
				// completion time is before the last run).
				"1001": {
					TaskId:       1001,
					Index:        41,
					Attempt:      0,
					PartitionId:  41,
					LaunchTime:   pastTimeFormatted,
					Duration:     5000,
					ExecutorId:   "executor-123",
					Status:       "SUCCESS",
					TaskLocality: "NODE_LOCAL",
					Speculative:  false,
					TaskMetrics: SparkTaskMetrics{
						ExecutorRunTime:            4500,
						InputMetrics: struct {
							BytesRead   int `json:"bytesRead"`
							RecordsRead int `json:"recordsRead"`
						}{
							BytesRead:   1048576,
						},
					},
				},
				// Test task launch before / complete "empty"
				// This task should produce no task events since the task was
				// launched before the last run and has not completed yet
				// (because the task status is "RUNNING").
				"1002": {
					TaskId:       1002,
					Index:        42,
					Attempt:      0,
					PartitionId:  42,
					LaunchTime:   pastTimeFormatted,
					Duration:     10000,
					ExecutorId:   "executor-123",
					Status:       "RUNNING",
					TaskLocality: "NODE_LOCAL",
					Speculative:  false,
					TaskMetrics: SparkTaskMetrics{
						ExecutorRunTime:            4500,
						InputMetrics: struct {
							BytesRead   int `json:"bytesRead"`
							RecordsRead int `json:"recordsRead"`
						}{
							BytesRead:   1048576,
						},
					},
				},
			},
			PeakExecutorMetrics: SparkExecutorPeakMemoryMetrics{
				JVMHeapMemory:          2097152,
			},
			ShuffleMergersCount:              0,
        },
        // Test stage start before / complete after
        // This stage should produce no stage start event since the stage was
		// started before the last run and should produce one stage complete
		// event since the stage completed after the last run.
		{
			Status:                           "complete",
			StageId:                          3,
			AttemptId:                        0,
			NumTasks:                         100,
			NumActiveTasks:                   75,
			NumCompleteTasks:                 20,
			NumFailedTasks:                   5,
			NumKilledTasks:                   0,
			NumCompletedIndices:              20,
			FirstTaskLaunchedTime:            pastTimeFormatted,
			CompletionTime:                   futureTimeFormatted,
			Description:                      "This is job description 1",
			Name:                             "Test Stage Start Before / Complete After",
			Details:                          "This is test stage 3 details",
			Tasks: map[string]SparkTask{
				// Test task launch before / complete after
				// This task should produce no task start event since the task
				// was launched before the last run and should produce one task
				// complete event since the task completed after the last run
				// (because the task status is "FAILED" and the calculated
				// completion time is after the last run).
				"1003": {
					TaskId:            1003,
					Index:             43,
					Attempt:           0,
					PartitionId:       43,
					LaunchTime:        pastTimeFormatted,
					Duration:          20000,
					ExecutorId:        "executor-123",
					Status:            "FAILED",
					TaskLocality:      "NODE_LOCAL",
					Speculative:       false,
					TaskMetrics: SparkTaskMetrics{
						ExecutorRunTime:            18000,
						InputMetrics: struct {
							BytesRead   int `json:"bytesRead"`
							RecordsRead int `json:"recordsRead"`
						}{
							BytesRead:   2097152,
						},
						SnapStartedTaskCount: 1,
					},
					SchedulerDelay:    5000,
					GettingResultTime: 8000,
				},
			},
			PeakExecutorMetrics: SparkExecutorPeakMemoryMetrics{
				JVMHeapMemory:          2097152,
			},
			ShuffleMergersCount: 2,
		},
		// Test stage start after complete empty
        // This stage should produce one stage start event since the stage was
		// started after the last run and should produce no stage complete
		// event since the stage has not completed yet (because the completion
		// time is empty).
		{
			Status:                           "active",
            StageId:                          4,
            AttemptId:                        1,
            NumTasks:                         80,
            NumActiveTasks:                   80,
            NumCompleteTasks:                 0,
            NumFailedTasks:                   0,
            NumKilledTasks:                   0,
            NumCompletedIndices:              0,
			FirstTaskLaunchedTime:            futureTimeFormatted,
			CompletionTime: 				  "",
			Description:                      "This is job description 2",
			Name:                             "Test Stage Start After / Complete Empty",
			Details:                          "This is test stage 4 details",
			Tasks: map[string]SparkTask{
				// Test task launch after / complete "empty"
				// This task should produce one task start event since the task
				// was launched after the last run and should produce no task
				// complete event (because the task status is "RUNNING").
				"1004": {
					TaskId:       1004,
					Index:        44,
					Attempt:      2,
					PartitionId:  44,
					LaunchTime:   launchTimeFormatted,
					Duration:     10000,
					ExecutorId:   "executor-123",
					Status:       "RUNNING",
					TaskLocality: "NODE_LOCAL",
					Speculative:  false,
					TaskMetrics: SparkTaskMetrics{
						ExecutorRunTime:            4500,
						InputMetrics: struct {
							BytesRead   int `json:"bytesRead"`
							RecordsRead int `json:"recordsRead"`
						}{
							BytesRead:   1048576,
						},
					},
				},
			},
			PeakExecutorMetrics: SparkExecutorPeakMemoryMetrics{
				JVMHeapMemory:          1048576,
			},
			ShuffleMergersCount: 2,
		},
		// Test stage start after complete after
		// This stage should produce one stage start event since the stage was
		// started after the last run and should produce one stage complete
		// event since the stage completed after the last run.
		{
			Status:                           "complete",
			StageId:                          5,
			AttemptId:                        2,
			NumTasks:                         120,
			NumActiveTasks:                   0,
			NumCompleteTasks:                 115,
			NumCompletedIndices:              115,
			FirstTaskLaunchedTime:            futureTimeFormatted,
			CompletionTime:                   futureTimeFormatted,
			Description:                      "This is job description 3",
			Name:                             "Test Stage Start After / Complete After",
			// This is a long string to test the truncation of the Details field
			Details:                          strings.Repeat("0", 5000),
			Tasks: map[string]SparkTask{
				// Test task launch after / complete after
				// This task should produce one task start event since the task
				// was launched after the last run and should produce one task
				// complete event since the task completed after the last run
				// (because the task status is "SUCCESS" and the calculated
				// completion time is after the last run).
				"1005": {
					TaskId:            1005,
					Index:             45,
					Attempt:           1,
					PartitionId:       45,
					LaunchTime:        launchTimeFormatted,
					Duration:          10000,
					ExecutorId:        "executor-456",
					Status:            "SUCCESS",
					TaskLocality:      "RACK_LOCAL",
					Speculative:       true,
					TaskMetrics: SparkTaskMetrics{
						ExecutorRunTime:            18000,
						InputMetrics: struct {
							BytesRead   int `json:"bytesRead"`
							RecordsRead int `json:"recordsRead"`
						}{
							BytesRead:   1048576,
						},
						SnapStartedTaskCount: 2,
					},
					SchedulerDelay:    3000,
					GettingResultTime: 10000,
				},
				// Test task empty launch time
				// This task should produce no events since it has an empty
				// launch time.
				//
				// NOTE: Technically, we can't tell the difference externally
				// from the function between an empty launch time and an invalid
				// launch time. We just know both should have the same result
				// which is that no events are produced. But since we don't want
				// either condition to return an error (because we want to
				// continue processing other tasks and stages), I'm not sure how
				// we test to distinguish between between the two cases. For now
				// we'll assume it's good enough to verify neither produces
				// events.
				"1006": {
					TaskId:       1006,
					Index:        46,
					Attempt:      0,
					PartitionId:  46,
					LaunchTime:   "",
					Duration:     7500,
					ExecutorId:   "executor-456",
					Status:       "RUNNING",
					TaskLocality: "RACK_LOCAL",
					Speculative:  true,
					TaskMetrics: SparkTaskMetrics{
						ExecutorRunTime:            7000,
						InputMetrics: struct {
							BytesRead   int `json:"bytesRead"`
							RecordsRead int `json:"recordsRead"`
						}{
							BytesRead:   2097152,
							RecordsRead: 75000,
						},
					},
				},
				// Test task invalid launch time
				// This task should produce no events since it has an invalid
				// launch time.
				//
				// NOTE: See note above.
				"1007": {
					TaskId:       1007,
					Index:        47,
					Attempt:      0,
					PartitionId:  47,
					LaunchTime:   "invalid_launch_time",
					Duration:     7500,
					ExecutorId:   "executor-456",
					Status:       "RUNNING",
					TaskLocality: "RACK_LOCAL",
					Speculative:  true,
					TaskMetrics: SparkTaskMetrics{
						ExecutorRunTime:            7000,
						InputMetrics: struct {
							BytesRead   int `json:"bytesRead"`
							RecordsRead int `json:"recordsRead"`
						}{
							BytesRead:   2097152,
							RecordsRead: 75000,
						},
					},
				},
			},
			PeakExecutorMetrics: SparkExecutorPeakMemoryMetrics{
				JVMHeapMemory:          1048576,
			},
			ShuffleMergersCount: 3,
		},
		// Test stage empty first task launched time
		// This stage should produce no events since it has an empty
		// first task launched time.
		//
		// NOTE: Technically, we can't tell the difference externally from the
		// function between an empty first task launched time and an invalid
		// first task launched time. We just know both should have the same
		// result which is that no events are produced. But since we don't want
		// either condition to return an error (because we want to continue
		// processing other stages), I'm not sure how we test to distinguish
		// between the two cases. For now, we'll assume it's good enough to
		// verify neither produces events.
		{
			Status:                           "unknown",
			StageId:                          6,
			AttemptId:                        0,
			NumTasks:                         80,
			NumActiveTasks:                   0,
			NumCompleteTasks:                 0,
			NumFailedTasks:                   0,
			NumKilledTasks:                   0,
			NumCompletedIndices:              0,
			FirstTaskLaunchedTime:            "",
			CompletionTime:                   "",
			PeakExecutorMetrics: SparkExecutorPeakMemoryMetrics{
				JVMHeapMemory:          1048576,
			},
			ShuffleMergersCount: 4,
		},
		// Test stage invalid first task launched time
		// This stage should produce no events since it has an invalid
		// first task launched time.
		//
		// NOTE: See note above.
		{
			Status:                           "complete",
			StageId:                          7,
			AttemptId:                        0,
			NumTasks:                         80,
			NumActiveTasks:                   0,
			NumCompleteTasks:                 0,
			NumFailedTasks:                   0,
			NumKilledTasks:                   0,
			NumCompletedIndices:              0,
			FirstTaskLaunchedTime:            "invalid_first_task_launched_time",
			CompletionTime:                   futureTimeFormatted,
			PeakExecutorMetrics: SparkExecutorPeakMemoryMetrics{
				JVMHeapMemory:          1048576,
			},
			ShuffleMergersCount: 4,
		},
		// Test stage invalid completion time
		// This stage should produce no events since it has an invalid
		// completion time.
		{
			Status:                           "complete",
			StageId:                          8,
			AttemptId:                        0,
			NumTasks:                         120,
			NumActiveTasks:                   0,
			NumCompleteTasks:                 80,
			NumFailedTasks:                   3,
			NumKilledTasks:                   2,
			NumCompletedIndices:              90,
			FirstTaskLaunchedTime:            pastTimeFormatted,
			CompletionTime:                   "invalid_completion_time",
			PeakExecutorMetrics: SparkExecutorPeakMemoryMetrics{
				JVMHeapMemory:          1048576,
			},
			ShuffleMergersCount: 5,
		},
		// Test stage valid submission time
        // This stage should produce one stage start event since the stage was
		// started after the last run and should produce no stage complete
		// event since the stage has not completed yet (because the completion
		// time is empty). The stage start event should have a valid submission
		// time.
		{
			Status:                           "active",
            StageId:                          9,
            AttemptId:                        0,
            NumTasks:                         80,
            NumActiveTasks:                   80,
            NumCompleteTasks:                 0,
            NumFailedTasks:                   0,
            NumKilledTasks:                   0,
            NumCompletedIndices:              0,
			SubmissionTime:                   futureTimeFormatted,
			FirstTaskLaunchedTime:            futureTimeFormatted,
			CompletionTime: 				  "",
			Description:                      "This is job description 4",
			Name:                             "Test Stage Valid Submission Time",
			Details:                          "This is test stage 9 details",
			Tasks: map[string]SparkTask{},
			PeakExecutorMetrics: SparkExecutorPeakMemoryMetrics{
				JVMHeapMemory:          1048576,
			},
			ShuffleMergersCount: 2,
		},
		// Test stage after complete before
        // This stage is specifically designed to test the case where a stage
		// has a first task launched time after the last run but a completion
		// time before the last run. Practically this should never happen since
		// we should never get a stage that starts after it completes, but we
		// want to ensure our logic handles this case gracefully. We test this
		// by ensuring that this stage generates no events.
		{
			Status:                           "active",
            StageId:                          10,
            AttemptId:                        0,
            NumTasks:                         80,
            NumActiveTasks:                   80,
            NumCompleteTasks:                 0,
            NumFailedTasks:                   0,
            NumKilledTasks:                   0,
            NumCompletedIndices:              0,
			FirstTaskLaunchedTime:            futureTimeFormatted,
			CompletionTime: 				  pastTimeFormatted,
			Name:                             "Test Stage Start After / Complete Before",
			Tasks: map[string]SparkTask{},
			PeakExecutorMetrics: SparkExecutorPeakMemoryMetrics{
				JVMHeapMemory:          1048576,
			},
			ShuffleMergersCount: 2,
		},
		// Test skipped stage that is part of a completed job
        // This stage should produce one stage complete event since the stage
		// was skipped and is part of the completedJobs list. This stage should
		// have no submission, first task launched, or completion time and
		// should only have status and taskCount attributes.
		{
			Status:                           "skipped",
            StageId:                          11,
            AttemptId:                        0,
            NumTasks:                         3,
            NumActiveTasks:                   0,
            NumCompleteTasks:                 0,
            NumFailedTasks:                   0,
            NumKilledTasks:                   0,
            NumCompletedIndices:              0,
			Description:                      "This is job description 5",
			Name:                             "Test Include Skipped Stage",
			Details:                          "This is test stage 11 details",
			Tasks: map[string]SparkTask{},
			PeakExecutorMetrics: SparkExecutorPeakMemoryMetrics{
				JVMHeapMemory:          1048576,
			},
			ShuffleMergersCount: 0,
		},
		// Test skipped stage that is not part of a completed job
        // This stage should produce no events since it is not part of the
		// completedJobs list.
		{
			Status:                           "skipped",
            StageId:                          12,
            AttemptId:                        0,
            NumTasks:                         3,
            NumActiveTasks:                   0,
            NumCompleteTasks:                 0,
            NumFailedTasks:                   0,
            NumKilledTasks:                   0,
            NumCompletedIndices:              0,
			Name:                             "Test Exclude Skipped Stage",
			Tasks: map[string]SparkTask{},
			PeakExecutorMetrics: SparkExecutorPeakMemoryMetrics{
				JVMHeapMemory:          0,
			},
			ShuffleMergersCount: 0,
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

	// Setup an events channel to receive events
	eventsChan := make(chan model.Event, 1000) // Buffer to prevent blocking

	// Execute the function under test
	err := collectSparkAppStageMetrics(
		context.Background(),
		mockClient,
		sparkApp,
		completedJobs,
		mockDecorator,
		lastRun,
		tags,
		eventsChan,
	)

	// Close the channel to iterate through it
	close(eventsChan)

	// Collect all events for verification
	events := make([]model.Event, 0)
	for event := range eventsChan {
		events = append(events, event)
	}

	// Verify results
	assert.NoError(t, err)
	assert.NotEmpty(t, events, "Should have received events")
	assert.Equal(t, 10, len(events), "Should have received ten events")

	validEventTypes := []string{"SparkStage", "SparkTask"}
	validEvents := []string{"start", "complete"}

	stageIds := []int{3, 4, 5, 9, 11}

	stageJobDescriptions := map[int]string{
		3: "This is job description 1",
		4: "This is job description 2",
		5: "This is job description 3",
		9: "This is job description 4",
		11: "This is job description 5",
	}

	stageNames := map[int]string{
		3: "Test Stage Start Before / Complete After",
        4: "Test Stage Start After / Complete Empty",
		5: "Test Stage Start After / Complete After",
		9: "Test Stage Valid Submission Time",
		11: "Test Include Skipped Stage",
    }

	stageDetails := map[int]string{
		3: "This is test stage 3 details",
		4: "This is test stage 4 details",
		// We generated a string greater than 4096 characters to test
		// truncation of the Details field. The result should be trimmed to
		// 4096 characters.
		5: strings.Repeat("0", 4096),
		9: "This is test stage 9 details",
		11: "This is test stage 11 details",
	}

	stageAttemptIds := map[int]int{
		3: 0,
		4: 1,
		5: 2,
		9: 0,
		11: 0,
	}

	stageFirstTaskLaunchedTimes := map[int]int64{
		3: pastTime.UnixMilli(),
		4: futureTime.UnixMilli(),
		5: futureTime.UnixMilli(),
		9: futureTime.UnixMilli(),
	}

	stageCompletionTimes := map[int]int64{
		3: futureTime.UnixMilli(),
		5: futureTime.UnixMilli(),
	}

	stageCompleteEventStatuses := map[int]string{
		3: "complete",
		5: "complete",
	}

	stageDurations := map[int]int64{
		3: 30000,
		5: 0,
	}

	stageCompletedTaskCounts := map[int]int{
		3: 20,
		5: 115,
	}

	stageCompletedIndexCounts := map[int]int{
		3: 20,
		5: 115,
	}

	stageJvmHeapPeakMemoryUsedBytes := map[int]int{
		3: 2097152,
		5: 1048576,
	}

	stageShuffleMergersCounts := map[int]int{
		3: 2,
		5: 3,
	}

	taskIds := []int{1003, 1004, 1005}

	taskJobDescriptions := map[int]string{
		1003: stageJobDescriptions[3],
		1004: stageJobDescriptions[4],
		1005: stageJobDescriptions[5],
	}

	taskStageIds := map[int]int{
		1003: 3,
		1004: 4,
		1005: 5,
	}

	taskStageNames := map[int]string{
		1003: "Test Stage Start Before / Complete After",
		1004: "Test Stage Start After / Complete Empty",
		1005: "Test Stage Start After / Complete After",
	}

	taskStageStatuses := map[int]string{
		1003: "complete",
		1004: "active",
		1005: "complete",
	}

	taskStageAttemptIds := map[int]int{
		1003: 0,
		1004: 1,
		1005: 2,
	}

	taskAttemptIds := map[int]int{
		1003: 0,
		1004: 2,
		1005: 1,
	}

	taskLaunchTimes := map[int]int64{
		1003: pastTime.UnixMilli(),
		1004: launchTime.UnixMilli(),
		1005: launchTime.UnixMilli(),
	}

	taskCompletionTimes := map[int]int64{
		1003: lastRun.UTC().Add(5 * time.Second).UnixMilli(),
		1005: lastRun.UTC().Add(15 * time.Second).UnixMilli(),
	}

	taskStatuses := map[int]string{
		1003: "failed",
		1005: "success",
    }

	taskExecutorIds := map[int]string{
		1003: "executor-123",
		1005: "executor-456",
    }

	taskLocalities := map[int]string{
		1003: "NODE_LOCAL",
        1005: "RACK_LOCAL",
    }

	taskSpeculativeFlags := map[int]bool{
		1003: false,
        1005: true,
    }

	taskDurations := map[int]int{
		1003: 20000,
		1005: 10000,
	}

	taskInputReadBytes := map[int]int{
		1003: 2097152,
		1005: 1048576,
	}

	taskSchedulerDelays := map[int]int{
		1003: 5000,
		1005: 3000,
	}

	taskGettingResultDurations := map[int]int{
		1003: 8000,
		1005: 10000,
	}

	taskSnapStartedTaskCounts := map[int]int{
		1003: 1,
		1005: 2,
	}

	found3Complete := false
	found4Start := false
	found5Start := false
	found5Complete := false
	found9Start := false
	found10Start := false
	found11Complete := false
	found12Complete := false
	foundStageOther := false
	found1003Complete := false
	found1004Start := false
	found1005Start := false
	found1005Complete := false
	foundTaskOther := false

	// Verify that all events have the expected attributes
	for _, event := range events {
		// Verify timestamp
		assert.Equal(
			t,
			now.UnixMilli(),
			event.Timestamp,
			"Should have the correct event timestamp",
		)

		// Verify event type
		assert.Contains(
			t,
			validEventTypes,
			event.Type,
		)

		// Verify attributes
		assert.NotNil(t, event.Attributes)

		// Verify basic attributes
		assert.Contains(t, event.Attributes, "sparkAppId")
		assert.Equal(t, "app-123456", event.Attributes["sparkAppId"])
		assert.Contains(t, event.Attributes, "sparkAppName")
		assert.Equal(t, "Test Spark App", event.Attributes["sparkAppName"])
		assert.Contains(t, event.Attributes, "environment")
		assert.Equal(t, "test", event.Attributes["environment"])

		// Verify workspace attributes
		assert.Contains(t, event.Attributes, "databricksWorkspaceId")
		assert.Equal(t, int64(12345), event.Attributes["databricksWorkspaceId"])
		assert.Contains(t, event.Attributes, "databricksWorkspaceName")
		assert.Equal(
			t,
			"foo.fakedomain.local",
			event.Attributes["databricksWorkspaceName"],
		)
		assert.Contains(t, event.Attributes, "databricksWorkspaceUrl")
		assert.Equal(
			t,
			"https://foo.fakedomain.local",
			event.Attributes["databricksWorkspaceUrl"],
		)

		// Verify cluster attributes
		assert.Contains(t, event.Attributes, "databricksClusterId")
		assert.Equal(
			t,
			"fake-cluster-id",
			event.Attributes["databricksClusterId"],
		)
		assert.Contains(t, event.Attributes, "databricksClusterName")
		assert.Equal(
			t,
			"fake-cluster-name",
			event.Attributes["databricksClusterName"],
		)
		assert.Contains(t, event.Attributes, "databricksclustername")
		assert.Equal(
			t,
			"fake-cluster-name",
			event.Attributes["databricksclustername"],
		)
		assert.Contains(t, event.Attributes, "databricksClusterSource")
		assert.Equal(
			t,
			"fake-cluster-source",
			event.Attributes["databricksClusterSource"],
		)
		assert.Contains(t, event.Attributes, "databricksClusterInstancePoolId")
		assert.Equal(
			t,
			"fake-cluster-instance-pool-id",
			event.Attributes["databricksClusterInstancePoolId"],
		)

		// Verify event
		assert.Contains(t, event.Attributes, "event")
		assert.Contains(
			t,
			validEvents,
			event.Attributes["event"],
		)

		evt := event.Attributes["event"].(string)

		if event.Type == "SparkStage" {
			// Verify SparkStage events have expected attributes
			assert.Contains(t, event.Attributes, "stageId")
			assert.Contains(t, stageIds, event.Attributes["stageId"])

			stageId := event.Attributes["stageId"].(int)

			assert.Contains(t, event.Attributes, "jobDescription")
			assert.Equal(
				t,
				stageJobDescriptions[stageId],
				event.Attributes["jobDescription"],
			)

			assert.Contains(t, event.Attributes, "stageName")
			assert.Equal(
				t,
				stageNames[stageId],
				event.Attributes["stageName"],
			)

			assert.Contains(t, event.Attributes, "details")
			assert.True(
				t,
				len(event.Attributes["details"].(string)) <= 4096,
				"Details should be less than or equal to 4096 characters",
			)
			assert.Equal(
				t,
				stageDetails[stageId],
				event.Attributes["details"],
			)

			assert.Contains(t, event.Attributes, "attemptId")
			assert.Equal(
				t,
				stageAttemptIds[stageId],
				event.Attributes["attemptId"],
			)

			if evt == "start" {
				if stageId == 4 {
					found4Start = true
				} else if stageId == 5 {
					found5Start = true
				} else if stageId == 9 {
					found9Start = true
				} else if stageId == 10 {
					// We aren't supposed to find a start event for stage 10 so
					// don't verify any attributes, just set the flag to true
					// to indicate we found it. This will cause the assertion at
					// the end of the test to fail and it will fail the test.
					found10Start = true
					continue
				} else {
					foundStageOther = true
					continue
				}

				assert.NotContains(t, event.Attributes, "status")

				assert.Contains(t, event.Attributes, "firstTaskLaunchedTime")
				assert.Equal(
					t,
					stageFirstTaskLaunchedTimes[stageId],
					event.Attributes["firstTaskLaunchedTime"],
				)

				if stageId == 9 {
					assert.Contains(t, event.Attributes, "submissionTime")
					assert.Equal(
						t,
						futureTime.UnixMilli(),
						event.Attributes["submissionTime"],
					)
				} else {
					assert.NotContains(t, event.Attributes, "submissionTime")
				}
			} else if evt == "complete" {
				if stageId == 3 {
					found3Complete = true
				} else if stageId == 5 {
					found5Complete = true
				} else if stageId == 11 {
					found11Complete = true

					assert.Contains(t, event.Attributes, "status")
					assert.Equal(
						t,
						"skipped",
						event.Attributes["status"],
					)

					assert.Contains(t, event.Attributes, "taskCount")
					assert.Equal(
						t,
						3,
						event.Attributes["taskCount"],
					)

					// Verify we don't see a few of the other attributes that
					// would normally be on a complete event.
					assert.NotContains(
						t,
						event.Attributes,
						"firstTaskLaunchedTime",
					)
					assert.NotContains(t, event.Attributes, "completionTime")
					assert.NotContains(t, event.Attributes, "duration")
					assert.NotContains(
						t,
						event.Attributes,
						"completedTaskCount",
					)
					assert.NotContains(
						t,
						event.Attributes,
						"completedIndexCount",
					)
					assert.NotContains(
						t,
						event.Attributes,
						"peakJvmHeapMemoryUsedBytes",
					)
					assert.NotContains(
						t,
						event.Attributes,
						"shuffleMergersCount",
					)

					continue
				} else if stageId == 12 {
					// We aren't supposed to find a complete event for stage 12
					// so don't verify any attributes, just set the flag to true
					// to indicate we found it. This will cause the assertion at
					// the end of the test to fail and it will fail the test.
					found12Complete = true
					continue
				} else {
					foundStageOther = true
					continue
				}

				assert.Contains(t, event.Attributes, "status")
				assert.Equal(
					t,
					stageCompleteEventStatuses[stageId],
					event.Attributes["status"],
				)

				assert.Contains(t, event.Attributes, "firstTaskLaunchedTime")
				assert.Equal(
					t,
					stageFirstTaskLaunchedTimes[stageId],
					event.Attributes["firstTaskLaunchedTime"],
				)

				assert.Contains(t, event.Attributes, "completionTime")
				assert.Equal(
					t,
					stageCompletionTimes[stageId],
					event.Attributes["completionTime"],
				)

				assert.Contains(t, event.Attributes, "duration")
				assert.Equal(
					t,
					stageDurations[stageId],
					event.Attributes["duration"],
				)

				assert.Contains(t, event.Attributes, "completedTaskCount")
				assert.Equal(
					t,
					stageCompletedTaskCounts[stageId],
					event.Attributes["completedTaskCount"],
				)

				assert.Contains(t, event.Attributes, "completedIndexCount")
				assert.Equal(
					t,
					stageCompletedIndexCounts[stageId],
					event.Attributes["completedIndexCount"],
				)

				assert.Contains(
					t,
					event.Attributes,
					"peakJvmHeapMemoryUsedBytes",
				)
				assert.Equal(
					t,
					stageJvmHeapPeakMemoryUsedBytes[stageId],
					event.Attributes["peakJvmHeapMemoryUsedBytes"],
				)

				assert.Contains(t, event.Attributes, "shuffleMergersCount")
				assert.Equal(
					t,
					stageShuffleMergersCounts[stageId],
					event.Attributes["shuffleMergersCount"],
				)
			} else {
				foundStageOther = true
			}
		} else if event.Type == "SparkTask" {
			// Verify SparkTask events have expected attributes
			assert.Contains(t, event.Attributes, "taskId")
			assert.Contains(t, taskIds, event.Attributes["taskId"])

			taskId := event.Attributes["taskId"].(int)

			assert.Contains(t, event.Attributes, "jobDescription")
			assert.Equal(
				t,
				taskJobDescriptions[taskId],
				event.Attributes["jobDescription"],
			)

			assert.Contains(t, event.Attributes, "stageId")
			assert.Equal(
				t,
				taskStageIds[taskId],
				event.Attributes["stageId"],
			)

			assert.Contains(t, event.Attributes, "stageName")
			assert.Equal(
				t,
				taskStageNames[taskId],
				event.Attributes["stageName"],
			)

			assert.Contains(t, event.Attributes, "stageStatus")
			assert.Equal(
				t,
				taskStageStatuses[taskId],
				event.Attributes["stageStatus"],
			)

			assert.Contains(t, event.Attributes, "stageAttemptId")
			assert.Equal(
				t,
				taskStageAttemptIds[taskId],
				event.Attributes["stageAttemptId"],
			)

			assert.Contains(t, event.Attributes, "attemptId")
			assert.Equal(
				t,
				taskAttemptIds[taskId],
				event.Attributes["attemptId"],
			)

			if evt == "start" {
				if taskId == 1004 {
					found1004Start = true
				} else if taskId == 1005 {
					found1005Start = true
				} else {
					foundTaskOther = true
					continue
				}

				assert.NotContains(t, event.Attributes, "status")

				assert.Contains(t, event.Attributes, "launchTime")
				assert.Equal(
					t,
					taskLaunchTimes[taskId],
					event.Attributes["launchTime"],
				)
			} else if evt == "complete" {
				if taskId == 1003 {
					found1003Complete = true
				} else if taskId == 1005 {
					found1005Complete = true
				} else {
					foundTaskOther = true
					continue
				}

				assert.Contains(t, event.Attributes, "status")
				assert.Equal(
					t,
					taskStatuses[taskId],
					event.Attributes["status"],
				)

				assert.Contains(t, event.Attributes, "launchTime")
				assert.Equal(
					t,
					taskLaunchTimes[taskId],
					event.Attributes["launchTime"],
				)

				assert.Contains(t, event.Attributes, "completionTime")
				assert.Equal(
					t,
					taskCompletionTimes[taskId],
					event.Attributes["completionTime"],
				)

				assert.Contains(t, event.Attributes, "executorId")
				assert.Equal(
					t,
					taskExecutorIds[taskId],
					event.Attributes["executorId"],
				)

				assert.Contains(t, event.Attributes, "locality")
				assert.Equal(
					t,
					taskLocalities[taskId],
					event.Attributes["locality"],
				)

				assert.Contains(t, event.Attributes, "speculative")
				assert.Equal(
					t,
					taskSpeculativeFlags[taskId],
					event.Attributes["speculative"],
				)

				assert.Contains(t, event.Attributes, "duration")
				assert.Equal(
					t,
					taskDurations[taskId],
					event.Attributes["duration"],
				)

				assert.Contains(t, event.Attributes, "inputReadBytes")
				assert.Equal(
					t,
					taskInputReadBytes[taskId],
					event.Attributes["inputReadBytes"],
				)

				assert.Contains(t, event.Attributes, "schedulerDelay")
				assert.Equal(
					t,
					taskSchedulerDelays[taskId],
					event.Attributes["schedulerDelay"],
				)

				assert.Contains(t, event.Attributes, "gettingResultDuration")
				assert.Equal(
					t,
					taskGettingResultDurations[taskId],
					event.Attributes["gettingResultDuration"],
				)

				assert.Contains(t, event.Attributes, "snapStartedTaskCount")
				assert.Equal(
					t,
					taskSnapStartedTaskCounts[taskId],
					event.Attributes["snapStartedTaskCount"],
				)
			} else {
				foundTaskOther = true
			}
		}
	}

	// Verify we saw exactly the events we expected
	assert.True(t, found3Complete)
	assert.True(t, found4Start)
	assert.True(t, found5Start)
	assert.True(t, found5Complete)
	assert.True(t, found9Start)
	assert.False(t, found10Start)
	assert.True(t, found11Complete)
	assert.False(t, found12Complete)
	assert.False(t, foundStageOther)
	assert.True(t, found1003Complete)
	assert.True(t, found1004Start)
	assert.True(t, found1005Start)
	assert.True(t, found1005Complete)
	assert.False(t, foundTaskOther)
}

func TestCollectSparkAppRDDMetrics_GetApplicationRDDsError(t *testing.T) {
	// Setup mock client
	mockClient := &MockSparkApiClient{}

	// Setup mock metrics decorator
	mockDecorator := &MockSparkEventDecorator{}

	// Setup mock tag map
	tags := map[string]string{
		"environment": "test",
		"cluster":     "spark-test",
	}

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

	// Setup an events channel to receive events
	eventsChan := make(chan model.Event, 100) // Buffer to prevent blocking

	// Execute the function under test
	err := collectSparkAppRDDMetrics(
		context.Background(),
		mockClient,
		sparkApp,
		mockDecorator,
		tags,
		eventsChan,
	)

	// Close the channel to iterate through it
	close(eventsChan)

	// Collect all events to make sure the channel is empty
	events := make([]model.Event, 0)
	for event := range eventsChan {
		events = append(events, event)
	}

	// Verify results
	assert.Error(t, err)
	assert.Equal(t, expectedError, err.Error())
	assert.True(
		t,
		getApplicationRDDsCalled,
		"GetApplicationRDDs should be called",
	)
	assert.Empty(t, events, "No events should be received")
}

func TestCollectSparkAppRDDMetrics(t *testing.T) {
	// Setup mock client
	mockClient := &MockSparkApiClient{}

	// Setup mock metrics decorator
	mockDecorator := &MockSparkEventDecorator{}

	// Setup mock tag map
	tags := map[string]string{
		"environment": "test",
	}

	// Mock Now
	now := time.Now()
	Now = func () time.Time { return now }

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
			StorageLevel:        "Disk Memory Serialized 1x Replicated",
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
				BlockName       string   `json:"blockName"`
				StorageLevel    string   `json:"storageLevel"`
				MemoryUsed      int      `json:"memoryUsed"`
				DiskUsed        int      `json:"diskUsed"`
				Executors       []string `json:"executors"`
			}{
				{
					BlockName:    "rdd_1_0",
					StorageLevel: "Disk Memory Serialized 1x Replicated",
					MemoryUsed:   131072, // 128KB
					DiskUsed:     65536,  // 64KB
					Executors:    []string{"driver", "executor-1"},
				},
				{
					BlockName:    "rdd_1_1",
					StorageLevel: "Disk Memory Serialized 2x Replicated",
					MemoryUsed:   65536, // 64KB
					DiskUsed:     65536, // 64KB
					Executors:    []string{"executor-1"},
				},
			},
		},
		{
			Id:                  2,
			Name:                "Test RDD 2",
			StorageLevel:        "Disk Memory Serialized 2x Replicated",
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
				BlockName    string   `json:"blockName"`
				StorageLevel string   `json:"storageLevel"`
				MemoryUsed   int      `json:"memoryUsed"`
				DiskUsed     int      `json:"diskUsed"`
				Executors    []string `json:"executors"`
			}{
				{
					BlockName:    "rdd_2_0",
					StorageLevel: "Disk Memory Serialized 3x Replicated",
					MemoryUsed:   1572864, // 1.5MB
					DiskUsed:     0,
					Executors:    []string{"executor-2"},
				},
				{
					BlockName:    "rdd_2_1",
					StorageLevel: "Disk Memory Serialized 4x Replicated",
					MemoryUsed:   786432, // 768KB
					DiskUsed:     0,
					Executors:    []string{"executor-2", "driver"},
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

	// Setup an events channel to receive events
	eventsChan := make(chan model.Event, 1000) // Buffer to prevent blocking

	// Execute the function under test
	err := collectSparkAppRDDMetrics(
		context.Background(),
		mockClient,
		sparkApp,
		mockDecorator,
		tags,
		eventsChan,
	)

	// Close the channel to iterate through it
	close(eventsChan)

	// Collect all events for verification
	events := make([]model.Event, 0)
	for event := range eventsChan {
		events = append(events, event)
	}

	// Verify results
	assert.NoError(t, err)
	assert.NotEmpty(t, events, "Should have received events")
	assert.Equal(t, 10, len(events), "Should have received ten events")

	validEventTypes := []string{
		"SparkRDDSample",
		"SparkRDDDistributionSample",
		"SparkRDDPartitionSample",
	}

	rddIds := []int{1, 2}

	rddNames := map[int]string{
		1: "Test RDD 1",
        2: "Test RDD 2",
	}

	rddStorageLevels := map[int]string{
		1: "Disk Memory Serialized 1x Replicated",
		2: "Disk Memory Serialized 2x Replicated",
	}

	rddMemoryUsedBytes := map[int]int{
		1: 1048576, // 1MB
		2: 2097152, // 2MB
	}

	rddDistributionIndices := []int{0, 1}

	rddDistributionMemoryUsedBytes := map[int]map[int]int{
		1: {0: 262144, 1: 524288},
		2: {0: 1048576, 1: 3145728},
	}

	rddPartitionBlockNames := []string{
		"rdd_1_0",
		"rdd_1_1",
		"rdd_2_0",
		"rdd_2_1",
	}

	rddPartitionStorageLevels := map[int]map[string]string{
		1: {"rdd_1_0": "Disk Memory Serialized 1x Replicated",
			"rdd_1_1": "Disk Memory Serialized 2x Replicated"},
		2: {"rdd_2_0": "Disk Memory Serialized 3x Replicated",
			"rdd_2_1": "Disk Memory Serialized 4x Replicated"},
	}

	rddPartitionMemoryUsedBytes := map[int]map[string]int{
		1: {"rdd_1_0": 131072, "rdd_1_1": 65536},
		2: {"rdd_2_0": 1572864, "rdd_2_1": 786432},
	}

	rddPartitionExecutorIds := map[int]map[string]string{
		1: {"rdd_1_0": "driver,executor-1", "rdd_1_1": "executor-1"},
		2: {"rdd_2_0": "executor-2", "rdd_2_1": "executor-2,driver"},
	}

	// Verify that all events have the expected attributes
	for _, event := range events {
		// Verify timestamp
		assert.Equal(
			t,
			now.UnixMilli(),
			event.Timestamp,
			"Should have the correct event timestamp",
		)

		// Verify event type
		assert.Contains(t, validEventTypes, event.Type)

		// Verify attributes
		assert.NotNil(t, event.Attributes)

		// Verify basic attributes
		assert.Contains(t, event.Attributes, "sparkAppId")
		assert.Equal(t, "app-123456", event.Attributes["sparkAppId"])
		assert.Contains(t, event.Attributes, "sparkAppName")
		assert.Equal(t, "Test Spark App", event.Attributes["sparkAppName"])
		assert.Contains(t, event.Attributes, "environment")
		assert.Equal(t, "test", event.Attributes["environment"])

		// Verify workspace attributes
		assert.Contains(t, event.Attributes, "databricksWorkspaceId")
		assert.Equal(t, int64(12345), event.Attributes["databricksWorkspaceId"])
		assert.Contains(t, event.Attributes, "databricksWorkspaceName")
		assert.Equal(
			t,
			"foo.fakedomain.local",
			event.Attributes["databricksWorkspaceName"],
		)
		assert.Contains(t, event.Attributes, "databricksWorkspaceUrl")
		assert.Equal(
			t,
			"https://foo.fakedomain.local",
			event.Attributes["databricksWorkspaceUrl"],
		)

		// Verify cluster attributes
		assert.Contains(t, event.Attributes, "databricksClusterId")
		assert.Equal(
			t,
			"fake-cluster-id",
			event.Attributes["databricksClusterId"],
		)
		assert.Contains(t, event.Attributes, "databricksClusterName")
		assert.Equal(
			t,
			"fake-cluster-name",
			event.Attributes["databricksClusterName"],
		)
		assert.Contains(t, event.Attributes, "databricksclustername")
		assert.Equal(
			t,
			"fake-cluster-name",
			event.Attributes["databricksclustername"],
		)
		assert.Contains(t, event.Attributes, "databricksClusterSource")
		assert.Equal(
			t,
			"fake-cluster-source",
			event.Attributes["databricksClusterSource"],
		)
		assert.Contains(t, event.Attributes, "databricksClusterInstancePoolId")
		assert.Equal(
			t,
			"fake-cluster-instance-pool-id",
			event.Attributes["databricksClusterInstancePoolId"],
		)

		// Verify RDD attributes (on all 3 RDD event typoes)
		assert.Contains(t, event.Attributes, "rddId")
		assert.Contains(t, rddIds, event.Attributes["rddId"])

		rddId := event.Attributes["rddId"].(int)

		assert.Contains(t, event.Attributes, "rddName")
		assert.Equal(t, rddNames[rddId], event.Attributes["rddName"])

		if event.Type == "SparkRDDSample" {
			// Verify SparkRDDSample events have expected attributes
			assert.Contains(t, event.Attributes, "storageLevel")
			assert.Equal(
				t,
				rddStorageLevels[rddId],
				event.Attributes["storageLevel"],
			)

			assert.Contains(t, event.Attributes, "memoryUsedBytes")
            assert.Equal(
                t,
                rddMemoryUsedBytes[rddId],
                event.Attributes["memoryUsedBytes"],
            )
		} else if event.Type == "SparkRDDDistributionSample" {
			assert.Contains(t, event.Attributes, "distributionIndex")
			assert.Contains(
				t,
				rddDistributionIndices,
				event.Attributes["distributionIndex"],
			)

			distributionIndex := event.Attributes["distributionIndex"].(int)

			assert.Contains(t, event.Attributes, "memoryUsedBytes")
			assert.Equal(
                t,
                rddDistributionMemoryUsedBytes[rddId][distributionIndex],
                event.Attributes["memoryUsedBytes"],
            )
		} else if event.Type == "SparkRDDPartitionSample" {
			assert.Contains(t, event.Attributes, "blockName")
			assert.Contains(
				t,
				rddPartitionBlockNames,
				event.Attributes["blockName"],
			)

			blockName := event.Attributes["blockName"].(string)

			assert.Contains(t, event.Attributes, "storageLevel")
			assert.Equal(
				t,
				rddPartitionStorageLevels[rddId][blockName],
				event.Attributes["storageLevel"],
			)

			assert.Contains(t, event.Attributes, "memoryUsedBytes")
			assert.Equal(
                t,
                rddPartitionMemoryUsedBytes[rddId][blockName],
				event.Attributes["memoryUsedBytes"],
            )

			assert.Contains(t, event.Attributes, "executorIds")
			assert.Equal(
				t,
				rddPartitionExecutorIds[rddId][blockName],
				event.Attributes["executorIds"],
			)
		}
	}
}

func TestShouldReportSkippedStage_CompletedJobContainsStageId(t *testing.T) {
    // Setup mock SparkStage
    stage := &SparkStage{
        StageId: 42,
    }

    // Setup completedJobs that contains a job with the mock stage ID in it's
	// list of stage IDs
    completedJobs := []SparkJob{
        {
			// This job includes the mock stage ID (42) in it's list of stage
			// IDs
            JobId:   1,
            StageIds: []int{41, 42, 43},
        },
        {
            JobId:   2,
            StageIds: []int{44, 45},
        },
    }

	// Execute the function under test
    shouldReport := shouldReportSkippedStage(completedJobs, stage)

	// Verify results
    assert.True(
    	t,
    	shouldReport,
    	"Stage should be reported if in stage ID list of a job in completedJobs",
    )
}

func TestShouldReportSkippedStage_CompletedJobDoesNotContainStageId(
	t *testing.T,
) {
    // Setup mock SparkStage
    stage := &SparkStage{
        StageId: 42,
    }

    // Setup completedJobs that does not contain a job with the mock stage ID
	// in it's list of stage IDs
    completedJobs := []SparkJob{
        {
			// This job does not include the mock stage ID (42) in it's list of
			// stage IDs
            JobId:   1,
            StageIds: []int{41, 43},
        },
    }

	// Execute the function under test
    shouldReport := shouldReportSkippedStage(completedJobs, stage)

	// Verify results
    assert.False(
    	t,
    	shouldReport,
    	"Stage should not be reported if not in stage ID list of a job in completedJobs",
    )
}

func TestShouldReportSkippedStage_CompletedJobsEmpty(t *testing.T) {
    // Setup mock SparkStage
    stage := &SparkStage{
        StageId: 42,
    }

    // Setup empty completedJobs
    completedJobs := []SparkJob{}

	// Execute the function under test
    shouldReport := shouldReportSkippedStage(completedJobs, stage)

	// Verify results
    assert.False(
    	t,
    	shouldReport,
    	"Stage should not be reported if completedJobs is empty",
    )
}
