package databricks

import (
	"context"
	"errors"
	"testing"
	"time"

	databricksSdkListing "github.com/databricks/databricks-sdk-go/listing"
	databricksSdkJobs "github.com/databricks/databricks-sdk-go/service/jobs"
	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration"
	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/model"
	"github.com/stretchr/testify/assert"
)

const (
	mockWorkspaceId  int64   = 12345
	mockWorkspaceHost        = "foo.fakedomain.local"
)

func setupMockWorkspace() *MockWorkspace {
	mock := &MockWorkspace{}
	mock.WorkspaceId = mockWorkspaceId
	// Note that in our documentation, we recommend using only the hostname
	// instead of the full URL for the workspaceHost. Internally, the SDK will
	// fix the value to be a valid URL by prepending the "https://" prefix and
	// storing the fixed up URL back in the Host field. Since this is a mock
	// workspace, we won't go through that fixup so we need to set the host
	// to the full URL so our tests can test for both the full URL and the
	// instance name which is stored into the WorkspaceInfo struct.
	mock.Config.Host = "https://" + mockWorkspaceHost

	// Return the mock workspace and a teardown function to restore the original
	// NewDatabricksWorkspace
	return mock
}

func setupMockWorkspaceAndInfo() (*MockWorkspace, func()) {
	mock := setupMockWorkspace()

	originalGetWorkspaceInfo := GetWorkspaceInfo
	GetWorkspaceInfo = func(
		ctx context.Context,
		w DatabricksWorkspace,
	) (*WorkspaceInfo, error) {
		return &WorkspaceInfo{
			Id:           mock.WorkspaceId,
			Url:          mock.Config.Host,
			InstanceName: mockWorkspaceHost,
		}, nil
	}

	// Return the mock workspace and a teardown function to restore the original
	// GetWorkspaceInfo
	return mock, func() {
		// Restore the original function
		GetWorkspaceInfo = originalGetWorkspaceInfo
	}
}

func setupMockClusterInfo() func() {
	originalGetClusterInfoById := GetClusterInfoById
	GetClusterInfoById = func(
		ctx context.Context,
		w DatabricksWorkspace,
		clusterId string,
	) (
		*ClusterInfo,
		error,
	) {
		return &ClusterInfo{
			Name:           "fake-cluster-name",
			Source:         "fake-cluster-source",
			InstancePoolId: "fake-cluster-instance-pool-id",
		}, nil
	}

	// Return a teardown function to restore the original GetClusterInfoById
	return func() {
		GetClusterInfoById = originalGetClusterInfoById
	}
}

func TestNewDatabricksJobRunReceiver_ValidParams(t *testing.T) {
	// Setup up mock workspace
	mockWorkspace := setupMockWorkspace()

	// Setup mock integration
	mockIntegration := &integration.LabsIntegration{
		Interval: 60,
	}

	// Setup mock tag map
	tags := map[string]string{
		"env": "production",
		"team": "data-engineering",
	}

	// Setup mock startOffset
	startOffset := time.Duration(60) * time.Second

	// Execute the function under test
	receiver := NewDatabricksJobRunReceiver(
		mockIntegration,
		mockWorkspace,
		startOffset,
		tags,
	)

	// Verify result
	assert.NotNil(t, receiver)

	assert.Equal(t, mockIntegration, receiver.i)
	assert.Equal(t, mockWorkspace, receiver.w)
	assert.Equal(t, startOffset, receiver.startOffset)
	assert.Equal(t, tags, receiver.tags)
}

func TestDatabricksJobRunReceiver_GetId(t *testing.T) {
	// Setup up mock workspace
	mockWorkspace := setupMockWorkspace()

	// Setup mock integration
	mockIntegration := &integration.LabsIntegration{
		Interval: 60,
	}

	// Setup mock tag map
	tags := map[string]string{
		"env": "production",
		"team": "data-engineering",
	}

	// Setup mock startOffset
	startOffset := time.Duration(60) * time.Second

	// Create job run receiver instance
	receiver := NewDatabricksJobRunReceiver(
		mockIntegration,
		mockWorkspace,
		startOffset,
		tags,
	)

	// Execute the function under test
	id := receiver.GetId()

	// Verify result
	assert.Equal(t, "databricks-job-run-receiver", id)
}

func TestPollEvents_ListJobRunsError(t *testing.T) {
	// Setup up mock workspace
	mockWorkspace := setupMockWorkspace()

	// Setup mock integration
	mockIntegration := &integration.LabsIntegration{
		Interval: 60,
	}

	// Setup mock tag map
	tags := map[string]string{
		"env": "production",
		"team": "data-engineering",
	}

	// Setup mock startOffset
	startOffset := time.Duration(60) * time.Second

	// Mock Now
	now := time.Now()
	Now = func () time.Time { return now }

	// Setup a mock iterator
	mockIterator := &MockIterator[databricksSdkJobs.BaseRun]{}

	// Setup function call trackers
	hasNextCalled := 0
	nextCalled := 0

	// Setup expected error message
	expectedError := "error listing job runs"

	// Setup the mock HasNext to increment the tracker and return true
	mockIterator.HasNextFunc = func(
		ctx context.Context,
	) bool {
		hasNextCalled += 1
		return true
	}

	// Setup the mock Next to increment the tracker and return an error
	mockIterator.NextFunc = func(
		ctx context.Context,
	) (databricksSdkJobs.BaseRun, error) {
		nextCalled += 1
		return databricksSdkJobs.BaseRun{}, errors.New(expectedError)
	}

	// Mock the ListJobRuns method to return the iterator with the error
	mockWorkspace.ListJobRunsFunc = func(
		ctx context.Context,
		startOffset time.Duration,
	) databricksSdkListing.Iterator[databricksSdkJobs.BaseRun] {
		return mockIterator
	}

	// Setup an events channel to receive events
	eventsChan := make(chan model.Event, 500) // Buffer to prevent blocking

	// Create job run receiver instance
	receiver := NewDatabricksJobRunReceiver(
		mockIntegration,
		mockWorkspace,
		startOffset,
		tags,
	)

	// Execute the function under test
	err := receiver.PollEvents(context.Background(), eventsChan)

	// Close the channel to iterate through it
	close(eventsChan)

	// Verify result
	assert.Error(t, err)
	assert.Equal(t, expectedError, err.Error())
	assert.Equal(t, hasNextCalled, 1, "HasNext should have been called once")
	assert.Equal(t, nextCalled, 1, "Next should have been called once")

	// Collect all events for verification
	events := make([]model.Event, 0)
	for event := range eventsChan {
		events = append(events, event)
	}

	assert.Empty(
		t,
		events,
		"Expected no events to be produced when ListJobRuns fails",
	)
}

func TestPollEvents_ListJobRunsEmpty(t *testing.T) {
	// Setup up mock workspace and workspace info
	mockWorkspace, teardown := setupMockWorkspaceAndInfo()
	defer teardown()

	// Setup mock integration
	mockIntegration := &integration.LabsIntegration{
		Interval: 60,
	}

	// Setup mock tag map
	tags := map[string]string{
		"env": "production",
		"team": "data-engineering",
	}

	// Setup mock startOffset
	startOffset := time.Duration(60) * time.Second

	// Mock Now
	now := time.Now()
	Now = func () time.Time { return now }

	// Setup a mock iterator
	mockIterator := &MockIterator[databricksSdkJobs.BaseRun]{}

	// Setup function call trackers
	hasNextCalled := 0
	nextCalled := 0

	// Setup the mock HasNext to increment the tracker and return false
	mockIterator.HasNextFunc = func(
		ctx context.Context,
	) bool {
		hasNextCalled += 1
		return false
	}

	// Setup the mock Next to increment the tracker and return a BaseRun with
	// zero values and no error. This should never be called since HasNext
	// returns false.
	mockIterator.NextFunc = func(
		ctx context.Context,
	) (databricksSdkJobs.BaseRun, error) {
		nextCalled += 1
		return databricksSdkJobs.BaseRun{}, nil
	}

	// Mock the ListJobRuns method to return the empty iterator
	mockWorkspace.ListJobRunsFunc = func(
		ctx context.Context,
		startOffset time.Duration,
	) databricksSdkListing.Iterator[databricksSdkJobs.BaseRun] {
		return mockIterator
	}

	// Setup an events channel to receive events
	eventsChan := make(chan model.Event, 500) // Buffer to prevent blocking

	// Create job run receiver instance
	receiver := NewDatabricksJobRunReceiver(
		mockIntegration,
		mockWorkspace,
		startOffset,
		tags,
	)

	// Execute the function under test
	err := receiver.PollEvents(context.Background(), eventsChan)

	// Close the channel to iterate through it
	close(eventsChan)

	// Verify result
	assert.NoError(t, err)

	// Collect all events for verification
	events := make([]model.Event, 0)
	for event := range eventsChan {
		events = append(events, event)
	}

	assert.Equal(
		t,
		1,
		len(events),
		"Expected one summary event to be produced when no job runs are found",
	)
	assert.Equal(t, hasNextCalled, 1, "HasNext should have been called once")
	assert.Equal(t, nextCalled, 0, "Next should not have been called")

	// Verify the event has the expected attributes
	event := events[0]

	// Verify timestamp
	assert.Equal(
		t,
		now.UnixMilli(),
		event.Timestamp, "Should have the correct event timestamp",
	)

	// Verify event type
	assert.Equal(t, "DatabricksJobRunSummary", event.Type)

	// Verify attributes
	assert.NotNil(t, event.Attributes)

	// Verify common attributes
	attrs := event.Attributes
	assert.NotEmpty(t, attrs)

	assert.Contains(t, attrs, "env")
	assert.Equal(t, "production", attrs["env"])
	assert.Contains(t, attrs, "team")
	assert.Equal(t, "data-engineering", attrs["team"])

	// Verify workspace attributes
	assert.Contains(t, attrs, "databricksWorkspaceId")
	assert.Equal(t, mockWorkspaceId, attrs["databricksWorkspaceId"])
	assert.Contains(t, attrs, "databricksWorkspaceName")
	assert.Equal(
		t,
		mockWorkspaceHost,
		attrs["databricksWorkspaceName"],
	)
	assert.Contains(t, attrs, "databricksWorkspaceUrl")
	assert.Equal(
		t,
		"https://" + mockWorkspaceHost,
		attrs["databricksWorkspaceUrl"],
	)

	// Verify summary counts
	assert.Contains(t, attrs, "blockedJobRunCount")
	assert.Equal(t, 0, attrs["blockedJobRunCount"])
	assert.Contains(t, attrs, "waitingJobRunCount")
	assert.Equal(t, 0, attrs["waitingJobRunCount"])
	assert.Contains(t, attrs, "pendingJobRunCount")
	assert.Equal(t, 0, attrs["pendingJobRunCount"])
	assert.Contains(t, attrs, "queuedJobRunCount")
	assert.Equal(t, 0, attrs["queuedJobRunCount"])
	assert.Contains(t, attrs, "runningJobRunCount")
	assert.Equal(t, 0, attrs["runningJobRunCount"])
	assert.Contains(t, attrs, "terminatingJobRunCount")
	assert.Equal(t, 0, attrs["terminatingJobRunCount"])
	assert.Contains(t, attrs, "blockedTaskRunCount")
	assert.Equal(t, 0, attrs["blockedTaskRunCount"])
	assert.Contains(t, attrs, "waitingTaskRunCount")
	assert.Equal(t, 0, attrs["waitingTaskRunCount"])
	assert.Contains(t, attrs, "pendingTaskRunCount")
	assert.Equal(t, 0, attrs["pendingTaskRunCount"])
	assert.Contains(t, attrs, "queuedTaskRunCount")
	assert.Equal(t, 0, attrs["queuedTaskRunCount"])
	assert.Contains(t, attrs, "runningTaskRunCount")
	assert.Equal(t, 0, attrs["runningTaskRunCount"])
	assert.Contains(t, attrs, "terminatingTaskRunCount")
	assert.Equal(t, 0, attrs["terminatingTaskRunCount"])
}

func TestPollEvents_LastRunUpdated(t *testing.T) {
	// Setup up mock workspace and workspace info
	mockWorkspace, teardown := setupMockWorkspaceAndInfo()
	defer teardown()

	// Setup mock integration
	mockIntegration := &integration.LabsIntegration{
		Interval: 60,
	}

	// Setup mock tag map
	tags := map[string]string{
		"env": "production",
		"team": "data-engineering",
	}

	// Setup mock startOffset
	startOffset := time.Duration(60) * time.Second

	// Mock Now
	now := time.Now()
	Now = func () time.Time { return now }

	// Setup a mock iterator
	mockIterator := &MockIterator[databricksSdkJobs.BaseRun]{}

	// Setup function call trackers
	hasNextCalled := 0
	nextCalled := 0

	// Setup the mock HasNext to increment the tracker and return false
	mockIterator.HasNextFunc = func(
		ctx context.Context,
	) bool {
		hasNextCalled += 1
		return false
	}

	// Setup the mock Next to increment the tracker and return a BaseRun with
	// zero values and no error. This should never be called since HasNext
	// returns false.
	mockIterator.NextFunc = func(
		ctx context.Context,
	) (databricksSdkJobs.BaseRun, error) {
		nextCalled += 1
		return databricksSdkJobs.BaseRun{}, nil
	}

	// Mock the ListJobRuns method to return the empty iterator
	mockWorkspace.ListJobRunsFunc = func(
		ctx context.Context,
		startOffset time.Duration,
	) databricksSdkListing.Iterator[databricksSdkJobs.BaseRun] {
		return mockIterator
	}

	// Setup an events channel to receive events
	eventsChan := make(chan model.Event, 500) // Buffer to prevent blocking

	// Create job run receiver instance
	receiver := NewDatabricksJobRunReceiver(
		mockIntegration,
		mockWorkspace,
		startOffset,
		tags,
	)

	// Verify that the initial lastRun timestamp is set correctly
	assert.Equal(t, now.UTC(), receiver.lastRun)

	// Now reset the mock Now function to simulate time progression
	now2 := now.Add(time.Minute)
	Now = func () time.Time { return now2 }

	// Execute the function under test
	err := receiver.PollEvents(context.Background(), eventsChan)

	// Close the channel to iterate through it
	close(eventsChan)

	// Verify result
	assert.NoError(t, err)
	assert.Equal(t, hasNextCalled, 1, "HasNext should have been called once")
	assert.Equal(t, nextCalled, 0, "Next should not have been called")

	// Verify that the lastRun timestamp is updated correctly
	assert.Equal(t, now2.UTC(), receiver.lastRun)

	// Collect all events for verification
	events := make([]model.Event, 0)
	for event := range eventsChan {
		events = append(events, event)
	}

	assert.Equal(
		t,
		1,
		len(events),
		"Expected one summary event to be produced when no job runs are found",
	)

	// Verify the event has the expected attributes
	event := events[0]

	// Verify timestamp
	assert.Equal(
		t,
		now2.UnixMilli(),
		event.Timestamp, "Should have the correct event timestamp",
	)

	// Verify event type
	assert.Equal(t, "DatabricksJobRunSummary", event.Type)

	// Verify attributes
	assert.NotNil(t, event.Attributes)

	// Verify common attributes
	attrs := event.Attributes
	assert.NotEmpty(t, attrs)

	assert.Contains(t, attrs, "env")
	assert.Equal(t, "production", attrs["env"])
	assert.Contains(t, attrs, "team")
	assert.Equal(t, "data-engineering", attrs["team"])

	// Verify workspace attributes
	assert.Contains(t, attrs, "databricksWorkspaceId")
	assert.Equal(t, mockWorkspaceId, attrs["databricksWorkspaceId"])
	assert.Contains(t, attrs, "databricksWorkspaceName")
	assert.Equal(
		t,
		mockWorkspaceHost,
		attrs["databricksWorkspaceName"],
	)
	assert.Contains(t, attrs, "databricksWorkspaceUrl")
	assert.Equal(
		t,
		"https://" + mockWorkspaceHost,
		attrs["databricksWorkspaceUrl"],
	)

	// Verify summary counts
	assert.Contains(t, attrs, "blockedJobRunCount")
	assert.Equal(t, 0, attrs["blockedJobRunCount"])
	assert.Contains(t, attrs, "waitingJobRunCount")
	assert.Equal(t, 0, attrs["waitingJobRunCount"])
	assert.Contains(t, attrs, "pendingJobRunCount")
	assert.Equal(t, 0, attrs["pendingJobRunCount"])
	assert.Contains(t, attrs, "queuedJobRunCount")
	assert.Equal(t, 0, attrs["queuedJobRunCount"])
	assert.Contains(t, attrs, "runningJobRunCount")
	assert.Equal(t, 0, attrs["runningJobRunCount"])
	assert.Contains(t, attrs, "terminatingJobRunCount")
	assert.Equal(t, 0, attrs["terminatingJobRunCount"])
	assert.Contains(t, attrs, "blockedTaskRunCount")
	assert.Equal(t, 0, attrs["blockedTaskRunCount"])
	assert.Contains(t, attrs, "waitingTaskRunCount")
	assert.Equal(t, 0, attrs["waitingTaskRunCount"])
	assert.Contains(t, attrs, "pendingTaskRunCount")
	assert.Equal(t, 0, attrs["pendingTaskRunCount"])
	assert.Contains(t, attrs, "queuedTaskRunCount")
	assert.Equal(t, 0, attrs["queuedTaskRunCount"])
	assert.Contains(t, attrs, "runningTaskRunCount")
	assert.Equal(t, 0, attrs["runningTaskRunCount"])
	assert.Contains(t, attrs, "terminatingTaskRunCount")
	assert.Equal(t, 0, attrs["terminatingTaskRunCount"])
}

func TestPollEvents_MakeJobRunSummaryAttributesError(t *testing.T) {
	// Setup up mock workspace
	mockWorkspace := setupMockWorkspace()

	// Setup expected error message
	expectedError := "error getting workspace info"

	// Save original GetWorkspaceInfo function and setup a defer function
	// to restore after the test
	originalGetWorkspaceInfo := GetWorkspaceInfo
	defer func() {
		GetWorkspaceInfo = originalGetWorkspaceInfo
	}()

	// Set GetWorkspaceInfo to return an error
	GetWorkspaceInfo = func(ctx context.Context, w DatabricksWorkspace) (
		*WorkspaceInfo,
		error,
	) {
		return nil, errors.New(expectedError)
	}

	// Setup mock integration
	mockIntegration := &integration.LabsIntegration{
		Interval: 60,
	}

	// Setup mock tag map
	tags := map[string]string{
		"env": "production",
		"team": "data-engineering",
	}

	// Setup mock startOffset
	startOffset := time.Duration(60) * time.Second

	// Mock Now
	now := time.Now()
	Now = func () time.Time { return now }

	// Setup a mock iterator
	mockIterator := &MockIterator[databricksSdkJobs.BaseRun]{}

	// Setup function call trackers
	hasNextCalled := 0
	nextCalled := 0

	// Setup the mock HasNext to increment the tracker and return false
	mockIterator.HasNextFunc = func(
		ctx context.Context,
	) bool {
		hasNextCalled += 1
		return false
	}

	// Setup the mock Next to increment the tracker and return a BaseRun with
	// zero values and no error. This should never be called since HasNext
	// returns false.
	mockIterator.NextFunc = func(
		ctx context.Context,
	) (databricksSdkJobs.BaseRun, error) {
		nextCalled += 1
		return databricksSdkJobs.BaseRun{}, nil
	}

	// Mock the ListJobRuns method to return the empty iterator
	mockWorkspace.ListJobRunsFunc = func(
		ctx context.Context,
		startOffset time.Duration,
	) databricksSdkListing.Iterator[databricksSdkJobs.BaseRun] {
		return mockIterator
	}

	// Setup an events channel to receive events
	eventsChan := make(chan model.Event, 500) // Buffer to prevent blocking

	// Create job run receiver instance
	receiver := NewDatabricksJobRunReceiver(
		mockIntegration,
		mockWorkspace,
		startOffset,
		tags,
	)

	// Execute the function under test
	err := receiver.PollEvents(context.Background(), eventsChan)

	// Close the channel
	close(eventsChan)

	// Verify result
	assert.Error(t, err)
	assert.Equal(t, expectedError, err.Error())
	assert.Equal(t, hasNextCalled, 1, "HasNext should have been called once")
	assert.Equal(t, nextCalled, 0, "Next should not have been called")

	// Collect all events for verification
	events := make([]model.Event, 0)
	for event := range eventsChan {
		events = append(events, event)
	}

	assert.Empty(
		t,
		events,
		"Expected no events to be produced when makeJobRunSummaryAttributes fails",
	)
}

func TestPollEvents_MakeJobRunStartAttributesError(t *testing.T) {
	// Setup up mock workspace
	mockWorkspace := setupMockWorkspace()

	// Setup expected error message
	expectedError := "error getting workspace info"

	// Save original GetWorkspaceInfo function and setup a defer function
	// to restore after the test
	originalGetWorkspaceInfo := GetWorkspaceInfo
	defer func() {
		GetWorkspaceInfo = originalGetWorkspaceInfo
	}()

	// Set GetWorkspaceInfo to return an error
	GetWorkspaceInfo = func(ctx context.Context, w DatabricksWorkspace) (
		*WorkspaceInfo,
		error,
	) {
		return nil, errors.New(expectedError)
	}

	// Setup mock integration
	mockIntegration := &integration.LabsIntegration{
		Interval: 60,
	}

	// Setup mock tag map
	tags := map[string]string{
		"env": "production",
		"team": "data-engineering",
	}

	// Setup mock startOffset
	startOffset := time.Duration(60) * time.Second

	// Mock Now
	now := time.Now()
	Now = func () time.Time { return now }

	// Setup times to be used to test behavior of event production based on
	// lastRun time.
	futureTime := now.Add(time.Hour)

	// Setup a mock iterator
	mockIterator := &MockIterator[databricksSdkJobs.BaseRun]{}

	// Setup function call trackers
	hasNextCalled := 0
	nextCalled := 0

	// Setup a mock run object
	mockRun := databricksSdkJobs.BaseRun{
		JobId:                0,
		RunId:                123,
		OriginalAttemptRunId: 123,
		RunName:              "This is run 1",
		AttemptNumber:        0,
		StartTime: futureTime.UnixMilli(),
		Status: &databricksSdkJobs.RunStatus{
			State:        databricksSdkJobs.RunLifecycleStateV2StateRunning,
		},
		Tasks: []databricksSdkJobs.RunTask{},
	}

	// Setup the mock HasNext to increment the tracker and return true once
	mockIterator.HasNextFunc = func(
		ctx context.Context,
	) bool {
		hasNextCalled += 1
		return hasNextCalled == 1 // Only return true once
	}

	// Setup the mock Next to increment the tracker and return the mock run
	mockIterator.NextFunc = func(
		ctx context.Context,
	) (databricksSdkJobs.BaseRun, error) {
		nextCalled += 1
		return mockRun, nil
	}

	// Mock the ListJobRuns method to return the iterator
	mockWorkspace.ListJobRunsFunc = func(
		ctx context.Context,
		startOffset time.Duration,
	) databricksSdkListing.Iterator[databricksSdkJobs.BaseRun] {
		return mockIterator
	}

	// Setup an events channel to receive events
	eventsChan := make(chan model.Event, 500) // Buffer to prevent blocking

	// Create job run receiver instance
	receiver := NewDatabricksJobRunReceiver(
		mockIntegration,
		mockWorkspace,
		startOffset,
		tags,
	)

	// Execute the function under test
	err := receiver.PollEvents(context.Background(), eventsChan)

	// Close the channel to iterate through it
	close(eventsChan)

	// Verify result
	assert.Error(t, err)
	assert.Equal(t, expectedError, err.Error())
	assert.Equal(t, hasNextCalled, 1, "HasNext should have been called once")
	assert.Equal(t, nextCalled, 1, "Next should have been called once")

	// Collect all events for verification
	events := make([]model.Event, 0)
	for event := range eventsChan {
		events = append(events, event)
	}

	assert.Empty(
		t,
		events,
		"Expected no events to be produced when makeJobRunStartAttributes fails",
	)
}

func TestPollEvents_RunStartedBefore(t *testing.T) {
	// Setup up mock workspace and workspace info
	mockWorkspace, teardown := setupMockWorkspaceAndInfo()
	defer teardown()

	// Setup mock integration
	mockIntegration := &integration.LabsIntegration{
		Interval: 60,
	}

	// Setup mock tag map
	tags := map[string]string{
		"env": "production",
		"team": "data-engineering",
	}

	// Setup mock startOffset
	startOffset := time.Duration(60) * time.Second

	// Mock Now
	now := time.Now()
	Now = func () time.Time { return now }

	// Setup times to be used to test behavior of event production based on
	// lastRun time.
	pastTime := now.Add(-time.Hour)

	// Setup a mock iterator
	mockIterator := &MockIterator[databricksSdkJobs.BaseRun]{}

	// Setup function call trackers
	hasNextCalled := 0
	nextCalled := 0

	// Setup a mock run object
	mockRun := databricksSdkJobs.BaseRun{
		JobId:                0,
		RunId:                123,
		OriginalAttemptRunId: 123,
		RunName:              "This is run 1",
		AttemptNumber:        0,
		StartTime:            pastTime.UnixMilli(),
		Status: &databricksSdkJobs.RunStatus{
			State:        databricksSdkJobs.RunLifecycleStateV2StateRunning,
		},
		Tasks: []databricksSdkJobs.RunTask{},
	}

	// Setup the mock HasNext to increment the tracker and return true once
	mockIterator.HasNextFunc = func(
		ctx context.Context,
	) bool {
		hasNextCalled += 1
		return hasNextCalled == 1 // Only return true once
	}

	// Setup the mock Next to increment the tracker and return the mock run
	mockIterator.NextFunc = func(
		ctx context.Context,
	) (databricksSdkJobs.BaseRun, error) {
		nextCalled += 1
		return mockRun, nil
	}

	// Mock the ListJobRuns method to return the mock iterator
	mockWorkspace.ListJobRunsFunc = func(
		ctx context.Context,
		startOffset time.Duration,
	) databricksSdkListing.Iterator[databricksSdkJobs.BaseRun] {
		return mockIterator
	}

	// Setup an events channel to receive events
	eventsChan := make(chan model.Event, 500) // Buffer to prevent blocking

	// Create job run receiver instance
	receiver := NewDatabricksJobRunReceiver(
		mockIntegration,
		mockWorkspace,
		startOffset,
		tags,
	)

	// Execute the function under test
	err := receiver.PollEvents(context.Background(), eventsChan)

	// Close the channel to iterate through it
	close(eventsChan)

	// Verify result
	assert.NoError(t, err)
	assert.Equal(t, hasNextCalled, 2, "HasNext should have been called twice")
	assert.Equal(t, nextCalled, 1, "Next should have been called once")

	// Collect all metrics for verification
	events := make([]model.Event, 0)
	for event := range eventsChan {
		events = append(events, event)
	}

	assert.Equal(
		t,
		1,
		len(events),
		"Expected one summary event to be produced when run starts before the lastRun time",
	)

	// Verify the event has the expected attributes
	event := events[0]

	// Verify timestamp
	assert.Equal(
		t,
		now.UnixMilli(),
		event.Timestamp, "Should have the correct event timestamp",
	)

	// Verify event type
	assert.Equal(t, "DatabricksJobRunSummary", event.Type)

	// Verify attributes
	assert.NotNil(t, event.Attributes)

	// Verify common attributes
	attrs := event.Attributes
	assert.NotEmpty(t, attrs)

	assert.Contains(t, attrs, "env")
	assert.Equal(t, "production", attrs["env"])
	assert.Contains(t, attrs, "team")
	assert.Equal(t, "data-engineering", attrs["team"])

	// Verify workspace attributes
	assert.Contains(t, attrs, "databricksWorkspaceId")
	assert.Equal(t, mockWorkspaceId, attrs["databricksWorkspaceId"])
	assert.Contains(t, attrs, "databricksWorkspaceName")
	assert.Equal(
		t,
		mockWorkspaceHost,
		attrs["databricksWorkspaceName"],
	)
	assert.Contains(t, attrs, "databricksWorkspaceUrl")
	assert.Equal(
		t,
		"https://" + mockWorkspaceHost,
		attrs["databricksWorkspaceUrl"],
	)

	// Verify summary counts
	assert.Contains(t, attrs, "blockedJobRunCount")
	assert.Equal(t, 0, attrs["blockedJobRunCount"])
	assert.Contains(t, attrs, "waitingJobRunCount")
	assert.Equal(t, 0, attrs["waitingJobRunCount"])
	assert.Contains(t, attrs, "pendingJobRunCount")
	assert.Equal(t, 0, attrs["pendingJobRunCount"])
	assert.Contains(t, attrs, "queuedJobRunCount")
	assert.Equal(t, 0, attrs["queuedJobRunCount"])
	assert.Contains(t, attrs, "runningJobRunCount")
	assert.Equal(t, 1, attrs["runningJobRunCount"])
	assert.Contains(t, attrs, "terminatingJobRunCount")
	assert.Equal(t, 0, attrs["terminatingJobRunCount"])
	assert.Contains(t, attrs, "blockedTaskRunCount")
	assert.Equal(t, 0, attrs["blockedTaskRunCount"])
	assert.Contains(t, attrs, "waitingTaskRunCount")
	assert.Equal(t, 0, attrs["waitingTaskRunCount"])
	assert.Contains(t, attrs, "pendingTaskRunCount")
	assert.Equal(t, 0, attrs["pendingTaskRunCount"])
	assert.Contains(t, attrs, "queuedTaskRunCount")
	assert.Equal(t, 0, attrs["queuedTaskRunCount"])
	assert.Contains(t, attrs, "runningTaskRunCount")
	assert.Equal(t, 0, attrs["runningTaskRunCount"])
	assert.Contains(t, attrs, "terminatingTaskRunCount")
	assert.Equal(t, 0, attrs["terminatingTaskRunCount"])
}

func TestPollEvents_RunStartedAfter(t *testing.T) {
	// Setup up mock workspace and workspace info
	mockWorkspace, teardown := setupMockWorkspaceAndInfo()
	defer teardown()

	// Setup mock integration
	mockIntegration := &integration.LabsIntegration{
		Interval: 60,
	}

	// Setup mock tag map
	tags := map[string]string{
		"env": "production",
		"team": "data-engineering",
	}

	// Setup mock startOffset
	startOffset := time.Duration(60) * time.Second

	// Mock Now
	now := time.Now()
	Now = func () time.Time { return now }

	// Setup times to be used to test behavior of event production based on
	// lastRun time.
	futureTime := now.Add(time.Hour)

	// Setup a mock iterator
	mockIterator := &MockIterator[databricksSdkJobs.BaseRun]{}

	// Setup function call trackers
	hasNextCalled := 0
	nextCalled := 0

	// Setup a mock run object
	mockRun := databricksSdkJobs.BaseRun{
		JobId:                0,
		RunId:                123,
		OriginalAttemptRunId: 123,
		RunName:              "This is run 1",
		AttemptNumber:        0,
		StartTime:            futureTime.UnixMilli(),
		Status: &databricksSdkJobs.RunStatus{
			State:        databricksSdkJobs.RunLifecycleStateV2StateRunning,
		},
		Tasks: []databricksSdkJobs.RunTask{},
	}

	// Setup the mock HasNext to increment the tracker and return true once
	mockIterator.HasNextFunc = func(
		ctx context.Context,
	) bool {
		hasNextCalled += 1
		return hasNextCalled == 1 // Only return true once
	}

	// Setup the mock Next to increment the tracker and return the mock run
	mockIterator.NextFunc = func(
		ctx context.Context,
	) (databricksSdkJobs.BaseRun, error) {
		nextCalled += 1
		return mockRun, nil
	}

	// Mock the ListJobRuns method to return the iterator
	mockWorkspace.ListJobRunsFunc = func(
		ctx context.Context,
		startOffset time.Duration,
	) databricksSdkListing.Iterator[databricksSdkJobs.BaseRun] {
		return mockIterator
	}

	// Setup an events channel to receive events
	eventsChan := make(chan model.Event, 500) // Buffer to prevent blocking

	// Create job run receiver instance
	receiver := NewDatabricksJobRunReceiver(
		mockIntegration,
		mockWorkspace,
		startOffset,
		tags,
	)

	// Execute the function under test
	err := receiver.PollEvents(context.Background(), eventsChan)

	// Close the channel to iterate through it
	close(eventsChan)

	// Verify result
	assert.NoError(t, err)
	assert.Equal(t, 2, hasNextCalled, "HasNext should have been called twice")
	assert.Equal(t, 1, nextCalled, "Next should have been called once")

	// Collect all events for verification
	events := make([]model.Event, 0)
	for event := range eventsChan {
		events = append(events, event)
	}

	assert.NotEmpty(
		t,
		events,
		"Expected events to be produced when job starts after the lastRun time",
	)
	assert.Equal(
		t,
		2,
		len(events),
		"Expected two events to be produced when job starts after the lastRun time",
	)

	validEventTypes := []string{"DatabricksJobRun", "DatabricksJobRunSummary"}

	foundJobRunEvent := false
	foundSummaryEvent := false

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

		// Verify common attributes
		attrs := event.Attributes
		assert.NotEmpty(t, attrs)

		assert.Contains(t, attrs, "env")
		assert.Equal(t, "production", attrs["env"])
		assert.Contains(t, attrs, "team")
		assert.Equal(t, "data-engineering", attrs["team"])

		// Verify workspace attributes
		assert.Contains(t, attrs, "databricksWorkspaceId")
		assert.Equal(t, mockWorkspaceId, attrs["databricksWorkspaceId"])
		assert.Contains(t, attrs, "databricksWorkspaceName")
		assert.Equal(
			t,
			mockWorkspaceHost,
			attrs["databricksWorkspaceName"],
		)
		assert.Contains(t, attrs, "databricksWorkspaceUrl")
		assert.Equal(
			t,
			"https://" + mockWorkspaceHost,
			attrs["databricksWorkspaceUrl"],
		)

		if event.Type == "DatabricksJobRun" {
			foundJobRunEvent = true

			// Verify base run attributes
			assert.Contains(t, attrs, "event")
			assert.Equal(t, "start", attrs["event"])
			assert.Contains(t, attrs, "jobId")
			assert.Equal(t, int64(0), attrs["jobId"])
			assert.Contains(t, attrs, "jobRunId")
			assert.Equal(t, int64(123), attrs["jobRunId"])
			assert.Contains(t, attrs, "jobRunName")
			assert.Equal(t, "This is run 1", attrs["jobRunName"])
			assert.Contains(t, attrs, "jobRunStartTime")
			assert.Equal(t, futureTime.UnixMilli(), attrs["jobRunStartTime"])
			assert.Contains(t, attrs, "attempt")
			assert.Equal(t, 0, attrs["attempt"])
			assert.Contains(t, attrs, "isRetry")
			assert.Equal(t, false, attrs["isRetry"])
		} else if event.Type == "DatabricksJobRunSummary" {
			foundSummaryEvent = true

			// Verify summary counts
			assert.Contains(t, attrs, "blockedJobRunCount")
			assert.Equal(t, 0, attrs["blockedJobRunCount"])
			assert.Contains(t, attrs, "waitingJobRunCount")
			assert.Equal(t, 0, attrs["waitingJobRunCount"])
			assert.Contains(t, attrs, "pendingJobRunCount")
			assert.Equal(t, 0, attrs["pendingJobRunCount"])
			assert.Contains(t, attrs, "queuedJobRunCount")
			assert.Equal(t, 0, attrs["queuedJobRunCount"])
			assert.Contains(t, attrs, "runningJobRunCount")
			assert.Equal(t, 1, attrs["runningJobRunCount"])
			assert.Contains(t, attrs, "terminatingJobRunCount")
			assert.Equal(t, 0, attrs["terminatingJobRunCount"])
			assert.Contains(t, attrs, "blockedTaskRunCount")
			assert.Equal(t, 0, attrs["blockedTaskRunCount"])
			assert.Contains(t, attrs, "waitingTaskRunCount")
			assert.Equal(t, 0, attrs["waitingTaskRunCount"])
			assert.Contains(t, attrs, "pendingTaskRunCount")
			assert.Equal(t, 0, attrs["pendingTaskRunCount"])
			assert.Contains(t, attrs, "queuedTaskRunCount")
			assert.Equal(t, 0, attrs["queuedTaskRunCount"])
			assert.Contains(t, attrs, "runningTaskRunCount")
			assert.Equal(t, 0, attrs["runningTaskRunCount"])
			assert.Contains(t, attrs, "terminatingTaskRunCount")
			assert.Equal(t, 0, attrs["terminatingTaskRunCount"])
		}
	}

	assert.True(t, foundJobRunEvent)
	assert.True(t, foundSummaryEvent)
}

func TestPollEvents_MakeJobRunCompleteAttributesError(t *testing.T) {
	// Setup up mock workspace
	mockWorkspace := setupMockWorkspace()

	// Setup expected error message
	expectedError := "error getting workspace info"

	// Save original GetWorkspaceInfo function and setup a defer function
	// to restore after the test
	originalGetWorkspaceInfo := GetWorkspaceInfo
	defer func() {
		GetWorkspaceInfo = originalGetWorkspaceInfo
	}()

	// Set GetWorkspaceInfo to return an error
	GetWorkspaceInfo = func(ctx context.Context, w DatabricksWorkspace) (
		*WorkspaceInfo,
		error,
	) {
		return nil, errors.New(expectedError)
	}

	// Setup mock integration
	mockIntegration := &integration.LabsIntegration{
		Interval: 60,
	}

	// Setup mock tag map
	tags := map[string]string{
		"env": "production",
		"team": "data-engineering",
	}

	// Setup mock startOffset
	startOffset := time.Duration(60) * time.Second

	// Mock Now
	now := time.Now()
	Now = func () time.Time { return now }

	// Setup times to be used to test behavior of event production based on
	// lastRun time.
	futureTime := now.Add(time.Hour)

	// Setup a mock iterator
	mockIterator := &MockIterator[databricksSdkJobs.BaseRun]{}

	// Setup function call trackers
	hasNextCalled := 0
	nextCalled := 0

	// Setup a mock run object
	mockRun := databricksSdkJobs.BaseRun{
		JobId:                0,
		RunId:                123,
		OriginalAttemptRunId: 123,
		RunName:              "This is run 1",
		AttemptNumber:        0,
		EndTime:              futureTime.UnixMilli(),
		QueueDuration:        1000,
		SetupDuration:        500,
		ExecutionDuration:    1000,
		CleanupDuration:      300,
		Status: &databricksSdkJobs.RunStatus{
			State:        databricksSdkJobs.RunLifecycleStateV2StateTerminated,
			TerminationDetails: &databricksSdkJobs.TerminationDetails{
				Code: databricksSdkJobs.TerminationCodeCodeSuccess,
				Type: databricksSdkJobs.TerminationTypeTypeSuccess,
			},
		},
		Tasks: []databricksSdkJobs.RunTask{},
	}

	// Setup the mock HasNext to increment the tracker and return true once
	mockIterator.HasNextFunc = func(
		ctx context.Context,
	) bool {
		hasNextCalled += 1
		return hasNextCalled == 1 // Only return true once
	}

	// Setup the mock Next to increment the tracker and return the mock run
	mockIterator.NextFunc = func(
		ctx context.Context,
	) (databricksSdkJobs.BaseRun, error) {
		nextCalled += 1
		return mockRun, nil
	}

	// Mock the ListJobRuns method to return the iterator
	mockWorkspace.ListJobRunsFunc = func(
		ctx context.Context,
		startOffset time.Duration,
	) databricksSdkListing.Iterator[databricksSdkJobs.BaseRun] {
		return mockIterator
	}

	// Setup an events channel to receive events
	eventsChan := make(chan model.Event, 500) // Buffer to prevent blocking

	// Create job run receiver instance
	receiver := NewDatabricksJobRunReceiver(
		mockIntegration,
		mockWorkspace,
		startOffset,
		tags,
	)

	// Execute the function under test
	err := receiver.PollEvents(context.Background(), eventsChan)

	// Close the channel to iterate through it
	close(eventsChan)

	// Verify result
	assert.Error(t, err)
	assert.Equal(t, expectedError, err.Error())
	assert.Equal(t, hasNextCalled, 1, "HasNext should have been called once")
	assert.Equal(t, nextCalled, 1, "Next should have been called once")

	// Collect all events for verification
	events := make([]model.Event, 0)
	for event := range eventsChan {
		events = append(events, event)
	}

	assert.Empty(
		t,
		events,
		"Expected no events to be produced when makeJobRunCompleteAttributes fails",
	)
}

func TestPollEvents_RunTerminatedBefore(t *testing.T) {
	// Setup up mock workspace and workspace info
	mockWorkspace, teardown := setupMockWorkspaceAndInfo()
	defer teardown()

	// Setup mock integration
	mockIntegration := &integration.LabsIntegration{
		Interval: 60,
	}

	// Setup mock tag map
	tags := map[string]string{
		"env": "production",
		"team": "data-engineering",
	}

	// Setup mock startOffset
	startOffset := time.Duration(60) * time.Second

	// Mock Now
	now := time.Now()
	Now = func () time.Time { return now }

	// Setup times to be used to test behavior of event production based on
	// lastRun time.
	pastTime := now.Add(-time.Hour)

	// Setup a mock iterator
	mockIterator := &MockIterator[databricksSdkJobs.BaseRun]{}

	// Setup function call trackers
	hasNextCalled := 0
	nextCalled := 0

	// Setup a mock run object
	mockRun := databricksSdkJobs.BaseRun{
		JobId:                0,
		RunId:                123,
		OriginalAttemptRunId: 123,
		RunName:              "This is run 1",
		AttemptNumber:        0,
		EndTime:              pastTime.UnixMilli(),
		QueueDuration:        1000,
		SetupDuration:        500,
		ExecutionDuration:    1000,
		CleanupDuration:      300,
		Status: &databricksSdkJobs.RunStatus{
			State:        databricksSdkJobs.RunLifecycleStateV2StateTerminated,
			TerminationDetails: &databricksSdkJobs.TerminationDetails{
				Code: databricksSdkJobs.TerminationCodeCodeSuccess,
				Type: databricksSdkJobs.TerminationTypeTypeSuccess,
			},
		},
		Tasks: []databricksSdkJobs.RunTask{},
	}

	// Setup the mock HasNext to increment the tracker and return true once
	mockIterator.HasNextFunc = func(
		ctx context.Context,
	) bool {
		hasNextCalled += 1
		return hasNextCalled == 1 // Only return true once
	}

	// Setup the mock Next to increment the tracker and return the mock run
	mockIterator.NextFunc = func(
		ctx context.Context,
	) (databricksSdkJobs.BaseRun, error) {
		nextCalled += 1
		return mockRun, nil
	}

	// Mock the ListJobRuns method to return the iterator
	mockWorkspace.ListJobRunsFunc = func(
		ctx context.Context,
		startOffset time.Duration,
	) databricksSdkListing.Iterator[databricksSdkJobs.BaseRun] {
		return mockIterator
	}

	// Setup an events channel to receive events
	eventsChan := make(chan model.Event, 500) // Buffer to prevent blocking

	// Create job run receiver instance
	receiver := NewDatabricksJobRunReceiver(
		mockIntegration,
		mockWorkspace,
		startOffset,
		tags,
	)

	// Execute the function under test
	err := receiver.PollEvents(context.Background(), eventsChan)

	// Close the channel to iterate through it
	close(eventsChan)

	// Verify result
	assert.NoError(t, err)
	assert.Equal(t, hasNextCalled, 2, "HasNext should have been called twice")
	assert.Equal(t, nextCalled, 1, "Next should have been called once")

	// Collect all events for verification
	events := make([]model.Event, 0)
	for event := range eventsChan {
		events = append(events, event)
	}

	assert.Equal(
		t,
		1,
		len(events),
		"Expected one summary event to be produced when run terminates before the lastRun time",
	)

	// Verify the event has the expected attributes
	event := events[0]

	// Verify timestamp
	assert.Equal(
		t,
		now.UnixMilli(),
		event.Timestamp, "Should have the correct event timestamp",
	)

	// Verify event type
	assert.Equal(t, "DatabricksJobRunSummary", event.Type)

	// Verify attributes
	assert.NotNil(t, event.Attributes)

	// Verify common attributes
	attrs := event.Attributes
	assert.NotEmpty(t, attrs)

	assert.Contains(t, attrs, "env")
	assert.Equal(t, "production", attrs["env"])
	assert.Contains(t, attrs, "team")
	assert.Equal(t, "data-engineering", attrs["team"])

	// Verify workspace attributes
	assert.Contains(t, attrs, "databricksWorkspaceId")
	assert.Equal(t, mockWorkspaceId, attrs["databricksWorkspaceId"])
	assert.Contains(t, attrs, "databricksWorkspaceName")
	assert.Equal(
		t,
		mockWorkspaceHost,
		attrs["databricksWorkspaceName"],
	)
	assert.Contains(t, attrs, "databricksWorkspaceUrl")
	assert.Equal(
		t,
		"https://" + mockWorkspaceHost,
		attrs["databricksWorkspaceUrl"],
	)

	// Verify summary counts
	assert.Contains(t, attrs, "blockedJobRunCount")
	assert.Equal(t, 0, attrs["blockedJobRunCount"])
	assert.Contains(t, attrs, "waitingJobRunCount")
	assert.Equal(t, 0, attrs["waitingJobRunCount"])
	assert.Contains(t, attrs, "pendingJobRunCount")
	assert.Equal(t, 0, attrs["pendingJobRunCount"])
	assert.Contains(t, attrs, "queuedJobRunCount")
	assert.Equal(t, 0, attrs["queuedJobRunCount"])
	assert.Contains(t, attrs, "runningJobRunCount")
	assert.Equal(t, 0, attrs["runningJobRunCount"])
	assert.Contains(t, attrs, "terminatingJobRunCount")
	assert.Equal(t, 0, attrs["terminatingJobRunCount"])
	assert.Contains(t, attrs, "blockedTaskRunCount")
	assert.Equal(t, 0, attrs["blockedTaskRunCount"])
	assert.Contains(t, attrs, "waitingTaskRunCount")
	assert.Equal(t, 0, attrs["waitingTaskRunCount"])
	assert.Contains(t, attrs, "pendingTaskRunCount")
	assert.Equal(t, 0, attrs["pendingTaskRunCount"])
	assert.Contains(t, attrs, "queuedTaskRunCount")
	assert.Equal(t, 0, attrs["queuedTaskRunCount"])
	assert.Contains(t, attrs, "runningTaskRunCount")
	assert.Equal(t, 0, attrs["runningTaskRunCount"])
	assert.Contains(t, attrs, "terminatingTaskRunCount")
	assert.Equal(t, 0, attrs["terminatingTaskRunCount"])
}

func TestPollEvents_RunTerminatedAfter(t *testing.T) {
	// Setup mock workspace and workspace info
	mockWorkspace, teardown := setupMockWorkspaceAndInfo()
	defer teardown()

	// Setup mock integration
	mockIntegration := &integration.LabsIntegration{
		Interval: 60,
	}

	// Setup mock tag map
	tags := map[string]string{
		"env": "production",
		"team": "data-engineering",
	}

	// Setup mock startOffset
	startOffset := time.Duration(60) * time.Second

	// Mock Now
	now := time.Now()
	Now = func () time.Time { return now }

	// Setup times to be used to test behavior of event production based on
	// lastRun time.
	pastTime := now.Add(-time.Hour)
	futureTime := now.Add(time.Hour)

	// Setup a mock iterator
	mockIterator := &MockIterator[databricksSdkJobs.BaseRun]{}

	// Setup function call trackers
	hasNextCalled := 0
	nextCalled := 0

	// Setup a mock run object
	mockRun := databricksSdkJobs.BaseRun{
		JobId:                0,
		RunId:                123,
		OriginalAttemptRunId: 123,
		RunName:              "This is run 1",
		AttemptNumber:        0,
		StartTime:            pastTime.UnixMilli(),
		EndTime:              futureTime.UnixMilli(),
		QueueDuration:        1000,
		SetupDuration:        500,
		ExecutionDuration:    1000,
		CleanupDuration:      300,
		Status: &databricksSdkJobs.RunStatus{
			State:        databricksSdkJobs.RunLifecycleStateV2StateTerminated,
			TerminationDetails: &databricksSdkJobs.TerminationDetails{
				Code: databricksSdkJobs.TerminationCodeCodeSuccess,
				Type: databricksSdkJobs.TerminationTypeTypeSuccess,
			},
		},
		Tasks: []databricksSdkJobs.RunTask{},
	}

	// Setup the mock HasNext to increment the tracker and return true once
	mockIterator.HasNextFunc = func(
		ctx context.Context,
	) bool {
		hasNextCalled += 1
		return hasNextCalled == 1 // Only return true once
	}

	// Setup the mock Next to increment the tracker and return the mock run
	mockIterator.NextFunc = func(
		ctx context.Context,
	) (databricksSdkJobs.BaseRun, error) {
		nextCalled += 1
		return mockRun, nil
	}

	// Mock the ListJobRuns method to return the iterator
	mockWorkspace.ListJobRunsFunc = func(
		ctx context.Context,
		startOffset time.Duration,
	) databricksSdkListing.Iterator[databricksSdkJobs.BaseRun] {
		return mockIterator
	}

	// Setup an events channel to receive events
	eventsChan := make(chan model.Event, 500) // Buffer to prevent blocking

	// Create job run receiver instance
	receiver := NewDatabricksJobRunReceiver(
		mockIntegration,
		mockWorkspace,
		startOffset,
		tags,
	)

	// Execute the function under test
	err := receiver.PollEvents(context.Background(), eventsChan)

	// Close the channel to iterate through it
	close(eventsChan)

	// Verify result
	assert.NoError(t, err)
	assert.Equal(t, hasNextCalled, 2, "HasNext should have been called twice")
	assert.Equal(t, nextCalled, 1, "Next should have been called once")

	// Collect all events for verification
	events := make([]model.Event, 0)
	for event := range eventsChan {
		events = append(events, event)
	}

	assert.NotEmpty(
		t,
		events,
		"Expected events to be produced when job terminates after the lastRun time",
	)
	assert.Equal(
		t,
		2,
		len(events),
		"Expected two events to be produced when job terminates after the lastRun time",
	)

	validEventTypes := []string{"DatabricksJobRun", "DatabricksJobRunSummary"}

	foundJobRunEvent := false
	foundSummaryEvent := false

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

		// Verify common attributes
		attrs := event.Attributes
		assert.NotEmpty(t, attrs)

		assert.Contains(t, attrs, "env")
		assert.Equal(t, "production", attrs["env"])
		assert.Contains(t, attrs, "team")
		assert.Equal(t, "data-engineering", attrs["team"])

		// Verify workspace attributes
		assert.Contains(t, attrs, "databricksWorkspaceId")
		assert.Equal(t, mockWorkspaceId, attrs["databricksWorkspaceId"])
		assert.Contains(t, attrs, "databricksWorkspaceName")
		assert.Equal(
			t,
			mockWorkspaceHost,
			attrs["databricksWorkspaceName"],
		)
		assert.Contains(t, attrs, "databricksWorkspaceUrl")
		assert.Equal(
			t,
			"https://" + mockWorkspaceHost,
			attrs["databricksWorkspaceUrl"],
		)

		if event.Type == "DatabricksJobRun" {
			foundJobRunEvent = true

			// Verify base run attributes
			assert.Contains(t, attrs, "event")
			assert.Equal(t, "complete", attrs["event"])
			assert.Contains(t, attrs, "jobId")
			assert.Equal(t, int64(0), attrs["jobId"])
			assert.Contains(t, attrs, "jobRunId")
			assert.Equal(t, int64(123), attrs["jobRunId"])
			assert.Contains(t, attrs, "jobRunName")
			assert.Equal(t, "This is run 1", attrs["jobRunName"])
			assert.Contains(t, attrs, "jobRunStartTime")
			assert.Equal(t, pastTime.UnixMilli(), attrs["jobRunStartTime"])
			assert.Contains(t, attrs, "attempt")
			assert.Equal(t, 0, attrs["attempt"])
			assert.Contains(t, attrs, "isRetry")
			assert.Equal(t, false, attrs["isRetry"])

			// Verify run complete attributes
			assert.Contains(t, attrs, "state")
			assert.Equal(
				t,
				string(databricksSdkJobs.RunLifecycleStateV2StateTerminated),
				attrs["state"],
			)
			assert.Contains(t, attrs, "terminationCode")
			assert.Equal(
				t,
				string(databricksSdkJobs.TerminationCodeCodeSuccess),
				attrs["terminationCode"],
			)
			assert.Contains(t, attrs, "terminationType")
			assert.Equal(
				t,
				string(databricksSdkJobs.TerminationTypeTypeSuccess),
				attrs["terminationType"],
			)
			assert.Contains(t, attrs, "jobRunEndTime")
			assert.Equal(t, futureTime.UnixMilli(), attrs["jobRunEndTime"])
			assert.Contains(t, attrs, "duration")
			assert.Equal(
				t,
				int64(1800), // 500 + 1000 + 300
				attrs["duration"],
			)
			assert.Contains(t, attrs, "queueDuration")
			assert.Equal(
				t,
				int64(1000),
				attrs["queueDuration"],
			)
			assert.Contains(t, attrs, "setupDuration")
			assert.Equal(
				t,
				int64(500),
				attrs["setupDuration"],
			)
			assert.Contains(t, attrs, "executionDuration")
			assert.Equal(
				t,
				int64(1000),
				attrs["executionDuration"],
			)
			assert.Contains(t, attrs, "cleanupDuration")
			assert.Equal(
				t,
				int64(300),
				attrs["cleanupDuration"],
			)
		} else if event.Type == "DatabricksJobRunSummary" {
			foundSummaryEvent = true

			// Verify summary counts
			assert.Contains(t, attrs, "blockedJobRunCount")
			assert.Equal(t, 0, attrs["blockedJobRunCount"])
			assert.Contains(t, attrs, "waitingJobRunCount")
			assert.Equal(t, 0, attrs["waitingJobRunCount"])
			assert.Contains(t, attrs, "pendingJobRunCount")
			assert.Equal(t, 0, attrs["pendingJobRunCount"])
			assert.Contains(t, attrs, "queuedJobRunCount")
			assert.Equal(t, 0, attrs["queuedJobRunCount"])
			assert.Contains(t, attrs, "runningJobRunCount")
			assert.Equal(t, 0, attrs["runningJobRunCount"])
			assert.Contains(t, attrs, "terminatingJobRunCount")
			assert.Equal(t, 0, attrs["terminatingJobRunCount"])
			assert.Contains(t, attrs, "blockedTaskRunCount")
			assert.Equal(t, 0, attrs["blockedTaskRunCount"])
			assert.Contains(t, attrs, "waitingTaskRunCount")
			assert.Equal(t, 0, attrs["waitingTaskRunCount"])
			assert.Contains(t, attrs, "pendingTaskRunCount")
			assert.Equal(t, 0, attrs["pendingTaskRunCount"])
			assert.Contains(t, attrs, "queuedTaskRunCount")
			assert.Equal(t, 0, attrs["queuedTaskRunCount"])
			assert.Contains(t, attrs, "runningTaskRunCount")
			assert.Equal(t, 0, attrs["runningTaskRunCount"])
			assert.Contains(t, attrs, "terminatingTaskRunCount")
			assert.Equal(t, 0, attrs["terminatingTaskRunCount"])
		}
	}

	assert.True(t, foundJobRunEvent)
	assert.True(t, foundSummaryEvent)
}

func TestPollEvents_Counters(t *testing.T) {
	// Setup up mock workspace and workspace info
	mockWorkspace, teardown := setupMockWorkspaceAndInfo()
	defer teardown()

	// Setup mock integration
	mockIntegration := &integration.LabsIntegration{
		Interval: 60,
	}

	// Setup mock tag map
	tags := map[string]string{
		"env": "production",
		"team": "data-engineering",
	}

	// Setup mock startOffset
	startOffset := time.Duration(60) * time.Second

	// Mock Now
	now := time.Now()
	Now = func () time.Time { return now }

	// Setup times to be used to test behavior of event production based on
	// lastRun time.
	pastTime := now.Add(-time.Hour)

	// Setup a mock iterator
	mockIterator := &MockIterator[databricksSdkJobs.BaseRun]{}

	// Setup hasNext controls
	index := 0

	// Setup a mock run object
	mockRuns := []databricksSdkJobs.BaseRun{
		{
			JobId:                0,
			RunId:                123,
			OriginalAttemptRunId: 123,
			RunName:              "This is run 1",
			AttemptNumber:        0,
			StartTime:            pastTime.UnixMilli(),
			Status: &databricksSdkJobs.RunStatus{
				State:        databricksSdkJobs.RunLifecycleStateV2StateRunning,
			},
			Tasks: []databricksSdkJobs.RunTask{
				{
					TaskKey:             "task1",
					RunId:               456,
					AttemptNumber:       0,
					Description:         "This is task run 1 description",
					// started before
					StartTime:           pastTime.UnixMilli(),
					ClusterInstance: &databricksSdkJobs.ClusterInstance{
						ClusterId:      "fake-cluster-id",
						SparkContextId: "fake-cluster-spark-context-id",
					},
					Status: &databricksSdkJobs.RunStatus{
						State: databricksSdkJobs.RunLifecycleStateV2StateBlocked,
					},
				},
				{
					TaskKey:             "task2",
					RunId:               457,
					AttemptNumber:       0,
					Description:         "This is task run 2 description",
					// started before
					StartTime:           pastTime.UnixMilli(),
					ClusterInstance: &databricksSdkJobs.ClusterInstance{
						ClusterId:      "fake-cluster-id",
						SparkContextId: "fake-cluster-spark-context-id",
					},
					Status: &databricksSdkJobs.RunStatus{
						State: databricksSdkJobs.RunLifecycleStateV2StatePending,
					},
				},
				{
					TaskKey:             "task3",
					RunId:               458,
					AttemptNumber:       0,
					Description:         "This is task run 3 description",
					// started before
					StartTime:           pastTime.UnixMilli(),
					ClusterInstance: &databricksSdkJobs.ClusterInstance{
						ClusterId:      "fake-cluster-id",
						SparkContextId: "fake-cluster-spark-context-id",
					},
					Status: &databricksSdkJobs.RunStatus{
						State: databricksSdkJobs.RunLifecycleStateV2StateRunning,
					},
				},
				{
					TaskKey:             "task4",
					RunId:               459,
					AttemptNumber:       0,
					Description:         "This is task run 4 description",
					// started before
					StartTime:           pastTime.UnixMilli(),
					ClusterInstance: &databricksSdkJobs.ClusterInstance{
						ClusterId:      "fake-cluster-id",
						SparkContextId: "fake-cluster-spark-context-id",
					},
					Status: &databricksSdkJobs.RunStatus{
						State: databricksSdkJobs.RunLifecycleStateV2StateTerminating,
					},
				},
			},
		},
		{
			JobId:                1,
			RunId:                124,
			OriginalAttemptRunId: 124,
			RunName:              "This is run 2",
			AttemptNumber:        0,
			StartTime:            pastTime.UnixMilli(),
			Status: &databricksSdkJobs.RunStatus{
				State:        databricksSdkJobs.RunLifecycleStateV2StateBlocked,
			},
			Tasks: []databricksSdkJobs.RunTask{},
		},
		{
			JobId:                2,
			RunId:                125,
			OriginalAttemptRunId: 125,
			RunName:              "This is run 3",
			AttemptNumber:        0,
			StartTime:            pastTime.UnixMilli(),
			Status: &databricksSdkJobs.RunStatus{
				State:        databricksSdkJobs.RunLifecycleStateV2StatePending,
			},
			Tasks: []databricksSdkJobs.RunTask{
				{
					TaskKey:             "task5",
					RunId:               460,
					AttemptNumber:       0,
					Description:         "This is task run 5 description",
					// started before
					StartTime:           pastTime.UnixMilli(),
					ClusterInstance: &databricksSdkJobs.ClusterInstance{
						ClusterId:      "fake-cluster-id",
						SparkContextId: "fake-cluster-spark-context-id",
					},
					Status: &databricksSdkJobs.RunStatus{
						State: databricksSdkJobs.RunLifecycleStateV2StatePending,
					},
				},
				{
					TaskKey:             "task6",
					RunId:               461,
					AttemptNumber:       0,
					Description:         "This is task run 6 description",
					// started before
					StartTime:           pastTime.UnixMilli(),
					ClusterInstance: &databricksSdkJobs.ClusterInstance{
						ClusterId:      "fake-cluster-id",
						SparkContextId: "fake-cluster-spark-context-id",
					},
					Status: &databricksSdkJobs.RunStatus{
						State: databricksSdkJobs.RunLifecycleStateV2StatePending,
					},
				},
			},
		},
		{
			JobId:                3,
			RunId:                126,
			OriginalAttemptRunId: 126,
			RunName:              "This is run 4",
			AttemptNumber:        0,
			StartTime:            pastTime.UnixMilli(),
			Status: &databricksSdkJobs.RunStatus{
				State:        databricksSdkJobs.RunLifecycleStateV2StateBlocked,
			},
			Tasks: []databricksSdkJobs.RunTask{
				{
					TaskKey:             "task7",
					RunId:               462,
					AttemptNumber:       0,
					Description:         "This is task run 7 description",
					// started before
					StartTime:           pastTime.UnixMilli(),
					ClusterInstance: &databricksSdkJobs.ClusterInstance{
						ClusterId:      "fake-cluster-id",
						SparkContextId: "fake-cluster-spark-context-id",
					},
					Status: &databricksSdkJobs.RunStatus{
						State: databricksSdkJobs.RunLifecycleStateV2StateBlocked,
					},
				},
			},
		},
		{
			JobId:                4,
			RunId:                127,
			OriginalAttemptRunId: 127,
			RunName:              "This is run 5",
			AttemptNumber:        0,
			StartTime:            pastTime.UnixMilli(),
			Status: &databricksSdkJobs.RunStatus{
				State:        databricksSdkJobs.RunLifecycleStateV2StateQueued,
			},
			Tasks: []databricksSdkJobs.RunTask{
				{
					TaskKey:             "task8",
					RunId:               463,
					AttemptNumber:       0,
					Description:         "This is task run 8 description",
					// started before
					StartTime:           pastTime.UnixMilli(),
					ClusterInstance: &databricksSdkJobs.ClusterInstance{
						ClusterId:      "fake-cluster-id",
						SparkContextId: "fake-cluster-spark-context-id",
					},
					Status: &databricksSdkJobs.RunStatus{
						State: databricksSdkJobs.RunLifecycleStateV2StateQueued,
					},
				},
				{
					TaskKey:             "task9",
					RunId:               464,
					AttemptNumber:       0,
					Description:         "This is task run 9 description",
					// started before
					StartTime:           pastTime.UnixMilli(),
					ClusterInstance: &databricksSdkJobs.ClusterInstance{
						ClusterId:      "fake-cluster-id",
						SparkContextId: "fake-cluster-spark-context-id",
					},
					Status: &databricksSdkJobs.RunStatus{
						State: databricksSdkJobs.RunLifecycleStateV2StateQueued,
					},
				},
			},
		},
		{
			JobId:                5,
			RunId:                128,
			OriginalAttemptRunId: 128,
			RunName:              "This is run 6",
			AttemptNumber:        0,
			StartTime:            pastTime.UnixMilli(),
			Status: &databricksSdkJobs.RunStatus{
				State:        databricksSdkJobs.RunLifecycleStateV2StateTerminating,
			},
			Tasks: []databricksSdkJobs.RunTask{},
		},
		{
			JobId:                6,
			RunId:                129,
			OriginalAttemptRunId: 129,
			RunName:              "This is run 7",
			AttemptNumber:        0,
			StartTime:            pastTime.UnixMilli(),
			Status: &databricksSdkJobs.RunStatus{
				State:        databricksSdkJobs.RunLifecycleStateV2StateRunning,
			},
			Tasks: []databricksSdkJobs.RunTask{
				{
					TaskKey:             "task10",
					RunId:               465,
					AttemptNumber:       0,
					Description:         "This is task run 10 description",
					// started before
					StartTime:           pastTime.UnixMilli(),
					ClusterInstance: &databricksSdkJobs.ClusterInstance{
						ClusterId:      "fake-cluster-id",
						SparkContextId: "fake-cluster-spark-context-id",
					},
					Status: &databricksSdkJobs.RunStatus{
						State: databricksSdkJobs.RunLifecycleStateV2StateRunning,
					},
				},
				{
					TaskKey:             "task11",
					RunId:               466,
					AttemptNumber:       0,
					Description:         "This is task run 11 description",
					// started before
					StartTime:           pastTime.UnixMilli(),
					ClusterInstance: &databricksSdkJobs.ClusterInstance{
						ClusterId:      "fake-cluster-id",
						SparkContextId: "fake-cluster-spark-context-id",
					},
					Status: &databricksSdkJobs.RunStatus{
						State: databricksSdkJobs.RunLifecycleStateV2StateRunning,
					},
				},
			},
		},
		{
			JobId:                7,
			RunId:                130,
			OriginalAttemptRunId: 130,
			RunName:              "This is run 8",
			AttemptNumber:        0,
			StartTime:            pastTime.UnixMilli(),
			Status: &databricksSdkJobs.RunStatus{
				State:        databricksSdkJobs.RunLifecycleStateV2StateBlocked,
			},
			Tasks: []databricksSdkJobs.RunTask{},
		},
		{
			JobId:                8,
			RunId:                131,
			OriginalAttemptRunId: 131,
			RunName:              "This is run 9",
			AttemptNumber:        0,
			StartTime:            pastTime.UnixMilli(),
			Status: &databricksSdkJobs.RunStatus{
				State:        databricksSdkJobs.RunLifecycleStateV2StatePending,
			},
			Tasks: []databricksSdkJobs.RunTask{
				{
					TaskKey:             "task12",
					RunId:               467,
					AttemptNumber:       0,
					Description:         "This is task run 12 description",
					// started before
					StartTime:           pastTime.UnixMilli(),
					ClusterInstance: &databricksSdkJobs.ClusterInstance{
						ClusterId:      "fake-cluster-id",
						SparkContextId: "fake-cluster-spark-context-id",
					},
					Status: &databricksSdkJobs.RunStatus{
						State: databricksSdkJobs.RunLifecycleStateV2StatePending,
					},
				},
			},
		},
		{
			JobId:                9,
			RunId:                132,
			OriginalAttemptRunId: 132,
			RunName:              "This is run 10",
			AttemptNumber:        0,
			StartTime:            pastTime.UnixMilli(),
			Status: &databricksSdkJobs.RunStatus{
				State:        databricksSdkJobs.RunLifecycleStateV2StateWaiting,
			},
			Tasks: []databricksSdkJobs.RunTask{
				{
					TaskKey:             "task13",
					RunId:               468,
					AttemptNumber:       0,
					Description:         "This is task run 13 description",
					// started before
					StartTime:           pastTime.UnixMilli(),
					ClusterInstance: &databricksSdkJobs.ClusterInstance{
						ClusterId:      "fake-cluster-id",
						SparkContextId: "fake-cluster-spark-context-id",
					},
					Status: &databricksSdkJobs.RunStatus{
						State: databricksSdkJobs.RunLifecycleStateV2StateWaiting,
					},
				},
			},
		},
	}

	// Setup the mock HasNext to return true when there are more runs to iterate
	// over.
	mockIterator.HasNextFunc = func(
		ctx context.Context,
	) bool {
		index += 1
		return index <= len(mockRuns)
	}

	// Setup the mock Next to return the next run in the list
	mockIterator.NextFunc = func(
		ctx context.Context,
	) (databricksSdkJobs.BaseRun, error) {
		return mockRuns[index - 1], nil
	}

	// Mock the ListJobRuns method to return the iterator
	mockWorkspace.ListJobRunsFunc = func(
		ctx context.Context,
		startOffset time.Duration,
	) databricksSdkListing.Iterator[databricksSdkJobs.BaseRun] {
		return mockIterator
	}

	// Setup an events channel to receive events
	eventsChan := make(chan model.Event, 500) // Buffer to prevent blocking

	// Create job run receiver instance
	receiver := NewDatabricksJobRunReceiver(
		mockIntegration,
		mockWorkspace,
		startOffset,
		tags,
	)

	// Execute the function under test
	err := receiver.PollEvents(context.Background(), eventsChan)

	// Close the channel to iterate through it
	close(eventsChan)

	// Verify result
	assert.NoError(t, err)

	// Collect all events for verification
	events := make([]model.Event, 0)
	for event := range eventsChan {
		events = append(events, event)
	}

	assert.Equal(
		t,
		1,
		len(events),
		"Expected one summary event to be produced",
	)

	// Verify the event has the expected attributes
	event := events[0]

	// Verify timestamp
	assert.Equal(
		t,
		now.UnixMilli(),
		event.Timestamp,
		"Should have the correct event timestamp",
	)

	// Verify event type
	assert.Equal(
		t,
		"DatabricksJobRunSummary",
		event.Type,
	)

	// Verify attributes
	assert.NotNil(t, event.Attributes)

	// Verify common attributes
	attrs := event.Attributes
	assert.NotEmpty(t, attrs)

	assert.Contains(t, attrs, "env")
	assert.Equal(t, "production", attrs["env"])
	assert.Contains(t, attrs, "team")
	assert.Equal(t, "data-engineering", attrs["team"])

	// Verify workspace attributes
	assert.Contains(t, attrs, "databricksWorkspaceId")
	assert.Equal(t, mockWorkspaceId, attrs["databricksWorkspaceId"])
	assert.Contains(t, attrs, "databricksWorkspaceName")
	assert.Equal(
		t,
		mockWorkspaceHost,
		attrs["databricksWorkspaceName"],
	)
	assert.Contains(t, attrs, "databricksWorkspaceUrl")
	assert.Equal(
		t,
		"https://" + mockWorkspaceHost,
		attrs["databricksWorkspaceUrl"],
	)

	// Verify summary counts
	assert.Contains(t, attrs, "blockedJobRunCount")
	assert.Equal(t, 3, attrs["blockedJobRunCount"])
	assert.Contains(t, attrs, "waitingJobRunCount")
	assert.Equal(t, 1, attrs["waitingJobRunCount"])
	assert.Contains(t, attrs, "pendingJobRunCount")
	assert.Equal(t, 2, attrs["pendingJobRunCount"])
	assert.Contains(t, attrs, "queuedJobRunCount")
	assert.Equal(t, 1, attrs["queuedJobRunCount"])
	assert.Contains(t, attrs, "runningJobRunCount")
	assert.Equal(t, 2, attrs["runningJobRunCount"])
	assert.Contains(t, attrs, "terminatingJobRunCount")
	assert.Equal(t, 1, attrs["terminatingJobRunCount"])
	assert.Contains(t, attrs, "blockedTaskRunCount")
	assert.Equal(t, 2, attrs["blockedTaskRunCount"])
	assert.Contains(t, attrs, "waitingTaskRunCount")
	assert.Equal(t, 1, attrs["waitingTaskRunCount"])
	assert.Contains(t, attrs, "pendingTaskRunCount")
	assert.Equal(t, 4, attrs["pendingTaskRunCount"])
	assert.Contains(t, attrs, "queuedTaskRunCount")
	assert.Equal(t, 2, attrs["queuedTaskRunCount"])
	assert.Contains(t, attrs, "runningTaskRunCount")
	assert.Equal(t, 3, attrs["runningTaskRunCount"])
	assert.Contains(t, attrs, "terminatingTaskRunCount")
	assert.Equal(t, 1, attrs["terminatingTaskRunCount"])
}

func TestPollEvents_Combined(t *testing.T) {
	// Setup mock workspace and workspace info
	mockWorkspace, teardown := setupMockWorkspaceAndInfo()
	defer teardown()

	// Setup mock cluster info
	teardown2 := setupMockClusterInfo()
	defer teardown2()

	// Setup mock integration
	mockIntegration := &integration.LabsIntegration{
		Interval: 60,
	}

	// Setup mock tag map
	tags := map[string]string{
		"env": "production",
		"team": "data-engineering",
	}

	// Setup mock startOffset
	startOffset := time.Duration(60) * time.Second

	// Mock Now
	now := time.Now()
	Now = func () time.Time { return now }

	// Setup times to be used to test behavior of event production based on
	// lastRun time.
	lastRun := now.UTC()
	pastStartTime := lastRun.Add(-10 * time.Second)
	pastEndTime := lastRun.Add(-5 * time.Second)
	futureStartTime := lastRun.Add(5 * time.Second)
	futureEndTime := lastRun.Add(10 * time.Second)

	// Setup a mock iterator
	mockIterator := &MockIterator[databricksSdkJobs.BaseRun]{}

	// Setup hasNext controls
	index := 0

	// Setup a mock run object
	mockRuns := []databricksSdkJobs.BaseRun{
		// This job should produce no events because the job run and the task
		// runs all started and ended before the lastRun time.
		{
			JobId:                  0,
			RunId:                  123,
			OriginalAttemptRunId:   123,
			RunType:                databricksSdkJobs.RunTypeJobRun,
			RunName:                "jobRun1",
			Description:            "This is run 1 description",
			AttemptNumber:          0,
			// started before
			StartTime:				pastStartTime.UnixMilli(),
			// ended before
			EndTime:                pastEndTime.UnixMilli(),
			Trigger:			   	databricksSdkJobs.TriggerTypeOneTime,
			QueueDuration:          1000,
			SetupDuration:          500,
			ExecutionDuration:      1000,
			CleanupDuration:        300,
			Status: &databricksSdkJobs.RunStatus{
				State:        databricksSdkJobs.RunLifecycleStateV2StateTerminated,
				TerminationDetails: &databricksSdkJobs.TerminationDetails{
					Code: databricksSdkJobs.TerminationCodeCodeSuccess,
					Type: databricksSdkJobs.TerminationTypeTypeSuccess,
				},
			},
			Tasks: []databricksSdkJobs.RunTask{
				{
					TaskKey:             "task1",
					RunId:               456,
					AttemptNumber:       0,
					Description:         "This is task run 1 description",
					// started before
					StartTime:           pastStartTime.UnixMilli(),
					// ended before
					EndTime:             pastEndTime.UnixMilli(),
					QueueDuration:       2000,
					SetupDuration:       300,
					ExecutionDuration:   3000,
					CleanupDuration:     100,
					ClusterInstance: &databricksSdkJobs.ClusterInstance{
						ClusterId:      "fake-cluster-id",
						SparkContextId: "fake-cluster-spark-context-id",
					},
					Status: &databricksSdkJobs.RunStatus{
						State: databricksSdkJobs.RunLifecycleStateV2StateTerminated,
						TerminationDetails: &databricksSdkJobs.TerminationDetails{
							Code: databricksSdkJobs.TerminationCodeCodeSuccess,
							Type: databricksSdkJobs.TerminationTypeTypeSuccess,
						},
					},
				},
			},
		},
		// This job should produce no job run events because it started before
		// the lastRun time and it is still running (no end time). It should
		// produce no task run start event for task2 because it started before
		// the lastRun time and should produce one task run complete event for
		// task2 because it ended after the lastRun time. It should produce one
		// task run start event for task3 because it started after the lastRun
		// time and should produce no task run complete event for task3 because
		// it is still running (no end time).
		{
			JobId:                  1,
			RunId:                  124,
			OriginalAttemptRunId:   124,
			RunType:                databricksSdkJobs.RunTypeJobRun,
			RunName:                "jobRun2",
			Description:            "This is run 2 description",
			AttemptNumber:          0,
			// started before
			StartTime:				pastStartTime.UnixMilli(),
			Trigger:			   	databricksSdkJobs.TriggerTypeOneTime,
			Status: &databricksSdkJobs.RunStatus{
				State:        databricksSdkJobs.RunLifecycleStateV2StateRunning,
			},
			Tasks: []databricksSdkJobs.RunTask{
				{
					TaskKey:             "task2",
					RunId:               457,
					AttemptNumber:       0,
					Description:         "This is task run 2 description",
					// started before
					StartTime:           pastStartTime.UnixMilli(),
					// ended after
					EndTime:             futureEndTime.UnixMilli(),
					QueueDuration:       2000,
					SetupDuration:       300,
					ExecutionDuration:   3000,
					CleanupDuration:     100,
					ClusterInstance: &databricksSdkJobs.ClusterInstance{
						ClusterId:      "fake-cluster-id",
						SparkContextId: "fake-cluster-spark-context-id",
					},
					Status: &databricksSdkJobs.RunStatus{
						State:        databricksSdkJobs.RunLifecycleStateV2StateTerminated,
						TerminationDetails: &databricksSdkJobs.TerminationDetails{
							Code: databricksSdkJobs.TerminationCodeCodeSuccess,
							Type: databricksSdkJobs.TerminationTypeTypeSuccess,
						},
					},
				},
				{
					TaskKey:             "task3",
					RunId:               458,
					AttemptNumber:       0,
					Description:         "This is task run 3 description",
					// started after
					StartTime:           futureStartTime.UnixMilli(),
					ClusterInstance: &databricksSdkJobs.ClusterInstance{
						ClusterId:      "fake-cluster-id",
						SparkContextId: "fake-cluster-spark-context-id",
					},
					Status: &databricksSdkJobs.RunStatus{
						State: databricksSdkJobs.RunLifecycleStateV2StateRunning,
					},
				},
			},
		},
		// This job should produce one job run start event because it started
		// after the lastRun time and should produce no job run complete event
		// because it is still running (no end time). It should produce one task
		// run start event for task4 because it started after the lastRun time
		// and should produce one task run complete event for task4 because it
		// ended after the lastRun time. It should produce one task run start
		// event for task5 because it started after the lastRun time and should
		// produce no task run complete event for task5 because it is still
		// running (no end time).
		{
			JobId:                  2,
			RunId:                  125,
			OriginalAttemptRunId:   125,
			RunType:                databricksSdkJobs.RunTypeJobRun,
			RunName:                "jobRun3",
			Description:            "This is run 3 description",
			AttemptNumber:          0,
			// started after
			StartTime:				futureStartTime.UnixMilli(),
			Trigger:			   	databricksSdkJobs.TriggerTypeOneTime,
			Status: &databricksSdkJobs.RunStatus{
				State:        databricksSdkJobs.RunLifecycleStateV2StateRunning,
			},
			Tasks: []databricksSdkJobs.RunTask{
				{
					TaskKey:             "task4",
					RunId:               459,
					AttemptNumber:       0,
					Description:         "This is task run 4 description",
					// started after
					StartTime:           futureStartTime.UnixMilli(),
					// ended after
					EndTime:			 futureEndTime.UnixMilli(),
					QueueDuration:       40,
					SetupDuration:       500,
					ExecutionDuration:   10000,
					CleanupDuration:     300,
					ClusterInstance: &databricksSdkJobs.ClusterInstance{
						ClusterId:      "fake-cluster-id",
						SparkContextId: "fake-cluster-spark-context-id",
					},
					Status: &databricksSdkJobs.RunStatus{
						State:        databricksSdkJobs.RunLifecycleStateV2StateTerminated,
						TerminationDetails: &databricksSdkJobs.TerminationDetails{
							Code: databricksSdkJobs.TerminationCodeCodeClusterError,
							Type: databricksSdkJobs.TerminationTypeTypeInternalError,
						},
					},
				},
				{
					TaskKey:             "task5",
					RunId:               460,
					AttemptNumber:       0,
					Description:         "This is task run 5 description",
					// started after
					StartTime:           futureStartTime.UnixMilli(),
					ClusterInstance: &databricksSdkJobs.ClusterInstance{
						ClusterId:      "fake-cluster-id",
						SparkContextId: "fake-cluster-spark-context-id",
					},
					Status: &databricksSdkJobs.RunStatus{
						State:        databricksSdkJobs.RunLifecycleStateV2StateRunning,
					},
				},
			},
		},
		// This job should produce no job run start event because it started
		// before the lastRun time and should produce one job run complete event
		// because it ended after the lastRun time. It should produce no task
		// run events for task6 because it started and ended before the lastRun
		// time. It should produce no task run start event for task7 because it
		// started before the lastRun time and should produce one task run
		// complete event for task7 because it ended after the lastRun time. It
		// should produce one task run start event for task8 because it started
		// after the lastRun time and should produce one task run complete event
		// for task8 because it ended after the lastRun time.
		{
			JobId:                  3,
			RunId:                  126,
			OriginalAttemptRunId:   126,
			RunType:                databricksSdkJobs.RunTypeJobRun,
			RunName:                "jobRun4",
			Description:            "This is run 4 description",
			AttemptNumber:          0,
			// started before
			StartTime:				pastStartTime.UnixMilli(),
			// ended after
			EndTime:                futureEndTime.UnixMilli(),
			Trigger:			   	databricksSdkJobs.TriggerTypeOneTime,
			QueueDuration:          100,
			RunDuration:		    123456,
			Status: &databricksSdkJobs.RunStatus{
				State:        databricksSdkJobs.RunLifecycleStateV2StateTerminated,
				TerminationDetails: &databricksSdkJobs.TerminationDetails{
					Code: databricksSdkJobs.TerminationCodeCodeSuccess,
					Type: databricksSdkJobs.TerminationTypeTypeSuccess,
				},
			},
			Tasks: []databricksSdkJobs.RunTask{
				{
					TaskKey:             "task6",
					RunId:               461,
					AttemptNumber:       0,
					Description:         "This is task run 6 description",
					// started before
					StartTime:           pastStartTime.UnixMilli(),
					// ended before
					EndTime:             pastEndTime.UnixMilli(),
					QueueDuration:       2000,
					SetupDuration:       300,
					ExecutionDuration:   3000,
					CleanupDuration:     100,
					ClusterInstance: &databricksSdkJobs.ClusterInstance{
						ClusterId:      "fake-cluster-id",
						SparkContextId: "fake-cluster-spark-context-id",
					},
					Status: &databricksSdkJobs.RunStatus{
						State: databricksSdkJobs.RunLifecycleStateV2StateTerminated,
						TerminationDetails: &databricksSdkJobs.TerminationDetails{
							Code: databricksSdkJobs.TerminationCodeCodeSuccess,
							Type: databricksSdkJobs.TerminationTypeTypeSuccess,
						},
					},
				},
				{
					TaskKey:             "task7",
					RunId:               462,
					AttemptNumber:       0,
					Description:         "This is task run 7 description",
					// started before
					StartTime:           pastStartTime.UnixMilli(),
					// ended after
					EndTime:             futureEndTime.UnixMilli(),
					QueueDuration:       0,
					SetupDuration:       0,
					ExecutionDuration:   4500,
					CleanupDuration:     100,
					ClusterInstance: &databricksSdkJobs.ClusterInstance{
						ClusterId:      "fake-cluster-id",
						SparkContextId: "fake-cluster-spark-context-id",
					},
					Status: &databricksSdkJobs.RunStatus{
						State: databricksSdkJobs.RunLifecycleStateV2StateTerminated,
						TerminationDetails: &databricksSdkJobs.TerminationDetails{
							Code: databricksSdkJobs.TerminationCodeCodeDriverError,
							Type: databricksSdkJobs.TerminationTypeTypeCloudFailure,
						},
					},
				},
				{
					TaskKey:             "task8",
					RunId:               463,
					AttemptNumber:       0,
					Description:         "This is task run 8 description",
					// started after
					StartTime:           futureStartTime.UnixMilli(),
					// ended after
					EndTime:             futureEndTime.UnixMilli(),
					QueueDuration:       0,
					SetupDuration:       100,
					ExecutionDuration:   9500,
					CleanupDuration:     0,
					ClusterInstance: &databricksSdkJobs.ClusterInstance{
						ClusterId:      "fake-cluster-id",
						SparkContextId: "fake-cluster-spark-context-id",
					},
					Status: &databricksSdkJobs.RunStatus{
						State: databricksSdkJobs.RunLifecycleStateV2StateTerminated,
						TerminationDetails: &databricksSdkJobs.TerminationDetails{
							Code: databricksSdkJobs.TerminationCodeCodeSuccess,
							Type: databricksSdkJobs.TerminationTypeTypeSuccess,
						},
					},
				},
			},
		},
		// This job should produce one job run start event because it started
		// after the lastRun time and should produce one job run complete event
		// because it ended after the lastRun time. It should produce one task
		// run start event for task9 because it started after the lastRun time
		// and should produce one task run complete event for task9 because it
		// ended after the lastRun time.
		{
			JobId:                  4,
			RunId:                  127,
			OriginalAttemptRunId:   127,
			RunType:                databricksSdkJobs.RunTypeJobRun,
			RunName:                "jobRun5",
			Description:            "This is run 5 description",
			AttemptNumber:          0,
			// started after
			StartTime:				futureStartTime.UnixMilli(),
			// ended after
			EndTime:                futureEndTime.UnixMilli(),
			Trigger:			   	databricksSdkJobs.TriggerTypeOneTime,
			QueueDuration:          400,
			SetupDuration:		    100,
			ExecutionDuration: 	    7890,
			CleanupDuration:        200,
			Status: &databricksSdkJobs.RunStatus{
				State:        databricksSdkJobs.RunLifecycleStateV2StateTerminated,
				TerminationDetails: &databricksSdkJobs.TerminationDetails{
					Code: databricksSdkJobs.TerminationCodeCodeCanceled,
					Type: databricksSdkJobs.TerminationTypeTypeClientError,
				},
			},
			Tasks: []databricksSdkJobs.RunTask{
				{
					TaskKey:             "task9",
					RunId:               464,
					AttemptNumber:       0,
					Description:         "This is task run 9 description",
					// started after
					StartTime:           futureStartTime.UnixMilli(),
					// ended after
					EndTime:             futureEndTime.UnixMilli(),
					QueueDuration:       250,
					SetupDuration:       50,
					ExecutionDuration:   1234,
					CleanupDuration:     50,
					ClusterInstance: &databricksSdkJobs.ClusterInstance{
						ClusterId:      "fake-cluster-id",
						SparkContextId: "fake-cluster-spark-context-id",
					},
					Status: &databricksSdkJobs.RunStatus{
						State: databricksSdkJobs.RunLifecycleStateV2StateTerminated,
						TerminationDetails: &databricksSdkJobs.TerminationDetails{
							Code: databricksSdkJobs.TerminationCodeCodeCanceled,
							Type: databricksSdkJobs.TerminationTypeTypeClientError,
						},
					},
				},
			},
		},
	}

	// Setup the mock HasNext to return true when there are more runs to iterate
	// over.
	mockIterator.HasNextFunc = func(
		ctx context.Context,
	) bool {
		index += 1
		return index <= len(mockRuns)
	}

	// Setup the mock Next to return the next run in the list
	mockIterator.NextFunc = func(
		ctx context.Context,
	) (databricksSdkJobs.BaseRun, error) {
		return mockRuns[index - 1], nil
	}

	// Mock the ListJobRuns method to return the iterator
	mockWorkspace.ListJobRunsFunc = func(
		ctx context.Context,
		startOffset time.Duration,
	) databricksSdkListing.Iterator[databricksSdkJobs.BaseRun] {
		return mockIterator
	}

	// Setup an events channel to receive events
	eventsChan := make(chan model.Event, 500) // Buffer to prevent blocking

	// Create job run receiver instance
	receiver := NewDatabricksJobRunReceiver(
		mockIntegration,
		mockWorkspace,
		startOffset,
		tags,
	)

	// Execute the function under test
	err := receiver.PollEvents(context.Background(), eventsChan)

	// Close the channel to iterate through it
	close(eventsChan)

	// Verify result
	assert.NoError(t, err)

	// Collect all events for verification
	events := make([]model.Event, 0)
	for event := range eventsChan {
		events = append(events, event)
	}

	// Verify the number of events produced
	// We expect:
	// - No events for jobRun1 and task1
	// - No job run events for jobRun2
	// - One task run complete event for task2
	// - One task run start event for task3
	// - One job run start event for jobRun3
	// - One task run start event and one task run complete event for task4
	// - One task run start event for task5
	// - One job run complete event for jobRun4
	// - No task run events for task6
	// - One task run complete event for task7
	// - One task run start event and one task run complete event for task8
	// - One job run start event and one job run complete event for jobRun5
	// - One task run start event and one task run complete event for task9
	// - One job run summary event
	// This gives us a total of 15 events
	assert.Equal(
		t,
		15,
		len(events),
		"Expected exactly fifteen events to be produced",
	)

	validEventTypes := []string{
		"DatabricksJobRun",
		"DatabricksTaskRun",
		"DatabricksJobRunSummary",
	}
	validEvents := []string{"start", "complete"}

	jobIds := []int64{2, 3, 4}

	jobRunIds := map[int64]int64{
		2: 125,
		3: 126,
		4: 127,
	}

	jobRunOriginalAttemptRunIds := map[int64]int64{
		2: 125,
		3: 126,
		4: 127,
	}

	jobRunNames := map[int64]string{
		2: "jobRun3",
		3: "jobRun4",
		4: "jobRun5",
	}

	jobRunDescriptions := map[int64]string{
		2: "This is run 3 description",
		3: "This is run 4 description",
		4: "This is run 5 description",
	}

	jobRunStartTimes := map[int64]int64{
		2: futureStartTime.UnixMilli(),
		3: pastStartTime.UnixMilli(),
		4: futureStartTime.UnixMilli(),
	}

	jobTerminationCodes := map[int64]string{
		3: string(databricksSdkJobs.TerminationCodeCodeSuccess),
		4: string(databricksSdkJobs.TerminationCodeCodeCanceled),
	}

	jobTerminationTypes := map[int64]string{
		3: string(databricksSdkJobs.TerminationTypeTypeSuccess),
		4: string(databricksSdkJobs.TerminationTypeTypeClientError),
	}

	jobRunEndTimes := map[int64]int64{
		3: futureEndTime.UnixMilli(),
		4: futureEndTime.UnixMilli(),
	}

	multiTaskJobs := map[int64]bool{
		3: true,
		4: false,
	}

	jobDurations := map[int64]int64{
		3: 123456,
		4: 8190,
	}

	jobQueueDurations := map[int64]int64{
		3: 100,
		4: 400,
	}

	jobSetupDurations := map[int64]int64{
		4: 100,
	}

	jobExecutionDurations := map[int64]int64{
		4: 7890,
	}

	jobCleanupDurations := map[int64]int64{
		4: 200,
	}

	taskRunIds := []int64{457, 458, 459, 460, 462, 463, 464}

	taskJobIds := map[int64]int64{
		457: 1,
		458: 1,
		459: 2,
		460: 2,
		462: 3,
		463: 3,
		464: 4,
	}

	taskJobRunIds := map[int64]int64{
		457: 124,
		458: 124,
		459: 125,
		460: 125,
		462: 126,
		463: 126,
		464: 127,
	}

	taskJobRunNames := map[int64]string{
		457: "jobRun2",
		458: "jobRun2",
		459: "jobRun3",
		460: "jobRun3",
		462: "jobRun4",
		463: "jobRun4",
		464: "jobRun5",
	}

	taskJobRunStartTimes := map[int64]int64{
		457: pastStartTime.UnixMilli(),
		458: pastStartTime.UnixMilli(),
		459: futureStartTime.UnixMilli(),
		460: futureStartTime.UnixMilli(),
		462: pastStartTime.UnixMilli(),
		463: pastStartTime.UnixMilli(),
		464: futureStartTime.UnixMilli(),
	}

	taskRunStartTimes := map[int64]int64{
		457: pastStartTime.UnixMilli(),
		458: futureStartTime.UnixMilli(),
		459: futureStartTime.UnixMilli(),
		460: futureStartTime.UnixMilli(),
		462: pastStartTime.UnixMilli(),
		463: futureStartTime.UnixMilli(),
		464: futureStartTime.UnixMilli(),
	}

	taskNames := map[int64]string{
		457: "task2",
		458: "task3",
		459: "task4",
		460: "task5",
		462: "task7",
		463: "task8",
		464: "task9",
	}

	taskDescriptions := map[int64]string{
		457: "This is task run 2 description",
		458: "This is task run 3 description",
		459: "This is task run 4 description",
		460: "This is task run 5 description",
		462: "This is task run 7 description",
		463: "This is task run 8 description",
		464: "This is task run 9 description",
	}

	taskTerminationCodes := map[int64]string{
		457: string(databricksSdkJobs.TerminationCodeCodeSuccess),
		459: string(databricksSdkJobs.TerminationCodeCodeClusterError),
		462: string(databricksSdkJobs.TerminationCodeCodeDriverError),
		463: string(databricksSdkJobs.TerminationCodeCodeSuccess),
		464: string(databricksSdkJobs.TerminationCodeCodeCanceled),
	}

	taskTerminationTypes := map[int64]string{
		457: string(databricksSdkJobs.TerminationTypeTypeSuccess),
		459: string(databricksSdkJobs.TerminationTypeTypeInternalError),
		462: string(databricksSdkJobs.TerminationTypeTypeCloudFailure),
		463: string(databricksSdkJobs.TerminationTypeTypeSuccess),
		464: string(databricksSdkJobs.TerminationTypeTypeClientError),
	}

	taskRunEndTimes := map[int64]int64{
		457: futureEndTime.UnixMilli(),
		459: futureEndTime.UnixMilli(),
		462: futureEndTime.UnixMilli(),
		463: futureEndTime.UnixMilli(),
		464: futureEndTime.UnixMilli(),
	}

	taskDurations := map[int64]int64{
		457: 3400, // 300 + 3000 + 100
		459: 10800, // 500 + 10000 + 300
		462: 4600,  // 0 + 4500 + 100
		463: 9600,  // 100 + 9500 + 0
		464: 1334, // 50 + 1234 + 50
	}

	taskQueueDurations := map[int64]int64{
		457: 2000,
		459: 40,
		462: 0,
		463: 0,
		464: 250,
	}

	taskSetupDurations := map[int64]int64{
		457: 300,
		459: 500,
		462: 0,
		463: 100,
		464: 50,
	}

	taskExecutionDurations := map[int64]int64{
		457: 3000,
		459: 10000,
		462: 4500,
		463: 9500,
		464: 1234,
	}

	taskCleanupDurations := map[int64]int64{
		457: 100,
		459: 300,
		462: 100,
		463: 0,
		464: 50,
	}

	// We expect 4 job run events
	jobRunEvents := 0
	found2Start := false
	found3Complete := false
	found4Start := false
	found4Complete := false
	// We expect 10 task run events
	taskRunEvents := 0
	found457Complete := false
	found458Start := false
	found459Start := false
	found459Complete := false
	found460Start := false
	found462Complete := false
	found463Start := false
	found463Complete := false
	found464Start := false
	found464Complete := false
	// We should find no other job run or task run events
	foundJobRunOther := false
	foundTaskRunOther := false
	// We expect one summary event
	foundSummaryEvent := false
	// We should find no other events
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
		assert.Contains(
			t,
			validEventTypes,
			event.Type,
		)

		// Verify attributes
		assert.NotNil(t, event.Attributes)

		// Verify common attributes
		attrs := event.Attributes
		assert.NotEmpty(t, attrs)

		assert.Contains(t, attrs, "env")
		assert.Equal(t, "production", attrs["env"])
		assert.Contains(t, attrs, "team")
		assert.Equal(t, "data-engineering", attrs["team"])

		// Verify workspace attributes
		assert.Contains(t, attrs, "databricksWorkspaceId")
		assert.Equal(t, mockWorkspaceId, attrs["databricksWorkspaceId"])
		assert.Contains(t, attrs, "databricksWorkspaceName")
		assert.Equal(
			t,
			mockWorkspaceHost,
			attrs["databricksWorkspaceName"],
		)
		assert.Contains(t, attrs, "databricksWorkspaceUrl")
		assert.Equal(
			t,
			"https://" + mockWorkspaceHost,
			attrs["databricksWorkspaceUrl"],
		)

		if event.Type == "DatabricksJobRun" {
			jobRunEvents += 1

			// Verify event
			assert.Contains(t, attrs, "event")
			assert.Contains(
				t,
				validEvents,
				attrs["event"],
			)

			// Verify no cluster attributes
			assert.NotContains(t, attrs, "databricksClusterId")
			assert.NotContains(t, attrs, "databricksClusterName")
			assert.NotContains(t, attrs, "databricksClusterSource")
			assert.NotContains(t, attrs, "databricksClusterInstancePoolId")
			assert.NotContains(t, attrs, "databricksClusterSparkContextId")

			assert.Contains(t, attrs, "jobId")
			assert.Contains(t, jobIds, attrs["jobId"])

			jobId := attrs["jobId"].(int64)

			assert.Contains(t, attrs, "jobRunId")
			assert.Equal(t, jobRunIds[jobId], attrs["jobRunId"])
			assert.Contains(t, attrs, "originalAttemptRunId")
			assert.Equal(
				t,
				jobRunOriginalAttemptRunIds[jobId],
				attrs["originalAttemptRunId"],
			)
			assert.Contains(t, attrs, "jobRunType")
			assert.Equal(
				t,
				string(databricksSdkJobs.RunTypeJobRun),
				attrs["jobRunType"],
			)
			assert.Contains(t, attrs, "jobRunName")
			assert.Equal(t, jobRunNames[jobId], attrs["jobRunName"])
			assert.Contains(t, attrs, "jobRunStartTime")
			assert.Equal(
				t,
				jobRunStartTimes[jobId],
				attrs["jobRunStartTime"],
			)
			assert.Contains(t, attrs, "description")
			assert.Equal(
				t,
				jobRunDescriptions[jobId],
				attrs["description"],
			)
			assert.Contains(t, attrs, "jobRunTrigger")
			assert.Equal(
				t,
				string(databricksSdkJobs.TriggerTypeOneTime),
				attrs["jobRunTrigger"],
			)

			evt := attrs["event"].(string)

			if evt == "start" {
				if jobId == 2 {
					found2Start = true
				} else if jobId == 4 {
					found4Start = true
				} else {
					foundJobRunOther = true
					continue
				}
			} else if evt == "complete" {
				if jobId == 3 {
					found3Complete = true
				} else if jobId == 4 {
					found4Complete = true
				} else {
					foundJobRunOther = true
					continue
				}

				assert.Contains(t, attrs, "state")
				assert.Equal(
					t,
					string(databricksSdkJobs.RunLifecycleStateV2StateTerminated),
					attrs["state"],
				)
				assert.Contains(t, attrs, "terminationCode")
				assert.Equal(
					t,
					jobTerminationCodes[jobId],
					attrs["terminationCode"],
				)
				assert.Contains(t, attrs, "terminationType")
				assert.Equal(
					t,
					jobTerminationTypes[jobId],
					attrs["terminationType"],
				)
				assert.Contains(t, attrs, "jobRunEndTime")
				assert.Equal(t, jobRunEndTimes[jobId], attrs["jobRunEndTime"])
				assert.Contains(t, attrs, "duration")
				assert.Equal(
					t,
					jobDurations[jobId],
					attrs["duration"],
				)
				assert.Contains(t, attrs, "queueDuration")
				assert.Equal(
					t,
					jobQueueDurations[jobId],
					attrs["queueDuration"],
				)

				if multiTaskJobs[jobId] {
					assert.NotContains(t, attrs, "setupDuration")
					assert.NotContains(t, attrs, "executionDuration")
					assert.NotContains(t, attrs, "cleanupDuration")
					continue
				}

				assert.Contains(t, attrs, "setupDuration")
				assert.Equal(
					t,
					jobSetupDurations[jobId],
					attrs["setupDuration"],
				)
				assert.Contains(t, attrs, "executionDuration")
				assert.Equal(
					t,
					jobExecutionDurations[jobId],
					attrs["executionDuration"],
				)
				assert.Contains(t, attrs, "cleanupDuration")
				assert.Equal(
					t,
					jobCleanupDurations[jobId],
					attrs["cleanupDuration"],
				)
			}
		} else if event.Type == "DatabricksTaskRun" {
			taskRunEvents += 1

			// Verify event
			assert.Contains(t, attrs, "event")
			assert.Contains(
				t,
				validEvents,
				attrs["event"],
			)

			// Verify cluster attributes
			assert.Contains(t, attrs, "databricksClusterId")
			assert.Equal(t, "fake-cluster-id", attrs["databricksClusterId"])
			assert.Contains(t, attrs, "databricksClusterName")
			assert.Equal(
				t,
				"fake-cluster-name",
				attrs["databricksClusterName"],
			)
			assert.Contains(t, attrs, "databricksClusterSource")
			assert.Equal(
				t,
				"fake-cluster-source",
				attrs["databricksClusterSource"],
			)
			assert.Contains(t, attrs, "databricksClusterInstancePoolId")
			assert.Equal(
				t,
				"fake-cluster-instance-pool-id",
				attrs["databricksClusterInstancePoolId"],
			)
			assert.Contains(t, attrs, "databricksClusterSparkContextId")
			assert.Equal(
				t,
				"fake-cluster-spark-context-id",
				attrs["databricksClusterSparkContextId"],
			)

			assert.Contains(t, attrs, "taskRunId")
			assert.Contains(t, taskRunIds, attrs["taskRunId"])

			taskRunId := attrs["taskRunId"].(int64)

			assert.Contains(t, attrs, "jobId")
			assert.Equal(t, taskJobIds[taskRunId], attrs["jobId"])
			assert.Contains(t, attrs, "jobRunId")
			assert.Equal(t, taskJobRunIds[taskRunId], attrs["jobRunId"])
			assert.Contains(t, attrs, "jobRunType")
			assert.Equal(
				t,
				string(databricksSdkJobs.RunTypeJobRun),
				attrs["jobRunType"],
			)
			assert.Contains(t, attrs, "jobRunName")
			assert.Equal(t, taskJobRunNames[taskRunId], attrs["jobRunName"])
			assert.Contains(t, attrs, "jobRunStartTime")
			assert.Equal(
				t,
				taskJobRunStartTimes[taskRunId],
				attrs["jobRunStartTime"],
			)
			assert.Contains(t, attrs, "jobRunTrigger")
			assert.Equal(
				t,
				string(databricksSdkJobs.TriggerTypeOneTime),
				attrs["jobRunTrigger"],
			)
			assert.Contains(t, attrs, "taskName")
			assert.Equal(t, taskNames[taskRunId], attrs["taskName"])
			assert.Contains(t, attrs, "taskRunStartTime")
			assert.Equal(
				t,
				taskRunStartTimes[taskRunId],
				attrs["taskRunStartTime"],
			)
			assert.Contains(t, attrs, "description")
			assert.Equal(t, taskDescriptions[taskRunId], attrs["description"])
			assert.Contains(t, attrs, "attempt")
			assert.Equal(t, 0, attrs["attempt"])
			assert.Contains(t, attrs, "isRetry")
			assert.Equal(t, false, attrs["isRetry"])

			evt := attrs["event"].(string)

			if evt == "start" {
				if taskRunId == 458 {
					found458Start = true
				} else if taskRunId == 459 {
					found459Start = true
				} else if taskRunId == 460 {
					found460Start = true
				} else if taskRunId == 463 {
					found463Start = true
				} else if taskRunId == 464 {
					found464Start = true
				} else {
					foundTaskRunOther = true
					continue
				}
			} else if evt == "complete" {
				if taskRunId == 457 {
					found457Complete = true
				} else if taskRunId == 459 {
					found459Complete = true
				} else if taskRunId == 462 {
					found462Complete = true
				} else if taskRunId == 463 {
					found463Complete = true
				} else if taskRunId == 464 {
					found464Complete = true
				} else {
					foundTaskRunOther = true
					continue
				}

				assert.Contains(t, attrs, "state")
				assert.Equal(
					t,
					string(databricksSdkJobs.RunLifecycleStateV2StateTerminated),
					attrs["state"],
				)
				assert.Contains(t, attrs, "terminationCode")
				assert.Equal(
					t,
					taskTerminationCodes[taskRunId],
					attrs["terminationCode"],
				)
				assert.Contains(t, attrs, "terminationType")
				assert.Equal(
					t,
					taskTerminationTypes[taskRunId],
					attrs["terminationType"],
				)
				assert.Contains(t, attrs, "taskRunEndTime")
				assert.Equal(t, taskRunEndTimes[taskRunId], attrs["taskRunEndTime"])

				assert.Contains(t, attrs, "duration")
				assert.Equal(
					t,
					taskDurations[taskRunId],
					attrs["duration"],
				)
				assert.Contains(t, attrs, "queueDuration")
				assert.Equal(
					t,
					taskQueueDurations[taskRunId],
					attrs["queueDuration"],
				)
				assert.Contains(t, attrs, "setupDuration")
				assert.Equal(
					t,
					taskSetupDurations[taskRunId],
					attrs["setupDuration"],
				)
				assert.Contains(t, attrs, "executionDuration")
				assert.Equal(
					t,
					taskExecutionDurations[taskRunId],
					attrs["executionDuration"],
				)
				assert.Contains(t, attrs, "cleanupDuration")
				assert.Equal(
					t,
					taskCleanupDurations[taskRunId],
					attrs["cleanupDuration"],
				)
			} else {
				foundTaskRunOther = true
				continue
			}
		} else if event.Type == "DatabricksJobRunSummary" {
			foundSummaryEvent = true

			// Verify summary counts
			assert.Contains(t, attrs, "blockedJobRunCount")
			assert.Equal(t, 0, attrs["blockedJobRunCount"])
			assert.Contains(t, attrs, "waitingJobRunCount")
			assert.Equal(t, 0, attrs["waitingJobRunCount"])
			assert.Contains(t, attrs, "pendingJobRunCount")
			assert.Equal(t, 0, attrs["pendingJobRunCount"])
			assert.Contains(t, attrs, "queuedJobRunCount")
			assert.Equal(t, 0, attrs["queuedJobRunCount"])
			assert.Contains(t, attrs, "runningJobRunCount")
			assert.Equal(t, 2, attrs["runningJobRunCount"])
			assert.Contains(t, attrs, "terminatingJobRunCount")
			assert.Equal(t, 0, attrs["terminatingJobRunCount"])
			assert.Contains(t, attrs, "blockedTaskRunCount")
			assert.Equal(t, 0, attrs["blockedTaskRunCount"])
			assert.Contains(t, attrs, "waitingTaskRunCount")
			assert.Equal(t, 0, attrs["waitingTaskRunCount"])
			assert.Contains(t, attrs, "pendingTaskRunCount")
			assert.Equal(t, 0, attrs["pendingTaskRunCount"])
			assert.Contains(t, attrs, "queuedTaskRunCount")
			assert.Equal(t, 0, attrs["queuedTaskRunCount"])
			assert.Contains(t, attrs, "runningTaskRunCount")
			assert.Equal(t, 2, attrs["runningTaskRunCount"])
			assert.Contains(t, attrs, "terminatingTaskRunCount")
			assert.Equal(t, 0, attrs["terminatingTaskRunCount"])
		} else {
			foundOther = true
		}
	}

	// Verify we saw exactly the events we expected
	assert.Equal(
		t,
		4,
		jobRunEvents,
		"Expected exactly four job run events to be produced",
	)
	assert.Equal(
		t,
		10,
		taskRunEvents,
		"Expected exactly ten task run events to be produced",
	)
	assert.True(
		t,
		found2Start,
		"Expected to find job run 2 start event",
	)
	assert.True(
		t,
		found3Complete,
		"Expected to find job run 3 complete event",
	)
	assert.True(
		t,
		found4Start,
		"Expected to find job run 4 start event",
	)
	assert.True(
		t,
		found4Complete,
		"Expected to find job run 4 complete event",
	)
	assert.True(
		t,
		found457Complete,
		"Expected to find task run 457 complete event",
	)
	assert.True(
		t,
		found458Start,
		"Expected to find task run 458 start event",
	)
	assert.True(
		t,
		found459Start,
		"Expected to find task run 459 start event",
	)
	assert.True(
		t,
		found459Complete,
		"Expected to find task run 459 complete event",
	)
	assert.True(
		t,
		found460Start,
		"Expected to find task run 460 start event",
	)
	assert.True(
		t,
		found462Complete,
		"Expected to find task run 462 complete event",
	)
	assert.True(
		t,
		found463Start,
		"Expected to find task run 463 start event",
	)
	assert.True(
		t,
		found463Complete,
		"Expected to find task run 463 complete event",
	)
	assert.True(
		t,
		found464Start,
		"Expected to find task run 464 start event",
	)
	assert.True(
		t,
		found464Complete,
		"Expected to find task run 464 complete event",
	)
	assert.False(
		t,
		foundJobRunOther,
		"Unexpected job run event found",
	)
	assert.False(
		t,
		foundTaskRunOther,
		"Unexpected task run event found",
	)
	assert.True(t, foundSummaryEvent, "Expected to find job run summary event")
	assert.False(
		t,
		foundOther,
		"Unexpected event found",
	)
}

func TestMakeJobRunSummaryAttributes(t *testing.T) {
	// Setup mock workspace and workspace info
	mockWorkspace, teardown := setupMockWorkspaceAndInfo()
	defer teardown()

	// Setup mock tag map
	tags := map[string]string{
		"env": "production",
		"team": "data-engineering",
	}

	// Setup mock job counters
	jobCounters := &counters{
		blocked:     2,
		waiting:     2,
		pending:     0,
		queued:      1,
		running:     3,
		terminating: 2,
	}

	// Setup mock task counters
	taskCounters := &counters{
		blocked:     3,
		waiting:     1,
		pending:     1,
		queued:      0,
		running:     2,
		terminating: 1,
	}

	// Execute the function under test
	jobRunSummaryAttributes, err := makeJobRunSummaryAttributes(
		context.Background(),
		mockWorkspace,
		jobCounters,
		taskCounters,
		tags,
	)

	// Verify result
	assert.NoError(t, err)

	// Verify attributes
	assert.NotNil(t, jobRunSummaryAttributes)
	assert.NotEmpty(t, jobRunSummaryAttributes)

	// Verify common attributes
	assert.Contains(t, jobRunSummaryAttributes, "env")
	assert.Equal(t, "production", jobRunSummaryAttributes["env"])
	assert.Contains(t, jobRunSummaryAttributes, "team")
	assert.Equal(t, "data-engineering", jobRunSummaryAttributes["team"])

	// Verify workspace attributes
	assert.Contains(t, jobRunSummaryAttributes, "databricksWorkspaceId")
	assert.Equal(
		t,
		mockWorkspaceId,
		jobRunSummaryAttributes["databricksWorkspaceId"],
	)
	assert.Contains(t, jobRunSummaryAttributes, "databricksWorkspaceName")
	assert.Equal(
		t,
		mockWorkspaceHost,
		jobRunSummaryAttributes["databricksWorkspaceName"],
	)
	assert.Contains(t, jobRunSummaryAttributes, "databricksWorkspaceUrl")
	assert.Equal(
		t,
		"https://" + mockWorkspaceHost,
		jobRunSummaryAttributes["databricksWorkspaceUrl"],
	)

	// Verify job run counts
	assert.Contains(t, jobRunSummaryAttributes, "blockedJobRunCount")
	assert.Equal(t, 2, jobRunSummaryAttributes["blockedJobRunCount"])
	assert.Contains(t, jobRunSummaryAttributes, "waitingJobRunCount")
	assert.Equal(t, 2, jobRunSummaryAttributes["waitingJobRunCount"])
	assert.Contains(t, jobRunSummaryAttributes, "pendingJobRunCount")
	assert.Equal(t, 0, jobRunSummaryAttributes["pendingJobRunCount"])
	assert.Contains(t, jobRunSummaryAttributes, "queuedJobRunCount")
	assert.Equal(t, 1, jobRunSummaryAttributes["queuedJobRunCount"])
	assert.Contains(t, jobRunSummaryAttributes, "runningJobRunCount")
	assert.Equal(t, 3, jobRunSummaryAttributes["runningJobRunCount"])
	assert.Contains(t, jobRunSummaryAttributes, "terminatingJobRunCount")
	assert.Equal(t, 2, jobRunSummaryAttributes["terminatingJobRunCount"])

	// Verify task run counts
	assert.Contains(t, jobRunSummaryAttributes, "blockedTaskRunCount")
	assert.Equal(t, 3, jobRunSummaryAttributes["blockedTaskRunCount"])
	assert.Contains(t, jobRunSummaryAttributes, "waitingTaskRunCount")
	assert.Equal(t, 1, jobRunSummaryAttributes["waitingTaskRunCount"])
	assert.Contains(t, jobRunSummaryAttributes, "pendingTaskRunCount")
	assert.Equal(t, 1, jobRunSummaryAttributes["pendingTaskRunCount"])
	assert.Contains(t, jobRunSummaryAttributes, "queuedTaskRunCount")
	assert.Equal(t, 0, jobRunSummaryAttributes["queuedTaskRunCount"])
	assert.Contains(t, jobRunSummaryAttributes, "runningTaskRunCount")
	assert.Equal(t, 2, jobRunSummaryAttributes["runningTaskRunCount"])
	assert.Contains(t, jobRunSummaryAttributes, "terminatingTaskRunCount")
	assert.Equal(t, 1, jobRunSummaryAttributes["terminatingTaskRunCount"])
}

func TestMakeBaseAttributes_GetWorkspaceInfoError(t *testing.T) {
	// Setup mock workspace
	mockWorkspace := setupMockWorkspace()

	// Setup expected error message
	expectedError := "error getting workspace info"

	// Save original GetWorkspaceInfo function and setup a defer function
	// to restore after the test
	originalGetWorkspaceInfo := GetWorkspaceInfo
	defer func() {
		GetWorkspaceInfo = originalGetWorkspaceInfo
	}()

	// Set GetWorkspaceInfo to return an error
	GetWorkspaceInfo = func(ctx context.Context, w DatabricksWorkspace) (
		*WorkspaceInfo,
		error,
	) {
		return nil, errors.New(expectedError)
	}

	// Setup mock tag map
	tags := map[string]string{
		"env": "production",
		"team": "data-engineering",
	}

	// Setup a mock cluster instance object
	clusterInstance := &databricksSdkJobs.ClusterInstance{
		ClusterId:      "fake-cluster-id",
		SparkContextId: "fake-cluster-spark-context-id",
	}

	// Execute the function under test
	baseAttributes, err := makeBaseAttributes(
		context.Background(),
		mockWorkspace,
		clusterInstance,
		tags,
	)

	// Verify the results
	assert.Error(t, err)
	assert.Nil(t, baseAttributes)
	assert.Equal(t, expectedError, err.Error())
}

func TestMakeBaseAttributes_GetClusterInfoByIdError(t *testing.T) {
	// Setup mock workspace and workspace info
	mockWorkspace, teardown := setupMockWorkspaceAndInfo()
	defer teardown()

	// Setup expected error message
	expectedError := "error getting cluster info by id"

	// Save original GetClusterInfoById function and setup a defer function
	// to restore after the test
	originalGetClusterInfoById := GetClusterInfoById
	defer func() {
		GetClusterInfoById = originalGetClusterInfoById
	}()

	// Set GetClusterInfoById to return an error
	GetClusterInfoById = func(
		ctx context.Context,
		w DatabricksWorkspace,
		clusterId string,
	) (
		*ClusterInfo,
		error,
	) {
		return nil, errors.New(expectedError)
	}

	// Setup mock tag map
	tags := map[string]string{
		"env": "production",
		"team": "data-engineering",
	}

	// Setup a mock cluster instance object
	clusterInstance := &databricksSdkJobs.ClusterInstance{
		ClusterId:      "fake-cluster-id",
		SparkContextId: "fake-cluster-spark-context-id",
	}

	// Execute the function under test
	baseAttributes, err := makeBaseAttributes(
		context.Background(),
		mockWorkspace,
		clusterInstance,
		tags,
	)

	// Verify the results
	assert.Error(t, err)
	assert.Nil(t, baseAttributes)
	assert.Equal(t, expectedError, err.Error())
}

func TestMakeBaseAttributes_ClusterInstanceNil(t *testing.T) {
	// Setup mock workspace and workspace info
	mockWorkspace, teardown := setupMockWorkspaceAndInfo()
	defer teardown()

	// Setup mock tag map
	tags := map[string]string{
		"env": "production",
		"team": "data-engineering",
	}

	// Execute the function under test
	baseAttributes, err := makeBaseAttributes(
		context.Background(),
		mockWorkspace,
		nil,
		tags,
	)

	// Verify the results
	assert.NoError(t, err)

	// Verify attributes
	assert.NotNil(t, baseAttributes)
	assert.NotEmpty(t, baseAttributes)

	// Verify common attributes
	assert.Contains(t, baseAttributes, "env")
	assert.Equal(t, "production", baseAttributes["env"])
	assert.Contains(t, baseAttributes, "team")
	assert.Equal(t, "data-engineering", baseAttributes["team"])

	// Verify workspace attributes
	assert.Contains(t, baseAttributes, "databricksWorkspaceId")
	assert.Equal(t, mockWorkspaceId, baseAttributes["databricksWorkspaceId"])
	assert.Contains(t, baseAttributes, "databricksWorkspaceName")
	assert.Equal(
		t,
		mockWorkspaceHost,
		baseAttributes["databricksWorkspaceName"],
	)
	assert.Contains(t, baseAttributes, "databricksWorkspaceUrl")
	assert.Equal(
		t,
		"https://" + mockWorkspaceHost,
		baseAttributes["databricksWorkspaceUrl"],
	)

	// Verify cluster attributes are not included
	assert.NotContains(t, baseAttributes, "databricksClusterId")
	assert.NotContains(t, baseAttributes, "databricksClusterName")
	assert.NotContains(t, baseAttributes, "databricksClusterSource")
	assert.NotContains(t, baseAttributes, "databricksClusterInstancePoolId")
	assert.NotContains(t, baseAttributes, "databricksClusterSparkContextId")
}

func TestMakeBaseAttributes(t *testing.T) {
	// Setup mock workspace and workspace info
	mockWorkspace, teardown := setupMockWorkspaceAndInfo()
	defer teardown()

	// Setup mock cluster info
	teardown2 := setupMockClusterInfo()
	defer teardown2()

	// Setup mock tag map
	tags := map[string]string{
		"env": "production",
		"team": "data-engineering",
	}

	// Setup a mock cluster instance object
	clusterInstance := &databricksSdkJobs.ClusterInstance{
		ClusterId:      "fake-cluster-id",
		SparkContextId: "fake-cluster-spark-context-id",
	}

	// Execute the function under test
	baseAttributes, err := makeBaseAttributes(
		context.Background(),
		mockWorkspace,
		clusterInstance,
		tags,
	)

	// Verify the results
	assert.NoError(t, err)

	// Verify attributes
	assert.NotNil(t, baseAttributes)
	assert.NotEmpty(t, baseAttributes)

	// Verify common attributes
	assert.Contains(t, baseAttributes, "env")
	assert.Equal(t, "production", baseAttributes["env"])
	assert.Contains(t, baseAttributes, "team")
	assert.Equal(t, "data-engineering", baseAttributes["team"])

	// Verify workspace attributes
	assert.Contains(t, baseAttributes, "databricksWorkspaceId")
	assert.Equal(t, mockWorkspaceId, baseAttributes["databricksWorkspaceId"])
	assert.Contains(t, baseAttributes, "databricksWorkspaceName")
	assert.Equal(
		t,
		mockWorkspaceHost,
		baseAttributes["databricksWorkspaceName"],
	)
	assert.Contains(t, baseAttributes, "databricksWorkspaceUrl")
	assert.Equal(
		t,
		"https://" + mockWorkspaceHost,
		baseAttributes["databricksWorkspaceUrl"],
	)

	// Verify cluster attributes
	assert.Contains(t, baseAttributes, "databricksClusterId")
	assert.Equal(t, "fake-cluster-id", baseAttributes["databricksClusterId"])
	assert.Contains(t, baseAttributes, "databricksClusterName")
	assert.Equal(
		t,
		"fake-cluster-name",
		baseAttributes["databricksClusterName"],
	)
	assert.Contains(t, baseAttributes, "databricksClusterSource")
	assert.Equal(
		t,
		"fake-cluster-source",
		baseAttributes["databricksClusterSource"],
	)
	assert.Contains(t, baseAttributes, "databricksClusterInstancePoolId")
	assert.Equal(
		t,
		"fake-cluster-instance-pool-id",
		baseAttributes["databricksClusterInstancePoolId"],
	)
	assert.Contains(t, baseAttributes, "databricksClusterSparkContextId")
	assert.Equal(
		t,
		"fake-cluster-spark-context-id",
		baseAttributes["databricksClusterSparkContextId"],
	)
}

func TestMakeJobRunBaseAttributes_MakeBaseAttributesError(t *testing.T) {
	// Setup mock workspace
	mockWorkspace := setupMockWorkspace()

	// Setup expected error message
	expectedError := "error getting workspace info"

	// Save original GetWorkspaceInfo function and setup a defer function
	// to restore after the test
	originalGetWorkspaceInfo := GetWorkspaceInfo
	defer func() {
		GetWorkspaceInfo = originalGetWorkspaceInfo
	}()

	// Set GetWorkspaceInfo to return an error so that makeBaseAttributes
	// will return an error
	GetWorkspaceInfo = func(
		ctx context.Context,
		w DatabricksWorkspace,
	) (
		*WorkspaceInfo,
		error,
	) {
		return nil, errors.New(expectedError)
	}

	// Setup mock tag map
	tags := map[string]string{
		"env": "production",
		"team": "data-engineering",
	}

	// Setup now
	jobRunStartTime := time.Now()
	jobRunEndTime := jobRunStartTime.Add(5 * time.Minute)

	// Setup a mock run object
	run := &databricksSdkJobs.BaseRun{
		JobId:                  0,
		RunId:                  123,
		OriginalAttemptRunId:   123,
		RunType:                databricksSdkJobs.RunTypeJobRun,
		RunName:                "jobRun1",
		Description:            "This is run 1 description",
		AttemptNumber:          0,
		StartTime:			    jobRunStartTime.UnixMilli(),
		EndTime:                jobRunEndTime.UnixMilli(),
		Trigger:			   	databricksSdkJobs.TriggerTypeOneTime,
		QueueDuration:          1000,
		SetupDuration:          500,
		ExecutionDuration:      1000,
		CleanupDuration:        300,
		Status: &databricksSdkJobs.RunStatus{
			State:        databricksSdkJobs.RunLifecycleStateV2StateTerminated,
			TerminationDetails: &databricksSdkJobs.TerminationDetails{
				Code: databricksSdkJobs.TerminationCodeCodeSuccess,
				Type: databricksSdkJobs.TerminationTypeTypeSuccess,
			},
		},
		Tasks: []databricksSdkJobs.RunTask{
			{
				RunId:         456,
				AttemptNumber: 0,
			},
		},
	}

	// Execute the function under test
	jobRunAttributes, err := makeJobRunBaseAttributes(
		context.Background(),
		mockWorkspace,
		run,
		"start",
		tags,
	)

	// Verify the results
	assert.Error(t, err)
	assert.Nil(t, jobRunAttributes)
	assert.Equal(t, expectedError, err.Error())
}

func TestMakeJobRunBaseAttributes_JobClusterInstance(t *testing.T) {
	// Setup mock workspace and workspace info
	mockWorkspace, teardown := setupMockWorkspaceAndInfo()
	defer teardown()

	// Setup mock cluster info
	teardown2 := setupMockClusterInfo()
	defer teardown2()

	// Setup mock tag map
	tags := map[string]string{
		"env": "production",
		"team": "data-engineering",
	}

	// Setup start time
	jobRunStartTime := time.Now()

	// Setup a mock run object
	run := &databricksSdkJobs.BaseRun{
		JobId:                  0,
		RunId:                  123,
		OriginalAttemptRunId:   123,
		RunType:                databricksSdkJobs.RunTypeJobRun,
		RunName:                "jobRun1",
		Description:            "This is run 1 description",
		AttemptNumber:          0,
		StartTime:			    jobRunStartTime.UnixMilli(),
		Trigger:			   	databricksSdkJobs.TriggerTypeOneTime,
		ClusterInstance: &databricksSdkJobs.ClusterInstance{
			ClusterId:      "fake-cluster-id",
			SparkContextId: "fake-cluster-spark-context-id",
		},
		Status: &databricksSdkJobs.RunStatus{
			State:        databricksSdkJobs.RunLifecycleStateV2StateRunning,
		},
		Tasks: []databricksSdkJobs.RunTask{
			{
				RunId: 456,
				AttemptNumber: 0,
			},
		},
	}

	// Execute the function under test
	jobRunAttributes, err := makeJobRunBaseAttributes(
		context.Background(),
		mockWorkspace,
		run,
		"start",
		tags,
	)

	// Verify the results
	assert.NoError(t, err)

	// Verify attributes
	assert.NotNil(t, jobRunAttributes)
	assert.NotEmpty(t, jobRunAttributes)

	// Verify common attributes
	assert.Contains(t, jobRunAttributes, "env")
	assert.Equal(t, "production", jobRunAttributes["env"])
	assert.Contains(t, jobRunAttributes, "team")
	assert.Equal(t, "data-engineering", jobRunAttributes["team"])

	// Verify workspace attributes
	assert.Contains(t, jobRunAttributes, "databricksWorkspaceId")
	assert.Equal(t, mockWorkspaceId, jobRunAttributes["databricksWorkspaceId"])
	assert.Contains(t, jobRunAttributes, "databricksWorkspaceName")
	assert.Equal(
		t,
		mockWorkspaceHost,
		jobRunAttributes["databricksWorkspaceName"],
	)
	assert.Contains(t, jobRunAttributes, "databricksWorkspaceUrl")
	assert.Equal(
		t,
		"https://" + mockWorkspaceHost,
		jobRunAttributes["databricksWorkspaceUrl"],
	)

	// Verify cluster attributes
	assert.Contains(t, jobRunAttributes, "databricksClusterId")
	assert.Equal(t, "fake-cluster-id", jobRunAttributes["databricksClusterId"])
	assert.Contains(t, jobRunAttributes, "databricksClusterName")
	assert.Equal(
		t,
		"fake-cluster-name",
		jobRunAttributes["databricksClusterName"],
	)
	assert.Contains(t, jobRunAttributes, "databricksClusterSource")
	assert.Equal(
		t,
		"fake-cluster-source",
		jobRunAttributes["databricksClusterSource"],
	)
	assert.Contains(t, jobRunAttributes, "databricksClusterInstancePoolId")
	assert.Equal(
		t,
		"fake-cluster-instance-pool-id",
		jobRunAttributes["databricksClusterInstancePoolId"],
	)
	assert.Contains(t, jobRunAttributes, "databricksClusterSparkContextId")
	assert.Equal(
		t,
		"fake-cluster-spark-context-id",
		jobRunAttributes["databricksClusterSparkContextId"],
	)

	// Verify event
	assert.Contains(t, jobRunAttributes, "event")
	assert.Equal(
		t,
		"start",
		jobRunAttributes["event"],
	)

	// Verify the job attributes
	assert.Contains(t, jobRunAttributes, "jobId")
	assert.Equal(t, int64(0), jobRunAttributes["jobId"])
	assert.Contains(t, jobRunAttributes, "jobRunId")
	assert.Equal(t, int64(123), jobRunAttributes["jobRunId"])
	assert.Contains(t, jobRunAttributes, "jobRunType")
	assert.Equal(
		t,
		string(databricksSdkJobs.RunTypeJobRun),
		jobRunAttributes["jobRunType"],
	)
	assert.Contains(t, jobRunAttributes, "jobRunName")
	assert.Equal(t, "jobRun1", jobRunAttributes["jobRunName"])
	assert.Contains(t, jobRunAttributes, "jobRunStartTime")
	assert.Equal(
		t,
		jobRunStartTime.UnixMilli(),
		jobRunAttributes["jobRunStartTime"],
	)
	assert.Contains(t, jobRunAttributes, "jobRunTrigger")
	assert.Equal(
		t,
		string(databricksSdkJobs.TriggerTypeOneTime),
		jobRunAttributes["jobRunTrigger"],
	)
	assert.Contains(t, jobRunAttributes, "originalAttemptRunId")
	assert.Equal(t, int64(123), jobRunAttributes["originalAttemptRunId"])
	assert.Contains(t, jobRunAttributes, "description")
	assert.Equal(
		t,
		"This is run 1 description",
		jobRunAttributes["description"],
	)
	assert.Contains(t, jobRunAttributes, "attempt")
	assert.Equal(t, 0, jobRunAttributes["attempt"])
	assert.Contains(t, jobRunAttributes, "isRetry")
	assert.Equal(t, false, jobRunAttributes["isRetry"])

	assert.NotContains(t, jobRunAttributes, "state")
	assert.NotContains(t, jobRunAttributes, "terminationCode")
	assert.NotContains(t, jobRunAttributes, "terminationType")
	assert.NotContains(t, jobRunAttributes, "jobRunEndTime")
	assert.NotContains(t, jobRunAttributes, "duration")
	assert.NotContains(t, jobRunAttributes, "queueDuration")
	assert.NotContains(t, jobRunAttributes, "setupDuration")
	assert.NotContains(t, jobRunAttributes, "executionDuration")
	assert.NotContains(t, jobRunAttributes, "cleanupDuration")
}

func TestMakeJobRunBaseAttributes(t *testing.T) {
	// Setup mock workspace and workspace info
	mockWorkspace, teardown := setupMockWorkspaceAndInfo()
	defer teardown()

	// Setup mock cluster info
	teardown2 := setupMockClusterInfo()
	defer teardown2()

	// Setup mock tag map
	tags := map[string]string{
		"env": "production",
		"team": "data-engineering",
	}

	// Setup start time
	jobRunStartTime := time.Now()

	// Setup a mock run object
	run := &databricksSdkJobs.BaseRun{
		JobId:                  0,
		RunId:                  123,
		OriginalAttemptRunId:   123,
		RunType:                databricksSdkJobs.RunTypeJobRun,
		RunName:                "jobRun1",
		Description:            "This is run 1 description",
		AttemptNumber:          0,
		StartTime:			    jobRunStartTime.UnixMilli(),
		Trigger:			   	databricksSdkJobs.TriggerTypeOneTime,
		Status: &databricksSdkJobs.RunStatus{
			State:        databricksSdkJobs.RunLifecycleStateV2StateRunning,
		},
		Tasks: []databricksSdkJobs.RunTask{
			{
				RunId: 456,
				AttemptNumber: 0,
			},
		},
	}

	// Execute the function under test
	jobRunAttributes, err := makeJobRunBaseAttributes(
		context.Background(),
		mockWorkspace,
		run,
		"start",
		tags,
	)

	// Verify the results
	assert.NoError(t, err)

	// Verify attributes
	assert.NotNil(t, jobRunAttributes)
	assert.NotEmpty(t, jobRunAttributes)

	// Verify common attributes
	assert.Contains(t, jobRunAttributes, "env")
	assert.Equal(t, "production", jobRunAttributes["env"])
	assert.Contains(t, jobRunAttributes, "team")
	assert.Equal(t, "data-engineering", jobRunAttributes["team"])

	// Verify workspace attributes
	assert.Contains(t, jobRunAttributes, "databricksWorkspaceId")
	assert.Equal(t, mockWorkspaceId, jobRunAttributes["databricksWorkspaceId"])
	assert.Contains(t, jobRunAttributes, "databricksWorkspaceName")
	assert.Equal(
		t,
		mockWorkspaceHost,
		jobRunAttributes["databricksWorkspaceName"],
	)
	assert.Contains(t, jobRunAttributes, "databricksWorkspaceUrl")
	assert.Equal(
		t,
		"https://" + mockWorkspaceHost,
		jobRunAttributes["databricksWorkspaceUrl"],
	)

	// Verify no cluster attributes
	assert.NotContains(t, jobRunAttributes, "databricksClusterId")
	assert.NotContains(t, jobRunAttributes, "databricksClusterName")
	assert.NotContains(t, jobRunAttributes, "databricksClusterSource")
	assert.NotContains(t, jobRunAttributes, "databricksClusterInstancePoolId")
	assert.NotContains(t, jobRunAttributes, "databricksClusterSparkContextId")

	// Verify event
	assert.Contains(t, jobRunAttributes, "event")
	assert.Equal(
		t,
		"start",
		jobRunAttributes["event"],
	)

	// Verify the job attributes
	assert.Contains(t, jobRunAttributes, "jobId")
	assert.Equal(t, int64(0), jobRunAttributes["jobId"])
	assert.Contains(t, jobRunAttributes, "jobRunId")
	assert.Equal(t, int64(123), jobRunAttributes["jobRunId"])
	assert.Contains(t, jobRunAttributes, "jobRunType")
	assert.Equal(
		t,
		string(databricksSdkJobs.RunTypeJobRun),
		jobRunAttributes["jobRunType"],
	)
	assert.Contains(t, jobRunAttributes, "jobRunName")
	assert.Equal(t, "jobRun1", jobRunAttributes["jobRunName"])
	assert.Contains(t, jobRunAttributes, "jobRunStartTime")
	assert.Equal(
		t,
		jobRunStartTime.UnixMilli(),
		jobRunAttributes["jobRunStartTime"],
	)
	assert.Contains(t, jobRunAttributes, "jobRunTrigger")
	assert.Equal(
		t,
		string(databricksSdkJobs.TriggerTypeOneTime),
		jobRunAttributes["jobRunTrigger"],
	)
	assert.Contains(t, jobRunAttributes, "originalAttemptRunId")
	assert.Equal(t, int64(123), jobRunAttributes["originalAttemptRunId"])
	assert.Contains(t, jobRunAttributes, "description")
	assert.Equal(
		t,
		"This is run 1 description",
		jobRunAttributes["description"],
	)
	assert.Contains(t, jobRunAttributes, "attempt")
	assert.Equal(t, 0, jobRunAttributes["attempt"])
	assert.Contains(t, jobRunAttributes, "isRetry")
	assert.Equal(t, false, jobRunAttributes["isRetry"])
	assert.NotContains(t, jobRunAttributes, "state")
	assert.NotContains(t, jobRunAttributes, "terminationCode")
	assert.NotContains(t, jobRunAttributes, "terminationType")
	assert.NotContains(t, jobRunAttributes, "jobRunEndTime")
	assert.NotContains(t, jobRunAttributes, "duration")
	assert.NotContains(t, jobRunAttributes, "queueDuration")
	assert.NotContains(t, jobRunAttributes, "setupDuration")
	assert.NotContains(t, jobRunAttributes, "executionDuration")
	assert.NotContains(t, jobRunAttributes, "cleanupDuration")
}

func TestMakeJobRunStartAttributes_MakeJobRunBaseAttributesError(t *testing.T) {
	// Setup mock workspace
	mockWorkspace := setupMockWorkspace()

	// Setup expected error message
	expectedError := "error getting workspace info"

	// Save original GetWorkspaceInfo function and setup a defer function
	// to restore after the test
	originalGetWorkspaceInfo := GetWorkspaceInfo
	defer func() {
		GetWorkspaceInfo = originalGetWorkspaceInfo
	}()

	// Set GetWorkspaceInfo to return an error so that makeJobRunBaseAttributes
	// will return an error
	GetWorkspaceInfo = func(
		ctx context.Context,
		w DatabricksWorkspace,
	) (
		*WorkspaceInfo,
		error,
	) {
		return nil, errors.New(expectedError)
	}

	// Setup mock tag map
	tags := map[string]string{
		"env": "production",
		"team": "data-engineering",
	}

	// Setup start time
	jobRunStartTime := time.Now()

	// Setup a mock run object
	run := &databricksSdkJobs.BaseRun{
		JobId:                  0,
		RunId:                  123,
		OriginalAttemptRunId:   123,
		RunType:                databricksSdkJobs.RunTypeJobRun,
		RunName:                "jobRun1",
		Description:            "This is run 1 description",
		AttemptNumber:          0,
		StartTime:			    jobRunStartTime.UnixMilli(),
		Trigger:			   	databricksSdkJobs.TriggerTypeOneTime,
		QueueDuration:          1000,
		SetupDuration:          500,
		ExecutionDuration:      1000,
		CleanupDuration:        300,
		Status: &databricksSdkJobs.RunStatus{
			State:        databricksSdkJobs.RunLifecycleStateV2StateTerminated,
			TerminationDetails: &databricksSdkJobs.TerminationDetails{
				Code: databricksSdkJobs.TerminationCodeCodeSuccess,
				Type: databricksSdkJobs.TerminationTypeTypeSuccess,
			},
		},
		Tasks: []databricksSdkJobs.RunTask{
			{
				RunId: 456,
				AttemptNumber: 0,
			},
		},
	}

	// Execute the function under test
	jobRunAttributes, err := makeJobRunStartAttributes(
		context.Background(),
		mockWorkspace,
		run,
		tags,
	)

	// Verify the results
	assert.Error(t, err)
	assert.Nil(t, jobRunAttributes)
	assert.Equal(t, expectedError, err.Error())
}

func TestMakeJobRunStartAttributes(t *testing.T) {
	// Setup mock workspace and workspace info
	mockWorkspace, teardown := setupMockWorkspaceAndInfo()
	defer teardown()

	// Setup mock cluster info
	teardown2 := setupMockClusterInfo()
	defer teardown2()

	// Setup mock tag map
	tags := map[string]string{
		"env": "production",
		"team": "data-engineering",
	}

	// Setup start time
	jobRunStartTime := time.Now()

	// Setup a mock run object
	run := &databricksSdkJobs.BaseRun{
		JobId:                  0,
		RunId:                  123,
		OriginalAttemptRunId:   123,
		RunType:                databricksSdkJobs.RunTypeJobRun,
		RunName:                "jobRun1",
		Description:            "This is run 1 description",
		AttemptNumber:          0,
		StartTime:			    jobRunStartTime.UnixMilli(),
		Trigger:			   	databricksSdkJobs.TriggerTypeOneTime,
		Status: &databricksSdkJobs.RunStatus{
			State:        databricksSdkJobs.RunLifecycleStateV2StateRunning,
		},
		Tasks: []databricksSdkJobs.RunTask{
			{
				RunId: 456,
				AttemptNumber: 0,
			},
		},
	}

	// Execute the function under test
	jobRunAttributes, err := makeJobRunStartAttributes(
		context.Background(),
		mockWorkspace,
		run,
		tags,
	)

	// Verify the results
	assert.NoError(t, err)

	// Verify attributes
	assert.NotNil(t, jobRunAttributes)
	assert.NotEmpty(t, jobRunAttributes)

	// Verify common attributes
	assert.Contains(t, jobRunAttributes, "env")
	assert.Equal(t, "production", jobRunAttributes["env"])
	assert.Contains(t, jobRunAttributes, "team")
	assert.Equal(t, "data-engineering", jobRunAttributes["team"])

	// Verify workspace attributes
	assert.Contains(t, jobRunAttributes, "databricksWorkspaceId")
	assert.Equal(t, mockWorkspaceId, jobRunAttributes["databricksWorkspaceId"])
	assert.Contains(t, jobRunAttributes, "databricksWorkspaceName")
	assert.Equal(
		t,
		mockWorkspaceHost,
		jobRunAttributes["databricksWorkspaceName"],
	)
	assert.Contains(t, jobRunAttributes, "databricksWorkspaceUrl")
	assert.Equal(
		t,
		"https://" + mockWorkspaceHost,
		jobRunAttributes["databricksWorkspaceUrl"],
	)

	// Verify no cluster attributes
	assert.NotContains(t, jobRunAttributes, "databricksClusterId")
	assert.NotContains(t, jobRunAttributes, "databricksClusterName")
	assert.NotContains(t, jobRunAttributes, "databricksClusterSource")
	assert.NotContains(t, jobRunAttributes, "databricksClusterInstancePoolId")
	assert.NotContains(t, jobRunAttributes, "databricksClusterSparkContextId")

	// Verify event
	assert.Contains(t, jobRunAttributes, "event")
	assert.Equal(
		t,
		"start",
		jobRunAttributes["event"],
	)

	// Verify the job attributes
	assert.Contains(t, jobRunAttributes, "jobId")
	assert.Equal(t, int64(0), jobRunAttributes["jobId"])
	assert.Contains(t, jobRunAttributes, "jobRunId")
	assert.Equal(t, int64(123), jobRunAttributes["jobRunId"])
	assert.Contains(t, jobRunAttributes, "jobRunType")
	assert.Equal(
		t,
		string(databricksSdkJobs.RunTypeJobRun),
		jobRunAttributes["jobRunType"],
	)
	assert.Contains(t, jobRunAttributes, "jobRunName")
	assert.Equal(t, "jobRun1", jobRunAttributes["jobRunName"])
	assert.Contains(t, jobRunAttributes, "jobRunStartTime")
	assert.Equal(
		t,
		jobRunStartTime.UnixMilli(),
		jobRunAttributes["jobRunStartTime"],
	)
	assert.Contains(t, jobRunAttributes, "jobRunTrigger")
	assert.Equal(
		t,
		string(databricksSdkJobs.TriggerTypeOneTime),
		jobRunAttributes["jobRunTrigger"],
	)
	assert.Contains(t, jobRunAttributes, "originalAttemptRunId")
	assert.Equal(t, int64(123), jobRunAttributes["originalAttemptRunId"])
	assert.Contains(t, jobRunAttributes, "description")
	assert.Equal(
		t,
		"This is run 1 description",
		jobRunAttributes["description"],
	)
	assert.Contains(t, jobRunAttributes, "attempt")
	assert.Equal(t, 0, jobRunAttributes["attempt"])
	assert.Contains(t, jobRunAttributes, "isRetry")
	assert.Equal(t, false, jobRunAttributes["isRetry"])
	assert.NotContains(t, jobRunAttributes, "state")
	assert.NotContains(t, jobRunAttributes, "terminationCode")
	assert.NotContains(t, jobRunAttributes, "terminationType")
	assert.NotContains(t, jobRunAttributes, "jobRunEndTime")
	assert.NotContains(t, jobRunAttributes, "duration")
	assert.NotContains(t, jobRunAttributes, "queueDuration")
	assert.NotContains(t, jobRunAttributes, "setupDuration")
	assert.NotContains(t, jobRunAttributes, "executionDuration")
	assert.NotContains(t, jobRunAttributes, "cleanupDuration")
}

func TestMakeJobRunCompleteAttributes_MakeJobRunBaseAttributesError(t *testing.T) {
	// Setup mock workspace
	mockWorkspace := setupMockWorkspace()

	// Setup expected error message
	expectedError := "error getting workspace info"

	// Save original GetWorkspaceInfo function and setup a defer function
	// to restore after the test
	originalGetWorkspaceInfo := GetWorkspaceInfo
	defer func() {
		GetWorkspaceInfo = originalGetWorkspaceInfo
	}()

	// Set GetWorkspaceInfo to return an error so that makeJobRunBaseAttributes
	// will return an error
	GetWorkspaceInfo = func(
		ctx context.Context,
		w DatabricksWorkspace,
	) (
		*WorkspaceInfo,
		error,
	) {
		return nil, errors.New(expectedError)
	}

	// Setup mock tag map
	tags := map[string]string{
		"env": "production",
		"team": "data-engineering",
	}

	// Setup start and end time
	jobRunStartTime := time.Now()
	jobRunEndTime := jobRunStartTime.Add(5 * time.Minute)

	// Setup a mock run object
	run := &databricksSdkJobs.BaseRun{
		JobId:                  0,
		RunId:                  123,
		OriginalAttemptRunId:   123,
		RunType:                databricksSdkJobs.RunTypeJobRun,
		RunName:                "jobRun1",
		Description:            "This is run 1 description",
		AttemptNumber:          0,
		StartTime:			    jobRunStartTime.UnixMilli(),
		EndTime: 			  	jobRunEndTime.UnixMilli(),
		Trigger:			   	databricksSdkJobs.TriggerTypeOneTime,
		QueueDuration:          1000,
		SetupDuration:          500,
		ExecutionDuration:      1000,
		CleanupDuration:        300,
		Status: &databricksSdkJobs.RunStatus{
			State:        databricksSdkJobs.RunLifecycleStateV2StateTerminated,
			TerminationDetails: &databricksSdkJobs.TerminationDetails{
				Code: databricksSdkJobs.TerminationCodeCodeSuccess,
				Type: databricksSdkJobs.TerminationTypeTypeSuccess,
			},
		},
		Tasks: []databricksSdkJobs.RunTask{
			{
				RunId: 456,
				AttemptNumber: 0,
			},
		},
	}

	// Execute the function under test
	jobRunAttributes, err := makeJobRunCompleteAttributes(
		context.Background(),
		mockWorkspace,
		run,
		tags,
	)

	// Verify the results
	assert.Error(t, err)
	assert.Nil(t, jobRunAttributes)
	assert.Equal(t, expectedError, err.Error())
}

func TestMakeJobRunCompleteAttributes(t *testing.T) {
	// Setup mock workspace and workspace info
	mockWorkspace, teardown := setupMockWorkspaceAndInfo()
	defer teardown()

	// Setup mock cluster info
	teardown2 := setupMockClusterInfo()
	defer teardown2()

	// Setup mock tag map
	tags := map[string]string{
		"env": "production",
		"team": "data-engineering",
	}

	// Setup start and end time
	jobRunStartTime := time.Now()
	jobRunEndTime := jobRunStartTime.Add(5 * time.Minute)

	// Setup a mock run object
	run := &databricksSdkJobs.BaseRun{
		JobId:                  0,
		RunId:                  123,
		OriginalAttemptRunId:   123,
		RunType:                databricksSdkJobs.RunTypeJobRun,
		RunName:                "jobRun1",
		Description:            "This is run 1 description",
		AttemptNumber:          0,
		StartTime:			    jobRunStartTime.UnixMilli(),
		EndTime: 			  	jobRunEndTime.UnixMilli(),
		Trigger:			   	databricksSdkJobs.TriggerTypeOneTime,
		QueueDuration:          1000,
		SetupDuration:          500,
		ExecutionDuration:      1000,
		CleanupDuration:        300,
		Status: &databricksSdkJobs.RunStatus{
			State:        databricksSdkJobs.RunLifecycleStateV2StateTerminated,
			TerminationDetails: &databricksSdkJobs.TerminationDetails{
				Code: databricksSdkJobs.TerminationCodeCodeSuccess,
				Type: databricksSdkJobs.TerminationTypeTypeSuccess,
			},
		},
		Tasks: []databricksSdkJobs.RunTask{
			{
				RunId: 456,
				AttemptNumber: 0,
			},
		},
	}

	// Execute the function under test
	jobRunAttributes, err := makeJobRunCompleteAttributes(
		context.Background(),
		mockWorkspace,
		run,
		tags,
	)

	// Verify the results
	assert.NoError(t, err)

	// Verify attributes
	assert.NotNil(t, jobRunAttributes)
	assert.NotEmpty(t, jobRunAttributes)

	// Verify common attributes
	assert.Contains(t, jobRunAttributes, "env")
	assert.Equal(t, "production", jobRunAttributes["env"])
	assert.Contains(t, jobRunAttributes, "team")
	assert.Equal(t, "data-engineering", jobRunAttributes["team"])

	// Verify workspace attributes
	assert.Contains(t, jobRunAttributes, "databricksWorkspaceId")
	assert.Equal(t, mockWorkspaceId, jobRunAttributes["databricksWorkspaceId"])
	assert.Contains(t, jobRunAttributes, "databricksWorkspaceName")
	assert.Equal(
		t,
		mockWorkspaceHost,
		jobRunAttributes["databricksWorkspaceName"],
	)
	assert.Contains(t, jobRunAttributes, "databricksWorkspaceUrl")
	assert.Equal(
		t,
		"https://" + mockWorkspaceHost,
		jobRunAttributes["databricksWorkspaceUrl"],
	)

	// Verify no cluster attributes
	assert.NotContains(t, jobRunAttributes, "databricksClusterId")
	assert.NotContains(t, jobRunAttributes, "databricksClusterName")
	assert.NotContains(t, jobRunAttributes, "databricksClusterSource")
	assert.NotContains(t, jobRunAttributes, "databricksClusterInstancePoolId")
	assert.NotContains(t, jobRunAttributes, "databricksClusterSparkContextId")

	// Verify event
	assert.Contains(t, jobRunAttributes, "event")
	assert.Equal(
		t,
		"complete",
		jobRunAttributes["event"],
	)

	// Verify the job attributes
	assert.Contains(t, jobRunAttributes, "jobId")
	assert.Equal(t, int64(0), jobRunAttributes["jobId"])
	assert.Contains(t, jobRunAttributes, "jobRunId")
	assert.Equal(t, int64(123), jobRunAttributes["jobRunId"])
	assert.Contains(t, jobRunAttributes, "jobRunType")
	assert.Equal(
		t,
		string(databricksSdkJobs.RunTypeJobRun),
		jobRunAttributes["jobRunType"],
	)
	assert.Contains(t, jobRunAttributes, "jobRunName")
	assert.Equal(t, "jobRun1", jobRunAttributes["jobRunName"])
	assert.Contains(t, jobRunAttributes, "jobRunStartTime")
	assert.Equal(
		t,
		jobRunStartTime.UnixMilli(),
		jobRunAttributes["jobRunStartTime"],
	)
	assert.Contains(t, jobRunAttributes, "jobRunTrigger")
	assert.Equal(
		t,
		string(databricksSdkJobs.TriggerTypeOneTime),
		jobRunAttributes["jobRunTrigger"],
	)
	assert.Contains(t, jobRunAttributes, "originalAttemptRunId")
	assert.Equal(t, int64(123), jobRunAttributes["originalAttemptRunId"])
	assert.Contains(t, jobRunAttributes, "description")
	assert.Equal(
		t,
		"This is run 1 description",
		jobRunAttributes["description"],
	)
	assert.Contains(t, jobRunAttributes, "attempt")
	assert.Equal(t, 0, jobRunAttributes["attempt"])
	assert.Contains(t, jobRunAttributes, "isRetry")
	assert.Equal(t, false, jobRunAttributes["isRetry"])
	assert.Contains(t, jobRunAttributes, "state")
	assert.Equal(
		t,
		string(databricksSdkJobs.RunLifecycleStateV2StateTerminated),
		jobRunAttributes["state"],
	)
	assert.Contains(t, jobRunAttributes, "terminationCode")
	assert.Equal(
		t,
		string(databricksSdkJobs.TerminationCodeCodeSuccess),
		jobRunAttributes["terminationCode"],
	)
	assert.Contains(t, jobRunAttributes, "terminationType")
	assert.Equal(
		t,
		string(databricksSdkJobs.TerminationTypeTypeSuccess),
		jobRunAttributes["terminationType"],
	)
	assert.Contains(t, jobRunAttributes, "jobRunEndTime")
	assert.Equal(
		t,
		jobRunEndTime.UnixMilli(),
		jobRunAttributes["jobRunEndTime"],
	)
	assert.Contains(t, jobRunAttributes, "duration")
	assert.Equal(
		t,
		int64(1800),
		jobRunAttributes["duration"],
	)
	assert.Contains(t, jobRunAttributes, "queueDuration")
	assert.Equal(
		t,
		int64(1000),
		jobRunAttributes["queueDuration"],
	)
	assert.Contains(t, jobRunAttributes, "setupDuration")
	assert.Equal(
		t,
		int64(500),
		jobRunAttributes["setupDuration"],
	)
	assert.Contains(t, jobRunAttributes, "executionDuration")
	assert.Equal(
		t,
		int64(1000),
		jobRunAttributes["executionDuration"],
	)
	assert.Contains(t, jobRunAttributes, "cleanupDuration")
	assert.Equal(
		t,
		int64(300),
		jobRunAttributes["cleanupDuration"],
	)
}

func TestMakeJobRunCompleteAttributes_MultipleTasks(t *testing.T) {
	// Setup mock workspace and workspace info
	mockWorkspace, teardown := setupMockWorkspaceAndInfo()
	defer teardown()

	// Setup mock cluster info
	teardown2 := setupMockClusterInfo()
	defer teardown2()

	// Setup mock tag map
	tags := map[string]string{
		"env": "production",
		"team": "data-engineering",
	}

	// Setup start and end time
	jobRunStartTime := time.Now()
	jobRunEndTime := jobRunStartTime.Add(5 * time.Minute)

	// Setup a mock run object
	run := &databricksSdkJobs.BaseRun{
		JobId:                  0,
		RunId:                  123,
		OriginalAttemptRunId:   123,
		RunType:                databricksSdkJobs.RunTypeJobRun,
		RunName:                "jobRun1",
		Description:            "This is run 1 description",
		AttemptNumber:          0,
		StartTime:			    jobRunStartTime.UnixMilli(),
		EndTime: 			  	jobRunEndTime.UnixMilli(),
		Trigger:			   	databricksSdkJobs.TriggerTypeOneTime,
		QueueDuration:          250,
		RunDuration:            3500,
		Status: &databricksSdkJobs.RunStatus{
			State:        databricksSdkJobs.RunLifecycleStateV2StateTerminated,
			TerminationDetails: &databricksSdkJobs.TerminationDetails{
				Code: databricksSdkJobs.TerminationCodeCodeSuccess,
				Type: databricksSdkJobs.TerminationTypeTypeSuccess,
			},
		},
		Tasks: []databricksSdkJobs.RunTask{
			{
				RunId: 456,
				AttemptNumber: 0,
			},
			{
				RunId: 789,
				AttemptNumber: 0,
			},
		},
	}

	// Execute the function under test
	jobRunAttributes, err := makeJobRunCompleteAttributes(
		context.Background(),
		mockWorkspace,
		run,
		tags,
	)

	// Verify the results
	assert.NoError(t, err)

	// Verify attributes
	assert.NotNil(t, jobRunAttributes)
	assert.NotEmpty(t, jobRunAttributes)

	// Verify common attributes
	assert.Contains(t, jobRunAttributes, "env")
	assert.Equal(t, "production", jobRunAttributes["env"])
	assert.Contains(t, jobRunAttributes, "team")
	assert.Equal(t, "data-engineering", jobRunAttributes["team"])

	// Verify workspace attributes
	assert.Contains(t, jobRunAttributes, "databricksWorkspaceId")
	assert.Equal(t, mockWorkspaceId, jobRunAttributes["databricksWorkspaceId"])
	assert.Contains(t, jobRunAttributes, "databricksWorkspaceName")
	assert.Equal(
		t,
		mockWorkspaceHost,
		jobRunAttributes["databricksWorkspaceName"],
	)
	assert.Contains(t, jobRunAttributes, "databricksWorkspaceUrl")
	assert.Equal(
		t,
		"https://" + mockWorkspaceHost,
		jobRunAttributes["databricksWorkspaceUrl"],
	)

	// Verify no cluster attributes
	assert.NotContains(t, jobRunAttributes, "databricksClusterId")
	assert.NotContains(t, jobRunAttributes, "databricksClusterName")
	assert.NotContains(t, jobRunAttributes, "databricksClusterSource")
	assert.NotContains(t, jobRunAttributes, "databricksClusterInstancePoolId")
	assert.NotContains(t, jobRunAttributes, "databricksClusterSparkContextId")

	// Verify event
	assert.Contains(t, jobRunAttributes, "event")
	assert.Equal(
		t,
		"complete",
		jobRunAttributes["event"],
	)

	// Verify the job attributes
	assert.Contains(t, jobRunAttributes, "jobId")
	assert.Equal(t, int64(0), jobRunAttributes["jobId"])
	assert.Contains(t, jobRunAttributes, "jobRunId")
	assert.Equal(t, int64(123), jobRunAttributes["jobRunId"])
	assert.Contains(t, jobRunAttributes, "jobRunType")
	assert.Equal(
		t,
		string(databricksSdkJobs.RunTypeJobRun),
		jobRunAttributes["jobRunType"],
	)
	assert.Contains(t, jobRunAttributes, "jobRunName")
	assert.Equal(t, "jobRun1", jobRunAttributes["jobRunName"])
	assert.Contains(t, jobRunAttributes, "jobRunStartTime")
	assert.Equal(
		t,
		jobRunStartTime.UnixMilli(),
		jobRunAttributes["jobRunStartTime"],
	)
	assert.Contains(t, jobRunAttributes, "jobRunTrigger")
	assert.Equal(
		t,
		string(databricksSdkJobs.TriggerTypeOneTime),
		jobRunAttributes["jobRunTrigger"],
	)
	assert.Contains(t, jobRunAttributes, "originalAttemptRunId")
	assert.Equal(t, int64(123), jobRunAttributes["originalAttemptRunId"])
	assert.Contains(t, jobRunAttributes, "description")
	assert.Equal(
		t,
		"This is run 1 description",
		jobRunAttributes["description"],
	)
	assert.Contains(t, jobRunAttributes, "attempt")
	assert.Equal(t, 0, jobRunAttributes["attempt"])
	assert.Contains(t, jobRunAttributes, "isRetry")
	assert.Equal(t, false, jobRunAttributes["isRetry"])
	assert.Contains(t, jobRunAttributes, "state")
	assert.Equal(
		t,
		string(databricksSdkJobs.RunLifecycleStateV2StateTerminated),
		jobRunAttributes["state"],
	)
	assert.Contains(t, jobRunAttributes, "terminationCode")
	assert.Equal(
		t,
		string(databricksSdkJobs.TerminationCodeCodeSuccess),
		jobRunAttributes["terminationCode"],
	)
	assert.Contains(t, jobRunAttributes, "terminationType")
	assert.Equal(
		t,
		string(databricksSdkJobs.TerminationTypeTypeSuccess),
		jobRunAttributes["terminationType"],
	)
	assert.Contains(t, jobRunAttributes, "jobRunEndTime")
	assert.Equal(
		t,
		jobRunEndTime.UnixMilli(),
		jobRunAttributes["jobRunEndTime"],
	)
	assert.Contains(t, jobRunAttributes, "duration")
	assert.Equal(
		t,
		int64(3500),
		jobRunAttributes["duration"],
	)
	assert.Contains(t, jobRunAttributes, "queueDuration")
	assert.Equal(
		t,
		int64(250),
		jobRunAttributes["queueDuration"],
	)
	assert.NotContains(t, jobRunAttributes, "setupDuration")
	assert.NotContains(t, jobRunAttributes, "executionDuration")
	assert.NotContains(t, jobRunAttributes, "cleanupDuration")
}

func TestProcessJobRunTask_MakeJobRunTaskStartAttributesError(t *testing.T) {
	// Setup mock workspace
	mockWorkspace := setupMockWorkspace()

	// Setup expected error message
	expectedError := "error getting workspace info"

	// Save original GetWorkspaceInfo function and setup a defer function
	// to restore after the test
	originalGetWorkspaceInfo := GetWorkspaceInfo
	defer func() {
		GetWorkspaceInfo = originalGetWorkspaceInfo
	}()

	// Set GetWorkspaceInfo to return an error so that makeJobRunTaskStartAttributes
	// will return an error
	GetWorkspaceInfo = func(
		ctx context.Context,
		w DatabricksWorkspace,
	) (
		*WorkspaceInfo,
		error,
	) {
		return nil, errors.New(expectedError)
	}

	// Setup mock tag map
	tags := map[string]string{
		"env": "production",
		"team": "data-engineering",
	}

	// Mock Now
	now := time.Now()
	Now = func () time.Time { return now }

	// Setup times to be used to test behavior of event production based on
	// lastRun time.
	lastRun := now
	lastRunMilli := lastRun.UnixMilli()
	// Job run started before
	jobRunStartTime := lastRun.Add(-10 * time.Second)
	// Task run started after
	taskRunStartTime := lastRun.Add(10 * time.Second)

	// Setup a mock run object
	run := &databricksSdkJobs.BaseRun{
		JobId:                  0,
		RunId:                  123,
		OriginalAttemptRunId:   123,
		RunType:                databricksSdkJobs.RunTypeJobRun,
		RunName:                "jobRun1",
		Description:            "This is run 1 description",
		AttemptNumber:          0,
		StartTime:			    jobRunStartTime.UnixMilli(),
		Trigger:			   	databricksSdkJobs.TriggerTypeOneTime,
		Status: &databricksSdkJobs.RunStatus{
			State:        databricksSdkJobs.RunLifecycleStateV2StateRunning,
		},
	}

	// Setup a mock task object
	task := &databricksSdkJobs.RunTask{
		TaskKey:       "task1",
		RunId:         456,
		AttemptNumber: 0,
		Description:   "This is task run 1 description",
		// started after to ensure we go through the start event production
		// logic
		StartTime:     taskRunStartTime.UnixMilli(),
		ClusterInstance: &databricksSdkJobs.ClusterInstance{
			ClusterId:      "fake-cluster-id",
			SparkContextId: "fake-cluster-spark-context-id",
		},
		Status: &databricksSdkJobs.RunStatus{
			// running to ensure we do not go through the complete event
			// production logic
			State: databricksSdkJobs.RunLifecycleStateV2StateRunning,
		},
	}

	// Setup an events channel to receive events
	eventsChan := make(chan model.Event, 500) // Buffer to prevent blocking

	// Execute the function under test
	err := processJobRunTask(
		context.Background(),
		mockWorkspace,
		run,
		task,
		lastRunMilli,
		tags,
		eventsChan,
	)

	// Close the channel
	close(eventsChan)

	// Verify result
	assert.Error(t, err)
	assert.Equal(t, expectedError, err.Error())
}

func TestProcessJobRunTask_RunStartedBefore(t *testing.T) {
	// Setup mock workspace
	mockWorkspace := setupMockWorkspace()

	// Setup mock tag map
	tags := map[string]string{
		"env": "production",
		"team": "data-engineering",
	}

	// Mock Now
	now := time.Now()
	Now = func () time.Time { return now }

	// Setup times to be used to test behavior of event production based on
	// lastRun time.
	lastRun := now
	lastRunMilli := lastRun.UnixMilli()
	// Job run started before
	jobRunStartTime := lastRun.Add(-10 * time.Second)
	// Task run started before
	taskRunStartTime := lastRun.Add(-5 * time.Second)

	// Setup a mock run object
	run := &databricksSdkJobs.BaseRun{
		JobId:                  0,
		RunId:                  123,
		OriginalAttemptRunId:   123,
		RunType:                databricksSdkJobs.RunTypeJobRun,
		RunName:                "jobRun1",
		Description:            "This is run 1 description",
		AttemptNumber:          0,
		StartTime:			    jobRunStartTime.UnixMilli(),
		Trigger:			   	databricksSdkJobs.TriggerTypeOneTime,
		Status: &databricksSdkJobs.RunStatus{
			State:        databricksSdkJobs.RunLifecycleStateV2StateRunning,
		},
	}

	// Setup a mock task object
	task := &databricksSdkJobs.RunTask{
		TaskKey:       "task1",
		RunId:         456,
		AttemptNumber: 0,
		Description:  "This is task run 1 description",
		// started before to ensure we don't go through start event production
		// logic
		StartTime:    taskRunStartTime.UnixMilli(),
		ClusterInstance: &databricksSdkJobs.ClusterInstance{
			ClusterId:      "fake-cluster-id",
			SparkContextId: "fake-cluster-spark-context-id",
		},
		Status: &databricksSdkJobs.RunStatus{
			// running to ensure we do not go through the complete event
			// production logic
			State: databricksSdkJobs.RunLifecycleStateV2StateRunning,
		},
	}

	// Setup an events channel to receive events
	eventsChan := make(chan model.Event, 500) // Buffer to prevent blocking

	// Execute the function under test
	err := processJobRunTask(
		context.Background(),
		mockWorkspace,
		run,
		task,
		lastRunMilli,
		tags,
		eventsChan,
	)

	// Close the channel to iterate through it
	close(eventsChan)

	// Verify result
	assert.NoError(t, err)

	// Collect all events for verification
	events := make([]model.Event, 0)
	for event := range eventsChan {
		events = append(events, event)
	}

	assert.Empty(
		t,
		events,
		"Expected no events to be produced when task is running and starts before the lastRun time",
	)
}

func TestProcessJobRunTask_RunStartedAfter(t *testing.T) {
	// Setup mock workspace and workspace info
	mockWorkspace, teardown := setupMockWorkspaceAndInfo()
	defer teardown()

	// Setup mock cluster info
	teardown2 := setupMockClusterInfo()
	defer teardown2()

	// Setup mock tag map
	tags := map[string]string{
		"env": "production",
		"team": "data-engineering",
	}

	// Mock Now
	now := time.Now()
	Now = func () time.Time { return now }

	// Setup times to be used to test behavior of event production based on
	// lastRun time.
	lastRun := now
	lastRunMilli := lastRun.UnixMilli()
	// Job run started before
	jobRunStartTime := lastRun.Add(-10 * time.Second)
	// Task run started after
	taskRunStartTime := lastRun.Add(10 * time.Second)

	// Setup a mock run object
	run := &databricksSdkJobs.BaseRun{
		JobId:                  0,
		RunId:                  123,
		OriginalAttemptRunId:   123,
		RunType:                databricksSdkJobs.RunTypeJobRun,
		RunName:                "jobRun1",
		Description:            "This is run 1 description",
		AttemptNumber:          0,
		StartTime:			    jobRunStartTime.UnixMilli(),
		Trigger:			   	databricksSdkJobs.TriggerTypeOneTime,
		Status: &databricksSdkJobs.RunStatus{
			State:        databricksSdkJobs.RunLifecycleStateV2StateRunning,
		},
	}

	// Setup a mock task object
	task := &databricksSdkJobs.RunTask{
		TaskKey:       "task1",
		RunId:         456,
		AttemptNumber: 0,
		Description:   "This is task run 1 description",
		// started after to ensure we go through the start event production
		// logic
		StartTime:     taskRunStartTime.UnixMilli(),
		ClusterInstance: &databricksSdkJobs.ClusterInstance{
			ClusterId:      "fake-cluster-id",
			SparkContextId: "fake-cluster-spark-context-id",
		},
		Status: &databricksSdkJobs.RunStatus{
			// running to ensure we do not go through the complete event
			// production logic
			State: databricksSdkJobs.RunLifecycleStateV2StateRunning,
		},
	}

	// Setup an events channel to receive events
	eventsChan := make(chan model.Event, 500) // Buffer to prevent blocking

	// Execute the function under test
	err := processJobRunTask(
		context.Background(),
		mockWorkspace,
		run,
		task,
		lastRunMilli,
		tags,
		eventsChan,
	)

	// Close the channel to iterate through it
	close(eventsChan)

	// Verify result
	assert.NoError(t, err)

	// Collect all events for verification
	events := make([]model.Event, 0)
	for event := range eventsChan {
		events = append(events, event)
	}

	assert.NotEmpty(
		t,
		events,
		"Expected events to be produced when task starts after the lastRun time",
	)
	assert.Equal(
		t,
		1,
		len(events),
		"Expected exactly one event to be produced when task is running and starts after the lastRun time",
	)

	// Verify the event has the expected attributes
	event := events[0]

	// Verify timestamp
	assert.Equal(
		t,
		now.UnixMilli(),
		event.Timestamp,
		"Should have the correct event timestamp",
	)

	// Verify event type
	assert.Equal(t, "DatabricksTaskRun", event.Type)

	// Verify attributes
	assert.NotNil(t, event.Attributes)

	// Verify common attributes
	attrs := event.Attributes
	assert.NotEmpty(t, attrs)

	// Verify common attributes
	assert.Contains(t, attrs, "env")
	assert.Equal(t, "production", attrs["env"])
	assert.Contains(t, attrs, "team")
	assert.Equal(t, "data-engineering", attrs["team"])

	// Verify workspace attributes
	assert.Contains(t, attrs, "databricksWorkspaceId")
	assert.Equal(t, mockWorkspaceId, attrs["databricksWorkspaceId"])
	assert.Contains(t, attrs, "databricksWorkspaceName")
	assert.Equal(
		t,
		mockWorkspaceHost,
		attrs["databricksWorkspaceName"],
	)
	assert.Contains(t, attrs, "databricksWorkspaceUrl")
	assert.Equal(
		t,
		"https://" + mockWorkspaceHost,
		attrs["databricksWorkspaceUrl"],
	)

	// Verify cluster attributes
	assert.Contains(t, attrs, "databricksClusterId")
	assert.Equal(t, "fake-cluster-id", attrs["databricksClusterId"])
	assert.Contains(t, attrs, "databricksClusterName")
	assert.Equal(
		t,
		"fake-cluster-name",
		attrs["databricksClusterName"],
	)
	assert.Contains(t, attrs, "databricksClusterSource")
	assert.Equal(
		t,
		"fake-cluster-source",
		attrs["databricksClusterSource"],
	)
	assert.Contains(t, attrs, "databricksClusterInstancePoolId")
	assert.Equal(
		t,
		"fake-cluster-instance-pool-id",
		attrs["databricksClusterInstancePoolId"],
	)
	assert.Contains(t, attrs, "databricksClusterSparkContextId")
	assert.Equal(
		t,
		"fake-cluster-spark-context-id",
		attrs["databricksClusterSparkContextId"],
	)

	// Verify event
	assert.Contains(t, attrs, "event")
	assert.Equal(
		t,
		"start",
		attrs["event"],
	)

	// Verify the job attributes
	assert.Contains(t, attrs, "jobId")
	assert.Equal(t, int64(0), attrs["jobId"])
	assert.Contains(t, attrs, "jobRunId")
	assert.Equal(t, int64(123), attrs["jobRunId"])
	assert.Contains(t, attrs, "jobRunType")
	assert.Equal(
		t,
		string(databricksSdkJobs.RunTypeJobRun),
		attrs["jobRunType"],
	)
	assert.Contains(t, attrs, "jobRunName")
	assert.Equal(t, "jobRun1", attrs["jobRunName"])
	assert.Contains(t, attrs, "jobRunStartTime")
	assert.Equal(
		t,
		jobRunStartTime.UnixMilli(),
		attrs["jobRunStartTime"],
	)
	assert.Contains(t, attrs, "jobRunTrigger")
	assert.Equal(
		t,
		string(databricksSdkJobs.TriggerTypeOneTime),
		attrs["jobRunTrigger"],
	)
	assert.Contains(t, attrs, "taskRunId")
	assert.Equal(t, int64(456), attrs["taskRunId"])
	assert.Contains(t, attrs, "taskName")
	assert.Equal(t, "task1", attrs["taskName"])
	assert.Contains(t, attrs, "taskRunStartTime")
	assert.Equal(t, taskRunStartTime.UnixMilli(), attrs["taskRunStartTime"])
	assert.Contains(t, attrs, "description")
	assert.Equal(
		t,
		"This is task run 1 description",
		attrs["description"],
	)
	assert.Contains(t, attrs, "attempt")
	assert.Equal(t, 0, attrs["attempt"])
	assert.Contains(t, attrs, "isRetry")
	assert.Equal(t, false, attrs["isRetry"])
	assert.NotContains(t, attrs, "state")
	assert.NotContains(t, attrs, "terminationCode")
	assert.NotContains(t, attrs, "terminationType")
	assert.NotContains(t, attrs, "taskRunEndTime")
	//assert.NotContains(t, attrs, "jobRunEndTime")
	assert.NotContains(t, attrs, "duration")
	assert.NotContains(t, attrs, "queueDuration")
	assert.NotContains(t, attrs, "setupDuration")
	assert.NotContains(t, attrs, "executionDuration")
	assert.NotContains(t, attrs, "cleanupDuration")
}

func TestProcessJobRunTask_MakeJobRunTaskCompleteAttributesError(t *testing.T) {
	// Setup mock workspace
	mockWorkspace := setupMockWorkspace()

	// Setup expected error message
	expectedError := "error getting workspace info"

	// Save original GetWorkspaceInfo function and setup a defer function
	// to restore after the test
	originalGetWorkspaceInfo := GetWorkspaceInfo
	defer func() {
		GetWorkspaceInfo = originalGetWorkspaceInfo
	}()

	// Set GetWorkspaceInfo to return an error so that
	// makeJobRunTaskCompleteAttributes will return an error
	GetWorkspaceInfo = func(
		ctx context.Context,
		w DatabricksWorkspace,
	) (
		*WorkspaceInfo,
		error,
	) {
		return nil, errors.New(expectedError)
	}

	// Setup mock tag map
	tags := map[string]string{
		"env": "production",
		"team": "data-engineering",
	}

	// Mock Now
	now := time.Now()
	Now = func () time.Time { return now }

	// Setup times to be used to test behavior of event production based on
	// lastRun time.
	lastRun := now
	lastRunMilli := lastRun.UnixMilli()
	// Job run started before
	jobRunStartTime := lastRun.Add(-10 * time.Second)
	// Task run started before
	taskRunStartTime := lastRun.Add(-5 * time.Second)
	// Task run terminated after
	taskRunEndTime := lastRun.Add(5 * time.Second)

	// Setup a mock run object
	run := &databricksSdkJobs.BaseRun{
		JobId:                  0,
		RunId:                  123,
		OriginalAttemptRunId:   123,
		RunType:                databricksSdkJobs.RunTypeJobRun,
		RunName:                "jobRun1",
		Description:            "This is run 1 description",
		AttemptNumber:          0,
		StartTime:			    jobRunStartTime.UnixMilli(),
		Trigger:			   	databricksSdkJobs.TriggerTypeOneTime,
		Status: &databricksSdkJobs.RunStatus{
			State:        databricksSdkJobs.RunLifecycleStateV2StateRunning,
		},
	}

	// Setup a mock task object
	task := &databricksSdkJobs.RunTask{
		TaskKey:           "task1",
		RunId:             456,
		AttemptNumber:     0,
		Description:       "This is task run 1 description",
		// started before to ensure we don't go through the start event
		// production logic
		StartTime:         taskRunStartTime.UnixMilli(),
		// terminated after + terminated status to ensure we go through the
		// complete event production logic
		EndTime:           taskRunEndTime.UnixMilli(),
		QueueDuration:     2000,
		SetupDuration:     300,
		ExecutionDuration: 3000,
		CleanupDuration:   100,
		ClusterInstance: &databricksSdkJobs.ClusterInstance{
			ClusterId:      "fake-cluster-id",
			SparkContextId: "fake-cluster-spark-context-id",
		},
		Status: &databricksSdkJobs.RunStatus{
			// terminated status + terminated after to ensure we go through the
			// complete event production logic
			State: databricksSdkJobs.RunLifecycleStateV2StateTerminated,
			TerminationDetails: &databricksSdkJobs.TerminationDetails{
				Code: databricksSdkJobs.TerminationCodeCodeSuccess,
				Type: databricksSdkJobs.TerminationTypeTypeSuccess,
			},
		},
	}

	// Setup an events channel to receive events
	eventsChan := make(chan model.Event, 500) // Buffer to prevent blocking

	// Execute the function under test
	err := processJobRunTask(
		context.Background(),
		mockWorkspace,
		run,
		task,
		lastRunMilli,
		tags,
		eventsChan,
	)

	// Close the channel
	close(eventsChan)

	// Verify result
	assert.Error(t, err)
	assert.Equal(t, expectedError, err.Error())
}

func TestProcessJobRunTask_RunTerminatedBefore(t *testing.T) {
	// Setup mock workspace
	mockWorkspace := setupMockWorkspace()

	// Setup mock tag map
	tags := map[string]string{
		"env": "production",
		"team": "data-engineering",
	}

	// Mock Now
	now := time.Now()
	Now = func () time.Time { return now }

	// Setup times to be used to test behavior of event production based on
	// lastRun time.
	lastRun := now
	lastRunMilli := lastRun.UnixMilli()
	// Job run started before
	jobRunStartTime := lastRun.Add(-10 * time.Second)
	// Task run started before
	taskRunStartTime := lastRun.Add(-10 * time.Second)
	// Task run ended before
	taskRunEndTime := lastRun.Add(-5 * time.Second)

	// Setup a mock run object
	run := &databricksSdkJobs.BaseRun{
		JobId:                  0,
		RunId:                  123,
		OriginalAttemptRunId:   123,
		RunType:                databricksSdkJobs.RunTypeJobRun,
		RunName:                "jobRun1",
		Description:            "This is run 1 description",
		AttemptNumber:          0,
		StartTime:			    jobRunStartTime.UnixMilli(),
		Trigger:			   	databricksSdkJobs.TriggerTypeOneTime,
		Status: &databricksSdkJobs.RunStatus{
			State:        databricksSdkJobs.RunLifecycleStateV2StateRunning,
		},
	}

	// Setup a mock task object
	task := &databricksSdkJobs.RunTask{
		TaskKey:           "task1",
		RunId:             456,
		AttemptNumber:     0,
		Description:       "This is task run 1 description",
		// started before to ensure we don't go through the start event
		// production logic
		StartTime:         taskRunStartTime.UnixMilli(),
		// terminated before + terminated status to ensure we don't go through
		// the complete event production logic
		EndTime:           taskRunEndTime.UnixMilli(),
		QueueDuration:     2000,
		SetupDuration:     300,
		ExecutionDuration: 3000,
		CleanupDuration:   100,
		ClusterInstance: &databricksSdkJobs.ClusterInstance{
			ClusterId:      "fake-cluster-id",
			SparkContextId: "fake-cluster-spark-context-id",
		},
		Status: &databricksSdkJobs.RunStatus{
			// terminated status + terminated before to ensure we don't go
			// through the complete event production logic
			State: databricksSdkJobs.RunLifecycleStateV2StateTerminated,
			TerminationDetails: &databricksSdkJobs.TerminationDetails{
				Code: databricksSdkJobs.TerminationCodeCodeSuccess,
				Type: databricksSdkJobs.TerminationTypeTypeSuccess,
			},
		},
	}

	// Setup an events channel to receive events
	eventsChan := make(chan model.Event, 500) // Buffer to prevent blocking

	// Execute the function under test
	err := processJobRunTask(
		context.Background(),
		mockWorkspace,
		run,
		task,
		lastRunMilli,
		tags,
		eventsChan,
	)

	// Close the channel to iterate through it
	close(eventsChan)

	// Verify result
	assert.NoError(t, err)

	// Collect all events for verification
	events := make([]model.Event, 0)
	for event := range eventsChan {
		events = append(events, event)
	}

	assert.Empty(
		t,
		events,
		"Expected no events to be produced when task terminates before the lastRun time",
	)
}

func TestProcessJobRunTask_RunTerminatedAfter(t *testing.T) {
	// Setup mock workspace and workspace info
	mockWorkspace, teardown := setupMockWorkspaceAndInfo()
	defer teardown()

	// Setup mock cluster info
	teardown2 := setupMockClusterInfo()
	defer teardown2()

	// Setup mock tag map
	tags := map[string]string{
		"env": "production",
		"team": "data-engineering",
	}

	// Mock Now
	now := time.Now()
	Now = func () time.Time { return now }

	// Setup times to be used to test behavior of event production based on
	// lastRun time.
	lastRun := now
	lastRunMilli := lastRun.UnixMilli()
	// Job run started before
	jobRunStartTime := lastRun.Add(-10 * time.Second)
	// Task run started before
	taskRunStartTime := lastRun.Add(-5 * time.Second)
	// Task run terminated after
	taskRunEndTime := lastRun.Add(5 * time.Second)

	// Setup a mock run object
	run := &databricksSdkJobs.BaseRun{
		JobId:                  0,
		RunId:                  123,
		OriginalAttemptRunId:   123,
		RunType:                databricksSdkJobs.RunTypeJobRun,
		RunName:                "jobRun1",
		Description:            "This is run 1 description",
		AttemptNumber:          0,
		StartTime:			    jobRunStartTime.UnixMilli(),
		Trigger:			   	databricksSdkJobs.TriggerTypeOneTime,
		Status: &databricksSdkJobs.RunStatus{
			State:        databricksSdkJobs.RunLifecycleStateV2StateRunning,
		},
	}

	// Setup a mock task object
	task := &databricksSdkJobs.RunTask{
		TaskKey:           "task1",
		RunId:             456,
		AttemptNumber:     0,
		Description:       "This is task run 1 description",
		// started before to ensure we don't go through the start event
		// production logic
		StartTime:         taskRunStartTime.UnixMilli(),
		// terminated after + terminated status to ensure we go through the
		// complete event production logic
		EndTime:           taskRunEndTime.UnixMilli(),
		QueueDuration:     2000,
		SetupDuration:     300,
		ExecutionDuration: 3000,
		CleanupDuration:   100,
		ClusterInstance: &databricksSdkJobs.ClusterInstance{
			ClusterId:      "fake-cluster-id",
			SparkContextId: "fake-cluster-spark-context-id",
		},
		Status: &databricksSdkJobs.RunStatus{
			// terminated status + terminated after to ensure we go through the
			// complete event production logic
			State: databricksSdkJobs.RunLifecycleStateV2StateTerminated,
			TerminationDetails: &databricksSdkJobs.TerminationDetails{
				Code: databricksSdkJobs.TerminationCodeCodeSuccess,
				Type: databricksSdkJobs.TerminationTypeTypeSuccess,
			},
		},
	}

	// Setup an events channel to receive events
	eventsChan := make(chan model.Event, 500) // Buffer to prevent blocking

	// Execute the function under test
	err := processJobRunTask(
		context.Background(),
		mockWorkspace,
		run,
		task,
		lastRunMilli,
		tags,
		eventsChan,
	)

	// Close the channel to iterate through it
	close(eventsChan)

	// Verify result
	assert.NoError(t, err)

	// Collect all events for verification
	events := make([]model.Event, 0)
	for event := range eventsChan {
		events = append(events, event)
	}

	assert.NotEmpty(
		t,
		events,
		"Expected events to be produced when task ends after the lastRun time",
	)
	assert.Equal(
		t,
		1,
		len(events),
		"Expected exactly one event to be produced when task ends after the lastRun time",
	)

	// Verify the event has the expected attributes
	event := events[0]

	// Verify timestamp
	assert.Equal(
		t,
		now.UnixMilli(),
		event.Timestamp, "Should have the correct event timestamp",
	)

	// Verify event type
	assert.Equal(t, "DatabricksTaskRun", event.Type)

	// Verify attributes
	assert.NotNil(t, event.Attributes)

	// Verify common attributes
	attrs := event.Attributes
	assert.NotEmpty(t, attrs)

	// Verify common attributes
	assert.Contains(t, attrs, "env")
	assert.Equal(t, "production", attrs["env"])
	assert.Contains(t, attrs, "team")
	assert.Equal(t, "data-engineering", attrs["team"])

	// Verify workspace attributes
	assert.Contains(t, attrs, "databricksWorkspaceId")
	assert.Equal(t, mockWorkspaceId, attrs["databricksWorkspaceId"])
	assert.Contains(t, attrs, "databricksWorkspaceName")
	assert.Equal(
		t,
		mockWorkspaceHost,
		attrs["databricksWorkspaceName"],
	)
	assert.Contains(t, attrs, "databricksWorkspaceUrl")
	assert.Equal(
		t,
		"https://" + mockWorkspaceHost,
		attrs["databricksWorkspaceUrl"],
	)

	// Verify cluster attributes
	assert.Contains(t, attrs, "databricksClusterId")
	assert.Equal(t, "fake-cluster-id", attrs["databricksClusterId"])
	assert.Contains(t, attrs, "databricksClusterName")
	assert.Equal(
		t,
		"fake-cluster-name",
		attrs["databricksClusterName"],
	)
	assert.Contains(t, attrs, "databricksClusterSource")
	assert.Equal(
		t,
		"fake-cluster-source",
		attrs["databricksClusterSource"],
	)
	assert.Contains(t, attrs, "databricksClusterInstancePoolId")
	assert.Equal(
		t,
		"fake-cluster-instance-pool-id",
		attrs["databricksClusterInstancePoolId"],
	)
	assert.Contains(t, attrs, "databricksClusterSparkContextId")
	assert.Equal(
		t,
		"fake-cluster-spark-context-id",
		attrs["databricksClusterSparkContextId"],
	)

	// Verify event
	assert.Contains(t, attrs, "event")
	assert.Equal(
		t,
		"complete",
		attrs["event"],
	)

	// Verify the job attributes
	assert.Contains(t, attrs, "jobId")
	assert.Equal(t, int64(0), attrs["jobId"])
	assert.Contains(t, attrs, "jobRunId")
	assert.Equal(t, int64(123), attrs["jobRunId"])
	assert.Contains(t, attrs, "jobRunType")
	assert.Equal(
		t,
		string(databricksSdkJobs.RunTypeJobRun),
		attrs["jobRunType"],
	)
	assert.Contains(t, attrs, "jobRunName")
	assert.Equal(t, "jobRun1", attrs["jobRunName"])
	assert.Contains(t, attrs, "jobRunStartTime")
	assert.Equal(
		t,
		jobRunStartTime.UnixMilli(),
		attrs["jobRunStartTime"],
	)
	assert.Contains(t, attrs, "jobRunTrigger")
	assert.Equal(
		t,
		string(databricksSdkJobs.TriggerTypeOneTime),
		attrs["jobRunTrigger"],
	)
	assert.Contains(t, attrs, "taskRunId")
	assert.Equal(t, int64(456), attrs["taskRunId"])
	assert.Contains(t, attrs, "taskName")
	assert.Equal(t, "task1", attrs["taskName"])
	assert.Contains(t, attrs, "taskRunStartTime")
	assert.Equal(t, taskRunStartTime.UnixMilli(), attrs["taskRunStartTime"])
	assert.Contains(t, attrs, "description")
	assert.Equal(
		t,
		"This is task run 1 description",
		attrs["description"],
	)
	assert.Contains(t, attrs, "attempt")
	assert.Equal(t, 0, attrs["attempt"])
	assert.Contains(t, attrs, "isRetry")
	assert.Equal(t, false, attrs["isRetry"])
	assert.Contains(t, attrs, "state")
	assert.Equal(
		t,
		string(databricksSdkJobs.RunLifecycleStateV2StateTerminated),
		attrs["state"],
	)
	assert.Contains(t, attrs, "terminationCode")
	assert.Equal(
		t,
		string(databricksSdkJobs.TerminationCodeCodeSuccess),
		attrs["terminationCode"],
	)
	assert.Contains(t, attrs, "terminationType")
	assert.Equal(
		t,
		string(databricksSdkJobs.TerminationTypeTypeSuccess),
		attrs["terminationType"],
	)
	assert.Contains(t, attrs, "taskRunEndTime")
	assert.Equal(t, taskRunEndTime.UnixMilli(), attrs["taskRunEndTime"])
	assert.Contains(t, attrs, "duration")
	assert.Equal(
		t,
		int64(3400),
		attrs["duration"],
	)
	assert.Contains(t, attrs, "queueDuration")
	assert.Equal(
		t,
		int64(2000),
		attrs["queueDuration"],
	)
	assert.Contains(t, attrs, "setupDuration")
	assert.Equal(
		t,
		int64(300),
		attrs["setupDuration"],
	)
	assert.Contains(t, attrs, "executionDuration")
	assert.Equal(
		t,
		int64(3000),
		attrs["executionDuration"],
	)
	assert.Contains(t, attrs, "cleanupDuration")
	assert.Equal(
		t,
		int64(100),
		attrs["cleanupDuration"],
	)
}

func TestMakeJobRunTaskBaseAttributes_MakeBaseAttributesError(t *testing.T) {
	// Setup mock workspace
	mockWorkspace := setupMockWorkspace()

	// Setup expected error message
	expectedError := "error getting workspace info"

	// Save original GetWorkspaceInfo function and setup a defer function
	// to restore after the test
	originalGetWorkspaceInfo := GetWorkspaceInfo
	defer func() {
		GetWorkspaceInfo = originalGetWorkspaceInfo
	}()

	// Set GetWorkspaceInfo to return an error so that makeBaseAttributes
	// will return an error
	GetWorkspaceInfo = func(
		ctx context.Context,
		w DatabricksWorkspace,
	) (
		*WorkspaceInfo,
		error,
	) {
		return nil, errors.New(expectedError)
	}

	// Setup mock tag map
	tags := map[string]string{
		"env": "production",
		"team": "data-engineering",
	}

	// Setup start time
	jobRunStartTime := time.Now()
	taskRunStartTime := jobRunStartTime.Add(5 * time.Second)

	// Setup a mock run object
	run := &databricksSdkJobs.BaseRun{
		JobId:                  0,
		RunId:                  123,
		OriginalAttemptRunId:   123,
		RunType:                databricksSdkJobs.RunTypeJobRun,
		RunName:                "jobRun1",
		Description:            "This is run 1 description",
		AttemptNumber:          0,
		StartTime:			    jobRunStartTime.UnixMilli(),
		Trigger:			   	databricksSdkJobs.TriggerTypeOneTime,
		Status: &databricksSdkJobs.RunStatus{
			State:        databricksSdkJobs.RunLifecycleStateV2StateRunning,
		},
	}

	// Setup a mock task object
	task := &databricksSdkJobs.RunTask{
		TaskKey:       "task1",
		RunId:         456,
		AttemptNumber: 0,
		Description:   "This is task run 1 description",
		StartTime:     taskRunStartTime.UnixMilli(),
		ClusterInstance: &databricksSdkJobs.ClusterInstance{
			ClusterId:      "fake-cluster-id",
			SparkContextId: "fake-cluster-spark-context-id",
		},
		Status: &databricksSdkJobs.RunStatus{
			State: databricksSdkJobs.RunLifecycleStateV2StateRunning,
		},
	}

	// Execute the function under test
	jobRunTaskAttributes, err := makeJobRunTaskBaseAttributes(
		context.Background(),
		mockWorkspace,
		run,
		task,
		"start",
		tags,
	)

	// Verify the results
	assert.Error(t, err)
	assert.Nil(t, jobRunTaskAttributes)
	assert.Equal(t, expectedError, err.Error())
}

func TestMakeJobRunTaskBaseAttributes_JobClusterInstance(t *testing.T) {
	// Setup mock workspace and workspace info
	mockWorkspace, teardown := setupMockWorkspaceAndInfo()
	defer teardown()

	// Setup mock cluster info
	teardown2 := setupMockClusterInfo()
	defer teardown2()

	// Setup mock tag map
	tags := map[string]string{
		"env": "production",
		"team": "data-engineering",
	}

	// Setup start time
	jobRunStartTime := time.Now()
	taskRunStartTime := jobRunStartTime.Add(5 * time.Second)

	// Setup a mock run object
	run := &databricksSdkJobs.BaseRun{
		JobId:                  0,
		RunId:                  123,
		OriginalAttemptRunId:   123,
		RunType:                databricksSdkJobs.RunTypeJobRun,
		RunName:                "jobRun1",
		Description:            "This is run 1 description",
		AttemptNumber:          0,
		StartTime:			    jobRunStartTime.UnixMilli(),
		Trigger:			   	databricksSdkJobs.TriggerTypeOneTime,
		ClusterInstance: &databricksSdkJobs.ClusterInstance{
			ClusterId:      "fake-cluster-id",
			SparkContextId: "fake-cluster-spark-context-id",
		},
		Status: &databricksSdkJobs.RunStatus{
			State:        databricksSdkJobs.RunLifecycleStateV2StateRunning,
		},
	}

	// Setup a mock task object
	task := &databricksSdkJobs.RunTask{
		TaskKey:       "task1",
		RunId:         456,
		AttemptNumber: 0,
		Description:   "This is task run 1 description",
		StartTime:     taskRunStartTime.UnixMilli(),
		Status: &databricksSdkJobs.RunStatus{
			State: databricksSdkJobs.RunLifecycleStateV2StateRunning,
		},
	}

	// Execute the function under test
	jobRunTaskAttributes, err := makeJobRunTaskBaseAttributes(
		context.Background(),
		mockWorkspace,
		run,
		task,
		"start",
		tags,
	)

	// Verify result
	assert.NoError(t, err)

	// Verify attributes
	assert.NotNil(t, jobRunTaskAttributes)
	assert.NotEmpty(t, jobRunTaskAttributes)

	// Verify common attributes
	assert.Contains(t, jobRunTaskAttributes, "env")
	assert.Equal(t, "production", jobRunTaskAttributes["env"])
	assert.Contains(t, jobRunTaskAttributes, "team")
	assert.Equal(t, "data-engineering", jobRunTaskAttributes["team"])

	// Verify workspace attributes
	assert.Contains(t, jobRunTaskAttributes, "databricksWorkspaceId")
	assert.Equal(
		t,
		mockWorkspaceId,
		jobRunTaskAttributes["databricksWorkspaceId"],
	)
	assert.Contains(t, jobRunTaskAttributes, "databricksWorkspaceName")
	assert.Equal(
		t,
		mockWorkspaceHost,
		jobRunTaskAttributes["databricksWorkspaceName"],
	)
	assert.Contains(t, jobRunTaskAttributes, "databricksWorkspaceUrl")
	assert.Equal(
		t,
		"https://" + mockWorkspaceHost,
		jobRunTaskAttributes["databricksWorkspaceUrl"],
	)

	// Verify cluster attributes
	assert.Contains(t, jobRunTaskAttributes, "databricksClusterId")
	assert.Equal(
		t,
		"fake-cluster-id",
		jobRunTaskAttributes["databricksClusterId"],
	)
	assert.Contains(t, jobRunTaskAttributes, "databricksClusterName")
	assert.Equal(
		t,
		"fake-cluster-name",
		jobRunTaskAttributes["databricksClusterName"],
	)
	assert.Contains(t, jobRunTaskAttributes, "databricksClusterSource")
	assert.Equal(
		t,
		"fake-cluster-source",
		jobRunTaskAttributes["databricksClusterSource"],
	)
	assert.Contains(t, jobRunTaskAttributes, "databricksClusterInstancePoolId")
	assert.Equal(
		t,
		"fake-cluster-instance-pool-id",
		jobRunTaskAttributes["databricksClusterInstancePoolId"],
	)
	assert.Contains(t, jobRunTaskAttributes, "databricksClusterSparkContextId")
	assert.Equal(
		t,
		"fake-cluster-spark-context-id",
		jobRunTaskAttributes["databricksClusterSparkContextId"],
	)

	// Verify event
	assert.Contains(t, jobRunTaskAttributes, "event")
	assert.Equal(
		t,
		"start",
		jobRunTaskAttributes["event"],
	)

	// Verify the job attributes
	assert.Contains(t, jobRunTaskAttributes, "jobId")
	assert.Equal(t, int64(0), jobRunTaskAttributes["jobId"])
	assert.Contains(t, jobRunTaskAttributes, "jobRunId")
	assert.Equal(t, int64(123), jobRunTaskAttributes["jobRunId"])
	assert.Contains(t, jobRunTaskAttributes, "jobRunType")
	assert.Equal(
		t,
		string(databricksSdkJobs.RunTypeJobRun),
		jobRunTaskAttributes["jobRunType"],
	)
	assert.Contains(t, jobRunTaskAttributes, "jobRunName")
	assert.Equal(t, "jobRun1", jobRunTaskAttributes["jobRunName"])
	assert.Contains(t, jobRunTaskAttributes, "jobRunStartTime")
	assert.Equal(
		t,
		jobRunStartTime.UnixMilli(),
		jobRunTaskAttributes["jobRunStartTime"],
	)
	assert.Contains(t, jobRunTaskAttributes, "jobRunTrigger")
	assert.Equal(
		t,
		string(databricksSdkJobs.TriggerTypeOneTime),
		jobRunTaskAttributes["jobRunTrigger"],
	)
	assert.Contains(t, jobRunTaskAttributes, "taskRunId")
	assert.Equal(t, int64(456), jobRunTaskAttributes["taskRunId"])
	assert.Contains(t, jobRunTaskAttributes, "taskName")
	assert.Equal(t, "task1", jobRunTaskAttributes["taskName"])
	assert.Contains(t, jobRunTaskAttributes, "taskRunStartTime")
	assert.Equal(
		t,
		taskRunStartTime.UnixMilli(),
		jobRunTaskAttributes["taskRunStartTime"],
	)
	assert.Contains(t, jobRunTaskAttributes, "description")
	assert.Equal(
		t,
		"This is task run 1 description",
		jobRunTaskAttributes["description"],
	)
	assert.Contains(t, jobRunTaskAttributes, "attempt")
	assert.Equal(t, 0, jobRunTaskAttributes["attempt"])
	assert.Contains(t, jobRunTaskAttributes, "isRetry")
	assert.Equal(t, false, jobRunTaskAttributes["isRetry"])
	assert.NotContains(t, jobRunTaskAttributes, "state")
	assert.NotContains(t, jobRunTaskAttributes, "terminationCode")
	assert.NotContains(t, jobRunTaskAttributes, "terminationType")
	assert.NotContains(t, jobRunTaskAttributes, "taskRunEndTime")
	assert.NotContains(t, jobRunTaskAttributes, "duration")
	assert.NotContains(t, jobRunTaskAttributes, "queueDuration")
	assert.NotContains(t, jobRunTaskAttributes, "setupDuration")
	assert.NotContains(t, jobRunTaskAttributes, "executionDuration")
	assert.NotContains(t, jobRunTaskAttributes, "cleanupDuration")
}

func TestMakeJobRunTaskBaseAttributes_TaskClusterInstance(t *testing.T) {
	// Setup mock workspace and workspace info
	mockWorkspace, teardown := setupMockWorkspaceAndInfo()
	defer teardown()

	// Setup mock cluster info
	teardown2 := setupMockClusterInfo()
	defer teardown2()

	// Setup mock tag map
	tags := map[string]string{
		"env": "production",
		"team": "data-engineering",
	}

	// Setup start time
	jobRunStartTime := time.Now()
	taskRunStartTime := jobRunStartTime.Add(5 * time.Second)

	// Setup a mock run object
	run := &databricksSdkJobs.BaseRun{
		JobId:                  0,
		RunId:                  123,
		OriginalAttemptRunId:   123,
		RunType:                databricksSdkJobs.RunTypeJobRun,
		RunName:                "jobRun1",
		Description:            "This is run 1 description",
		AttemptNumber:          0,
		StartTime:			    jobRunStartTime.UnixMilli(),
		Trigger:			   	databricksSdkJobs.TriggerTypeOneTime,
		ClusterInstance: &databricksSdkJobs.ClusterInstance{
			ClusterId:      "fake-cluster-id",
			SparkContextId: "fake-cluster-spark-context-id",
		},
		Status: &databricksSdkJobs.RunStatus{
			State: databricksSdkJobs.RunLifecycleStateV2StateRunning,
		},
	}

	// Setup a mock task object
	task := &databricksSdkJobs.RunTask{
		TaskKey:       "task1",
		RunId:         456,
		AttemptNumber: 0,
		Description:   "This is task run 1 description",
		StartTime:     taskRunStartTime.UnixMilli(),
		ClusterInstance: &databricksSdkJobs.ClusterInstance{
			ClusterId:      "fake-cluster-id-2",
			SparkContextId: "fake-cluster-spark-context-id-2",
		},
		Status: &databricksSdkJobs.RunStatus{
			State: databricksSdkJobs.RunLifecycleStateV2StateRunning,
		},
	}

	// Execute the function under test
	jobRunTaskAttributes, err := makeJobRunTaskBaseAttributes(
		context.Background(),
		mockWorkspace,
		run,
		task,
		"start",
		tags,
	)

	// Verify result
	assert.NoError(t, err)

	// Verify attributes
	assert.NotNil(t, jobRunTaskAttributes)
	assert.NotEmpty(t, jobRunTaskAttributes)

	// Verify common attributes
	assert.Contains(t, jobRunTaskAttributes, "env")
	assert.Equal(t, "production", jobRunTaskAttributes["env"])
	assert.Contains(t, jobRunTaskAttributes, "team")
	assert.Equal(t, "data-engineering", jobRunTaskAttributes["team"])

	// Verify workspace attributes
	assert.Contains(t, jobRunTaskAttributes, "databricksWorkspaceId")
	assert.Equal(
		t,
		mockWorkspaceId,
		jobRunTaskAttributes["databricksWorkspaceId"],
	)
	assert.Contains(t, jobRunTaskAttributes, "databricksWorkspaceName")
	assert.Equal(
		t,
		mockWorkspaceHost,
		jobRunTaskAttributes["databricksWorkspaceName"],
	)
	assert.Contains(t, jobRunTaskAttributes, "databricksWorkspaceUrl")
	assert.Equal(
		t,
		"https://" + mockWorkspaceHost,
		jobRunTaskAttributes["databricksWorkspaceUrl"],
	)

	// Verify cluster attributes
	// Notice here that we look specifically for the cluster instance defined
	// in the task (cluster id and Spark context id with the -2 suffix) since it
	// should take precedence. The cluster name, source, and instance pool ID
	// don't have the suffix since they are populated by setupMockClusterInfo()
	// but it is sufficient to verify the -2 suffix on the cluster ID and Spark
	// context ID.
	assert.Contains(t, jobRunTaskAttributes, "databricksClusterId")
	assert.Equal(
		t,
		"fake-cluster-id-2",
		jobRunTaskAttributes["databricksClusterId"],
	)
	assert.Contains(t, jobRunTaskAttributes, "databricksClusterName")
	assert.Equal(
		t,
		"fake-cluster-name",
		jobRunTaskAttributes["databricksClusterName"],
	)
	assert.Contains(t, jobRunTaskAttributes, "databricksClusterSource")
	assert.Equal(
		t,
		"fake-cluster-source",
		jobRunTaskAttributes["databricksClusterSource"],
	)
	assert.Contains(t, jobRunTaskAttributes, "databricksClusterInstancePoolId")
	assert.Equal(
		t,
		"fake-cluster-instance-pool-id",
		jobRunTaskAttributes["databricksClusterInstancePoolId"],
	)
	assert.Contains(t, jobRunTaskAttributes, "databricksClusterSparkContextId")
	assert.Equal(
		t,
		"fake-cluster-spark-context-id-2",
		jobRunTaskAttributes["databricksClusterSparkContextId"],
	)

	// Verify event
	assert.Contains(t, jobRunTaskAttributes, "event")
	assert.Equal(
		t,
		"start",
		jobRunTaskAttributes["event"],
	)

	// Verify the job attributes
	assert.Contains(t, jobRunTaskAttributes, "jobId")
	assert.Equal(t, int64(0), jobRunTaskAttributes["jobId"])
	assert.Contains(t, jobRunTaskAttributes, "jobRunId")
	assert.Equal(t, int64(123), jobRunTaskAttributes["jobRunId"])
	assert.Contains(t, jobRunTaskAttributes, "jobRunType")
	assert.Equal(
		t,
		string(databricksSdkJobs.RunTypeJobRun),
		jobRunTaskAttributes["jobRunType"],
	)
	assert.Contains(t, jobRunTaskAttributes, "jobRunName")
	assert.Equal(t, "jobRun1", jobRunTaskAttributes["jobRunName"])
	assert.Contains(t, jobRunTaskAttributes, "jobRunStartTime")
	assert.Equal(
		t,
		jobRunStartTime.UnixMilli(),
		jobRunTaskAttributes["jobRunStartTime"],
	)
	assert.Contains(t, jobRunTaskAttributes, "jobRunTrigger")
	assert.Equal(
		t,
		string(databricksSdkJobs.TriggerTypeOneTime),
		jobRunTaskAttributes["jobRunTrigger"],
	)
	assert.Contains(t, jobRunTaskAttributes, "taskRunId")
	assert.Equal(t, int64(456), jobRunTaskAttributes["taskRunId"])
	assert.Contains(t, jobRunTaskAttributes, "taskName")
	assert.Equal(t, "task1", jobRunTaskAttributes["taskName"])
	assert.Contains(t, jobRunTaskAttributes, "taskRunStartTime")
	assert.Equal(
		t,
		taskRunStartTime.UnixMilli(),
		jobRunTaskAttributes["taskRunStartTime"],
	)
	assert.Contains(t, jobRunTaskAttributes, "description")
	assert.Equal(
		t,
		"This is task run 1 description",
		jobRunTaskAttributes["description"],
	)
	assert.Contains(t, jobRunTaskAttributes, "attempt")
	assert.Equal(t, 0, jobRunTaskAttributes["attempt"])
	assert.Contains(t, jobRunTaskAttributes, "isRetry")
	assert.Equal(t, false, jobRunTaskAttributes["isRetry"])
	assert.NotContains(t, jobRunTaskAttributes, "state")
	assert.NotContains(t, jobRunTaskAttributes, "terminationCode")
	assert.NotContains(t, jobRunTaskAttributes, "terminationType")
	assert.NotContains(t, jobRunTaskAttributes, "taskRunEndTime")
	assert.NotContains(t, jobRunTaskAttributes, "duration")
	assert.NotContains(t, jobRunTaskAttributes, "queueDuration")
	assert.NotContains(t, jobRunTaskAttributes, "setupDuration")
	assert.NotContains(t, jobRunTaskAttributes, "executionDuration")
	assert.NotContains(t, jobRunTaskAttributes, "cleanupDuration")
}

func TestMakeJobRunTaskBaseAttributes_NoClusterInstance(t *testing.T) {
	// Setup mock workspace and workspace info
	mockWorkspace, teardown := setupMockWorkspaceAndInfo()
	defer teardown()

	// Setup mock cluster info
	teardown2 := setupMockClusterInfo()
	defer teardown2()

	// Setup mock tag map
	tags := map[string]string{
		"env": "production",
		"team": "data-engineering",
	}

	// Setup start time
	jobRunStartTime := time.Now()
	taskRunStartTime := jobRunStartTime.Add(5 * time.Second)

	// Setup a mock run object
	run := &databricksSdkJobs.BaseRun{
		JobId:                  0,
		RunId:                  123,
		OriginalAttemptRunId:   123,
		RunType:                databricksSdkJobs.RunTypeJobRun,
		RunName:                "jobRun1",
		Description:            "This is run 1 description",
		AttemptNumber:          0,
		StartTime:			    jobRunStartTime.UnixMilli(),
		Trigger:			   	databricksSdkJobs.TriggerTypeOneTime,
		Status: &databricksSdkJobs.RunStatus{
			State:        databricksSdkJobs.RunLifecycleStateV2StateRunning,
		},
	}

	// Setup a mock task object
	task := &databricksSdkJobs.RunTask{
		TaskKey:       "task1",
		RunId:         456,
		AttemptNumber: 0,
		Description:  "This is task run 1 description",
		StartTime:     taskRunStartTime.UnixMilli(),
		Status: &databricksSdkJobs.RunStatus{
			State: databricksSdkJobs.RunLifecycleStateV2StateRunning,
		},
	}

	// Execute the function under test
	jobRunTaskAttributes, err := makeJobRunTaskBaseAttributes(
		context.Background(),
		mockWorkspace,
		run,
		task,
		"start",
		tags,
	)

	// Verify result
	assert.NoError(t, err)

	// Verify attributes
	assert.NotNil(t, jobRunTaskAttributes)
	assert.NotEmpty(t, jobRunTaskAttributes)

	// Verify common attributes
	assert.Contains(t, jobRunTaskAttributes, "env")
	assert.Equal(t, "production", jobRunTaskAttributes["env"])
	assert.Contains(t, jobRunTaskAttributes, "team")
	assert.Equal(t, "data-engineering", jobRunTaskAttributes["team"])

	// Verify workspace attributes
	assert.Contains(t, jobRunTaskAttributes, "databricksWorkspaceId")
	assert.Equal(
		t,
		mockWorkspaceId,
		jobRunTaskAttributes["databricksWorkspaceId"],
	)
	assert.Contains(t, jobRunTaskAttributes, "databricksWorkspaceName")
	assert.Equal(
		t,
		mockWorkspaceHost,
		jobRunTaskAttributes["databricksWorkspaceName"],
	)
	assert.Contains(t, jobRunTaskAttributes, "databricksWorkspaceUrl")
	assert.Equal(
		t,
		"https://" + mockWorkspaceHost,
		jobRunTaskAttributes["databricksWorkspaceUrl"],
	)

	// Verify no cluster attributes
	assert.NotContains(t, jobRunTaskAttributes, "databricksClusterId")
	assert.NotContains(t, jobRunTaskAttributes, "databricksClusterName")
	assert.NotContains(t, jobRunTaskAttributes, "databricksClusterSource")
	assert.NotContains(
		t,
		jobRunTaskAttributes,
		"databricksClusterInstancePoolId",
	)
	assert.NotContains(
		t,
		jobRunTaskAttributes,
		"databricksClusterSparkContextId",
	)

	// Verify event
	assert.Contains(t, jobRunTaskAttributes, "event")
	assert.Equal(
		t,
		"start",
		jobRunTaskAttributes["event"],
	)

	// Verify the job attributes
	assert.Contains(t, jobRunTaskAttributes, "jobId")
	assert.Equal(t, int64(0), jobRunTaskAttributes["jobId"])
	assert.Contains(t, jobRunTaskAttributes, "jobRunId")
	assert.Equal(t, int64(123), jobRunTaskAttributes["jobRunId"])
	assert.Contains(t, jobRunTaskAttributes, "jobRunType")
	assert.Equal(
		t,
		string(databricksSdkJobs.RunTypeJobRun),
		jobRunTaskAttributes["jobRunType"],
	)
	assert.Contains(t, jobRunTaskAttributes, "jobRunName")
	assert.Equal(t, "jobRun1", jobRunTaskAttributes["jobRunName"])
	assert.Contains(t, jobRunTaskAttributes, "jobRunStartTime")
	assert.Equal(
		t,
		jobRunStartTime.UnixMilli(),
		jobRunTaskAttributes["jobRunStartTime"],
	)
	assert.Contains(t, jobRunTaskAttributes, "jobRunTrigger")
	assert.Equal(
		t,
		string(databricksSdkJobs.TriggerTypeOneTime),
		jobRunTaskAttributes["jobRunTrigger"],
	)
	assert.Contains(t, jobRunTaskAttributes, "taskRunId")
	assert.Equal(t, int64(456), jobRunTaskAttributes["taskRunId"])
	assert.Contains(t, jobRunTaskAttributes, "taskName")
	assert.Equal(t, "task1", jobRunTaskAttributes["taskName"])
	assert.Contains(t, jobRunTaskAttributes, "taskRunStartTime")
	assert.Equal(
		t,
		taskRunStartTime.UnixMilli(),
		jobRunTaskAttributes["taskRunStartTime"],
	)
	assert.Contains(t, jobRunTaskAttributes, "description")
	assert.Equal(
		t,
		"This is task run 1 description",
		jobRunTaskAttributes["description"],
	)
	assert.Contains(t, jobRunTaskAttributes, "attempt")
	assert.Equal(t, 0, jobRunTaskAttributes["attempt"])
	assert.Contains(t, jobRunTaskAttributes, "isRetry")
	assert.Equal(t, false, jobRunTaskAttributes["isRetry"])
	assert.NotContains(t, jobRunTaskAttributes, "state")
	assert.NotContains(t, jobRunTaskAttributes, "terminationCode")
	assert.NotContains(t, jobRunTaskAttributes, "terminationType")
	assert.NotContains(t, jobRunTaskAttributes, "taskRunEndTime")
	assert.NotContains(t, jobRunTaskAttributes, "duration")
	assert.NotContains(t, jobRunTaskAttributes, "queueDuration")
	assert.NotContains(t, jobRunTaskAttributes, "setupDuration")
	assert.NotContains(t, jobRunTaskAttributes, "executionDuration")
	assert.NotContains(t, jobRunTaskAttributes, "cleanupDuration")
}

func TestMakeJobRunTaskStartAttributes_MakeJobRunTaskBaseAttributesError(t *testing.T) {
	// Setup mock workspace
	mockWorkspace := setupMockWorkspace()

	// Setup expected error message
	expectedError := "error getting workspace info"

	// Save original GetWorkspaceInfo function and setup a defer function
	// to restore after the test
	originalGetWorkspaceInfo := GetWorkspaceInfo
	defer func() {
		GetWorkspaceInfo = originalGetWorkspaceInfo
	}()

	// Set GetWorkspaceInfo to return an error so that
	// makeJobRunTaskBaseAttributes will return an error
	GetWorkspaceInfo = func(
		ctx context.Context,
		w DatabricksWorkspace,
	) (
		*WorkspaceInfo,
		error,
	) {
		return nil, errors.New(expectedError)
	}

	// Setup mock tag map
	tags := map[string]string{
		"env": "production",
		"team": "data-engineering",
	}

	// Setup start time
	jobRunStartTime := time.Now()
	taskRunStartTime := jobRunStartTime.Add(5 * time.Second)

	// Setup a mock run object
	run := &databricksSdkJobs.BaseRun{
		JobId:                  0,
		RunId:                  123,
		OriginalAttemptRunId:   123,
		RunType:                databricksSdkJobs.RunTypeJobRun,
		RunName:                "jobRun1",
		Description:            "This is run 1 description",
		AttemptNumber:          0,
		StartTime:			    jobRunStartTime.UnixMilli(),
		Trigger:			   	databricksSdkJobs.TriggerTypeOneTime,
		Status: &databricksSdkJobs.RunStatus{
			State:        databricksSdkJobs.RunLifecycleStateV2StateRunning,
		},
	}

	// Setup a mock task object
	task := &databricksSdkJobs.RunTask{
		TaskKey:       "task1",
		RunId:         456,
		AttemptNumber: 0,
		Description:   "This is task run 1 description",
		StartTime:     taskRunStartTime.UnixMilli(),
		ClusterInstance: &databricksSdkJobs.ClusterInstance{
			ClusterId:      "fake-cluster-id",
			SparkContextId: "fake-cluster-spark-context-id",
		},
		Status: &databricksSdkJobs.RunStatus{
			State: databricksSdkJobs.RunLifecycleStateV2StateRunning,
		},
	}

	// Execute the function under test
	jobRunTaskAttributes, err := makeJobRunTaskStartAttributes(
		context.Background(),
		mockWorkspace,
		run,
		task,
		tags,
	)

	// Verify the results
	assert.Error(t, err)
	assert.Nil(t, jobRunTaskAttributes)
	assert.Equal(t, expectedError, err.Error())
}

func TestMakeJobRunTaskStartAttributes(t *testing.T) {
	// Setup mock workspace and workspace info
	mockWorkspace, teardown := setupMockWorkspaceAndInfo()
	defer teardown()

	// Setup mock cluster info
	teardown2 := setupMockClusterInfo()
	defer teardown2()

	// Setup mock tag map
	tags := map[string]string{
		"env": "production",
		"team": "data-engineering",
	}

	// Setup start time
	jobRunStartTime := time.Now()
	taskRunStartTime := jobRunStartTime.Add(5 * time.Second)

	// Setup a mock run object
	run := &databricksSdkJobs.BaseRun{
		JobId:                  0,
		RunId:                  123,
		OriginalAttemptRunId:   123,
		RunType:                databricksSdkJobs.RunTypeJobRun,
		RunName:                "jobRun1",
		Description:            "This is run 1 description",
		AttemptNumber:          0,
		StartTime:			    jobRunStartTime.UnixMilli(),
		Trigger:			   	databricksSdkJobs.TriggerTypeOneTime,
		Status: &databricksSdkJobs.RunStatus{
			State:        databricksSdkJobs.RunLifecycleStateV2StateRunning,
		},
	}

	// Setup a mock task object
	task := &databricksSdkJobs.RunTask{
		TaskKey:       "task1",
		RunId:         456,
		AttemptNumber: 0,
		Description:   "This is task run 1 description",
		StartTime:     taskRunStartTime.UnixMilli(),
		ClusterInstance: &databricksSdkJobs.ClusterInstance{
			ClusterId:      "fake-cluster-id",
			SparkContextId: "fake-cluster-spark-context-id",
		},
		Status: &databricksSdkJobs.RunStatus{
			State: databricksSdkJobs.RunLifecycleStateV2StateRunning,
		},
	}

	// Execute the function under test
	jobRunTaskAttributes, err := makeJobRunTaskStartAttributes(
		context.Background(),
		mockWorkspace,
		run,
		task,
		tags,
	)

	// Verify result
	assert.NoError(t, err)

	// Verify attributes
	assert.NotNil(t, jobRunTaskAttributes)
	assert.NotEmpty(t, jobRunTaskAttributes)

	// Verify common attributes
	assert.Contains(t, jobRunTaskAttributes, "env")
	assert.Equal(t, "production", jobRunTaskAttributes["env"])
	assert.Contains(t, jobRunTaskAttributes, "team")
	assert.Equal(t, "data-engineering", jobRunTaskAttributes["team"])

	// Verify workspace attributes
	assert.Contains(t, jobRunTaskAttributes, "databricksWorkspaceId")
	assert.Equal(
		t,
		mockWorkspaceId,
		jobRunTaskAttributes["databricksWorkspaceId"],
	)
	assert.Contains(t, jobRunTaskAttributes, "databricksWorkspaceName")
	assert.Equal(
		t,
		mockWorkspaceHost,
		jobRunTaskAttributes["databricksWorkspaceName"],
	)
	assert.Contains(t, jobRunTaskAttributes, "databricksWorkspaceUrl")
	assert.Equal(
		t,
		"https://" + mockWorkspaceHost,
		jobRunTaskAttributes["databricksWorkspaceUrl"],
	)

	// Verify cluster attributes
	assert.Contains(t, jobRunTaskAttributes, "databricksClusterId")
	assert.Equal(
		t,
		"fake-cluster-id",
		jobRunTaskAttributes["databricksClusterId"],
	)
	assert.Contains(t, jobRunTaskAttributes, "databricksClusterName")
	assert.Equal(
		t,
		"fake-cluster-name",
		jobRunTaskAttributes["databricksClusterName"],
	)
	assert.Contains(t, jobRunTaskAttributes, "databricksClusterSource")
	assert.Equal(
		t,
		"fake-cluster-source",
		jobRunTaskAttributes["databricksClusterSource"],
	)
	assert.Contains(t, jobRunTaskAttributes, "databricksClusterInstancePoolId")
	assert.Equal(
		t,
		"fake-cluster-instance-pool-id",
		jobRunTaskAttributes["databricksClusterInstancePoolId"],
	)
	assert.Contains(t, jobRunTaskAttributes, "databricksClusterSparkContextId")
	assert.Equal(
		t,
		"fake-cluster-spark-context-id",
		jobRunTaskAttributes["databricksClusterSparkContextId"],
	)

	// Verify event
	assert.Contains(t, jobRunTaskAttributes, "event")
	assert.Equal(
		t,
		"start",
		jobRunTaskAttributes["event"],
	)

	// Verify the job attributes
	assert.Contains(t, jobRunTaskAttributes, "jobId")
	assert.Equal(t, int64(0), jobRunTaskAttributes["jobId"])
	assert.Contains(t, jobRunTaskAttributes, "jobRunId")
	assert.Equal(t, int64(123), jobRunTaskAttributes["jobRunId"])
	assert.Contains(t, jobRunTaskAttributes, "jobRunType")
	assert.Equal(
		t,
		string(databricksSdkJobs.RunTypeJobRun),
		jobRunTaskAttributes["jobRunType"],
	)
	assert.Contains(t, jobRunTaskAttributes, "jobRunName")
	assert.Equal(t, "jobRun1", jobRunTaskAttributes["jobRunName"])
	assert.Contains(t, jobRunTaskAttributes, "jobRunStartTime")
	assert.Equal(
		t,
		jobRunStartTime.UnixMilli(),
		jobRunTaskAttributes["jobRunStartTime"],
	)
	assert.Contains(t, jobRunTaskAttributes, "jobRunTrigger")
	assert.Equal(
		t,
		string(databricksSdkJobs.TriggerTypeOneTime),
		jobRunTaskAttributes["jobRunTrigger"],
	)
	assert.Contains(t, jobRunTaskAttributes, "taskRunId")
	assert.Equal(t, int64(456), jobRunTaskAttributes["taskRunId"])
	assert.Contains(t, jobRunTaskAttributes, "taskName")
	assert.Equal(t, "task1", jobRunTaskAttributes["taskName"])
	assert.Contains(t, jobRunTaskAttributes, "taskRunStartTime")
	assert.Equal(
		t,
		taskRunStartTime.UnixMilli(),
		jobRunTaskAttributes["taskRunStartTime"],
	)
	assert.Contains(t, jobRunTaskAttributes, "description")
	assert.Equal(
		t,
		"This is task run 1 description",
		jobRunTaskAttributes["description"],
	)
	assert.Contains(t, jobRunTaskAttributes, "attempt")
	assert.Equal(t, 0, jobRunTaskAttributes["attempt"])
	assert.Contains(t, jobRunTaskAttributes, "isRetry")
	assert.Equal(t, false, jobRunTaskAttributes["isRetry"])
	assert.NotContains(t, jobRunTaskAttributes, "state")
	assert.NotContains(t, jobRunTaskAttributes, "terminationCode")
	assert.NotContains(t, jobRunTaskAttributes, "terminationType")
	assert.NotContains(t, jobRunTaskAttributes, "taskRunEndTime")
	assert.NotContains(t, jobRunTaskAttributes, "duration")
	assert.NotContains(t, jobRunTaskAttributes, "queueDuration")
	assert.NotContains(t, jobRunTaskAttributes, "setupDuration")
	assert.NotContains(t, jobRunTaskAttributes, "executionDuration")
	assert.NotContains(t, jobRunTaskAttributes, "cleanupDuration")
}

func TestMakeJobRunTaskCompleteAttributes_MakeJobRunTaskBaseAttributesError(t *testing.T) {
	// Setup mock workspace
	mockWorkspace := setupMockWorkspace()

	// Setup expected error message
	expectedError := "error getting workspace info"

	// Save original GetWorkspaceInfo function and setup a defer function
	// to restore after the test
	originalGetWorkspaceInfo := GetWorkspaceInfo
	defer func() {
		GetWorkspaceInfo = originalGetWorkspaceInfo
	}()

	// Set GetWorkspaceInfo to return an error so that
	// makeJobRunTaskBaseAttributes will return an error.
	GetWorkspaceInfo = func(
		ctx context.Context,
		w DatabricksWorkspace,
	) (
		*WorkspaceInfo,
		error,
	) {
		return nil, errors.New(expectedError)
	}

	// Setup mock tag map
	tags := map[string]string{
		"env": "production",
		"team": "data-engineering",
	}

	// Setup start time
	jobRunStartTime := time.Now()
	taskRunStartTime := jobRunStartTime.Add(5 * time.Second)
	taskRunEndTime := taskRunStartTime.Add(10 * time.Second)

	// Setup a mock run object
	run := &databricksSdkJobs.BaseRun{
		JobId:                  0,
		RunId:                  123,
		OriginalAttemptRunId:   123,
		RunType:                databricksSdkJobs.RunTypeJobRun,
		RunName:                "jobRun1",
		Description:            "This is run 1 description",
		AttemptNumber:          0,
		StartTime:			    jobRunStartTime.UnixMilli(),
		Trigger:			   	databricksSdkJobs.TriggerTypeOneTime,
		Status: &databricksSdkJobs.RunStatus{
			State:        databricksSdkJobs.RunLifecycleStateV2StateRunning,
		},
	}

	// Setup a mock task object
	task := &databricksSdkJobs.RunTask{
		TaskKey:           "task1",
		RunId:             456,
		AttemptNumber:     0,
		Description:       "This is task run 1 description",
		StartTime:         taskRunStartTime.UnixMilli(),
		EndTime:           taskRunEndTime.UnixMilli(),
		QueueDuration:     2000,
		SetupDuration:     300,
		ExecutionDuration: 3000,
		CleanupDuration:   100,
		ClusterInstance: &databricksSdkJobs.ClusterInstance{
			ClusterId:      "fake-cluster-id",
			SparkContextId: "fake-cluster-spark-context-id",
		},
		Status: &databricksSdkJobs.RunStatus{
			State: databricksSdkJobs.RunLifecycleStateV2StateTerminated,
			TerminationDetails: &databricksSdkJobs.TerminationDetails{
				Code: databricksSdkJobs.TerminationCodeCodeSuccess,
				Type: databricksSdkJobs.TerminationTypeTypeSuccess,
			},
		},
	}

	// Execute the function under test
	taskAttributes, err := makeJobRunTaskCompleteAttributes(
		context.Background(),
		mockWorkspace,
		run,
		task,
		tags,
	)

	// Verify the results
	assert.Error(t, err)
	assert.Nil(t, taskAttributes)
	assert.Equal(t, expectedError, err.Error())
}

func TestMakeJobRunTaskCompleteAttributes(t *testing.T) {
	// Setup mock workspace and workspace info
	mockWorkspace, teardown := setupMockWorkspaceAndInfo()
	defer teardown()

	// Setup mock cluster info
	teardown2 := setupMockClusterInfo()
	defer teardown2()

	// Setup mock tag map
	tags := map[string]string{
		"env": "production",
		"team": "data-engineering",
	}

	// Setup start time
	jobRunStartTime := time.Now()
	taskRunStartTime := jobRunStartTime.Add(5 * time.Second)
	taskRunEndTime := taskRunStartTime.Add(10 * time.Second)

	// Setup a mock run object
	run := &databricksSdkJobs.BaseRun{
		JobId:                  0,
		RunId:                  123,
		OriginalAttemptRunId:   123,
		RunType:                databricksSdkJobs.RunTypeJobRun,
		RunName:                "jobRun1",
		Description:            "This is run 1 description",
		AttemptNumber:          0,
		StartTime:			    jobRunStartTime.UnixMilli(),
		Trigger:			   	databricksSdkJobs.TriggerTypeOneTime,
		Status: &databricksSdkJobs.RunStatus{
			State:        databricksSdkJobs.RunLifecycleStateV2StateRunning,
		},
	}

	// Setup a mock task object
	task := &databricksSdkJobs.RunTask{
		TaskKey:           "task1",
		RunId:             456,
		AttemptNumber:     0,
		Description:       "This is task run 1 description",
		StartTime:         taskRunStartTime.UnixMilli(),
		EndTime:           taskRunEndTime.UnixMilli(),
		QueueDuration:     2000,
		SetupDuration:     300,
		ExecutionDuration: 3000,
		CleanupDuration:   100,
		ClusterInstance: &databricksSdkJobs.ClusterInstance{
			ClusterId:      "fake-cluster-id",
			SparkContextId: "fake-cluster-spark-context-id",
		},
		Status: &databricksSdkJobs.RunStatus{
			State: databricksSdkJobs.RunLifecycleStateV2StateTerminated,
			TerminationDetails: &databricksSdkJobs.TerminationDetails{
				Code: databricksSdkJobs.TerminationCodeCodeSuccess,
				Type: databricksSdkJobs.TerminationTypeTypeSuccess,
			},
		},
	}

	// Execute the function under test
	jobRunTaskAttributes, err := makeJobRunTaskCompleteAttributes(
		context.Background(),
		mockWorkspace,
		run,
		task,
		tags,
	)

	// Verify result
	assert.NoError(t, err)

	// Verify attributes
	assert.NotNil(t, jobRunTaskAttributes)
	assert.NotEmpty(t, jobRunTaskAttributes)

	// Verify common attributes
	assert.Contains(t, jobRunTaskAttributes, "env")
	assert.Equal(t, "production", jobRunTaskAttributes["env"])
	assert.Contains(t, jobRunTaskAttributes, "team")
	assert.Equal(t, "data-engineering", jobRunTaskAttributes["team"])

	// Verify workspace attributes
	assert.Contains(t, jobRunTaskAttributes, "databricksWorkspaceId")
	assert.Equal(
		t,
		mockWorkspaceId,
		jobRunTaskAttributes["databricksWorkspaceId"],
	)
	assert.Contains(t, jobRunTaskAttributes, "databricksWorkspaceName")
	assert.Equal(
		t,
		mockWorkspaceHost,
		jobRunTaskAttributes["databricksWorkspaceName"],
	)
	assert.Contains(t, jobRunTaskAttributes, "databricksWorkspaceUrl")
	assert.Equal(
		t,
		"https://" + mockWorkspaceHost,
		jobRunTaskAttributes["databricksWorkspaceUrl"],
	)

	// Verify cluster attributes
	assert.Contains(t, jobRunTaskAttributes, "databricksClusterId")
	assert.Equal(
		t,
		"fake-cluster-id",
		jobRunTaskAttributes["databricksClusterId"],
	)
	assert.Contains(t, jobRunTaskAttributes, "databricksClusterName")
	assert.Equal(
		t,
		"fake-cluster-name",
		jobRunTaskAttributes["databricksClusterName"],
	)
	assert.Contains(t, jobRunTaskAttributes, "databricksClusterSource")
	assert.Equal(
		t,
		"fake-cluster-source",
		jobRunTaskAttributes["databricksClusterSource"],
	)
	assert.Contains(t, jobRunTaskAttributes, "databricksClusterInstancePoolId")
	assert.Equal(
		t,
		"fake-cluster-instance-pool-id",
		jobRunTaskAttributes["databricksClusterInstancePoolId"],
	)
	assert.Contains(t, jobRunTaskAttributes, "databricksClusterSparkContextId")
	assert.Equal(
		t,
		"fake-cluster-spark-context-id",
		jobRunTaskAttributes["databricksClusterSparkContextId"],
	)

	// Verify event
	assert.Contains(t, jobRunTaskAttributes, "event")
	assert.Equal(
		t,
		"complete",
		jobRunTaskAttributes["event"],
	)

	// Verify the job attributes
	assert.Contains(t, jobRunTaskAttributes, "jobId")
	assert.Equal(t, int64(0), jobRunTaskAttributes["jobId"])
	assert.Contains(t, jobRunTaskAttributes, "jobRunId")
	assert.Equal(t, int64(123), jobRunTaskAttributes["jobRunId"])
	assert.Contains(t, jobRunTaskAttributes, "jobRunType")
	assert.Equal(
		t,
		string(databricksSdkJobs.RunTypeJobRun),
		jobRunTaskAttributes["jobRunType"],
	)
	assert.Contains(t, jobRunTaskAttributes, "jobRunName")
	assert.Equal(t, "jobRun1", jobRunTaskAttributes["jobRunName"])
	assert.Contains(t, jobRunTaskAttributes, "jobRunStartTime")
	assert.Equal(
		t,
		jobRunStartTime.UnixMilli(),
		jobRunTaskAttributes["jobRunStartTime"],
	)
	assert.Contains(t, jobRunTaskAttributes, "jobRunTrigger")
	assert.Equal(
		t,
		string(databricksSdkJobs.TriggerTypeOneTime),
		jobRunTaskAttributes["jobRunTrigger"],
	)
	assert.Contains(t, jobRunTaskAttributes, "taskRunId")
	assert.Equal(t, int64(456), jobRunTaskAttributes["taskRunId"])
	assert.Contains(t, jobRunTaskAttributes, "taskName")
	assert.Equal(t, "task1", jobRunTaskAttributes["taskName"])
	assert.Contains(t, jobRunTaskAttributes, "taskRunStartTime")
	assert.Equal(
		t,
		taskRunStartTime.UnixMilli(),
		jobRunTaskAttributes["taskRunStartTime"],
	)
	assert.Contains(t, jobRunTaskAttributes, "description")
	assert.Equal(
		t,
		"This is task run 1 description",
		jobRunTaskAttributes["description"],
	)
	assert.Contains(t, jobRunTaskAttributes, "attempt")
	assert.Equal(t, 0, jobRunTaskAttributes["attempt"])
	assert.Contains(t, jobRunTaskAttributes, "isRetry")
	assert.Equal(t, false, jobRunTaskAttributes["isRetry"])
	assert.Contains(t, jobRunTaskAttributes, "state")
	assert.Equal(
		t,
		string(databricksSdkJobs.RunLifecycleStateV2StateTerminated),
		jobRunTaskAttributes["state"],
	)
	assert.Contains(t, jobRunTaskAttributes, "terminationCode")
	assert.Equal(
		t,
		string(databricksSdkJobs.TerminationCodeCodeSuccess),
		jobRunTaskAttributes["terminationCode"],
	)
	assert.Contains(t, jobRunTaskAttributes, "terminationType")
	assert.Equal(
		t,
		string(databricksSdkJobs.TerminationTypeTypeSuccess),
		jobRunTaskAttributes["terminationType"],
	)
	assert.Contains(t, jobRunTaskAttributes, "taskRunEndTime")
	assert.Equal(
		t,
		taskRunEndTime.UnixMilli(),
		jobRunTaskAttributes["taskRunEndTime"],
	)
	assert.Contains(t, jobRunTaskAttributes, "duration")
	assert.Equal(
		t,
		int64(3400), // 300 + 3000 + 100,
		jobRunTaskAttributes["duration"],
	)
	assert.Contains(t, jobRunTaskAttributes, "queueDuration")
	assert.Equal(
		t,
		int64(2000),
		jobRunTaskAttributes["queueDuration"],
	)
	assert.Contains(t, jobRunTaskAttributes, "setupDuration")
	assert.Equal(
		t,
		int64(300),
		jobRunTaskAttributes["setupDuration"],
	)
	assert.Contains(t, jobRunTaskAttributes, "executionDuration")
	assert.Equal(
		t,
		int64(3000),
		jobRunTaskAttributes["executionDuration"],
	)
	assert.Contains(t, jobRunTaskAttributes, "cleanupDuration")
	assert.Equal(
		t,
		int64(100),
		jobRunTaskAttributes["cleanupDuration"],
	)
}

func TestUpdateStateCounters(t *testing.T) {
	// Create a counters instance
	counters := counters{}

	// Assert preconditions
	assert.Equal(t, 0, counters.blocked)
	assert.Equal(t, 0, counters.waiting)
	assert.Equal(t, 0, counters.pending)
	assert.Equal(t, 0, counters.queued)
	assert.Equal(t, 0, counters.running)
	assert.Equal(t, 0, counters.terminating)

	// Execute the function under test
	updateStateCounters(
		databricksSdkJobs.RunLifecycleStateV2StateRunning,
		&counters,
	)

	// Verify the results
	assert.Equal(t, 0, counters.blocked)
	assert.Equal(t, 0, counters.waiting)
	assert.Equal(t, 0, counters.pending)
	assert.Equal(t, 0, counters.queued)
	assert.Equal(t, 1, counters.running)
	assert.Equal(t, 0, counters.terminating)

	// Execute the function under test
	updateStateCounters(
		databricksSdkJobs.RunLifecycleStateV2StateBlocked,
		&counters,
	)

	// Verify the results
	assert.Equal(t, 1, counters.blocked)
	assert.Equal(t, 0, counters.waiting)
	assert.Equal(t, 0, counters.pending)
	assert.Equal(t, 0, counters.queued)
	assert.Equal(t, 1, counters.running)
	assert.Equal(t, 0, counters.terminating)

	// Execute the function under test
	updateStateCounters(
		databricksSdkJobs.RunLifecycleStateV2StateTerminating,
		&counters,
	)

	// Verify the results
	assert.Equal(t, 1, counters.blocked)
	assert.Equal(t, 0, counters.waiting)
	assert.Equal(t, 0, counters.pending)
	assert.Equal(t, 0, counters.queued)
	assert.Equal(t, 1, counters.running)
	assert.Equal(t, 1, counters.terminating)

	// Execute the function under test
	updateStateCounters(
		databricksSdkJobs.RunLifecycleStateV2StatePending,
		&counters,
	)

	// Verify the results
	assert.Equal(t, 1, counters.blocked)
	assert.Equal(t, 0, counters.waiting)
	assert.Equal(t, 1, counters.pending)
	assert.Equal(t, 0, counters.queued)
	assert.Equal(t, 1, counters.running)
	assert.Equal(t, 1, counters.terminating)

	// Execute the function under test
	updateStateCounters(
		databricksSdkJobs.RunLifecycleStateV2StateQueued,
		&counters,
	)

	// Verify the results
	assert.Equal(t, 1, counters.blocked)
	assert.Equal(t, 0, counters.waiting)
	assert.Equal(t, 1, counters.pending)
	assert.Equal(t, 1, counters.queued)
	assert.Equal(t, 1, counters.running)
	assert.Equal(t, 1, counters.terminating)

	// Execute the function under test
	updateStateCounters(
		databricksSdkJobs.RunLifecycleStateV2StateRunning,
		&counters,
	)

	// Verify the results
	assert.Equal(t, 1, counters.blocked)
	assert.Equal(t, 0, counters.waiting)
	assert.Equal(t, 1, counters.pending)
	assert.Equal(t, 1, counters.queued)
	assert.Equal(t, 2, counters.running)
	assert.Equal(t, 1, counters.terminating)

	// Execute the function under test
	updateStateCounters(
		databricksSdkJobs.RunLifecycleStateV2StateRunning,
		&counters,
	)

	// Verify the results
	assert.Equal(t, 1, counters.blocked)
	assert.Equal(t, 0, counters.waiting)
	assert.Equal(t, 1, counters.pending)
	assert.Equal(t, 1, counters.queued)
	assert.Equal(t, 3, counters.running)
	assert.Equal(t, 1, counters.terminating)

	// Execute the function under test
	updateStateCounters(
		databricksSdkJobs.RunLifecycleStateV2StateBlocked,
		&counters,
	)

	// Verify the results
	assert.Equal(t, 2, counters.blocked)
	assert.Equal(t, 0, counters.waiting)
	assert.Equal(t, 1, counters.pending)
	assert.Equal(t, 1, counters.queued)
	assert.Equal(t, 3, counters.running)
	assert.Equal(t, 1, counters.terminating)

	// Execute the function under test
	updateStateCounters(
		databricksSdkJobs.RunLifecycleStateV2StateWaiting,
		&counters,
	)

	// Verify the results
	assert.Equal(t, 2, counters.blocked)
	assert.Equal(t, 1, counters.waiting)
	assert.Equal(t, 1, counters.pending)
	assert.Equal(t, 1, counters.queued)
	assert.Equal(t, 3, counters.running)
	assert.Equal(t, 1, counters.terminating)
}

func TestUpdateStateCounters_InvalidState(t *testing.T) {
	// Create a counters instance
	counters := counters{}

	// Assert preconditions
	assert.Equal(t, 0, counters.blocked)
	assert.Equal(t, 0, counters.waiting)
	assert.Equal(t, 0, counters.pending)
	assert.Equal(t, 0, counters.queued)
	assert.Equal(t, 0, counters.running)
	assert.Equal(t, 0, counters.terminating)

	// Execute the function under test
	updateStateCounters(
		databricksSdkJobs.RunLifecycleStateV2State("invalid_state"),
		&counters,
	)

	// Verify the results
	assert.Equal(t, 0, counters.blocked)
	assert.Equal(t, 0, counters.waiting)
	assert.Equal(t, 0, counters.pending)
	assert.Equal(t, 0, counters.queued)
	assert.Equal(t, 0, counters.running)
	assert.Equal(t, 0, counters.terminating)
}
