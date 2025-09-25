package databricks

import (
	"context"
	"errors"
	"testing"
	"time"

	databricksSdkListing "github.com/databricks/databricks-sdk-go/listing"
	databricksSdkPipelines "github.com/databricks/databricks-sdk-go/service/pipelines"
	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration"
	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/model"
	"github.com/stretchr/testify/assert"
)

func verifyCommonAttributes(t *testing.T, attrs map[string]interface{}) {
	assert.NotNil(t, attrs)
	assert.NotEmpty(t, attrs)

	// Verify tag attributes
	assert.Contains(t, attrs, "env")
	assert.Equal(t, "production", attrs["env"])
	assert.Contains(t, attrs, "team")
	assert.Equal(t, "data-engineering", attrs["team"])

	// Verify workspace attributes
	assert.Contains(t, attrs, "databricksWorkspaceId")
	assert.Equal(t, mockWorkspaceId, attrs["databricksWorkspaceId"])
	assert.Contains(t, attrs, "databricksWorkspaceName")
	assert.Equal(t, mockWorkspaceHost, attrs["databricksWorkspaceName"])
	assert.Contains(t, attrs, "databricksWorkspaceUrl")
	assert.Equal(
		t,
		"https://" + mockWorkspaceHost,
		attrs["databricksWorkspaceUrl"],
	)
}

func verifyPipelineCommonAttributes(
	t *testing.T,
	pipelineId string,
	pipelineName string,
	updateId string,
	attrs map[string]interface{},
) {
	verifyCommonAttributes(t, attrs)

	// Verify pipeline and update attributes
	assert.Contains(t, attrs, "databricksPipelineId")
	assert.Equal(t, pipelineId, attrs["databricksPipelineId"])
	assert.Contains(t, attrs, "databricksPipelineUpdateId")
	assert.Equal(t, updateId, attrs["databricksPipelineUpdateId"])
	assert.Contains(t, attrs, "pipelineName")
	assert.Equal(t, pipelineName, attrs["pipelineName"])
}

func verifyTestPipelineCommonAttributes(
	t *testing.T,
	attrs map[string]interface{},
) {
	verifyPipelineCommonAttributes(t, "12345", "Test Pipeline", "abcde", attrs)
}

func verifyNoPipelineCommonAttributes(
	t *testing.T,
	attrs map[string]interface{},
) {
	// Verify no pipeline and update attributes
	assert.NotContains(t, attrs, "databricksPipelineId")
	assert.NotContains(t, attrs, "databricksPipelineUpdateId")
	assert.NotContains(t, attrs, "pipelineName")
}

func verifyPipelineClusterAttributes(
	t *testing.T,
	attrs map[string]interface{},
) {
	// Verify cluster attributes
	assert.Contains(t, attrs, "databricksClusterId")
	assert.Equal(t, "fake-cluster-id", attrs["databricksClusterId"])
	assert.Contains(t, attrs, "databricksClusterName")
	assert.Equal(t, "fake-cluster-name", attrs["databricksClusterName"])
	assert.Contains(t, attrs, "databricksClusterSource")
	assert.Equal(t, "fake-cluster-source", attrs["databricksClusterSource"])
	assert.Contains(t, attrs, "databricksClusterInstancePoolId")
	assert.Equal(
		t,
		"fake-cluster-instance-pool-id",
		attrs["databricksClusterInstancePoolId"],
	)
}

func verifyNoPipelineClusterAttributes(
	t *testing.T,
	attrs map[string]interface{},
) {
	// Verify no cluster attributes
	assert.NotContains(t, attrs, "databricksClusterId")
	assert.NotContains(t, attrs, "databricksClusterName")
	assert.NotContains(t, attrs, "databricksClusterSource")
	assert.NotContains(t, attrs, "databricksClusterInstancePoolId")
}

// The creationTime parameter is the _expected_ value.
func verifyPipelineUpdateBaseAttributes(
	t *testing.T,
	creationTime time.Time,
	attrs map[string]interface{},
) {
	assert.Contains(t, attrs, "creationTime")
	assert.Equal(t, creationTime.UnixMilli(), attrs["creationTime"])
}

// The time parameters are the _expected_ values.
func verifyPipelineUpdateCompleteAttributes(
	t *testing.T,
	creationTime time.Time,
	waitStartTime time.Time,
	startTime time.Time,
	completionTime time.Time,
	attrs map[string]interface{},
) {
	// Verify update complete attributes
	assert.Contains(t, attrs, "status")
	assert.Equal(
		t,
		string(databricksSdkPipelines.UpdateInfoStateCompleted),
		attrs["status"],
	)
	assert.Contains(t, attrs, "completionTime")
	assert.Equal(t, completionTime.UnixMilli(), attrs["completionTime"])

	assert.Contains(t, attrs, "duration")
	assert.Equal(
		t,
		int64(completionTime.Sub(creationTime) / time.Millisecond),
		attrs["duration"],
	)

	if waitStartTime.IsZero() {
		assert.NotContains(t, attrs, "waitStartTime")
		assert.NotContains(t, attrs, "waitDuration")
	} else {
		assert.Contains(t, attrs, "waitStartTime")
		assert.Equal(t, waitStartTime.UnixMilli(), attrs["waitStartTime"])

		assert.Contains(t, attrs, "waitDuration")
		if !startTime.IsZero() {
			assert.Equal(
				t,
				int64(startTime.Sub(waitStartTime) / time.Millisecond),
				attrs["waitDuration"],
			)
		} else {
			assert.Equal(
				t,
				int64(completionTime.Sub(waitStartTime) / time.Millisecond),
				attrs["waitDuration"],
			)
		}
	}

	if startTime.IsZero() {
		assert.NotContains(t, attrs, "startTime")
		assert.NotContains(t, attrs, "runDuration")
	} else {
		assert.Contains(t, attrs, "startTime")
		assert.Equal(t, startTime.UnixMilli(), attrs["startTime"])
		assert.Contains(t, attrs, "runDuration")
		assert.Equal(
			t,
			int64(completionTime.Sub(startTime) / time.Millisecond),
			attrs["runDuration"],
		)
	}
}

func verifyNoPipelineUpdateCompleteAttributes(
	t *testing.T,
	attrs map[string]interface{},
) {
	// Verify no update end attributes
	assert.NotContains(t, attrs, "status")
	assert.NotContains(t, attrs, "completionTime")
	assert.NotContains(t, attrs, "duration")
	assert.NotContains(t, attrs, "waitStartTime")
	assert.NotContains(t, attrs, "waitDuration")
	assert.NotContains(t, attrs, "startTime")
	assert.NotContains(t, attrs, "runDuration")
}

// The id, name, and queueStartTime are the _expected_ values.
func verifyPipelineFlowBaseAttributes(
	t *testing.T,
	id string,
	name string,
	queueStartTime time.Time,
	attrs map[string]interface{},
) {
	// Verify flow base attributes
	assert.Contains(t, attrs, "databricksPipelineFlowId")
	assert.Equal(t, id, attrs["databricksPipelineFlowId"])
	assert.Contains(t, attrs, "databricksPipelineFlowName")
	assert.Equal(t, name, attrs["databricksPipelineFlowName"])
	assert.Contains(t, attrs, "queueStartTime")
	assert.Equal(t, queueStartTime.UnixMilli(), attrs["queueStartTime"])
}

// The time and metric parameters are the _expected_ values.
func verifyPipelineFlowCompleteAttributes(
	t *testing.T,
	queueStartTime time.Time,
	planStartTime time.Time,
	startTime time.Time,
	completionTime time.Time,
	backlogBytes *float64,
	backlogFiles *float64,
	numOutputRows *float64,
	droppedRecords *float64,
	attrs map[string]interface{},
) {
	// Verify flow complete attributes
	assert.Contains(t, attrs, "status")
	assert.Equal(t, string(FlowInfoStatusCompleted), attrs["status"])

	assert.Contains(t, attrs, "completionTime")
	assert.Equal(t, completionTime.UnixMilli(), attrs["completionTime"])

	assert.Contains(t, attrs, "queueDuration")
	if !planStartTime.IsZero() {
		assert.Equal(
			t,
			int64(planStartTime.Sub(queueStartTime) / time.Millisecond),
			attrs["queueDuration"],
		)
	} else if !startTime.IsZero() {
		assert.Equal(
			t,
			int64(startTime.Sub(queueStartTime) / time.Millisecond),
			attrs["queueDuration"],
		)
	} else {
		assert.Equal(
			t,
			int64(completionTime.Sub(queueStartTime) / time.Millisecond),
			attrs["queueDuration"],
		)
	}

	if planStartTime.IsZero() {
		assert.NotContains(t, attrs, "planStartTime")
		assert.NotContains(t, attrs, "planDuration")
	} else {
		assert.Contains(t, attrs, "planStartTime")
		assert.Equal(t, planStartTime.UnixMilli(), attrs["planStartTime"])

		assert.Contains(t, attrs, "planDuration")
		if !startTime.IsZero() {
			assert.Equal(
				t,
				int64(startTime.Sub(planStartTime) / time.Millisecond),
				attrs["planDuration"],
			)
		} else {
			assert.Equal(
				t,
				int64(completionTime.Sub(planStartTime) / time.Millisecond),
				attrs["planDuration"],
			)
		}
	}

	if startTime.IsZero() {
		assert.NotContains(t, attrs, "startTime")
		assert.NotContains(t, attrs, "duration")
	} else {
		assert.Contains(t, attrs, "startTime")
		assert.Equal(t, startTime.UnixMilli(), attrs["startTime"])
		assert.Contains(t, attrs, "duration")
		assert.Equal(
			t,
			int64(completionTime.Sub(startTime) / time.Millisecond),
			attrs["duration"],
		)
	}

	if backlogBytes != nil {
		assert.Contains(t, attrs, "backlogBytes")
		assert.Equal(t, *backlogBytes, attrs["backlogBytes"])
	} else {
		assert.NotContains(t, attrs, "backlogBytes")
	}

	if backlogFiles != nil {
		assert.Contains(t, attrs, "backlogFileCount")
		assert.Equal(t, *backlogFiles, attrs["backlogFileCount"])
	} else {
		assert.NotContains(t, attrs, "backlogFileCount")
	}

	if numOutputRows != nil {
		assert.Contains(t, attrs, "outputRowCount")
		assert.Equal(t, *numOutputRows, attrs["outputRowCount"])
	} else {
		assert.NotContains(t, attrs, "outputRowCount")
	}

	if droppedRecords != nil {
		assert.Contains(t, attrs, "droppedRecordCount")
		assert.Equal(t, *droppedRecords, attrs["droppedRecordCount"])
	} else {
		assert.NotContains(t, attrs, "droppedRecordCount")
	}
}

func verifyNoPipelineFlowCompleteAttributes(
	t *testing.T,
	attrs map[string]interface{},
) {
	// Verify no flow complete attributes
	assert.NotContains(t, attrs, "status")
	assert.NotContains(t, attrs, "completionTime")
	assert.NotContains(t, attrs, "queueDuration")
	assert.NotContains(t, attrs, "planStartTime")
	assert.NotContains(t, attrs, "planDuration")
	assert.NotContains(t, attrs, "startTime")
	assert.NotContains(t, attrs, "duration")
	assert.NotContains(t, attrs, "backlogBytes")
	assert.NotContains(t, attrs, "backlogFileCount")
	assert.NotContains(t, attrs, "outputRowCount")
	assert.NotContains(t, attrs, "droppedRecordCount")
}

func verifyExpectationAttributes(
	t *testing.T,
	flowId string,
	flowName string,
	expectationName string,
	expectationDataset string,
	expectationPassedRecords float64,
	expectationFailedRecords float64,
	attrs map[string]interface{},
) {
	assert.Contains(t, attrs, "databricksPipelineFlowId")
	assert.Equal(t, flowId, attrs["databricksPipelineFlowId"])
	assert.Contains(t, attrs, "databricksPipelineFlowName")
	assert.Equal(t, flowName, attrs["databricksPipelineFlowName"])

	assert.Contains(t, attrs, "name")
	assert.Equal(t, expectationName, attrs["name"])
	assert.Contains(t, attrs, "dataset")
	assert.Equal(t, expectationDataset, attrs["dataset"])
	assert.Contains(t, attrs, "passedRecordCount")
	assert.Equal(t, expectationPassedRecords, attrs["passedRecordCount"])
	assert.Contains(t, attrs, "failedRecordCount")
	assert.Equal(t, expectationFailedRecords, attrs["failedRecordCount"])
}

func verifyPipelineSummaryAttributes(
	t *testing.T,
	pipelineCounters *pipelineCounters,
	updateCounters *updateCounters,
	flowCounters *flowCounters,
	attrs map[string]interface{},
) {
	// Verify pipeline counts
	assert.Contains(t, attrs, "deletedPipelineCount")
	assert.Equal(t, pipelineCounters.deleted, attrs["deletedPipelineCount"])
	assert.Contains(t, attrs, "deployingPipelineCount")
	assert.Equal(t, pipelineCounters.deploying, attrs["deployingPipelineCount"])
	assert.Contains(t, attrs, "failedPipelineCount")
	assert.Equal(t, pipelineCounters.failed, attrs["failedPipelineCount"])
	assert.Contains(t, attrs, "idlePipelineCount")
	assert.Equal(t, pipelineCounters.idle, attrs["idlePipelineCount"])
	assert.Contains(t, attrs, "recoveringPipelineCount")
	assert.Equal(
		t,
		pipelineCounters.recovering,
		attrs["recoveringPipelineCount"],
	)
	assert.Contains(t, attrs, "resettingPipelineCount")
	assert.Equal(t, pipelineCounters.resetting, attrs["resettingPipelineCount"])
	assert.Contains(t, attrs, "runningPipelineCount")
	assert.Equal(t, pipelineCounters.running, attrs["runningPipelineCount"])
	assert.Contains(t, attrs, "startingPipelineCount")
	assert.Equal(t, pipelineCounters.starting, attrs["startingPipelineCount"])
	assert.Contains(t, attrs, "stoppingPipelineCount")
	assert.Equal(t, pipelineCounters.stopping, attrs["stoppingPipelineCount"])

	// Verify update counts
	assert.Contains(t, attrs, "createdUpdateCount")
	assert.Equal(t, updateCounters.created, attrs["createdUpdateCount"])
	assert.Contains(t, attrs, "initializingUpdateCount")
	assert.Equal(
		t,
		updateCounters.initializing,
		attrs["initializingUpdateCount"],
	)
	assert.Contains(t, attrs, "queuedUpdateCount")
	assert.Equal(t, updateCounters.queued, attrs["queuedUpdateCount"])
	assert.Contains(t, attrs, "resettingUpdateCount")
	assert.Equal(t, updateCounters.resetting, attrs["resettingUpdateCount"])
	assert.Contains(t, attrs, "runningUpdateCount")
	assert.Equal(t, updateCounters.running, attrs["runningUpdateCount"])
	assert.Contains(t, attrs, "settingUpTablesUpdateCount")
	assert.Equal(
		t,
		updateCounters.settingUpTables,
		attrs["settingUpTablesUpdateCount"],
	)
	assert.Contains(t, attrs, "stoppingUpdateCount")
	assert.Equal(t, updateCounters.stopping, attrs["stoppingUpdateCount"])
	assert.Contains(t, attrs, "waitingForResourcesUpdateCount")
	assert.Equal(
		t,
		updateCounters.waitingForResources,
		attrs["waitingForResourcesUpdateCount"],
	)

	// Verify flow counts
	assert.Contains(t, attrs, "idleFlowCount")
	assert.Equal(t, flowCounters.idle, attrs["idleFlowCount"])
	assert.Contains(t, attrs, "planningFlowCount")
	assert.Equal(t, flowCounters.planning, attrs["planningFlowCount"])
	assert.Contains(t, attrs, "queuedFlowCount")
	assert.Equal(t, flowCounters.queued, attrs["queuedFlowCount"])
	assert.Contains(t, attrs, "runningFlowCount")
	assert.Equal(t, flowCounters.running, attrs["runningFlowCount"])
	assert.Contains(t, attrs, "startingFlowCount")
	assert.Equal(t, flowCounters.starting, attrs["startingFlowCount"])
}

func TestNewFlowData(t *testing.T) {
	// Setup mock origin
	origin := &databricksSdkPipelines.Origin{
		PipelineId:   "12345",
		PipelineName: "Test Pipeline",
		ClusterId:    "fake-cluster-id",
		UpdateId:     "abcde",
		FlowId:       "flow-1",
		FlowName:     "flow_1",
	}

	// Execute the function under test
	flowData := newFlowData(origin)

	// Verify result
	assert.Equal(t, origin.FlowId, flowData.id)
	assert.Equal(t, origin.FlowName, flowData.name)
	assert.Equal(t, origin.PipelineId, flowData.pipelineId)
	assert.Equal(t, origin.PipelineName, flowData.pipelineName)
	assert.Equal(t, origin.ClusterId, flowData.clusterId)
	assert.Equal(t, origin.UpdateId, flowData.updateId)
	assert.True(t, flowData.queueStartTime.IsZero())
	assert.True(t, flowData.planStartTime.IsZero())
	assert.True(t, flowData.startTime.IsZero())
	assert.True(t, flowData.completionTime.IsZero())
	assert.Nil(t, flowData.backlogBytes)
	assert.Nil(t, flowData.backlogFiles)
	assert.Nil(t, flowData.numOutputRows)
	assert.Nil(t, flowData.droppedRecords)
	assert.Equal(t, "", flowData.status)
	assert.Nil(t, flowData.expectations)
}

func TestGetOrCreateFlowData_CreateFlow(t *testing.T) {
	// Setup mock flow map
	flows := make(map[string]*flowData)

	// Setup mock flow progress
	flowProgress := &FlowProgressDetails{}

	// Setup mock origin
	origin := &databricksSdkPipelines.Origin{
		PipelineId:   "12345",
		PipelineName: "Test Pipeline",
		ClusterId:    "fake-cluster-id",
		UpdateId:     "abcde",
		FlowId:       "flow-1",
		FlowName:     "flow_1",
	}

	// Execute the function under test
	flowData := getOrCreateFlowData(
		flows,
		flowProgress,
		origin,
		"abcde",
		time.Time{},
		time.Time{},
		time.Time{},
		time.Time{},
		FlowInfoStatusRunning,
	)

	// Verify result
	assert.NotNil(t, flowData)
	assert.Equal(t, origin.FlowId, flowData.id)
	assert.Equal(t, origin.FlowName, flowData.name)
	assert.Equal(t, origin.PipelineId, flowData.pipelineId)
	assert.Equal(t, origin.PipelineName, flowData.pipelineName)
	assert.Equal(t, origin.ClusterId, flowData.clusterId)
	assert.Equal(t, origin.UpdateId, flowData.updateId)
	assert.True(t, flowData.queueStartTime.IsZero())
	assert.True(t, flowData.planStartTime.IsZero())
	assert.True(t, flowData.startTime.IsZero())
	assert.True(t, flowData.completionTime.IsZero())
	assert.Nil(t, flowData.backlogBytes)
	assert.Nil(t, flowData.backlogFiles)
	assert.Nil(t, flowData.numOutputRows)
	assert.Nil(t, flowData.droppedRecords)
	assert.Equal(t, FlowInfoStatusRunning, flowData.status)
	assert.Nil(t, flowData.expectations)
}

func TestGetOrCreateFlowData_GetFlow(t *testing.T) {
	// Setup now
	now := time.Now()

	// Setup backlog metrics
	backlogBytes := float64(1024)
	backlogFiles := float64(10)

	// Setup row metrics
	numOutputRows := float64(500)
	droppedRecords := float64(50)

	// Setup mock flow map
	flows := map[string]*flowData{
		"abcde.flow_1": {
			eventData: eventData{
				pipelineId:   "12345",
				pipelineName: "Test Pipeline",
				clusterId:    "fake-cluster-id",
				updateId:     "abcde",
			},
			id:               "flow-1",
			name:             "flow_1",
			status:           FlowInfoStatusCompleted,
			queueStartTime:   now,
			planStartTime:    now,
			startTime:        now,
			completionTime:   now,
			backlogBytes:     &backlogBytes,
			backlogFiles:     &backlogFiles,
			numOutputRows:    &numOutputRows,
			droppedRecords:   &droppedRecords,
		},
	}

	// Setup mock flow progress
	flowProgress := &FlowProgressDetails{}

	// Setup mock origin
	origin := &databricksSdkPipelines.Origin{
		PipelineId:   "12345",
		PipelineName: "Test Pipeline",
		ClusterId:    "fake-cluster-id",
		UpdateId:     "abcde",
		FlowId:       "flow-1",
		FlowName:     "flow_1",
	}

	// Execute the function under test
	flowData := getOrCreateFlowData(
		flows,
		flowProgress,
		origin,
		"abcde",
		time.Time{},
		time.Time{},
		time.Time{},
		time.Time{},
		FlowInfoStatusRunning,
	)

	// Verify result
	assert.NotNil(t, flowData)
	assert.Equal(t, "flow-1", flowData.id)
	assert.Equal(t, "flow_1", flowData.name)
	assert.Equal(t, "12345", flowData.pipelineId)
	assert.Equal(t, "Test Pipeline", flowData.pipelineName)
	assert.Equal(t, "fake-cluster-id", flowData.clusterId)
	assert.Equal(t, "abcde", flowData.updateId)

	// We know we are looking at the existing flow from the map instead of a new
	// one because a new one would have it's times set to the zero value
	assert.Equal(t, now, flowData.queueStartTime)
	assert.Equal(t, now, flowData.planStartTime)
	assert.Equal(t, now, flowData.startTime)
	assert.Equal(t, now, flowData.completionTime)

	// Likewise, a new flow would not have metrics set
	assert.NotNil(t, flowData.backlogBytes)
	assert.Equal(t, backlogBytes, *flowData.backlogBytes)
	assert.NotNil(t, flowData.backlogFiles)
	assert.Equal(t, backlogFiles, *flowData.backlogFiles)
	assert.NotNil(t, flowData.numOutputRows)
	assert.Equal(t, numOutputRows, *flowData.numOutputRows)
	assert.NotNil(t, flowData.droppedRecords)
	assert.Equal(t, droppedRecords, *flowData.droppedRecords)

	// And a new one would have it's status set to FlowInfoStatusRunning since
	// that is what we passed on the call to getOrCreateFlowData()
	assert.Equal(t, FlowInfoStatusCompleted, flowData.status)

	assert.Nil(t, flowData.expectations)
}

func TestGetOrCreateFlowData_QueueStartTime(t *testing.T) {
	// Setup now
	now := time.Now()

	// Setup mock flow map
	flows := map[string]*flowData{
		"abcde.flow_1": {
			eventData: eventData{
				pipelineId:   "12345",
				pipelineName: "Test Pipeline",
				clusterId:    "fake-cluster-id",
				updateId:     "abcde",
			},
			id:               "flow-1",
			name:             "flow_1",
			status:           FlowInfoStatusCompleted,
		},
	}

	// Setup mock flow progress
	flowProgress := &FlowProgressDetails{}

	// Setup mock origin
	origin := &databricksSdkPipelines.Origin{
		PipelineId:   "12345",
		PipelineName: "Test Pipeline",
		ClusterId:    "fake-cluster-id",
		UpdateId:     "abcde",
		FlowId:       "flow-1",
		FlowName:     "flow_1",
	}

	// Execute the function under test
	flowData := getOrCreateFlowData(
		flows,
		flowProgress,
		origin,
		"abcde",
		now,
		time.Time{},
		time.Time{},
		time.Time{},
		FlowInfoStatusRunning,
	)

	// Verify result
	assert.NotNil(t, flowData)

	// Verify we are looking at the existing flow by making sure the status
	// is set to the state on the existing flow indicating the passed status was
	// ignored
	assert.Equal(t, FlowInfoStatusCompleted, flowData.status)

	// Verify queueStartTime was set properly and other times remain zero
	assert.Equal(t, now, flowData.queueStartTime)
	assert.True(t, flowData.planStartTime.IsZero())
	assert.True(t, flowData.startTime.IsZero())
	assert.True(t, flowData.completionTime.IsZero())
}

func TestGetOrCreateFlowData_PlanStartTime(t *testing.T) {
	// Setup now
	now := time.Now()

	// Setup mock flow map
	flows := map[string]*flowData{
		"abcde.flow_1": {
			eventData: eventData{
				pipelineId:   "12345",
				pipelineName: "Test Pipeline",
				clusterId:    "fake-cluster-id",
				updateId:     "abcde",
			},
			id:               "flow-1",
			name:             "flow_1",
			status:           FlowInfoStatusCompleted,
		},
	}

	// Setup mock flow progress
	flowProgress := &FlowProgressDetails{}

	// Setup mock origin
	origin := &databricksSdkPipelines.Origin{
		PipelineId:   "12345",
		PipelineName: "Test Pipeline",
		ClusterId:    "fake-cluster-id",
		UpdateId:     "abcde",
		FlowId:       "flow-1",
		FlowName:     "flow_1",
	}

	// Execute the function under test
	flowData := getOrCreateFlowData(
		flows,
		flowProgress,
		origin,
		"abcde",
		time.Time{},
		now,
		time.Time{},
		time.Time{},
		FlowInfoStatusRunning,
	)

	// Verify result
	assert.NotNil(t, flowData)

	// Verify we are looking at the existing flow by making sure the status
	// is set to the state on the existing flow indicating the passed status was
	// ignored
	assert.Equal(t, FlowInfoStatusCompleted, flowData.status)

	// Verify planStartTime was set properly and other times remain zero
	assert.True(t, flowData.queueStartTime.IsZero())
	assert.Equal(t, now, flowData.planStartTime)
	assert.True(t, flowData.startTime.IsZero())
	assert.True(t, flowData.completionTime.IsZero())
}

func TestGetOrCreateFlowData_StartTime(t *testing.T) {
	// Setup now
	now := time.Now()

	// Setup mock flow map
	flows := map[string]*flowData{
		"abcde.flow_1": {
			eventData: eventData{
				pipelineId:   "12345",
				pipelineName: "Test Pipeline",
				clusterId:    "fake-cluster-id",
				updateId:     "abcde",
			},
			id:               "flow-1",
			name:             "flow_1",
			status:           FlowInfoStatusCompleted,
		},
	}

	// Setup mock flow progress
	flowProgress := &FlowProgressDetails{}

	// Setup mock origin
	origin := &databricksSdkPipelines.Origin{
		PipelineId:   "12345",
		PipelineName: "Test Pipeline",
		ClusterId:    "fake-cluster-id",
		UpdateId:     "abcde",
		FlowId:       "flow-1",
		FlowName:     "flow_1",
	}

	// Execute the function under test
	flowData := getOrCreateFlowData(
		flows,
		flowProgress,
		origin,
		"abcde",
		time.Time{},
		time.Time{},
		now,
		time.Time{},
		FlowInfoStatusRunning,
	)

	// Verify result
	assert.NotNil(t, flowData)

	// Verify we are looking at the existing flow by making sure the status
	// is set to the state on the existing flow indicating the passed status was
	// ignored
	assert.Equal(t, FlowInfoStatusCompleted, flowData.status)

	// Verify startTime was set properly and other times remain zero
	assert.True(t, flowData.queueStartTime.IsZero())
	assert.True(t, flowData.planStartTime.IsZero())
	assert.Equal(t, now, flowData.startTime)
	assert.True(t, flowData.completionTime.IsZero())
}

func TestGetOrCreateFlowData_CompletionTime(t *testing.T) {
	// Setup now
	now := time.Now()

	// Setup backlog metrics
	backlogBytes := float64(1024)
	backlogFiles := float64(10)

	// Setup row metrics
	numOutputRows := float64(500)

	// Setup dropped records
	droppedRecords := float64(50)

	// Setup mock flow map
	flows := map[string]*flowData{
		"abcde.flow_1": {
			eventData: eventData{
				pipelineId:   "12345",
				pipelineName: "Test Pipeline",
				clusterId:    "fake-cluster-id",
				updateId:     "abcde",
			},
			id:               "flow-1",
			name:             "flow_1",
			status:           FlowInfoStatusCompleted,
		},
	}

	// Setup mock flow progress
	flowProgress := &FlowProgressDetails{
		Metrics: &FlowProgressMetrics{
			BacklogBytes:  &backlogBytes,
			BacklogFiles:  &backlogFiles,
			NumOutputRows: &numOutputRows,
		},
		DataQuality: &FlowProgressDataQuality{
			DroppedRecords: &droppedRecords,
			Expectations:   []FlowProgressExpectations{
				{
					Name:          "expectation_1",
					Dataset:       "dataset_1",
					PassedRecords: 100,
					FailedRecords: 0,
				},
			},
		},
	}

	// Setup mock origin
	origin := &databricksSdkPipelines.Origin{
		PipelineId:   "12345",
		PipelineName: "Test Pipeline",
		ClusterId:    "fake-cluster-id",
		UpdateId:     "abcde",
		FlowId:       "flow-1",
		FlowName:     "flow_1",
	}

	// Execute the function under test
	flowData := getOrCreateFlowData(
		flows,
		flowProgress,
		origin,
		"abcde",
		time.Time{},
		time.Time{},
		time.Time{},
		now,
		FlowInfoStatusRunning,
	)

	// Verify result
	assert.NotNil(t, flowData)

	// Verify we are looking at the existing flow by making sure the status
	// is set to the state on the existing flow indicating the passed status was
	// ignored
	assert.Equal(t, FlowInfoStatusCompleted, flowData.status)

	// Verify result
	assert.NotNil(t, flowData)
	assert.Equal(t, "flow-1", flowData.id)
	assert.Equal(t, "flow_1", flowData.name)
	assert.Equal(t, "12345", flowData.pipelineId)
	assert.Equal(t, "Test Pipeline", flowData.pipelineName)
	assert.Equal(t, "fake-cluster-id", flowData.clusterId)
	assert.Equal(t, "abcde", flowData.updateId)

	// Verify completionTime was set properly and other times remain zero
	assert.True(t, flowData.queueStartTime.IsZero())
	assert.True(t, flowData.planStartTime.IsZero())
	assert.True(t, flowData.startTime.IsZero())
	assert.Equal(t, now, flowData.completionTime)

	// Verify metrics and expectations were copied from the flow progress
	// details
	assert.NotNil(t, flowData.backlogBytes)
	assert.Equal(t, *flowProgress.Metrics.BacklogBytes, *flowData.backlogBytes)
	assert.NotNil(t, flowData.backlogFiles)
	assert.Equal(t, *flowProgress.Metrics.BacklogFiles, *flowData.backlogFiles)
	assert.NotNil(t, flowData.numOutputRows)
	assert.Equal(
		t,
		*flowProgress.Metrics.NumOutputRows,
		*flowData.numOutputRows,
	)
	assert.NotNil(t, flowData.droppedRecords)
	assert.Equal(
		t,
		*flowProgress.DataQuality.DroppedRecords,
		*flowData.droppedRecords,
	)
	assert.NotNil(t, flowData.expectations)
	assert.Equal(t, 1, len(flowData.expectations))
	assert.Equal(t, "expectation_1", flowData.expectations[0].Name)
	assert.Equal(t, "dataset_1", flowData.expectations[0].Dataset)
	assert.Equal(t, float64(100), flowData.expectations[0].PassedRecords)
	assert.Equal(t, float64(0), flowData.expectations[0].FailedRecords)
}

func TestGetOrCreateFlowData_CompletionTimeExists(t *testing.T) {
	// Setup now
	now := time.Now()

	// Setup backlog metrics
	backlogBytes := float64(1024)
	backlogFiles := float64(10)

	// Setup row metrics
	numOutputRows := float64(500)

	// Setup dropped records
	droppedRecords := float64(50)

	// Setup mock flow map
	flows := map[string]*flowData{
		"abcde.flow_1": {
			eventData: eventData{
				pipelineId:   "12345",
				pipelineName: "Test Pipeline",
				clusterId:    "fake-cluster-id",
				updateId:     "abcde",
			},
			id:               "flow-1",
			name:             "flow_1",
			status:           FlowInfoStatusCompleted,
			completionTime:   now,
		},
	}

	// Setup mock flow progress
	flowProgress := &FlowProgressDetails{
		Metrics: &FlowProgressMetrics{
			BacklogBytes:  &backlogBytes,
			BacklogFiles:  &backlogFiles,
			NumOutputRows: &numOutputRows,
		},
		DataQuality: &FlowProgressDataQuality{
			DroppedRecords: &droppedRecords,
			Expectations:   []FlowProgressExpectations{
				{
					Name:          "expectation_1",
					Dataset:       "dataset_1",
					PassedRecords: 100,
					FailedRecords: 0,
				},
			},
		},
	}

	// Setup mock origin
	origin := &databricksSdkPipelines.Origin{
		PipelineId:   "12345",
		PipelineName: "Test Pipeline",
		ClusterId:    "fake-cluster-id",
		UpdateId:     "abcde",
		FlowId:       "flow-1",
		FlowName:     "flow_1",
	}

	// Execute the function under test
	flowData := getOrCreateFlowData(
		flows,
		flowProgress,
		origin,
		"abcde",
		time.Time{},
		time.Time{},
		time.Time{},
		time.Time{},
		FlowInfoStatusRunning,
	)

	// Verify result
	assert.NotNil(t, flowData)

	// Verify we are looking at the existing flow by making sure the status
	// is set to the state on the existing flow indicating the passed status was
	// ignored
	assert.Equal(t, FlowInfoStatusCompleted, flowData.status)

	// Verify result
	assert.NotNil(t, flowData)
	assert.Equal(t, "flow-1", flowData.id)
	assert.Equal(t, "flow_1", flowData.name)
	assert.Equal(t, "12345", flowData.pipelineId)
	assert.Equal(t, "Test Pipeline", flowData.pipelineName)
	assert.Equal(t, "fake-cluster-id", flowData.clusterId)
	assert.Equal(t, "abcde", flowData.updateId)

	// Verify completionTime is still set and other times remain zero
	assert.True(t, flowData.queueStartTime.IsZero())
	assert.True(t, flowData.planStartTime.IsZero())
	assert.True(t, flowData.startTime.IsZero())
	assert.Equal(t, now, flowData.completionTime)

	// Verify metrics and expectations were copied from the flow progress
	// details
	assert.NotNil(t, flowData.backlogBytes)
	assert.Equal(t, *flowProgress.Metrics.BacklogBytes, *flowData.backlogBytes)
	assert.NotNil(t, flowData.backlogFiles)
	assert.Equal(t, *flowProgress.Metrics.BacklogFiles, *flowData.backlogFiles)
	assert.NotNil(t, flowData.numOutputRows)
	assert.Equal(
		t,
		*flowProgress.Metrics.NumOutputRows,
		*flowData.numOutputRows,
	)
	assert.NotNil(t, flowData.droppedRecords)
	assert.Equal(
		t,
		*flowProgress.DataQuality.DroppedRecords,
		*flowData.droppedRecords,
	)
	assert.NotNil(t, flowData.expectations)
	assert.Equal(t, 1, len(flowData.expectations))
	assert.Equal(t, "expectation_1", flowData.expectations[0].Name)
	assert.Equal(t, "dataset_1", flowData.expectations[0].Dataset)
	assert.Equal(t, float64(100), flowData.expectations[0].PassedRecords)
	assert.Equal(t, float64(0), flowData.expectations[0].FailedRecords)
}

func TestGetOrCreateFlowData_Status(t *testing.T) {
	// Setup now
	now := time.Now()

	// Setup mock flow map
	flows := map[string]*flowData{
		"abcde.flow_1": {
			eventData: eventData{
				pipelineId:   "12345",
				pipelineName: "Test Pipeline",
				clusterId:    "fake-cluster-id",
				updateId:     "abcde",
			},
			id:               "flow-1",
			name:             "flow_1",
			completionTime:   now.Add(-5 * time.Second),
		},
	}

	// Setup mock flow progress
	flowProgress := &FlowProgressDetails{}

	// Setup mock origin
	origin := &databricksSdkPipelines.Origin{
		PipelineId:   "12345",
		PipelineName: "Test Pipeline",
		ClusterId:    "fake-cluster-id",
		UpdateId:     "abcde",
		FlowId:       "flow-1",
		FlowName:     "flow_1",
	}

	// Execute the function under test
	flowData := getOrCreateFlowData(
		flows,
		flowProgress,
		origin,
		"abcde",
		time.Time{},
		time.Time{},
		time.Time{},
		time.Time{},
		FlowInfoStatusFailed,
	)

	// Verify result
	assert.NotNil(t, flowData)

	// Verify we are looking at the existing flow by making sure the
	// completionTime is set to what it was in the map and not zero
	assert.Equal(t, now.Add(-5 * time.Second), flowData.completionTime)

	// Verify status was set properly
	assert.Equal(t, FlowInfoStatusFailed, flowData.status)
}

func TestProcessFlowData_NoMetrics_NoDataQuality(t *testing.T) {
	// Setup mock flow
	flow := &flowData{
		eventData: eventData{
			pipelineId:   "12345",
			pipelineName: "Test Pipeline",
			clusterId:    "fake-cluster-id",
			updateId:     "abcde",
		},
		id:               "flow-1",
		name:             "flow_1",
		status:           FlowInfoStatusCompleted,
	}

	// Setup mock flow progress
	flowProgress := &FlowProgressDetails{}

	// Execute the function under test
	processFlowData(
		flowProgress,
		flow,
	)

	// Verify result
	assert.Nil(t, flow.backlogBytes)
	assert.Nil(t, flow.backlogFiles)
	assert.Nil(t, flow.numOutputRows)
	assert.Nil(t, flow.droppedRecords)
	assert.Nil(t, flow.expectations)
}

func TestProcessFlowData_WithMetrics_NoDataQuality(t *testing.T) {
	// Setup backlog metrics
	backlogBytes := float64(1024)
	backlogFiles := float64(10)

	// Setup row metrics
	numOutputRows := float64(500)

	// Setup mock flow map
	flow := &flowData{
		eventData: eventData{
			pipelineId:   "12345",
			pipelineName: "Test Pipeline",
			clusterId:    "fake-cluster-id",
			updateId:     "abcde",
		},
		id:               "flow-1",
		name:             "flow_1",
		status:           FlowInfoStatusCompleted,
	}

	// Setup mock flow progress
	flowProgress := &FlowProgressDetails{
		Metrics: &FlowProgressMetrics{
			BacklogBytes:  &backlogBytes,
			BacklogFiles:  &backlogFiles,
			NumOutputRows: &numOutputRows,
		},
	}

	// Execute the function under test
	processFlowData(
		flowProgress,
		flow,
	)

	// Verify result
	assert.NotNil(t, flow.backlogBytes)
	assert.Equal(t, backlogBytes, *flow.backlogBytes)
	assert.NotNil(t, flow.backlogFiles)
	assert.Equal(t, backlogFiles, *flow.backlogFiles)
	assert.NotNil(t, flow.numOutputRows)
	assert.Equal(t, numOutputRows, *flow.numOutputRows)
	assert.Nil(t, flow.droppedRecords)
	assert.Nil(t, flow.expectations)
}

func TestProcessFlowData_WithMetrics_NoBacklogBytes_NoDataQuality(
	t *testing.T,
) {
	// Setup backlog metrics
	backlogFiles := float64(10)

	// Setup row metrics
	numOutputRows := float64(500)

	// Setup mock flow map
	flow := &flowData{
		eventData: eventData{
			pipelineId:   "12345",
			pipelineName: "Test Pipeline",
			clusterId:    "fake-cluster-id",
			updateId:     "abcde",
		},
		id:               "flow-1",
		name:             "flow_1",
		status:           FlowInfoStatusCompleted,
	}

	// Setup mock flow progress
	flowProgress := &FlowProgressDetails{
		Metrics: &FlowProgressMetrics{
			BacklogFiles:  &backlogFiles,
			NumOutputRows: &numOutputRows,
		},
	}

	// Execute the function under test
	processFlowData(
		flowProgress,
		flow,
	)

	// Verify result
	assert.Nil(t, flow.backlogBytes)
	assert.NotNil(t, flow.backlogFiles)
	assert.Equal(t, backlogFiles, *flow.backlogFiles)
	assert.NotNil(t, flow.numOutputRows)
	assert.Equal(t, numOutputRows, *flow.numOutputRows)
	assert.Nil(t, flow.droppedRecords)
	assert.Nil(t, flow.expectations)
}

func TestProcessFlowData_WithMetrics_NoBacklogFiles_NoDataQuality(
	t *testing.T,
) {
	// Setup backlog metrics
	backlogBytes := float64(1024)

	// Setup row metrics
	numOutputRows := float64(500)

	// Setup mock flow map
	flow := &flowData{
		eventData: eventData{
			pipelineId:   "12345",
			pipelineName: "Test Pipeline",
			clusterId:    "fake-cluster-id",
			updateId:     "abcde",
		},
		id:               "flow-1",
		name:             "flow_1",
		status:           FlowInfoStatusCompleted,
	}

	// Setup mock flow progress
	flowProgress := &FlowProgressDetails{
		Metrics: &FlowProgressMetrics{
			BacklogBytes:  &backlogBytes,
			NumOutputRows: &numOutputRows,
		},
	}

	// Execute the function under test
	processFlowData(
		flowProgress,
		flow,
	)

	// Verify result
	assert.NotNil(t, flow.backlogBytes)
	assert.Equal(t, backlogBytes, *flow.backlogBytes)
	assert.Nil(t, flow.backlogFiles)
	assert.NotNil(t, flow.numOutputRows)
	assert.Equal(t, numOutputRows, *flow.numOutputRows)
	assert.Nil(t, flow.droppedRecords)
	assert.Nil(t, flow.expectations)
}

func TestProcessFlowData_WithMetrics_NoNumOutputRows_NoDataQuality(
	t *testing.T,
) {
	// Setup backlog metrics
	backlogBytes := float64(1024)
	backlogFiles := float64(10)

	// Setup mock flow map
	flow := &flowData{
		eventData: eventData{
			pipelineId:   "12345",
			pipelineName: "Test Pipeline",
			clusterId:    "fake-cluster-id",
			updateId:     "abcde",
		},
		id:               "flow-1",
		name:             "flow_1",
		status:           FlowInfoStatusCompleted,
	}

	// Setup mock flow progress
	flowProgress := &FlowProgressDetails{
		Metrics: &FlowProgressMetrics{
			BacklogBytes:  &backlogBytes,
			BacklogFiles:  &backlogFiles,
		},
	}

	// Execute the function under test
	processFlowData(
		flowProgress,
		flow,
	)

	// Verify result
	assert.NotNil(t, flow.backlogBytes)
	assert.Equal(t, backlogBytes, *flow.backlogBytes)
	assert.NotNil(t, flow.backlogFiles)
	assert.Equal(t, backlogFiles, *flow.backlogFiles)
	assert.Nil(t, flow.numOutputRows)
	assert.Nil(t, flow.droppedRecords)
	assert.Nil(t, flow.expectations)
}

func TestProcessFlowData_NoMetrics_WithDataQuality(
	t *testing.T,
) {
	// Setup dropped records
	droppedRecords := float64(50)

	// Setup mock flow map
	flow := &flowData{
		eventData: eventData{
			pipelineId:   "12345",
			pipelineName: "Test Pipeline",
			clusterId:    "fake-cluster-id",
			updateId:     "abcde",
		},
		id:               "flow-1",
		name:             "flow_1",
		status:           FlowInfoStatusCompleted,
	}

	// Setup mock flow progress
	flowProgress := &FlowProgressDetails{
		DataQuality: &FlowProgressDataQuality{
			DroppedRecords: &droppedRecords,
			Expectations: []FlowProgressExpectations{
				{
					Name:    "expectation_1",
					Dataset: "dataset_1",
					PassedRecords: 100,
					FailedRecords: 0,
				},
			},
		},
	}

	// Execute the function under test
	processFlowData(
		flowProgress,
		flow,
	)

	// Verify result
	assert.Nil(t, flow.backlogBytes)
	assert.Nil(t, flow.backlogFiles)
	assert.Nil(t, flow.numOutputRows)
	assert.NotNil(t, flow.droppedRecords)
	assert.Equal(t, droppedRecords, *flow.droppedRecords)
	assert.NotNil(t, flow.expectations)
	assert.Equal(t, 1, len(flow.expectations))
	assert.Equal(t, "expectation_1", flow.expectations[0].Name)
	assert.Equal(t, "dataset_1", flow.expectations[0].Dataset)
	assert.Equal(t, float64(100), flow.expectations[0].PassedRecords)
	assert.Equal(t, float64(0), flow.expectations[0].FailedRecords)
}

func TestProcessFlowData_NoMetrics_WithDataQuality_NoDroppedRecords(
	t *testing.T,
) {
	// Setup mock flow map
	flow := &flowData{
		eventData: eventData{
			pipelineId:   "12345",
			pipelineName: "Test Pipeline",
			clusterId:    "fake-cluster-id",
			updateId:     "abcde",
		},
		id:               "flow-1",
		name:             "flow_1",
		status:           FlowInfoStatusCompleted,
	}

	// Setup mock flow progress
	flowProgress := &FlowProgressDetails{
		DataQuality: &FlowProgressDataQuality{
			Expectations: []FlowProgressExpectations{
				{
					Name:    "expectation_1",
					Dataset: "dataset_1",
					PassedRecords: 100,
					FailedRecords: 0,
				},
			},
		},
	}

	// Execute the function under test
	processFlowData(
		flowProgress,
		flow,
	)

	// Verify result
	assert.Nil(t, flow.backlogBytes)
	assert.Nil(t, flow.backlogFiles)
	assert.Nil(t, flow.numOutputRows)
	assert.Nil(t, flow.droppedRecords)
	assert.NotNil(t, flow.expectations)
	assert.Equal(t, 1, len(flow.expectations))
	assert.Equal(t, "expectation_1", flow.expectations[0].Name)
	assert.Equal(t, "dataset_1", flow.expectations[0].Dataset)
	assert.Equal(t, float64(100), flow.expectations[0].PassedRecords)
	assert.Equal(t, float64(0), flow.expectations[0].FailedRecords)
}

func TestProcessFlowData_NoMetrics_WithDataQuality_NoExpectations(
	t *testing.T,
) {
	// Setup row metrics
	droppedRecords := float64(50)

	// Setup mock flow map
	flow := &flowData{
		eventData: eventData{
			pipelineId:   "12345",
			pipelineName: "Test Pipeline",
			clusterId:    "fake-cluster-id",
			updateId:     "abcde",
		},
		id:               "flow-1",
		name:             "flow_1",
		status:           FlowInfoStatusCompleted,
	}

	// Setup mock flow progress
	flowProgress := &FlowProgressDetails{
		DataQuality: &FlowProgressDataQuality{
			DroppedRecords: &droppedRecords,
		},
	}

	// Execute the function under test
	processFlowData(
		flowProgress,
		flow,
	)

	// Verify result
	assert.Nil(t, flow.backlogBytes)
	assert.Nil(t, flow.backlogFiles)
	assert.Nil(t, flow.numOutputRows)
	assert.NotNil(t, flow.droppedRecords)
	assert.Equal(t, droppedRecords, *flow.droppedRecords)
	assert.Nil(t, flow.expectations)
}

func TestIsFlowTerminated(t *testing.T) {
	tests := []struct{
		name   string
		status string
		want   bool
	}{
		{
			name: "completed",
			status: FlowInfoStatusCompleted,
			want: true,
		},
		{
			name: "stopped",
			status: FlowInfoStatusStopped,
			want: true,
		},
		{
			name: "skipped",
			status: FlowInfoStatusSkipped,
			want: true,
		},
		{
			name: "failed",
			status: FlowInfoStatusFailed,
			want: true,
		},
		{
			name: "excluded",
			status: FlowInfoStatusExcluded,
			want: true,
		},
		{
			name: "running",
			status: FlowInfoStatusRunning,
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Execute the function under test
			got := isFlowTerminated(tt.status)

			// Verify result
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestFlowName(t *testing.T) {
	// Execute the function under test
	name := flowName("abcde", "flow_1")

	// Verify result
	assert.Equal(t, "abcde.flow_1", name)
}

func TestNewUpdateData(t *testing.T) {
	// Setup mock origin
	origin := &databricksSdkPipelines.Origin{
		PipelineId:   "12345",
		PipelineName: "Test Pipeline",
		ClusterId:    "fake-cluster-id",
		UpdateId:     "abcde",
	}

	// Execute the function under test
	updateData := newUpdateData(origin)

	// Verify result
	assert.Equal(t, origin.PipelineId, updateData.pipelineId)
	assert.Equal(t, origin.PipelineName, updateData.pipelineName)
	assert.Equal(t, origin.ClusterId, updateData.clusterId)
	assert.Equal(t, origin.UpdateId, updateData.updateId)
	assert.True(t, updateData.creationTime.IsZero())
	assert.True(t, updateData.waitStartTime.IsZero())
	assert.True(t, updateData.startTime.IsZero())
	assert.True(t, updateData.completionTime.IsZero())
	assert.Equal(
		t,
		databricksSdkPipelines.UpdateInfoState(""),
		updateData.state,
	)
}

func TestGetOrCreateUpdateData_CreateUpdate(t *testing.T) {
	// Setup mock update map
	updates := make(map[string]*updateData)

	// Setup mock origin
	origin := &databricksSdkPipelines.Origin{
		PipelineId:   "12345",
		PipelineName: "Test Pipeline",
		ClusterId:    "fake-cluster-id",
		UpdateId:     "abcde",
	}

	// Execute the function under test
	updateData := getOrCreateUpdateData(
		updates,
		origin,
	)

	// Verify result
	assert.NotNil(t, updateData)
	assert.Equal(t, origin.PipelineId, updateData.pipelineId)
	assert.Equal(t, origin.PipelineName, updateData.pipelineName)
	assert.Equal(t, origin.ClusterId, updateData.clusterId)
	assert.Equal(t, origin.UpdateId, updateData.updateId)
	assert.True(t, updateData.creationTime.IsZero())
	assert.True(t, updateData.waitStartTime.IsZero())
	assert.True(t, updateData.startTime.IsZero())
	assert.True(t, updateData.completionTime.IsZero())
	assert.Equal(
		t,
		databricksSdkPipelines.UpdateInfoState(""),
		updateData.state,
	)
}

func TestGetOrCreateUpdateData_GetUpdate(t *testing.T) {
	// Setup now
	now := time.Now()

	// Setup mock update map
	updates := map[string]*updateData{
		"abcde": {
			eventData: eventData{
				pipelineId:   "12345",
				pipelineName: "Test Pipeline",
				clusterId:    "fake-cluster-id",
				updateId:     "abcde",
			},
			creationTime:   now,
			waitStartTime:  now,
			startTime:      now,
			completionTime: now,
			state:          databricksSdkPipelines.UpdateInfoStateCompleted,
		},
	}

	// Setup mock origin
	origin := &databricksSdkPipelines.Origin{
		PipelineId:   "12345",
		PipelineName: "Test Pipeline",
		ClusterId:    "fake-cluster-id",
		UpdateId:     "abcde",
	}

	// Execute the function under test
	updateData := getOrCreateUpdateData(
		updates,
		origin,
	)

	// Verify result
	assert.NotNil(t, updateData)
	assert.Equal(t, "12345", updateData.pipelineId)
	assert.Equal(t, "Test Pipeline", updateData.pipelineName)
	assert.Equal(t, "fake-cluster-id", updateData.clusterId)
	assert.Equal(t, "abcde", updateData.updateId)

	// We know we are looking at the existing update from the map instead of a
	// new one because a new one would have it's times set to the zero value
	assert.Equal(t, now, updateData.creationTime)
	assert.Equal(t, now, updateData.waitStartTime)
	assert.Equal(t, now, updateData.startTime)
	assert.Equal(t, now, updateData.completionTime)

	// And a new one would have it's status set to ""
	assert.Equal(
		t,
		databricksSdkPipelines.UpdateInfoStateCompleted,
		updateData.state,
	)
}

func TestGetOrCreateUpdateDataForUpdateProgress(t *testing.T) {
	// Setup now
	now := time.Now()

	// Setup mock update map
	updates := map[string]*updateData{
		"abcde": {
			eventData: eventData{
				pipelineId:   "12345",
				pipelineName: "Test Pipeline",
				clusterId:    "fake-cluster-id",
				updateId:     "abcde",
			},
			creationTime:     now,
			state:            databricksSdkPipelines.UpdateInfoStateRunning,
		},
	}

	// Setup mock origin
	origin := &databricksSdkPipelines.Origin{
		PipelineId:   "12345",
		PipelineName: "Test Pipeline",
		ClusterId:    "fake-cluster-id",
		UpdateId:     "abcde",
	}

	// Execute the function under test
	updateData := getOrCreateUpdateDataForUpdateProgress(
		updates,
		origin,
		time.Time{},
		time.Time{},
		time.Time{},
		databricksSdkPipelines.UpdateInfoStateStopping,
	)

	// Verify result
	assert.NotNil(t, updateData)
	assert.Equal(t, "12345", updateData.pipelineId)
	assert.Equal(t, "Test Pipeline", updateData.pipelineName)
	assert.Equal(t, "fake-cluster-id", updateData.clusterId)
	assert.Equal(t, "abcde", updateData.updateId)

	// Verify the creation time is set to the state on the existing update
	assert.Equal(t, now, updateData.creationTime)

	// Verify the times have not been set
	assert.True(t, updateData.waitStartTime.IsZero())
	assert.True(t, updateData.startTime.IsZero())
	assert.True(t, updateData.completionTime.IsZero())

	// Verify the state is set to the state on the existing update indicating
	// the passed state was ignored
	assert.Equal(
		t,
		databricksSdkPipelines.UpdateInfoStateRunning,
		updateData.state,
	)
}

func TestGetOrCreateUpdateDataForUpdateProgress_WaitStartTime(t *testing.T) {
	// Setup now
	now := time.Now()

	// Setup times
	waitStartTime := now

	// Setup mock update map
	updates := map[string]*updateData{
		"abcde": {
			eventData: eventData{
				pipelineId:   "12345",
				pipelineName: "Test Pipeline",
				clusterId:    "fake-cluster-id",
				updateId:     "abcde",
			},
			creationTime:     now,
			state:            databricksSdkPipelines.UpdateInfoStateRunning,
		},
	}

	// Setup mock origin
	origin := &databricksSdkPipelines.Origin{
		PipelineId:   "12345",
		PipelineName: "Test Pipeline",
		ClusterId:    "fake-cluster-id",
		UpdateId:     "abcde",
	}

	// Execute the function under test
	updateData := getOrCreateUpdateDataForUpdateProgress(
		updates,
		origin,
		waitStartTime,
		time.Time{},
		time.Time{},
		databricksSdkPipelines.UpdateInfoStateStopping,
	)

	// Verify result
	assert.NotNil(t, updateData)
	assert.Equal(t, "12345", updateData.pipelineId)
	assert.Equal(t, "Test Pipeline", updateData.pipelineName)
	assert.Equal(t, "fake-cluster-id", updateData.clusterId)
	assert.Equal(t, "abcde", updateData.updateId)

	// Verify the creation time is set to the state on the existing update
	assert.Equal(t, now, updateData.creationTime)

	// Verify the the wait start time has been set and the others are zero
	assert.Equal(t, now, updateData.waitStartTime)
	assert.True(t, updateData.startTime.IsZero())
	assert.True(t, updateData.completionTime.IsZero())

	// Verify the state is set to the state on the existing update indicating
	// the passed state was ignored
	assert.Equal(
		t,
		databricksSdkPipelines.UpdateInfoStateRunning,
		updateData.state,
	)
}

func TestGetOrCreateUpdateDataForUpdateProgress_StartTime(t *testing.T) {
	// Setup now
	now := time.Now()

	// Setup times
	startTime := now

	// Setup mock update map
	updates := map[string]*updateData{
		"abcde": {
			eventData: eventData{
				pipelineId:   "12345",
				pipelineName: "Test Pipeline",
				clusterId:    "fake-cluster-id",
				updateId:     "abcde",
			},
			creationTime:     now,
			state:            databricksSdkPipelines.UpdateInfoStateRunning,
		},
	}

	// Setup mock origin
	origin := &databricksSdkPipelines.Origin{
		PipelineId:   "12345",
		PipelineName: "Test Pipeline",
		ClusterId:    "fake-cluster-id",
		UpdateId:     "abcde",
	}

	// Execute the function under test
	updateData := getOrCreateUpdateDataForUpdateProgress(
		updates,
		origin,
		time.Time{},
		startTime,
		time.Time{},
		databricksSdkPipelines.UpdateInfoStateStopping,
	)

	// Verify result
	assert.NotNil(t, updateData)
	assert.Equal(t, "12345", updateData.pipelineId)
	assert.Equal(t, "Test Pipeline", updateData.pipelineName)
	assert.Equal(t, "fake-cluster-id", updateData.clusterId)
	assert.Equal(t, "abcde", updateData.updateId)

	// Verify the creation time is set to the state on the existing update
	assert.Equal(t, now, updateData.creationTime)

	// Verify the start time has been set and the others are zero
	assert.True(t, updateData.waitStartTime.IsZero())
	assert.Equal(t, now, updateData.startTime)
	assert.True(t, updateData.completionTime.IsZero())

	// Verify the state is set to the state on the existing update indicating
	// the passed state was ignored
	assert.Equal(
		t,
		databricksSdkPipelines.UpdateInfoStateRunning,
		updateData.state,
	)
}

func TestGetOrCreateUpdateDataForUpdateProgress_CompletionTime(t *testing.T) {
	// Setup now
	now := time.Now()

	// Setup times
	completionTime := now

	// Setup mock update map
	updates := map[string]*updateData{
		"abcde": {
			eventData: eventData{
				pipelineId:   "12345",
				pipelineName: "Test Pipeline",
				clusterId:    "fake-cluster-id",
				updateId:     "abcde",
			},
			creationTime:     now,
			state:            databricksSdkPipelines.UpdateInfoStateRunning,
		},
	}

	// Setup mock origin
	origin := &databricksSdkPipelines.Origin{
		PipelineId:   "12345",
		PipelineName: "Test Pipeline",
		ClusterId:    "fake-cluster-id",
		UpdateId:     "abcde",
	}

	// Execute the function under test
	updateData := getOrCreateUpdateDataForUpdateProgress(
		updates,
		origin,
		time.Time{},
		time.Time{},
		completionTime,
		databricksSdkPipelines.UpdateInfoStateStopping,
	)

	// Verify result
	assert.NotNil(t, updateData)
	assert.Equal(t, "12345", updateData.pipelineId)
	assert.Equal(t, "Test Pipeline", updateData.pipelineName)
	assert.Equal(t, "fake-cluster-id", updateData.clusterId)
	assert.Equal(t, "abcde", updateData.updateId)

	// Verify the creation time is set to the state on the existing update
	assert.Equal(t, now, updateData.creationTime)

	// Verify the completion time has been set and the others are zero
	assert.True(t, updateData.waitStartTime.IsZero())
	assert.True(t, updateData.startTime.IsZero())
	assert.Equal(t, now, updateData.completionTime)

	// Verify the state is set to the state on the existing update indicating
	// the passed state was ignored
	assert.Equal(
		t,
		databricksSdkPipelines.UpdateInfoStateRunning,
		updateData.state,
	)
}

func TestGetOrCreateUpdateDataForUpdateProgress_Status(t *testing.T) {
	// Setup now
	now := time.Now()

	// Setup mock update map
	updates := map[string]*updateData{
		"abcde": {
			eventData: eventData{
				pipelineId:   "12345",
				pipelineName: "Test Pipeline",
				clusterId:    "fake-cluster-id",
				updateId:     "abcde",
			},
			creationTime:     now,
		},
	}

	// Setup mock origin
	origin := &databricksSdkPipelines.Origin{
		PipelineId:   "12345",
		PipelineName: "Test Pipeline",
		ClusterId:    "fake-cluster-id",
		UpdateId:     "abcde",
	}

	// Execute the function under test
	updateData := getOrCreateUpdateDataForUpdateProgress(
		updates,
		origin,
		time.Time{},
		time.Time{},
		time.Time{},
		databricksSdkPipelines.UpdateInfoStateStopping,
	)

	// Verify result
	assert.NotNil(t, updateData)
	assert.Equal(t, "12345", updateData.pipelineId)
	assert.Equal(t, "Test Pipeline", updateData.pipelineName)
	assert.Equal(t, "fake-cluster-id", updateData.clusterId)
	assert.Equal(t, "abcde", updateData.updateId)

	// Verify the creation time is set to the state on the existing update
	assert.Equal(t, now, updateData.creationTime)

	// Verify the times have not been set
	assert.True(t, updateData.waitStartTime.IsZero())
	assert.True(t, updateData.startTime.IsZero())
	assert.True(t, updateData.completionTime.IsZero())

	// Verify the state is set to the passed state
	assert.Equal(
		t,
		databricksSdkPipelines.UpdateInfoStateStopping,
		updateData.state,
	)
}

func TestIsUpdateTerminated(t *testing.T) {
	tests := []struct{
		name   string
		state databricksSdkPipelines.UpdateInfoState
		want   bool
	}{
		{
			name: "completed",
			state: databricksSdkPipelines.UpdateInfoStateCompleted,
			want: true,
		},
		{
			name: "canceled",
			state: databricksSdkPipelines.UpdateInfoStateCanceled,
			want: true,
		},
		{
			name: "failed",
			state: databricksSdkPipelines.UpdateInfoStateFailed,
			want: true,
		},
		{
			name: "running",
			state: databricksSdkPipelines.UpdateInfoStateRunning,
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isUpdateTerminated(tt.state)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestIsTargetEventType(t *testing.T) {
	tests := []struct{
		eventType   string
		want        bool
	}{
		{
			eventType: "create_update",
			want:     true,
		},
		{
			eventType: "update_progress",
			want:     true,
		},
		{
			eventType: "flow_progress",
			want:     true,
		},
		{
			eventType: "other_event",
			want:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.eventType, func(t *testing.T) {
			got := isTargetEventType(tt.eventType)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestNewDatabricksPipelineMetricsReceiver_ValidParams(t *testing.T) {
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

	// Mock now
	now := time.Now()
	Now = func () time.Time { return now }

	// Setup mock startOffset
	startOffset := time.Duration(60) * time.Second

	// Setup mock intervalOffset
	intervalOffset := time.Duration(5) * time.Second

	// Execute the function under test
	receiver := NewDatabricksPipelineMetricsReceiver(
		mockIntegration,
		mockWorkspace,
		startOffset,
		intervalOffset,
		tags,
	)

	// Verify result
	assert.NotNil(t, receiver)
	assert.Equal(t, mockIntegration, receiver.i)
	assert.Equal(t, mockWorkspace, receiver.w)
	assert.Equal(t, startOffset, receiver.startOffset)
	assert.Equal(t, intervalOffset, receiver.intervalOffset)
	assert.Equal(t, tags, receiver.tags)
	assert.Equal(
		t,
		now.UTC().Add(-mockIntegration.Interval * time.Second),
		receiver.lastRun,
	)
}

func TestPipelineMetricsReceiver_GetId(t *testing.T) {
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

	// Setup mock intervalOffset
	intervalOffset := time.Duration(5) * time.Second

	// Execute the function under test
	receiver := NewDatabricksPipelineMetricsReceiver(
		mockIntegration,
		mockWorkspace,
		startOffset,
		intervalOffset,
		tags,
	)

	// Execute the function under test
	id := receiver.GetId()

	// Verify result
	assert.Equal(t, "databricks-pipeline-metrics-receiver", id)
}

func TestPipelineMetricsReceiver_PollEvents_MakePipelineSummaryAttributesError(
	t *testing.T,
) {
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
	// makePipelineBaseAttributes will return an error so that
	// makePipelineSummaryAttributes will return an error
	GetWorkspaceInfo = func(
		ctx context.Context,
		w DatabricksWorkspace,
	) (
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

	// Mock now
	now := time.Now()
	Now = func () time.Time { return now }

	// Setup mock startOffset
	startOffset := time.Duration(60) * time.Second

	// Setup mock intervalOffset
	intervalOffset := time.Duration(5) * time.Second

	// Setup function call trackers
	hasNextCalled := 0
	nextCalled := 0

	// Setup a mock pipelines iterator
	mockIterator := &MockIterator[databricksSdkPipelines.PipelineStateInfo]{}

	// Setup the mock HasNext to increment the tracker and return false
	mockIterator.HasNextFunc = func(
		ctx context.Context,
	) bool {
		hasNextCalled += 1
		return false
	}

	// Setup the mock Next to increment the tracker and return a
	// PipelineStateInfo with zero values and no error. This should never be
	// called since HasNext returns false.
	mockIterator.NextFunc = func(
		ctx context.Context,
	) (databricksSdkPipelines.PipelineStateInfo, error) {
		nextCalled += 1
		return databricksSdkPipelines.PipelineStateInfo{}, nil
	}

	// Mock the ListPipelines method to return the empty iterator
	mockWorkspace.ListPipelinesFunc = func(
		ctx context.Context,
	) databricksSdkListing.Iterator[databricksSdkPipelines.PipelineStateInfo] {
		return mockIterator
	}

	// Setup an events channel to receive events
	eventsChan := make(chan model.Event, 500) // Buffer to prevent blocking

	// Create pipeline metric receiver instance
	receiver := NewDatabricksPipelineMetricsReceiver(
		mockIntegration,
		mockWorkspace,
		startOffset,
		intervalOffset,
		tags,
	)

	// Execute the function under test
	err := receiver.PollEvents(
		context.Background(),
		eventsChan,
	)

	// Close the channel to iterate through it
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
		"Expected no events to be produced when makePipelineSummaryAttributes returns an error",
	)
}

func TestPipelineMetricsReceiver_PollEvents_ListPipelinesError(t *testing.T) {
	// Setup mock workspace
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

	// Mock now
	now := time.Now()
	Now = func () time.Time { return now }

	// Setup mock startOffset
	startOffset := time.Duration(60) * time.Second

	// Setup mock intervalOffset
	intervalOffset := time.Duration(5) * time.Second

	// Setup mock pipeline state info
	pipelineStateInfo := databricksSdkPipelines.PipelineStateInfo{
		PipelineId: "12345",
		Name:       "Test Pipeline",
		ClusterId:  "fake-cluster-id",
		State:     databricksSdkPipelines.PipelineStateRunning,
	}

	// Setup function call trackers
	hasNextCalled := 0
	nextCalled := 0

	// Setup expected error message
	expectedError := "error listing pipelines"

	// Setup a mock iterator
	mockIterator := &MockIterator[databricksSdkPipelines.PipelineStateInfo]{}

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
	) (databricksSdkPipelines.PipelineStateInfo, error) {
		nextCalled += 1
		return pipelineStateInfo, errors.New(expectedError)
	}

	// Mock the ListPipelines function to return the empty iterator
	mockWorkspace.ListPipelinesFunc = func(
		ctx context.Context,
	) databricksSdkListing.Iterator[databricksSdkPipelines.PipelineStateInfo] {
		return mockIterator
	}

	// Setup an events channel to receive events
	eventsChan := make(chan model.Event, 500) // Buffer to prevent blocking

	// Create pipeline metric receiver instance
	receiver := NewDatabricksPipelineMetricsReceiver(
		mockIntegration,
		mockWorkspace,
		startOffset,
		intervalOffset,
		tags,
	)

	// Execute the function under test
	err := receiver.PollEvents(
		context.Background(),
		eventsChan,
	)

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
		"Expected no events to be produced when ListPipelines fails",
	)
}

func TestPipelineMetricsReceiver_PollEvents_ListPipelinesEmpty(t *testing.T) {
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

	// Mock now
	now := time.Now()
	Now = func () time.Time { return now }

	// Setup mock startOffset
	startOffset := time.Duration(60) * time.Second

	// Setup mock intervalOffset
	intervalOffset := time.Duration(5) * time.Second

	// Setup expected counters
	expectedPipelineCounters := &pipelineCounters{}
	expectedUpdateCounters := &updateCounters{}
	expectedFlowCounters := &flowCounters{}

	// Setup function call trackers
	hasNextCalled := 0
	nextCalled := 0

	// Setup a mock iterator
	mockIterator := &MockIterator[databricksSdkPipelines.PipelineStateInfo]{}

	// Setup the mock HasNext to increment the tracker and return false
	mockIterator.HasNextFunc = func(
		ctx context.Context,
	) bool {
		hasNextCalled += 1
		return false
	}

	// Setup the mock Next to increment the tracker and return a
	// PipelineStateInfo with zero values and no error. This should never be
	// called since HasNext returns false.
	mockIterator.NextFunc = func(
		ctx context.Context,
	) (databricksSdkPipelines.PipelineStateInfo, error) {
		nextCalled += 1
		return databricksSdkPipelines.PipelineStateInfo{}, nil
	}

	// Mock the ListPipelines function to return the empty iterator
	mockWorkspace.ListPipelinesFunc = func(
		ctx context.Context,
	) databricksSdkListing.Iterator[databricksSdkPipelines.PipelineStateInfo] {
		return mockIterator
	}

	// Setup an events channel to receive events
	eventsChan := make(chan model.Event, 500) // Buffer to prevent blocking

	// Create pipeline metric receiver instance
	receiver := NewDatabricksPipelineMetricsReceiver(
		mockIntegration,
		mockWorkspace,
		startOffset,
		intervalOffset,
		tags,
	)

	// Execute the function under test
	err := receiver.PollEvents(
		context.Background(),
		eventsChan,
	)

	// Close the channel to iterate through it
	close(eventsChan)

	// Verify result
	assert.NoError(t, err)
	assert.Equal(t, hasNextCalled, 1, "HasNext should have been called once")
	assert.Equal(t, nextCalled, 0, "Next should not have been called")

	// Collect all events for verification
	events := make([]model.Event, 0)
	for event := range eventsChan {
		events = append(events, event)
	}

	assert.Equal(
		t,
		1,
		len(events),
		"Expected pipeline summary event to be produced when ListPipelines returns no pipelines",
	)

	event := events[0]

	// Verify it's the pipeline summary event
	assert.Equal(
		t,
		"DatabricksPipelineSummary",
		event.Type,
		"Expected pipeline summary event type",
	)

	// Verify timestamp
	assert.Equal(
		t,
		now.UnixMilli(),
		event.Timestamp,
		"Should have the correct event timestamp",
	)

	// Verify attributes
	attrs := event.Attributes

	// Verify pipeline summary attributes
	verifyCommonAttributes(t, attrs)

	// Verify counts
	verifyPipelineSummaryAttributes(
		t,
		expectedPipelineCounters,
		expectedUpdateCounters,
		expectedFlowCounters,
		attrs,
	)
}

func TestPipelineMetricsReceiver_PollEvents_LastRunUpdated(t *testing.T) {
	// Setup mock workspace
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

	// Mock now
	now := time.Now()
	Now = func () time.Time { return now }

	// Setup mock startOffset
	startOffset := time.Duration(60) * time.Second

	// Setup mock intervalOffset
	intervalOffset := time.Duration(5) * time.Second

	// Setup expected counters
	expectedPipelineCounters := &pipelineCounters{}
	expectedUpdateCounters := &updateCounters{}
	expectedFlowCounters := &flowCounters{}

	// Setup function call trackers
	hasNextCalled := 0
	nextCalled := 0

	// Setup a mock iterator
	mockIterator := &MockIterator[databricksSdkPipelines.PipelineStateInfo]{}

	// Setup the mock HasNext to increment the tracker and return false
	mockIterator.HasNextFunc = func(
		ctx context.Context,
	) bool {
		hasNextCalled += 1
		return false
	}

	// Setup the mock Next to increment the tracker and return a
	// PipelineStateInfo with zero values and no error. This should never be
	// called since HasNext returns false.
	mockIterator.NextFunc = func(
		ctx context.Context,
	) (databricksSdkPipelines.PipelineStateInfo, error) {
		nextCalled += 1
		return databricksSdkPipelines.PipelineStateInfo{}, nil
	}

	// Mock the ListPipelines function to return the empty iterator
	mockWorkspace.ListPipelinesFunc = func(
		ctx context.Context,
	) databricksSdkListing.Iterator[databricksSdkPipelines.PipelineStateInfo] {
		return mockIterator
	}

	// Setup an events channel to receive events
	eventsChan := make(chan model.Event, 500) // Buffer to prevent blocking

	// Create pipeline metric receiver instance
	receiver := NewDatabricksPipelineMetricsReceiver(
		mockIntegration,
		mockWorkspace,
		startOffset,
		intervalOffset,
		tags,
	)

	// Verify that the initial lastRun timestamp is set correctly
	assert.Equal(
		t,
		now.UTC().Add(-mockIntegration.Interval * time.Second),
		receiver.lastRun,
	)

	// Now reset the mock Now function to simulate time progression
	now2 := now.Add(time.Minute)
	Now = func () time.Time { return now2 }

	// Execute the function under test
	err := receiver.PollEvents(
		context.Background(),
		eventsChan,
	)

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
		"Expected pipeline summary event to be produced when ListPipelines returns no pipelines",
	)

	event := events[0]

	// Verify it's the pipeline summary event
	assert.Equal(
		t,
		"DatabricksPipelineSummary",
		event.Type,
		"Expected pipeline summary event type",
	)

	// Verify timestamp
	assert.Equal(
		t,
		now2.UnixMilli(),
		event.Timestamp,
		"Should have the correct event timestamp",
	)

	// Verify attributes
	attrs := event.Attributes

	// Verify pipeline summary attributes
	verifyCommonAttributes(t, attrs)

	// Verify counts
	verifyPipelineSummaryAttributes(
		t,
		expectedPipelineCounters,
		expectedUpdateCounters,
		expectedFlowCounters,
		attrs,
	)
}

func TestPipelineMetricsReceiver_PollEvents_WithEvents(t *testing.T) {
	// Setup mock workspace
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

	// Mock now
	now := time.Now()
	Now = func () time.Time { return now }

	// Setup times to be used to test behavior of event production based on
	// lastRun time.
	lastRun := now.UTC().Add(-mockIntegration.Interval * time.Second)
	update1CreationTime := lastRun.Add(-10 * time.Second)
	update1WaitStartTime := lastRun.Add(-9 * time.Second)
	update2CreationTime := lastRun
	update2WaitStartTime := lastRun.Add(1 * time.Second)
	update2StartTime := lastRun.Add(2 * time.Second)
	update2CompletionTime := lastRun.Add(5 * time.Second)
	update2Flow1QueueStartTime := lastRun.Add(2 * time.Second)
	update2Flow1StartTime := lastRun.Add(3 * time.Second)
	update2Flow1CompletionTime := lastRun.Add(4 * time.Second)
	update3CompletionTime := lastRun.Add(5 * time.Second)
	update4CreationTime := lastRun.Add(-10 * time.Second)
	update4StartTime := lastRun.Add(-9 * time.Second)
	update4Flow1QueueStartTime := lastRun.Add(-8 * time.Second)
	update4Flow1StartTime := lastRun.Add(-7 * time.Second)
	update4Flow1CompletionTime := lastRun.Add(-6 * time.Second)
	update4Flow2QueueStartTime := lastRun.Add(-8 * time.Second)
	update4Flow2PlanStartTime := lastRun.Add(-6 * time.Second)
	update4Flow2StartTime := lastRun.Add(-5 * time.Second)
	update4Flow2CompletionTime := lastRun.Add(2 * time.Second)
	update4Flow3QueueStartTime := lastRun.Add(1 * time.Second)
	update4Flow3StartTime := lastRun.Add(2 * time.Second)
	update4Flow3CompletionTime := lastRun.Add(8 * time.Second)
	update4CompletionTime := lastRun.Add(25 * time.Second)
	update5CreationTime := lastRun.Add(26 * time.Second)
	update5StartTime := lastRun.Add(27 * time.Second)
	update5Flow1QueueStartTime := lastRun.Add(28 * time.Second)
	update5Flow1PlanStartTime := lastRun.Add(29 * time.Second)

	// Setup flow backlog and row metrics
	update2Flow1BacklogBytes := float64(1024)
	update2Flow1BacklogFiles := float64(10)
	update2Flow1NumOutputRows := float64(100)
	update2Flow1DroppedRecords := float64(5)
	update4Flow2BacklogBytes := float64(2048)
	update4Flow2BacklogFiles := float64(20)
	update4Flow2NumOutputRows := float64(200)
	update4Flow2DroppedRecords := float64(10)
	update4Flow3BacklogBytes := float64(4096)
	update4Flow3BacklogFiles := float64(40)
	update4Flow3NumOutputRows := float64(300)
	update4Flow3DroppedRecords := float64(15)

	// Setup expectation passed and failed records
	update4Flow2Expectation1PassedRecords := float64(100)
	update4Flow2Expectation1FailedRecords := float64(5)
	update4Flow2Expectation2PassedRecords := float64(200)
	update4Flow2Expectation2FailedRecords := float64(10)
	update4Flow3Expectation1PassedRecords := float64(300)
	update4Flow3Expectation1FailedRecords := float64(15)

	// Setup mock startOffset
	startOffset := time.Duration(60) * time.Second

	// Setup mock intervalOffset
	intervalOffset := time.Duration(5) * time.Second

	// Setup mock pipeline state info
	pipelines := []databricksSdkPipelines.PipelineStateInfo{
		{
			PipelineId: "12345",
			Name:       "Test Pipeline",
			ClusterId:  "fake-cluster-id",
			State:     databricksSdkPipelines.PipelineStateRunning,
		},
		{
			PipelineId: "34567",
			Name:       "Test Pipeline 2",
			ClusterId:  "fake-cluster-id",
			State:     databricksSdkPipelines.PipelineStateIdle,
		},
		{
			PipelineId: "56789",
			Name:       "Test Pipeline 3",
			ClusterId:  "fake-cluster-id",
			State:     databricksSdkPipelines.PipelineStateIdle,
		},
		{
			PipelineId: "67890",
			Name:       "Test Pipeline 4",
			ClusterId:  "fake-cluster-id",
			State:     databricksSdkPipelines.PipelineStateRunning,
		},
	}

	// Setup hasNext controls
	index := 0
	eventsIndex1 := 0
	eventsIndex2 := 0
	eventsIndex3 := 0
	eventsIndex4 := 0

	// Setup mock origins for each update
	originUpdate1 := &databricksSdkPipelines.Origin{
		PipelineId:   "12345",
		PipelineName: "Test Pipeline",
		ClusterId:    "fake-cluster-id",
		UpdateId:     "update-1",
	}
	originUpdate3 := &databricksSdkPipelines.Origin{
		PipelineId:   "34567",
		PipelineName: "Test Pipeline 2",
		ClusterId:    "fake-cluster-id",
		UpdateId:     "update-3",
	}
	originUpdate2 := &databricksSdkPipelines.Origin{
		PipelineId:   "56789",
		PipelineName: "Test Pipeline 3",
		ClusterId:    "fake-cluster-id",
		UpdateId:     "update-2",
	}
	originUpdate2Flow1 := &databricksSdkPipelines.Origin{
		PipelineId:   "56789",
		PipelineName: "Test Pipeline 3",
		ClusterId:    "fake-cluster-id",
		UpdateId:     "update-2",
		FlowId:       "flow-1",
		FlowName:     "flow_1",
	}
	originUpdate4 := &databricksSdkPipelines.Origin{
		PipelineId:   "67890",
		PipelineName: "Test Pipeline 4",
		ClusterId:    "fake-cluster-id",
		UpdateId:     "update-4",
	}
	originUpdate4Flow1 := &databricksSdkPipelines.Origin{
		PipelineId:   "67890",
		PipelineName: "Test Pipeline 4",
		ClusterId:    "fake-cluster-id",
		UpdateId:     "update-4",
		FlowId:       "flow-1",
		FlowName:     "flow_1",
	}
	originUpdate4Flow2 := &databricksSdkPipelines.Origin{
		PipelineId:   "67890",
		PipelineName: "Test Pipeline 4",
		ClusterId:    "fake-cluster-id",
		UpdateId:     "update-4",
		FlowId:       "flow-2",
		FlowName:     "flow_2",
	}
	originUpdate4Flow3 := &databricksSdkPipelines.Origin{
		PipelineId:   "67890",
		PipelineName: "Test Pipeline 4",
		ClusterId:    "fake-cluster-id",
		UpdateId:     "update-4",
		FlowId:       "flow-3",
		FlowName:     "flow_3",
	}
	originUpdate5 := &databricksSdkPipelines.Origin{
		PipelineId:   "67890",
		PipelineName: "Test Pipeline 4",
		ClusterId:    "fake-cluster-id",
		UpdateId:     "update-5",
	}
	originUpdate5Flow1 := &databricksSdkPipelines.Origin{
		PipelineId:   "67890",
		PipelineName: "Test Pipeline 4",
		ClusterId:    "fake-cluster-id",
		UpdateId:     "update-5",
		FlowId:       "flow-1",
		FlowName:     "flow_1",
	}

	// Setup mock pipeline events for pipeline 1
	// This pipeline has one update that was created before the last run and is
	// waiting for resources.  It produces no events.
	pipelineEvents1 := []PipelineEvent{
		// This event should not produce an update start event because it occurs
		// before the last run.
		{
			EventType: "create_update",
			Id: "update-1-event-1",
			Origin: originUpdate1,
			Timestamp: update1CreationTime.Format(RFC_3339_MILLI_LAYOUT),
			Details: &PipelineEventDetails{},
		},
		{
			EventType: "update_progress",
			Id: "update-1-event-2",
			Origin: originUpdate1,
			Timestamp: update1WaitStartTime.Format(RFC_3339_MILLI_LAYOUT),
			Details: &PipelineEventDetails{
				UpdateProgress: &UpdateProgressDetails{
					State: databricksSdkPipelines.UpdateInfoStateWaitingForResources,
				},
			},
		},
	}

	// Setup mock pipeline events for pipeline 2
	// This pipeline has one update that completed after the last run but since
	// it has no corresponding create_update event, it will be skipped. This
	// simulates when the create_update event may be missing due to it being
	// created prior to the startOffset. It produces no events.
	pipelineEvents2 := []PipelineEvent{
		// This event should not produce a complete event even though it occurs
		// after (at) the last run because no corresponding create_update event
		// exists and so it will have no creationTime so addUpdates should skip
		// it.
		{
			EventType: "update_progress",
			Id: "update-3-event-1",
			Origin: originUpdate3,
			Timestamp: update3CompletionTime.Format(RFC_3339_MILLI_LAYOUT),
			Details: &PipelineEventDetails{
				UpdateProgress: &UpdateProgressDetails{
					State: databricksSdkPipelines.UpdateInfoStateCompleted,
				},
			},
		},
	}

	// Setup mock pipeline events for pipeline 3
	// This pipeline has one update with one flow. The entire update occurs
	// after the last run so it should produce an update start and complete
	// event and a flow start and complete event, a total of 4 events.
	pipelineEvents3 := []PipelineEvent{
		// This event should produce an update start event because it occurs
		// after the last run.
		{
			EventType: "create_update",
			Id: "update-2-event-1",
			Origin: originUpdate2,
			Timestamp: update2CreationTime.Format(RFC_3339_MILLI_LAYOUT),
			Details: &PipelineEventDetails{},
		},
		{
			EventType: "update_progress",
			Id: "update-2-event-2",
			Origin: originUpdate2,
			Timestamp: update2WaitStartTime.Format(RFC_3339_MILLI_LAYOUT),
			Details: &PipelineEventDetails{
				UpdateProgress: &UpdateProgressDetails{
					State: databricksSdkPipelines.UpdateInfoStateWaitingForResources,
				},
			},
		},
		{
			EventType: "update_progress",
			Id: "update-2-event-3",
			Origin: originUpdate2,
			Timestamp: update2StartTime.Format(RFC_3339_MILLI_LAYOUT),
			Details: &PipelineEventDetails{
				UpdateProgress: &UpdateProgressDetails{
					State: databricksSdkPipelines.UpdateInfoStateInitializing,
				},
			},
		},
		{
			EventType: "update_progress",
			Id: "update-2-event-4",
			Origin: originUpdate2,
			Timestamp: update2StartTime.Format(RFC_3339_MILLI_LAYOUT),
			Details: &PipelineEventDetails{
				UpdateProgress: &UpdateProgressDetails{
					State: databricksSdkPipelines.UpdateInfoStateRunning,
				},
			},
		},
		// This event should produce a flow start event because it occurs after
		// the last run.
		{
			EventType: "flow_progress",
			Id: "update-2-event-5",
			Origin: originUpdate2Flow1,
			Timestamp: update2Flow1QueueStartTime.Format(RFC_3339_MILLI_LAYOUT),
			Details: &PipelineEventDetails{
				FlowProgress: &FlowProgressDetails{
					Status: FlowInfoStatusQueued,
				},
			},
		},
		{
			EventType: "flow_progress",
			Id: "update-2-event-6",
			Origin: originUpdate2Flow1,
			Timestamp: update2Flow1StartTime.Format(RFC_3339_MILLI_LAYOUT),
			Details: &PipelineEventDetails{
				FlowProgress: &FlowProgressDetails{
					Status: FlowInfoStatusStarting,
				},
			},
		},
		{
			EventType: "flow_progress",
			Id: "update-2-event-7",
			Origin: originUpdate2Flow1,
			Timestamp: update2Flow1StartTime.Format(RFC_3339_MILLI_LAYOUT),
			Details: &PipelineEventDetails{
				FlowProgress: &FlowProgressDetails{
					Status: FlowInfoStatusRunning,
				},
			},
		},
		// This event should produce a flow complete event because it occurs
		// after the last run.
		{
			EventType: "flow_progress",
			Id: "update-2-event-8",
			Origin: originUpdate2Flow1,
			Timestamp: update2Flow1CompletionTime.Format(RFC_3339_MILLI_LAYOUT),
			Details: &PipelineEventDetails{
				FlowProgress: &FlowProgressDetails{
					Status: FlowInfoStatusCompleted,
					Metrics: &FlowProgressMetrics{
						BacklogBytes: &update2Flow1BacklogBytes,
						BacklogFiles: &update2Flow1BacklogFiles,
						NumOutputRows: &update2Flow1NumOutputRows,
					},
					DataQuality: &FlowProgressDataQuality{
						DroppedRecords: &update2Flow1DroppedRecords,
					},
				},
			},
		},
		// This event should produce an update complete event because it occurs
		// after the last run.
		{
			EventType: "update_progress",
			Id: "update-2-event-9",
			Origin: originUpdate2,
			Timestamp: update2CompletionTime.Format(RFC_3339_MILLI_LAYOUT),
			Details: &PipelineEventDetails{
				UpdateProgress: &UpdateProgressDetails{
					State: databricksSdkPipelines.UpdateInfoStateCompleted,
				},
			},
		},
	}

	// Setup mock events for pipeline 4
	// This pipeline has two updates. The first update starts before the last
	// run and completes after the last run so produces 1 event. It has 3 flows.
	// The first flow starts and ends before the last run so produces no events.
	// The second flow starts before the last run but ends after the last run
	// and produces 1 event. It also produces 2 flow expectation events. The
	// third flow starts and ends after the last run so produces 2 events. It
	// also produces 1 flow expectation event. The entire update produces 7
	// events. The second update starts after the last run but has not completed
	// so it produces 1 event. It has one flow which starts after the last run
	// but also has not completed so it produces 1 event. This update produces 2
	// events. This pipeline also test the scenario where parts of two updates
	// have occurred after the last run.
	pipelineEvents4 := []PipelineEvent{
		// This event should not produce an update start event because it occurs
		// before the last run.
		{
			EventType: "create_update",
			Id: "update-4-event-1",
			Origin: originUpdate4,
			Timestamp: update4CreationTime.Format(RFC_3339_MILLI_LAYOUT),
			Details: &PipelineEventDetails{},
		},
		{
			EventType: "update_progress",
			Id: "update-4-event-2",
			Origin: originUpdate4,
			Timestamp: update4StartTime.Format(RFC_3339_MILLI_LAYOUT),
			Details: &PipelineEventDetails{
				UpdateProgress: &UpdateProgressDetails{
					State: databricksSdkPipelines.UpdateInfoStateInitializing,
				},
			},
		},
		{
			EventType: "update_progress",
			Id: "update-4-event-3",
			Origin: originUpdate4,
			Timestamp: update4StartTime.Format(RFC_3339_MILLI_LAYOUT),
			Details: &PipelineEventDetails{
				UpdateProgress: &UpdateProgressDetails{
					State: databricksSdkPipelines.UpdateInfoStateRunning,
				},
			},
		},
		// This event should not produce a flow start event because it occurs
		// before the last run.
		{
			EventType: "flow_progress",
			Id: "update-4-event-4",
			Origin: originUpdate4Flow1,
			Timestamp: update4Flow1QueueStartTime.Format(RFC_3339_MILLI_LAYOUT),
			Details: &PipelineEventDetails{
				FlowProgress: &FlowProgressDetails{
					Status: FlowInfoStatusQueued,
				},
			},
		},
		// This event should not produce a flow start event because it occurs
		// before the last run.
		{
			EventType: "flow_progress",
			Id: "update-4-event-5",
			Origin: originUpdate4Flow2,
			Timestamp: update4Flow2QueueStartTime.Format(RFC_3339_MILLI_LAYOUT),
			Details: &PipelineEventDetails{
				FlowProgress: &FlowProgressDetails{
					Status: FlowInfoStatusQueued,
				},
			},
		},
		{
			EventType: "flow_progress",
			Id: "update-4-event-6",
			Origin: originUpdate4Flow1,
			Timestamp: update4Flow1StartTime.Format(RFC_3339_MILLI_LAYOUT),
			Details: &PipelineEventDetails{
				FlowProgress: &FlowProgressDetails{
					Status: FlowInfoStatusStarting,
				},
			},
		},
		{
			EventType: "flow_progress",
			Id: "update-4-event-7",
			Origin: originUpdate4Flow1,
			Timestamp: update4Flow1StartTime.Format(RFC_3339_MILLI_LAYOUT),
			Details: &PipelineEventDetails{
				FlowProgress: &FlowProgressDetails{
					Status: FlowInfoStatusRunning,
				},
			},
		},
		// This event should not produce a flow complete event because it occurs
		// before the last run.
		{
			EventType: "flow_progress",
			Id: "update-4-event-8",
			Origin: originUpdate4Flow1,
			Timestamp: update4Flow1CompletionTime.Format(RFC_3339_MILLI_LAYOUT),
			Details: &PipelineEventDetails{
				FlowProgress: &FlowProgressDetails{
					Status: FlowInfoStatusCompleted,
				},
			},
		},
		{
			EventType: "flow_progress",
			Id: "update-4-event-9",
			Origin: originUpdate4Flow2,
			Timestamp: update4Flow2PlanStartTime.Format(RFC_3339_MILLI_LAYOUT),
			Details: &PipelineEventDetails{
				FlowProgress: &FlowProgressDetails{
					Status: FlowInfoStatusPlanning,
				},
			},
		},
		{
			EventType: "flow_progress",
			Id: "update-4-event-10",
			Origin: originUpdate4Flow2,
			Timestamp: update4Flow2StartTime.Format(RFC_3339_MILLI_LAYOUT),
			Details: &PipelineEventDetails{
				FlowProgress: &FlowProgressDetails{
					Status: FlowInfoStatusStarting,
				},
			},
		},
		{
			EventType: "flow_progress",
			Id: "update-4-event-11",
			Origin: originUpdate4Flow2,
			Timestamp: update4Flow2StartTime.Format(RFC_3339_MILLI_LAYOUT),
			Details: &PipelineEventDetails{
				FlowProgress: &FlowProgressDetails{
					Status: FlowInfoStatusRunning,
				},
			},
		},
		// This event should produce a flow start event because it occurs after
		// the last run.
		{
			EventType: "flow_progress",
			Id: "update-4-event-12",
			Origin: originUpdate4Flow3,
			Timestamp: update4Flow3QueueStartTime.Format(RFC_3339_MILLI_LAYOUT),
			Details: &PipelineEventDetails{
				FlowProgress: &FlowProgressDetails{
					Status: FlowInfoStatusQueued,
				},
			},
		},
		// This event should produce a flow complete event because it occurs
		// after the last run. It should also produce two flow expectation
		// events.
		{
			EventType: "flow_progress",
			Id: "update-4-event-13",
			Origin: originUpdate4Flow2,
			Timestamp: update4Flow2CompletionTime.Format(RFC_3339_MILLI_LAYOUT),
			Details: &PipelineEventDetails{
				FlowProgress: &FlowProgressDetails{
					Status: FlowInfoStatusCompleted,
					Metrics: &FlowProgressMetrics{
						BacklogBytes: &update4Flow2BacklogBytes,
						BacklogFiles: &update4Flow2BacklogFiles,
						NumOutputRows: &update4Flow2NumOutputRows,
					},
					DataQuality: &FlowProgressDataQuality{
						DroppedRecords: &update4Flow2DroppedRecords,
						Expectations: []FlowProgressExpectations{
							{
								Name: "expectation-1",
								Dataset: "dataset-1",
								PassedRecords: update4Flow2Expectation1PassedRecords,
								FailedRecords: update4Flow2Expectation1FailedRecords,
							},
							{
								Name: "expectation-2",
								Dataset: "dataset-2",
								PassedRecords: update4Flow2Expectation2PassedRecords,
								FailedRecords: update4Flow2Expectation2FailedRecords,
							},
						},
					},
				},
			},
		},
		{
			EventType: "flow_progress",
			Id: "update-4-event-14",
			Origin: originUpdate4Flow3,
			Timestamp: update4Flow3StartTime.Format(RFC_3339_MILLI_LAYOUT),
			Details: &PipelineEventDetails{
				FlowProgress: &FlowProgressDetails{
					Status: FlowInfoStatusStarting,
				},
			},
		},
		{
			EventType: "flow_progress",
			Id: "update-4-event-15",
			Origin: originUpdate4Flow3,
			Timestamp: update4Flow3StartTime.Format(RFC_3339_MILLI_LAYOUT),
			Details: &PipelineEventDetails{
				FlowProgress: &FlowProgressDetails{
					Status: FlowInfoStatusRunning,
				},
			},
		},
		// This event should produce a flow complete event because it occurs
		// after the last run. It should also produce one flow expectation
		// event.
		{
			EventType: "flow_progress",
			Id: "update-4-event-16",
			Origin: originUpdate4Flow3,
			Timestamp: update4Flow3CompletionTime.Format(RFC_3339_MILLI_LAYOUT),
			Details: &PipelineEventDetails{
				FlowProgress: &FlowProgressDetails{
					Status: FlowInfoStatusCompleted,
					Metrics: &FlowProgressMetrics{
						BacklogBytes: &update4Flow3BacklogBytes,
						BacklogFiles: &update4Flow3BacklogFiles,
						NumOutputRows: &update4Flow3NumOutputRows,
					},
					DataQuality: &FlowProgressDataQuality{
						DroppedRecords: &update4Flow3DroppedRecords,
						Expectations: []FlowProgressExpectations{
							{
								Name: "expectation-1",
								Dataset: "dataset-1",
								PassedRecords: update4Flow3Expectation1PassedRecords,
								FailedRecords: update4Flow3Expectation1FailedRecords,
							},
						},
					},
				},
			},
		},
		// This event should produce an update complete event because it occurs
		// after the last run.
		{
			EventType: "update_progress",
			Id: "update-4-event-17",
			Origin: originUpdate4,
			Timestamp: update4CompletionTime.Format(RFC_3339_MILLI_LAYOUT),
			Details: &PipelineEventDetails{
				UpdateProgress: &UpdateProgressDetails{
					State: databricksSdkPipelines.UpdateInfoStateCompleted,
				},
			},
		},
		// This event should produce an update start event because it occurs
		// after the last run.
		{
			EventType: "create_update",
			Id: "update-5-event-1",
			Origin: originUpdate5,
			Timestamp: update5CreationTime.Format(RFC_3339_MILLI_LAYOUT),
			Details: &PipelineEventDetails{},
		},
		{
			EventType: "update_progress",
			Id: "update-5-event-2",
			Origin: originUpdate5,
			Timestamp: update5StartTime.Format(RFC_3339_MILLI_LAYOUT),
			Details: &PipelineEventDetails{
				UpdateProgress: &UpdateProgressDetails{
					State: databricksSdkPipelines.UpdateInfoStateInitializing,
				},
			},
		},
		{
			EventType: "update_progress",
			Id: "update-5-event-3",
			Origin: originUpdate5,
			Timestamp: update5StartTime.Format(RFC_3339_MILLI_LAYOUT),
			Details: &PipelineEventDetails{
				UpdateProgress: &UpdateProgressDetails{
					State: databricksSdkPipelines.UpdateInfoStateRunning,
				},
			},
		},
		// This event should produce a flow start event because it occurs after
		// the last run.
		{
			EventType: "flow_progress",
			Id: "update-5-event-4",
			Origin: originUpdate5Flow1,
			Timestamp: update5Flow1QueueStartTime.Format(RFC_3339_MILLI_LAYOUT),
			Details: &PipelineEventDetails{
				FlowProgress: &FlowProgressDetails{
					Status: FlowInfoStatusQueued,
				},
			},
		},
		{
			EventType: "flow_progress",
			Id: "update-5-event-5",
			Origin: originUpdate5Flow1,
			Timestamp: update5Flow1PlanStartTime.Format(RFC_3339_MILLI_LAYOUT),
			Details: &PipelineEventDetails{
				FlowProgress: &FlowProgressDetails{
					Status: FlowInfoStatusPlanning,
				},
			},
		},
	}

	// Setup the first mock events iterator for pipeline 1
	mockPipelineEventsIterator1 := &MockIterator[PipelineEvent]{}

	// Setup the first mock events iterator HasNext to increment the events
	// index
	mockPipelineEventsIterator1.HasNextFunc = func(
		ctx context.Context,
	) bool {
		eventsIndex1 += 1
		return eventsIndex1 <= len(pipelineEvents1)
	}

	// Setup the first mock events iterator Next to return the next event
	// in reverse order to simulate the way events are returned by the API
	// (descending by timestamp)
	mockPipelineEventsIterator1.NextFunc = func(
		ctx context.Context,
	) (PipelineEvent, error) {
		index := len(pipelineEvents1) - eventsIndex1
		return pipelineEvents1[index], nil
	}

	// Setup the second mock events iterator for pipeline 2
	mockPipelineEventsIterator2 := &MockIterator[PipelineEvent]{}

	// Setup the second mock events iterator HasNext to increment the events
	// index
	mockPipelineEventsIterator2.HasNextFunc = func(
		ctx context.Context,
	) bool {
		eventsIndex2 += 1
		return eventsIndex2 <= len(pipelineEvents2)
	}

	// Setup the second mock events iterator Next to return the next event
	// in reverse order to simulate the way events are returned by the API
	// (descending by timestamp)
	mockPipelineEventsIterator2.NextFunc = func(
		ctx context.Context,
	) (PipelineEvent, error) {
		index := len(pipelineEvents2) - eventsIndex2
		return pipelineEvents2[index], nil
	}

	// Setup the third mock events iterator for pipeline 3
	mockPipelineEventsIterator3 := &MockIterator[PipelineEvent]{}

	// Setup the third mock events iterator HasNext to increment the events
	// index
	mockPipelineEventsIterator3.HasNextFunc = func(
		ctx context.Context,
	) bool {
		eventsIndex3 += 1
		return eventsIndex3 <= len(pipelineEvents3)
	}

	// Setup the third mock events iterator Next to return the next event
	// in reverse order to simulate the way events are returned by the API
	// (descending by timestamp)
	mockPipelineEventsIterator3.NextFunc = func(
		ctx context.Context,
	) (PipelineEvent, error) {
		index := len(pipelineEvents3) - eventsIndex3
		return pipelineEvents3[index], nil
	}

	// Setup the fourth mock events iterator for pipeline 4
	mockPipelineEventsIterator4 := &MockIterator[PipelineEvent]{}

	// Setup the fourth mock events iterator HasNext to increment the events
	// index
	mockPipelineEventsIterator4.HasNextFunc = func(
		ctx context.Context,
	) bool {
		eventsIndex4 += 1
		return eventsIndex4 <= len(pipelineEvents4)
	}

	// Setup the fourth mock events iterator Next to return the next event
	// in reverse order to simulate the way events are returned by the API
	// (descending by timestamp)
	mockPipelineEventsIterator4.NextFunc = func(
		ctx context.Context,
	) (PipelineEvent, error) {
		index := len(pipelineEvents4) - eventsIndex4
		return pipelineEvents4[index], nil
	}

	// Mock the ListPipelineEventsWithDetails method to return the appropriate
	// iterator
	mockWorkspace.ListPipelineEventsWithDetailsFunc = func(
		ctx context.Context,
		startOffset time.Duration,
		pipelineId string,
	) databricksSdkListing.Iterator[PipelineEvent] {
		if pipelineId == "12345" {
			return mockPipelineEventsIterator1
		} else if pipelineId == "34567" {
			return mockPipelineEventsIterator2
		} else if pipelineId == "56789" {
			return mockPipelineEventsIterator3
		}

		return mockPipelineEventsIterator4
	}

	// Setup a mock iterator for ListPipelines
	mockPipelineIterator := &MockIterator[databricksSdkPipelines.PipelineStateInfo]{}

	// Setup the mock HasNext to return true when there are more pipelines to
	// iterate over.
	mockPipelineIterator.HasNextFunc = func(
		ctx context.Context,
	) bool {
		index += 1
		return index <= len(pipelines)
	}

	// Setup the mock Next to return the next pipeline
	mockPipelineIterator.NextFunc = func(
		ctx context.Context,
	) (databricksSdkPipelines.PipelineStateInfo, error) {
		return pipelines[index - 1], nil
	}

	// Mock the ListPipelines method to return the mock pipeline iterator
	mockWorkspace.ListPipelinesFunc = func(
		ctx context.Context,
	) databricksSdkListing.Iterator[databricksSdkPipelines.PipelineStateInfo] {
		return mockPipelineIterator
	}

	// Setup an events channel to receive events
	eventsChan := make(chan model.Event, 500) // Buffer to prevent blocking

	// Create pipeline metric receiver instance
	receiver := NewDatabricksPipelineMetricsReceiver(
		mockIntegration,
		mockWorkspace,
		startOffset,
		intervalOffset,
		tags,
	)

	// Execute the function under test
	err := receiver.PollEvents(
		context.Background(),
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

	// Verify the number of events produced
	// We expect:
	// - No events for update-1
	// - No events for update-3
	// - For update-2
	//   - One update start event
	//   - One flow start event for flow-1
	//   - One flow complete event for flow-1
	//   - One update complete event
	//   - A total of 4 events
	// - For update-4
	//   - Two flow expectation events for flow-2
	//   - One flow complete event for flow-2
	//   - One flow start event for flow-3
	//   - One flow expectation event for flow-3
	//   - One flow complete event for flow-3
	//   - One update complete event
	//   - A total of 7 events
	// - For update-5
	//   - One update start event
	//   - One flow start event for flow-1
	//   - A total of 2 events
	// - One pipeline summary event
	// This gives us a total of 14 events
	assert.Equal(
		t,
		14,
		len(events),
		"Expected fourteen events to be produced",
	)

	// These should stay false
	foundUpdate1Start := false
	foundUpdate1Complete := false
	// These should flip to true
	foundUpdate2Start := false
	foundUpdate2Flow1Start := false
	foundUpdate2Flow1Complete := false
	foundUpdate2Complete := false
	// These should stay false
	foundUpdate3Start := false
	foundUpdate3Complete := false
	// These should stay false
	foundUpdate4Start := false
	foundUpdate4Flow1Start := false
	foundUpdate4Flow1Complete := false
	foundUpdate4Flow2Start := false
	// These should flip to true
	foundUpdate4Flow2Expectation1 := false
	foundUpdate4Flow2Expectation2 := false
	foundUpdate4Flow2Complete := false
	foundUpdate4Flow3Start := false
	foundUpdate4Flow3Expectation1 := false
	foundUpdate4Flow3Complete := false
	foundUpdate4Complete := false
	// These should flip to true
	foundUpdate5Start := false
	foundUpdate5Flow1Start := false
	// These should stay false
	foundUpdate5Flow1Complete := false
	foundUpdate5Complete := false
	// This should flip to true
	foundPipelineSummary := false
	// This should stay false
	foundOther := false

	// Valid event types
	validEventTypes := []string{
		"DatabricksPipelineUpdate",
		"DatabricksPipelineFlow",
		"DatabricksPipelineFlowExpectation",
		"DatabricksPipelineSummary",
	}

	// Valid events
	validEvents := []string{
		"start",
		"complete",
	}

	// Valid update IDs
	validUpdateIds := []string{
		"update-1",
		"update-2",
		"update-3",
		"update-4",
		"update-5",
	}

	// Valid flow IDs by update ID
	validFlowIds := map[string][]string {
		"update-1": {},
		"update-2": {"flow-1"},
		"update-3": {},
		"update-4": {"flow-1", "flow-2", "flow-3"},
		"update-5": {"flow-1"},
	}

	// Valid expectation names by flow ID
	update4ValidExpectationNames := map[string][]string{
		"flow-2": { "expectation-1", "expectation-2" },
		"flow-3": { "expectation-1" },
	}

	// Expected counters for pipeline summary
	expectedPipelineCounters := &pipelineCounters{
		idle: 2,
		running: 2,
	}
	expectedUpdateCounters := &updateCounters{
		running: 1,
		waitingForResources: 1,
	}
	expectedFlowCounters := &flowCounters{
		planning: 1,
	}

	// Verify that all events have the expected attributes
	for _, event := range events {
		// Verify it is a valid event type first
		assert.Contains(t, validEventTypes, event.Type, "Unexpected event type")
		eventType := event.Type

		// Verify timestamp
		assert.Equal(
			t,
			now.UnixMilli(),
			event.Timestamp,
			"Should have the correct event timestamp",
		)

		// Verify attributes
		assert.NotNil(t, event.Attributes)
		attrs := event.Attributes
		assert.NotEmpty(t, attrs)

		if eventType == "DatabricksPipelineUpdate" {
			assert.Contains(t, attrs, "databricksPipelineUpdateId")
			assert.Contains(
				t,
				validUpdateIds,
				attrs["databricksPipelineUpdateId"],
			)

			updateId := attrs["databricksPipelineUpdateId"].(string)

			// Verify event
			assert.Contains(t, attrs, "event")
			assert.Contains(
				t,
				validEvents,
				attrs["event"],
			)

			evt := attrs["event"].(string)

			if updateId == "update-1" {
				// This should never happen
				if evt == "start" {
					foundUpdate1Start = true
				} else if evt == "complete" {
					foundUpdate1Complete = true
				} else {
					foundOther = true
				}
			} else if updateId == "update-2" {
				// Verify pipeline common attributes
				verifyPipelineCommonAttributes(
					t,
					"56789",
					"Test Pipeline 3",
					"update-2",
					attrs,
				)

				// Verify cluster attributes
				verifyPipelineClusterAttributes(t, attrs)

				if evt == "start" {
					foundUpdate2Start = true

					// Verify pipeline update attributes
					verifyPipelineUpdateBaseAttributes(
						t,
						update2CreationTime,
						attrs,
					)
					verifyNoPipelineUpdateCompleteAttributes(t, attrs)
				} else if evt == "complete" {
					foundUpdate2Complete = true

					// Verify pipeline update attributes
					verifyPipelineUpdateBaseAttributes(
						t,
						update2CreationTime,
						attrs,
					)
					verifyPipelineUpdateCompleteAttributes(
						t,
						update2CreationTime,
						update2WaitStartTime,
						update2StartTime,
						update2CompletionTime,
						attrs,
					)
				} else {
					// This should never happen
					foundOther = true
				}
			} else if updateId == "update-3" {
				// This should never happen
				if evt == "start" {
					foundUpdate3Start = true
				} else if evt == "complete" {
					foundUpdate3Complete = true
				} else {
					foundOther = true
				}
			} else if updateId == "update-4" {
				if evt == "start" {
					// This should never happen
					foundUpdate4Start = true
				} else if evt == "complete" {
					foundUpdate4Complete = true

					// Verify pipeline common attributes
					verifyPipelineCommonAttributes(
						t,
						"67890",
						"Test Pipeline 4",
						"update-4",
						attrs,
					)

					// Verify cluster attributes
					verifyPipelineClusterAttributes(t, attrs)

					// Verify pipeline update attributes
					verifyPipelineUpdateBaseAttributes(
						t,
						update4CreationTime,
						attrs,
					)
					verifyPipelineUpdateCompleteAttributes(
						t,
						update4CreationTime,
						time.Time{},
						update4StartTime,
						update4CompletionTime,
						attrs,
					)
				} else {
					// This should never happen
					foundOther = true
				}
			} else if updateId == "update-5" {
				if evt == "start" {
					foundUpdate5Start = true

					// Verify pipeline update attributes
					verifyPipelineUpdateBaseAttributes(
						t,
						update5CreationTime,
						attrs,
					)
					verifyNoPipelineUpdateCompleteAttributes(t, attrs)
				} else if evt == "complete" {
					// This should never happen
					foundUpdate5Complete = true
				} else {
					// This should never happen
					foundOther = true
				}
			} else {
				// This should never happen
				foundOther = true
			}
		} else if eventType == "DatabricksPipelineFlow" {
			assert.Contains(t, attrs, "databricksPipelineUpdateId")
			assert.Contains(
				t,
				validUpdateIds,
				attrs["databricksPipelineUpdateId"],
			)

			updateId := attrs["databricksPipelineUpdateId"].(string)

			assert.Contains(t, attrs, "databricksPipelineFlowId")
			assert.Contains(
				t,
				validFlowIds[updateId],
				attrs["databricksPipelineFlowId"],
			)

			flowId := attrs["databricksPipelineFlowId"].(string)

			// Verify event
			assert.Contains(t, attrs, "event")
			assert.Contains(
				t,
				validEvents,
				attrs["event"],
			)

			evt := attrs["event"].(string)

			if updateId == "update-2" {
				if flowId == "flow-1" {
					// Verify pipeline common attributes
					verifyPipelineCommonAttributes(
						t,
						"56789",
						"Test Pipeline 3",
						"update-2",
						attrs,
					)

					// Verify cluster attributes
					verifyPipelineClusterAttributes(t, attrs)

					if evt == "start" {
						foundUpdate2Flow1Start = true

						// Verify flow base attributes
						verifyPipelineFlowBaseAttributes(
							t,
							"flow-1",
							"flow_1",
							update2Flow1QueueStartTime,
							attrs,
						)

						// Verify no flow complete attributes
						verifyNoPipelineFlowCompleteAttributes(t, attrs)
					} else if evt == "complete" {
						foundUpdate2Flow1Complete = true

						// Verify flow base attributes
						verifyPipelineFlowBaseAttributes(
							t,
							"flow-1",
							"flow_1",
							update2Flow1QueueStartTime,
							attrs,
						)

						// Verify flow complete attributes
						verifyPipelineFlowCompleteAttributes(
							t,
							update2Flow1QueueStartTime,
							time.Time{},
							update2Flow1StartTime,
							update2Flow1CompletionTime,
							&update2Flow1BacklogBytes,
							&update2Flow1BacklogFiles,
							&update2Flow1NumOutputRows,
							&update2Flow1DroppedRecords,
							attrs,
						)
					} else {
						// This should never happen
						foundOther = true
					}
				} else {
					// This should never happen
					foundOther = true
				}
			} else if updateId == "update-4" {
				if flowId == "flow-1" {
					// This should never happen
					if evt == "start" {
						foundUpdate4Flow1Start = true
					} else if evt == "complete" {
						foundUpdate4Flow1Complete = true
					} else {
						foundOther = true
					}
				} else if flowId == "flow-2" {
					if evt == "start" {
						// This should never happen
						foundUpdate4Flow2Start = true
					} else if evt == "complete" {
						foundUpdate4Flow2Complete = true

						// Verify pipeline common attributes
						verifyPipelineCommonAttributes(
							t,
							"67890",
							"Test Pipeline 4",
							"update-4",
							attrs,
						)

						// Verify cluster attributes
						verifyPipelineClusterAttributes(t, attrs)

						// Verify flow base attributes
						verifyPipelineFlowBaseAttributes(
							t,
							"flow-2",
							"flow_2",
							update4Flow2QueueStartTime,
							attrs,
						)

						// Verify flow complete attributes
						verifyPipelineFlowCompleteAttributes(
							t,
							update4Flow2QueueStartTime,
							update4Flow2PlanStartTime,
							update4Flow2StartTime,
							update4Flow2CompletionTime,
							&update4Flow2BacklogBytes,
							&update4Flow2BacklogFiles,
							&update4Flow2NumOutputRows,
							&update4Flow2DroppedRecords,
							attrs,
						)
					} else {
						// This should never happen
						foundOther = true
					}
				} else if flowId == "flow-3" {
					// Verify pipeline common attributes
					verifyPipelineCommonAttributes(
						t,
						"67890",
						"Test Pipeline 4",
						"update-4",
						attrs,
					)

					// Verify cluster attributes
					verifyPipelineClusterAttributes(t, attrs)

					if evt == "start" {
						foundUpdate4Flow3Start = true

						// Verify flow base attributes
						verifyPipelineFlowBaseAttributes(
							t,
							"flow-3",
							"flow_3",
							update4Flow3QueueStartTime,
							attrs,
						)

						// Verify no flow complete attributes
						verifyNoPipelineFlowCompleteAttributes(t, attrs)
					} else if evt == "complete" {
						foundUpdate4Flow3Complete = true

						// Verify flow base attributes
						verifyPipelineFlowBaseAttributes(
							t,
							"flow-3",
							"flow_3",
							update4Flow3QueueStartTime,
							attrs,
						)

						// Verify flow complete attributes
						verifyPipelineFlowCompleteAttributes(
							t,
							update4Flow3QueueStartTime,
							time.Time{},
							update4Flow3StartTime,
							update4Flow3CompletionTime,
							&update4Flow3BacklogBytes,
							&update4Flow3BacklogFiles,
							&update4Flow3NumOutputRows,
							&update4Flow3DroppedRecords,
							attrs,
						)
					} else {
						// This should never happen
						foundOther = true
					}
				} else {
					// This should never happen
					foundOther = true
				}
			} else if updateId == "update-5" {
				if flowId == "flow-1" {
					if evt == "start" {
						foundUpdate5Flow1Start = true

						// Verify flow base attributes
						verifyPipelineFlowBaseAttributes(
							t,
							"flow-1",
							"flow_1",
							update5Flow1QueueStartTime,
							attrs,
						)

						// Verify no flow complete attributes
						verifyNoPipelineFlowCompleteAttributes(t, attrs)
					} else if evt == "complete" {
						// This should never happen
						foundUpdate5Flow1Complete = true
					} else {
						// This should never happen
						foundOther = true
					}
				} else {
					// This should never happen
					foundOther = true
				}
			} else {
				// This should never happen
				foundOther = true
			}
		} else if eventType == "DatabricksPipelineFlowExpectation" {
			// Verify pipeline common attributes
			verifyPipelineCommonAttributes(
				t,
				"67890",
				"Test Pipeline 4",
				"update-4",
				attrs,
			)

			assert.Contains(t, attrs, "databricksPipelineUpdateId")
			assert.Contains(
				t,
				validUpdateIds,
				attrs["databricksPipelineUpdateId"],
			)

			updateId := attrs["databricksPipelineUpdateId"].(string)

			assert.Contains(t, attrs, "databricksPipelineFlowId")
			assert.Contains(
				t,
				validFlowIds[updateId],
				attrs["databricksPipelineFlowId"],
			)

			flowId := attrs["databricksPipelineFlowId"].(string)

			if updateId == "update-4" {
				assert.Contains(t, update4ValidExpectationNames, flowId)
				expectationNames := update4ValidExpectationNames[flowId]

				assert.Contains(t, attrs, "name")
				assert.Contains(t, expectationNames, attrs["name"])

				expectationName := attrs["name"]

				if flowId == "flow-2" && expectationName == "expectation-1" {
					foundUpdate4Flow2Expectation1 = true

					verifyExpectationAttributes(
						t,
						"flow-2",
						"flow_2",
						"expectation-1",
						"dataset-1",
						update4Flow2Expectation1PassedRecords,
						update4Flow2Expectation1FailedRecords,
						attrs,
					)
				} else if flowId == "flow-2" &&
					expectationName == "expectation-2" {
					foundUpdate4Flow2Expectation2 = true

					verifyExpectationAttributes(
						t,
						"flow-2",
						"flow_2",
						"expectation-2",
						"dataset-2",
						update4Flow2Expectation2PassedRecords,
						update4Flow2Expectation2FailedRecords,
						attrs,
					)
				} else if flowId == "flow-3" &&
					expectationName == "expectation-1" {
					foundUpdate4Flow3Expectation1 = true

					verifyExpectationAttributes(
						t,
						"flow-3",
						"flow_3",
						"expectation-1",
						"dataset-1",
						update4Flow3Expectation1PassedRecords,
						update4Flow3Expectation1FailedRecords,
						attrs,
					)
				} else {
					// This should never happen
					foundOther = true
				}
			} else {
				// This should never happen
				foundOther = true
			}
		} else if eventType == "DatabricksPipelineSummary" {
			foundPipelineSummary = true

			// Verify common attributes
			verifyCommonAttributes(t, attrs)

			// Verify counts
			verifyPipelineSummaryAttributes(
				t,
				expectedPipelineCounters,
				expectedUpdateCounters,
				expectedFlowCounters,
				attrs,
			)
		} else {
			// This should never happen
			foundOther = true
		}
	}

	// Verify that we have seen expected events

	assert.False(t, foundUpdate1Start, "Unexpected update-1 start event")
	assert.False(t, foundUpdate1Complete, "Unexpected update-1 complete event")
	assert.True(t, foundUpdate2Start, "Expected update-2 start event")
	assert.True(
		t,
		foundUpdate2Flow1Start,
		"Expected update-2 flow-1 start event",
	)
	assert.True(
		t,
		foundUpdate2Flow1Complete,
		"Expected update-2 flow-1 complete event",
	)
	assert.True(t, foundUpdate2Complete, "Expected update-2 complete event")
	assert.False(t, foundUpdate3Start, "Unexpected update-3 start event")
	assert.False(t, foundUpdate3Complete, "Unexpected update-3 complete event")
	assert.False(t, foundUpdate4Start, "Unexpected update-4 start event")
	assert.False(
		t,
		foundUpdate4Flow1Start,
		"Unexpected update-4 flow-1 start event",
	)
	assert.False(
		t,
		foundUpdate4Flow1Complete,
		"Unexpected update-4 flow-1 complete event",
	)
	assert.False(
		t,
		foundUpdate4Flow2Start,
		"Unexpected update-4 flow-2 start event",
	)
	assert.True(
		t,
		foundUpdate4Flow2Expectation1,
		"Expected update-4 flow-2 expectation-1 event",
	)
	assert.True(
		t,
		foundUpdate4Flow2Expectation2,
		"Expected update-4 flow-2 expectation-2 event",
	)
	assert.True(
		t,
		foundUpdate4Flow2Complete,
		"Expected update-4 flow-2 complete event",
	)
	assert.True(
		t,
		foundUpdate4Flow3Start,
		"Expected update-4 flow-3 start event",
	)
	assert.True(
		t,
		foundUpdate4Flow3Expectation1,
		"Expected update-4 flow-3 expectation-1 event",
	)
	assert.True(
		t,
		foundUpdate4Flow3Complete,
		"Expected update-4 flow-3 complete event",
	)
	assert.True(t, foundUpdate4Complete, "Expected update-4 complete event")
	assert.True(t, foundUpdate5Start, "Expected update-5 start event")
	assert.True(
		t,
		foundUpdate5Flow1Start,
		"Expected update-5 flow-1 start event",
	)
	assert.False(
		t,
		foundUpdate5Flow1Complete,
		"Unexpected update-5 flow-1 complete event",
	)
	assert.False(t, foundUpdate5Complete, "Unexpected update-5 complete event")
	assert.True(t, foundPipelineSummary, "Expected pipeline summary event")
	assert.False(t, foundOther, "Unexpected other event")
}

func TestPipelineMetricsReceiver_ProcessPipelineEvent_ListPipelineEventsError(t *testing.T) {
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

	// Setup lastRun
	lastRun := time.Now()

	// Setup mock startOffset
	startOffset := time.Duration(60) * time.Second

	// Setup mock intervalOffset
	intervalOffset := time.Duration(5) * time.Second

	// Setup mock pipeline state info
	pipelineStateInfo := databricksSdkPipelines.PipelineStateInfo{
		PipelineId: "12345",
		Name:       "Test Pipeline",
		ClusterId:  "fake-cluster-id",
		State:     databricksSdkPipelines.PipelineStateRunning,
	}

	// Setup expected update counters
	expectedUpdateCounters := &updateCounters{}

	// Setup mock update counters
	updateCounters := &updateCounters{}

	// Setup expected flow counters
	expectedFlowCounters := &flowCounters{}

	// Setup mock flow counters
	flowCounters := &flowCounters{}

	// Setup a mock iterator
	mockIterator := &MockIterator[PipelineEvent]{}

	// Setup function call trackers
	hasNextCalled := 0
	nextCalled := 0

	// Setup expected error message
	expectedError := "error listing pipeline events"

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
	) (PipelineEvent, error) {
		nextCalled += 1
		return PipelineEvent{}, errors.New(expectedError)
	}

	// Mock the ListPipelineEventsWithDetails function to return the iterator
	mockWorkspace.ListPipelineEventsWithDetailsFunc = func(
		ctx context.Context,
		startOffset time.Duration,
		pipelineId string,
	) databricksSdkListing.Iterator[PipelineEvent] {
		return mockIterator
	}

	// Setup an events channel to receive events
	eventsChan := make(chan model.Event, 500) // Buffer to prevent blocking

	// Create pipeline metric receiver instance
	receiver := NewDatabricksPipelineMetricsReceiver(
		mockIntegration,
		mockWorkspace,
		startOffset,
		intervalOffset,
		tags,
	)

	// Execute the function under test
	receiver.processPipelineEvents(
		context.Background(),
		pipelineStateInfo,
		lastRun,
		updateCounters,
		flowCounters,
		eventsChan,
	)

	// Close the channel to iterate through it
	close(eventsChan)

	// There is no way to verify the actual error here since it is not returned.
	// Instead only a warning message is output and the function returns so
	// that the next pipeline can be processed.

	// Verify trackers
	assert.Equal(t, hasNextCalled, 1, "HasNext should have been called once")
	assert.Equal(t, nextCalled, 1, "Next should have been called once")

	// Collect all events for verification
	events := make([]model.Event, 0)
	for event := range eventsChan {
		events = append(events, event)
	}

	// Verify no events were written
	assert.Equal(
		t,
		0,
		len(events),
		"Expected no events to be produced when ListPipelineEventsWithDetails fails",
	)

	// Verify counters were not updated
	assert.Equal(t, expectedUpdateCounters, updateCounters)
	assert.Equal(t, expectedFlowCounters, flowCounters)
}

func TestPipelineMetricsReceiver_ProcessPipelineEvent_ListPipelineEventsEmpty(t *testing.T) {
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

	// Setup lastRun.
	lastRun := time.Now()

	// Setup mock startOffset
	startOffset := time.Duration(60) * time.Second

	// Setup mock intervalOffset
	intervalOffset := time.Duration(5) * time.Second

	// Setup mock pipeline state info
	pipelineStateInfo := databricksSdkPipelines.PipelineStateInfo{
		PipelineId: "12345",
		Name:       "Test Pipeline",
		ClusterId:  "fake-cluster-id",
		State:     databricksSdkPipelines.PipelineStateRunning,
	}

	// Setup expected update counters
	expectedUpdateCounters := &updateCounters{}

	// Setup mock update counters
	updateCounters := &updateCounters{}

	// Setup expected flow counters
	expectedFlowCounters := &flowCounters{}

	// Setup mock flow counters
	flowCounters := &flowCounters{}

	// Setup a mock iterator
	mockIterator := &MockIterator[PipelineEvent]{}

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

	// Setup the mock Next to increment the tracker and return a PipelineEvent
	// with zero values and no error. This should never be called since HasNext
	// returns false.
	mockIterator.NextFunc = func(
		ctx context.Context,
	) (PipelineEvent, error) {
		nextCalled += 1
		return PipelineEvent{}, nil
	}

	// Mock the ListPipelineEventsWithDetails function to return the empty
	// iterator
	mockWorkspace.ListPipelineEventsWithDetailsFunc = func(
		ctx context.Context,
		startOffset time.Duration,
		pipelineId string,
	) databricksSdkListing.Iterator[PipelineEvent] {
		return mockIterator
	}

	// Setup an events channel to receive events
	eventsChan := make(chan model.Event, 500) // Buffer to prevent blocking

	// Create pipeline metric receiver instance
	receiver := NewDatabricksPipelineMetricsReceiver(
		mockIntegration,
		mockWorkspace,
		startOffset,
		intervalOffset,
		tags,
	)

	// Execute the function under test
	receiver.processPipelineEvents(
		context.Background(),
		pipelineStateInfo,
		lastRun,
		updateCounters,
		flowCounters,
		eventsChan,
	)

	// Close the channel to iterate through it
	close(eventsChan)

	// Verify trackers
	assert.Equal(t, hasNextCalled, 1, "HasNext should have been called once")
	assert.Equal(t, nextCalled, 0, "Next should not have been called")

	// Collect all events for verification
	events := make([]model.Event, 0)
	for event := range eventsChan {
		events = append(events, event)
	}

	// Verify no events were written
	assert.Equal(
		t,
		0,
		len(events),
		"Expected no events to be produced when no pipeline events are found",
	)

	// Verify counters
	assert.Equal(t, expectedUpdateCounters, updateCounters)
	assert.Equal(t, expectedFlowCounters, flowCounters)
}

func TestPipelineMetricsReceiver_ProcessPipelineEvent_ShouldNotProcessEvent(t *testing.T) {
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

	// Setup now
	now := time.Now()

	// Setup lastRun
	lastRun := now

	// Setup mock startOffset
	startOffset := time.Duration(60) * time.Second

	// Setup mock intervalOffset
	intervalOffset := time.Duration(5) * time.Second

	// Setup mock pipeline state info
	pipelineStateInfo := databricksSdkPipelines.PipelineStateInfo{
		PipelineId: "12345",
		Name:       "Test Pipeline",
		ClusterId:  "fake-cluster-id",
		State:     databricksSdkPipelines.PipelineStateRunning,
	}

	// Setup expected update counters
	expectedUpdateCounters := &updateCounters{}

	// Setup mock update counters
	updateCounters := &updateCounters{}

	// Setup expected flow counters
	expectedFlowCounters := &flowCounters{}

	// Setup mock flow counters
	flowCounters := &flowCounters{}

	// Setup mock pipeline event with an event type we ignore
	pipelineEvent := PipelineEvent{
		EventType: "ignored_event_type",
		Id: "event-1",
		Origin: &databricksSdkPipelines.Origin{
			PipelineId:   "12345",
			PipelineName: "Test Pipeline",
			ClusterId:    "fake-cluster-id",
			UpdateId:     "abcde",
		},
		Timestamp: now.Format(RFC_3339_MILLI_LAYOUT),
	}

	// Setup a mock iterator
	mockIterator := &MockIterator[PipelineEvent]{}

	// Setup function call trackers
	hasNextCalled := 0
	nextCalled := 0

	// Setup the mock HasNext to increment the tracker and return true only for
	// the first call
	mockIterator.HasNextFunc = func(
		ctx context.Context,
	) bool {
		hasNextCalled += 1
		return hasNextCalled == 1
	}

	// Setup the mock Next to increment the tracker and return the mock event
	mockIterator.NextFunc = func(
		ctx context.Context,
	) (PipelineEvent, error) {
		nextCalled += 1
		return pipelineEvent, nil
	}

	// Mock the ListPipelineEventsWithDetails function to return the mock
	// iterator
	mockWorkspace.ListPipelineEventsWithDetailsFunc = func(
		ctx context.Context,
		startOffset time.Duration,
		pipelineId string,
	) databricksSdkListing.Iterator[PipelineEvent] {
		return mockIterator
	}

	// Setup an events channel to receive events
	eventsChan := make(chan model.Event, 500) // Buffer to prevent blocking

	// Create pipeline metric receiver instance
	receiver := NewDatabricksPipelineMetricsReceiver(
		mockIntegration,
		mockWorkspace,
		startOffset,
		intervalOffset,
		tags,
	)

	// Execute the function under test
	receiver.processPipelineEvents(
		context.Background(),
		pipelineStateInfo,
		lastRun,
		updateCounters,
		flowCounters,
		eventsChan,
	)

	// Close the channel to iterate through it
	close(eventsChan)

	// Verify trackers
	assert.Equal(t, hasNextCalled, 2, "HasNext should have been called twice")
	assert.Equal(t, nextCalled, 1, "Next should have been called once")

	// Collect all events for verification
	events := make([]model.Event, 0)
	for event := range eventsChan {
		events = append(events, event)
	}

	// Verify no events were written
	assert.Equal(
		t,
		0,
		len(events),
		"Expected no events to be produced when shouldNotProcessEvent() returns false",
	)

	// Verify counters
	assert.Equal(t, expectedUpdateCounters, updateCounters)
	assert.Equal(t, expectedFlowCounters, flowCounters)
}

func TestPipelineMetricsReceiver_ProcessPipelineEvent_ProcessCreateUpdate(t *testing.T) {
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

	// Mock now
	now := time.Now()
	Now = func () time.Time { return now }

	// Setup times to be used to test behavior of event production based on
	// lastRun time.
	lastRun := now.Add(-30 * time.Second)
	eventTime := lastRun.Add(10 * time.Second)

	// Setup mock startOffset
	startOffset := time.Duration(60) * time.Second

	// Setup mock intervalOffset
	intervalOffset := time.Duration(5) * time.Second

	// Setup mock pipeline state info
	pipelineStateInfo := databricksSdkPipelines.PipelineStateInfo{
		PipelineId: "12345",
		Name:       "Test Pipeline",
		ClusterId:  "fake-cluster-id",
		State:     databricksSdkPipelines.PipelineStateRunning,
	}

	// Setup mock update counters
	updateCounters := &updateCounters{}

	// Setup mock flow counters
	flowCounters := &flowCounters{}

	// Setup mock create_update pipeline event
	pipelineEvent := PipelineEvent{
		EventType: "create_update",
		Id: "event-1",
		Origin: &databricksSdkPipelines.Origin{
			PipelineId:   "12345",
			PipelineName: "Test Pipeline",
			ClusterId:    "fake-cluster-id",
			UpdateId:     "abcde",
		},
		Timestamp: eventTime.Format(RFC_3339_MILLI_LAYOUT),
		Details: &PipelineEventDetails{},
	}

	// Setup a mock iterator
	mockIterator := &MockIterator[PipelineEvent]{}

	// Setup function call trackers
	hasNextCalled := 0
	nextCalled := 0

	// Setup the mock HasNext to increment the tracker and return true only for
	// the first call
	mockIterator.HasNextFunc = func(
		ctx context.Context,
	) bool {
		hasNextCalled += 1
		return hasNextCalled == 1
	}

	// Setup the mock Next to increment the tracker and return the mock
	// create_update event
	mockIterator.NextFunc = func(
		ctx context.Context,
	) (PipelineEvent, error) {
		nextCalled += 1
		return pipelineEvent, nil
	}

	// Mock the ListPipelineEventsWithDetails function to return the mock
	// iterator
	mockWorkspace.ListPipelineEventsWithDetailsFunc = func(
		ctx context.Context,
		startOffset time.Duration,
		pipelineId string,
	) databricksSdkListing.Iterator[PipelineEvent] {
		return mockIterator
	}

	// Setup an events channel to receive events
	eventsChan := make(chan model.Event, 500) // Buffer to prevent blocking

	// Create pipeline metric receiver instance
	receiver := NewDatabricksPipelineMetricsReceiver(
		mockIntegration,
		mockWorkspace,
		startOffset,
		intervalOffset,
		tags,
	)

	// Execute the function under test
	receiver.processPipelineEvents(
		context.Background(),
		pipelineStateInfo,
		lastRun,
		updateCounters,
		flowCounters,
		eventsChan,
	)

	// Close the channel to iterate through it
	close(eventsChan)

	// Verify trackers
	assert.Equal(t, hasNextCalled, 2, "HasNext should have been called twice")
	assert.Equal(t, nextCalled, 1, "Next should have been called once")

	// Collect all events for verification
	events := make([]model.Event, 0)
	for event := range eventsChan {
		events = append(events, event)
	}

	// Verify one event was written
	assert.Equal(
		t,
		1,
		len(events),
		"Expected one event to be produced when create_update pipeline event is found",
	)

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
		"DatabricksPipelineUpdate",
		event.Type,
	)

	// Verify attributes
	assert.NotNil(t, event.Attributes)
	attrs := event.Attributes
	assert.NotEmpty(t, attrs)

	// Verify attributes
	verifyTestPipelineCommonAttributes(t, attrs)

	// Verify cluster attributes
	verifyPipelineClusterAttributes(t, attrs)

	// Verify event
	assert.Contains(t, attrs, "event")
	assert.Equal(t, "start", attrs["event"])

	// Verify update base attributes
	verifyPipelineUpdateBaseAttributes(t, eventTime, attrs)

	// Verify no update complete attributes
	verifyNoPipelineUpdateCompleteAttributes(t, attrs)

	// No need to verify counters here because create_update events have no
	// state information and so no counters are incremented.
}

func TestPipelineMetricsReceiver_ProcessPipelineEvent_ProcessUpdateProgress(t *testing.T) {
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

	// Mock now
	now := time.Now()
	Now = func () time.Time { return now }

	// Setup times to be used to test behavior of event production based on
	// lastRun time.
	lastRun := now.Add(-30 * time.Second)
	eventTime := lastRun.Add(10 * time.Second)

	// Setup mock startOffset
	startOffset := time.Duration(60) * time.Second

	// Setup mock intervalOffset
	intervalOffset := time.Duration(5) * time.Second

	// Setup mock pipeline state info
	pipelineStateInfo := databricksSdkPipelines.PipelineStateInfo{
		PipelineId: "12345",
		Name:       "Test Pipeline",
		ClusterId:  "fake-cluster-id",
		State:     databricksSdkPipelines.PipelineStateRunning,
	}

	// Setup expected update counters
	expectedUpdateCounters := &updateCounters{
		running: 1,
	}

	// Setup mock update counters
	updateCounters := &updateCounters{}

	// Setup expected flow counters
	expectedFlowCounters := &flowCounters{}

	// Setup mock flow counters
	flowCounters := &flowCounters{}

	// Setup hasNext controls
	index := 0

	// Setup mock pipeline event
	pipelineEvents := []PipelineEvent{
		{
			EventType: "create_update",
			Id: "event-1",
			Origin: &databricksSdkPipelines.Origin{
				PipelineId:   "12345",
				PipelineName: "Test Pipeline",
				ClusterId:    "fake-cluster-id",
				UpdateId:     "abcde",
			},
			Timestamp: eventTime.Format(RFC_3339_MILLI_LAYOUT),
			Details: &PipelineEventDetails{},
		},
		// Having an update running update_progress event right after an update
		// create_update event is not realistic but it works for just testing
		// the handling of update_progress events and incrementing the counters.
		// See the TestPipelineMetricsReceiver_PollEvents_WithEvents for a
		// realistic example of update events.
		{
			EventType: "update_progress",
			Id: "event-1",
			Origin: &databricksSdkPipelines.Origin{
				PipelineId:   "12345",
				PipelineName: "Test Pipeline",
				ClusterId:    "fake-cluster-id",
				UpdateId:     "abcde",
			},
			Timestamp: eventTime.Format(RFC_3339_MILLI_LAYOUT),
			Details: &PipelineEventDetails{
				UpdateProgress: &UpdateProgressDetails{
					State: databricksSdkPipelines.UpdateInfoStateRunning,
				},
			},
		},
	}

	// Setup a mock iterator
	mockIterator := &MockIterator[PipelineEvent]{}

	// Setup the mock events iterator HasNext to increment the events index
	mockIterator.HasNextFunc = func(
		ctx context.Context,
	) bool {
		index += 1
		return index <= len(pipelineEvents)
	}

	// Setup the mock events iterator Next to return the next event in reverse
	// order to simulate the way events are returned by the API (descending by
	// timestamp)
	mockIterator.NextFunc = func(
		ctx context.Context,
	) (PipelineEvent, error) {
		eventIndex := len(pipelineEvents) - index
		return pipelineEvents[eventIndex], nil
	}

	// Mock the ListPipelineEventsWithDetails function to return the mock
	// iterator
	mockWorkspace.ListPipelineEventsWithDetailsFunc = func(
		ctx context.Context,
		startOffset time.Duration,
		pipelineId string,
	) databricksSdkListing.Iterator[PipelineEvent] {
		return mockIterator
	}

	// Setup an events channel to receive events
	eventsChan := make(chan model.Event, 500) // Buffer to prevent blocking

	// Create pipeline metric receiver instance
	receiver := NewDatabricksPipelineMetricsReceiver(
		mockIntegration,
		mockWorkspace,
		startOffset,
		intervalOffset,
		tags,
	)

	// Execute the function under test
	receiver.processPipelineEvents(
		context.Background(),
		pipelineStateInfo,
		lastRun,
		updateCounters,
		flowCounters,
		eventsChan,
	)

	// Close the channel to iterate through it
	close(eventsChan)

	// Collect all events for verification
	events := make([]model.Event, 0)
	for event := range eventsChan {
		events = append(events, event)
	}

	// Verify one event was written
	assert.Equal(
		t,
		1,
		len(events),
		"Expected one event to be produced when create_update pipeline event is found",
	)

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
		"DatabricksPipelineUpdate",
		event.Type,
	)

	// Verify attributes
	assert.NotNil(t, event.Attributes)
	attrs := event.Attributes
	assert.NotEmpty(t, attrs)

	// Verify attributes
	verifyTestPipelineCommonAttributes(t, attrs)

	// Verify cluster attributes
	verifyPipelineClusterAttributes(t, attrs)

	// Verify event
	assert.Contains(t, attrs, "event")
	assert.Equal(t, "start", attrs["event"])

	// Verify update base attributes
	verifyPipelineUpdateBaseAttributes(t, eventTime, attrs)

	// Verify no update complete attributes
	verifyNoPipelineUpdateCompleteAttributes(t, attrs)

	// Verify counters
	assert.Equal(t, expectedUpdateCounters, updateCounters)
	assert.Equal(t, expectedFlowCounters, flowCounters)
}

func TestPipelineMetricsReceiver_ProcessPipelineEvent_ProcessFlowProgress(t *testing.T) {
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

	// Mock now
	now := time.Now()
	Now = func () time.Time { return now }

	// Setup times to be used to test behavior of event production based on
	// lastRun time.
	lastRun := now.Add(-30 * time.Second)
	eventTime := lastRun.Add(10 * time.Second)

	// Setup mock startOffset
	startOffset := time.Duration(60) * time.Second

	// Setup mock intervalOffset
	intervalOffset := time.Duration(5) * time.Second

	// Setup mock pipeline state info
	pipelineStateInfo := databricksSdkPipelines.PipelineStateInfo{
		PipelineId: "12345",
		Name:       "Test Pipeline",
		ClusterId:  "fake-cluster-id",
		State:     databricksSdkPipelines.PipelineStateRunning,
	}

	// Setup expected update counters
	expectedUpdateCounters := &updateCounters{}

	// Setup mock update counters
	updateCounters := &updateCounters{}

	// Setup expected flow counters
	expectedFlowCounters := &flowCounters{
		queued: 1,
	}

	// Setup mock flow counters
	flowCounters := &flowCounters{}

	// Setup hasNext controls
	index := 0

	// Setup mock pipeline event
	pipelineEvents := []PipelineEvent{
		// Having a single flow_progress event for an update is not realistic
		// but it works for just testing the handling of flow_progress events
		// and incrementing the counters. See the
		// TestPipelineMetricsReceiver_PollEvents_WithEvents for a realistic
		// example of update events.
		{
			EventType: "flow_progress",
			Id: "event-1",
			Origin: &databricksSdkPipelines.Origin{
				PipelineId:   "12345",
				PipelineName: "Test Pipeline",
				ClusterId:    "fake-cluster-id",
				UpdateId:     "abcde",
				FlowId:       "flow-1",
				FlowName:     "flow_1",
			},
			Timestamp: eventTime.Format(RFC_3339_MILLI_LAYOUT),
			Details: &PipelineEventDetails{
				FlowProgress: &FlowProgressDetails{
					Status: FlowInfoStatusQueued,
				},
			},
		},
	}

	// Setup a mock iterator
	mockIterator := &MockIterator[PipelineEvent]{}

	// Setup the mock events iterator HasNext to increment the events index
	mockIterator.HasNextFunc = func(
		ctx context.Context,
	) bool {
		index += 1
		return index <= len(pipelineEvents)
	}

	// Setup the mock events iterator Next to return the next event in reverse
	// order to simulate the way events are returned by the API (descending by
	// timestamp)
	mockIterator.NextFunc = func(
		ctx context.Context,
	) (PipelineEvent, error) {
		eventIndex := len(pipelineEvents) - index
		return pipelineEvents[eventIndex], nil
	}

	// Mock the ListPipelineEventsWithDetails function to return the mock
	// iterator
	mockWorkspace.ListPipelineEventsWithDetailsFunc = func(
		ctx context.Context,
		startOffset time.Duration,
		pipelineId string,
	) databricksSdkListing.Iterator[PipelineEvent] {
		return mockIterator
	}

	// Setup an events channel to receive events
	eventsChan := make(chan model.Event, 500) // Buffer to prevent blocking

	// Create pipeline metric receiver instance
	receiver := NewDatabricksPipelineMetricsReceiver(
		mockIntegration,
		mockWorkspace,
		startOffset,
		intervalOffset,
		tags,
	)

	// Execute the function under test
	receiver.processPipelineEvents(
		context.Background(),
		pipelineStateInfo,
		lastRun,
		updateCounters,
		flowCounters,
		eventsChan,
	)

	// Close the channel to iterate through it
	close(eventsChan)

	// Collect all events for verification
	events := make([]model.Event, 0)
	for event := range eventsChan {
		events = append(events, event)
	}

	// Verify one event was written
	assert.Equal(
		t,
		1,
		len(events),
		"Expected one event to be produced when flow_progress QUEUED pipeline event is found",
	)

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
		"DatabricksPipelineFlow",
		event.Type,
	)

	// Verify attributes
	assert.NotNil(t, event.Attributes)
	attrs := event.Attributes
	assert.NotEmpty(t, attrs)

	// Verify attributes
	verifyTestPipelineCommonAttributes(t, attrs)

	// Verify cluster attributes
	verifyPipelineClusterAttributes(t, attrs)

	// Verify event
	assert.Contains(t, attrs, "event")
	assert.Equal(t, "start", attrs["event"])

	// Verify flow base attributes
	verifyPipelineFlowBaseAttributes(t, "flow-1", "flow_1", eventTime, attrs)

	// Verify no flow complete attributes
	verifyNoPipelineFlowCompleteAttributes(t, attrs)

	// Verify counters
	assert.Equal(t, expectedUpdateCounters, updateCounters)
	assert.Equal(t, expectedFlowCounters, flowCounters)
}

func TestShouldProcessEvent_IgnoredEventType(t *testing.T) {
	// Setup now
	now := time.Now()

	// Mock endTime
	endTime := now

	// Setup mock pipeline event
	event := &PipelineEvent{
		EventType: "ignored_event_type",
		Id: "event-1",
		Origin: &databricksSdkPipelines.Origin{
			PipelineId:   "12345",
			PipelineName: "Test Pipeline",
			ClusterId:    "fake-cluster-id",
			UpdateId:     "abcde",
		},
		Timestamp: now.Format(RFC_3339_MILLI_LAYOUT),
	}

	// Setup mock pipeline state info
	pipelineStateInfo := &databricksSdkPipelines.PipelineStateInfo{
		PipelineId: "12345",
		Name:       "Test Pipeline",
		ClusterId:  "fake-cluster-id",
		State:     databricksSdkPipelines.PipelineStateRunning,
	}

	// Setup old updates
	oldUpdates := map[string]struct{}{}

	// Execute the function under test
	eventTimestamp, shouldProcess := shouldProcessEvent(
		event,
		pipelineStateInfo,
		"ignored_event_type",
		endTime,
		oldUpdates,
	)

	// Verify result
	assert.True(t, eventTimestamp.IsZero())
	assert.False(t, shouldProcess)
}

func TestShouldProcessEvent_OriginNil(t *testing.T) {
	// Setup now
	now := time.Now()

	// Mock endTime
	endTime := now

	// Setup mock pipeline event
	event := &PipelineEvent{
		EventType: "update_progress",
		Id: "event-1",
		Timestamp: now.Format(RFC_3339_MILLI_LAYOUT),
	}

	// Setup mock pipeline state info
	pipelineStateInfo := &databricksSdkPipelines.PipelineStateInfo{
		PipelineId: "12345",
		Name:       "Test Pipeline",
		ClusterId:  "fake-cluster-id",
		State:     databricksSdkPipelines.PipelineStateRunning,
	}

	// Setup old updates
	oldUpdates := map[string]struct{}{}

	// Execute the function under test
	eventTimestamp, shouldProcess := shouldProcessEvent(
		event,
		pipelineStateInfo,
		"update_progress",
		endTime,
		oldUpdates,
	)

	// Verify result
	assert.True(t, eventTimestamp.IsZero())
	assert.False(t, shouldProcess)
}

func TestShouldProcessEvent_DetailsNil(t *testing.T) {
	// Setup now
	now := time.Now()

	// Mock endTime
	endTime := now

	// Setup mock pipeline event
	event := &PipelineEvent{
		EventType: "update_progress",
		Id: "event-1",
		Origin: &databricksSdkPipelines.Origin{
			PipelineId:   "12345",
			PipelineName: "Test Pipeline",
			ClusterId:    "fake-cluster-id",
			UpdateId:     "abcde",
		},
		Timestamp: now.Format(RFC_3339_MILLI_LAYOUT),
	}

	// Setup mock pipeline state info
	pipelineStateInfo := &databricksSdkPipelines.PipelineStateInfo{
		PipelineId: "12345",
		Name:       "Test Pipeline",
		ClusterId:  "fake-cluster-id",
		State:     databricksSdkPipelines.PipelineStateRunning,
	}

	// Setup old updates
	oldUpdates := map[string]struct{}{}

	// Execute the function under test
	eventTimestamp, shouldProcess := shouldProcessEvent(
		event,
		pipelineStateInfo,
		"update_progress",
		endTime,
		oldUpdates,
	)

	// Verify result
	assert.True(t, eventTimestamp.IsZero())
	assert.False(t, shouldProcess)
}

func TestShouldProcessEvent_InvalidTimestamp(t *testing.T) {
	// Setup now
	now := time.Now()

	// Mock endTime
	endTime := now

	// Setup mock pipeline event
	event := &PipelineEvent{
		EventType: "update_progress",
		Id: "event-1",
		Origin: &databricksSdkPipelines.Origin{
			PipelineId:   "12345",
			PipelineName: "Test Pipeline",
			ClusterId:    "fake-cluster-id",
			UpdateId:     "abcde",
		},
		Timestamp: "abcdefg",
		Details: &PipelineEventDetails{},
	}

	// Setup mock pipeline state info
	pipelineStateInfo := &databricksSdkPipelines.PipelineStateInfo{
		PipelineId: "12345",
		Name:       "Test Pipeline",
		ClusterId:  "fake-cluster-id",
		State:     databricksSdkPipelines.PipelineStateRunning,
	}

	// Setup old updates
	oldUpdates := map[string]struct{}{}

	// Execute the function under test
	eventTimestamp, shouldProcess := shouldProcessEvent(
		event,
		pipelineStateInfo,
		"update_progress",
		endTime,
		oldUpdates,
	)

	// Verify result
	assert.True(t, eventTimestamp.IsZero())
	assert.False(t, shouldProcess)
}

func TestShouldProcessEvent_EventAfterEndTime(t *testing.T) {
	// Setup now
	now := time.Now()

	// Setup eventTime to be after endTime
	eventTime := now
	endTime := now.Add(-5 * time.Second)

	// Setup mock pipeline event
	event := &PipelineEvent{
		EventType: "update_progress",
		Id: "event-1",
		Origin: &databricksSdkPipelines.Origin{
			PipelineId:   "12345",
			PipelineName: "Test Pipeline",
			ClusterId:    "fake-cluster-id",
			UpdateId:     "abcde",
		},
		Timestamp: eventTime.Format(RFC_3339_MILLI_LAYOUT),
		Details: &PipelineEventDetails{},
	}

	// Setup mock pipeline state info
	pipelineStateInfo := &databricksSdkPipelines.PipelineStateInfo{
		PipelineId: "12345",
		Name:       "Test Pipeline",
		ClusterId:  "fake-cluster-id",
		State:     databricksSdkPipelines.PipelineStateRunning,
	}

	// Setup old updates
	oldUpdates := map[string]struct{}{}

	// Execute the function under test
	eventTimestamp, shouldProcess := shouldProcessEvent(
		event,
		pipelineStateInfo,
		"update_progress",
		endTime,
		oldUpdates,
	)

	// Verify result
	assert.True(t, eventTimestamp.IsZero())
	assert.False(t, shouldProcess)
}

func TestShouldProcessEvent_OldUpdate(t *testing.T) {
	// Setup now
	now := time.Now()

	// Mock end time
	endTime := now

	// Setup mock pipeline event
	event := &PipelineEvent{
		EventType: "update_progress",
		Id: "event-1",
		Origin: &databricksSdkPipelines.Origin{
			PipelineId:   "12345",
			PipelineName: "Test Pipeline",
			ClusterId:    "fake-cluster-id",
			UpdateId:     "abcde",
		},
		Timestamp: now.Format(RFC_3339_MILLI_LAYOUT),
		Details: &PipelineEventDetails{},
	}

	// Setup mock pipeline state info
	pipelineStateInfo := &databricksSdkPipelines.PipelineStateInfo{
		PipelineId: "12345",
		Name:       "Test Pipeline",
		ClusterId:  "fake-cluster-id",
		State:     databricksSdkPipelines.PipelineStateRunning,
	}

	// Setup old updates and include abcde so it will be ignored
	oldUpdates := map[string]struct{}{
		"abcde": {},
	}

	// Execute the function under test
	eventTimestamp, shouldProcess := shouldProcessEvent(
		event,
		pipelineStateInfo,
		"update_progress",
		endTime,
		oldUpdates,
	)

	// Verify result
	assert.True(t, eventTimestamp.IsZero())
	assert.False(t, shouldProcess)
}

func TestShouldProcessEvent_ValidEvent(t *testing.T) {
	// Setup now
	now := time.Now()

	// Mock endTime
	endTime := now

	// Setup mock pipeline event
	event := &PipelineEvent{
		EventType: "update_progress",
		Id: "event-1",
		Origin: &databricksSdkPipelines.Origin{
			PipelineId:   "12345",
			PipelineName: "Test Pipeline",
			ClusterId:    "fake-cluster-id",
			UpdateId:     "abcde",
		},
		Timestamp: now.Format(RFC_3339_MILLI_LAYOUT),
		Details: &PipelineEventDetails{},
	}

	// Setup mock pipeline state info
	pipelineStateInfo := &databricksSdkPipelines.PipelineStateInfo{
		PipelineId: "12345",
		Name:       "Test Pipeline",
		ClusterId:  "fake-cluster-id",
		State:     databricksSdkPipelines.PipelineStateRunning,
	}

	// Setup old updates
	oldUpdates := map[string]struct{}{}

	// Setup expected event timestamp
	expectedEventTimestamp, _ := time.Parse(
		RFC_3339_MILLI_LAYOUT,
		event.Timestamp,
	)

	// Execute the function under test
	eventTimestamp, shouldProcess := shouldProcessEvent(
		event,
		pipelineStateInfo,
		"update_progress",
		endTime,
		oldUpdates,
	)

	// Verify result
	assert.Equal(t, expectedEventTimestamp, eventTimestamp)
	assert.True(t, shouldProcess)
}

func TestProcessCreateUpdate(t *testing.T) {
	// Setup now
	now := time.Now()

	// Setup mock pipeline event
	event := &PipelineEvent{
		EventType: "create_update",
		Id: "event-1",
		Origin: &databricksSdkPipelines.Origin{
			PipelineId:   "12345",
			PipelineName: "Test Pipeline",
			ClusterId:    "fake-cluster-id",
			UpdateId:     "abcde",
		},
		Timestamp: now.Format(RFC_3339_MILLI_LAYOUT),
		Details: &PipelineEventDetails{},
	}

	// Setup mock updates with no updates so a new one will be created
	updates := map[string]*updateData{}

	// Execute the function under test
	update := processCreateUpdate(
		event,
		event.Origin,
		event.Origin.UpdateId,
		now,
		updates,
	)

	// Verify result
	assert.NotNil(t, update)
	assert.Equal(t, event.Origin.PipelineId, update.pipelineId)
	assert.Equal(t, event.Origin.PipelineName, update.pipelineName)
	assert.Equal(t, event.Origin.ClusterId, update.clusterId)
	assert.Equal(t, event.Origin.UpdateId, update.updateId)
	assert.Equal(t, now, update.creationTime)
	assert.True(t, update.waitStartTime.IsZero())
	assert.True(t, update.startTime.IsZero())
	assert.True(t, update.completionTime.IsZero())
	assert.Equal(t, databricksSdkPipelines.UpdateInfoState(""), update.state)
}

func TestProcessUpdate(t *testing.T) {
	// Setup now
	now := time.Now()

	// Mock lastRun and eventTimestamp
	lastRun := now
	eventTimestamp := now

	// Setup mock pipeline event
	event := &PipelineEvent{
		EventType: "update_progress",
		Id: "event-1",
		Origin: &databricksSdkPipelines.Origin{
			PipelineId:   "12345",
			PipelineName: "Test Pipeline",
			ClusterId:    "fake-cluster-id",
			UpdateId:     "abcde",
		},
		Timestamp: eventTimestamp.Format(RFC_3339_MILLI_LAYOUT),
		Details: &PipelineEventDetails{
			UpdateProgress: &UpdateProgressDetails{
				State: databricksSdkPipelines.UpdateInfoStateRunning,
			},
		},
	}

	// Setup mock update map
	updates := map[string]*updateData{}

	// Setup old updates
	oldUpdates := map[string]struct{}{}

	// Execute the function under test
	update := processUpdate(
		event,
		event.Origin,
		event.Origin.UpdateId,
		event.Details.UpdateProgress,
		eventTimestamp,
		lastRun,
		updates,
		oldUpdates,
	)

	// Verify result
	assert.NotNil(t, update)
	assert.Equal(t, event.Origin.PipelineId, update.pipelineId)
	assert.Equal(t, event.Origin.PipelineName, update.pipelineName)
	assert.Equal(t, event.Origin.ClusterId, update.clusterId)
	assert.Equal(t, event.Origin.UpdateId, update.updateId)
	assert.True(t, update.creationTime.IsZero())
	assert.True(t, update.waitStartTime.IsZero())
	assert.True(t, update.startTime.IsZero())
	assert.True(t, update.completionTime.IsZero())
	assert.Equal(
		t,
		databricksSdkPipelines.UpdateInfoStateRunning,
		update.state,
	)
}

func TestProcessUpdate_UpdateCompletedBefore(t *testing.T) {
	// Setup now
	now := time.Now()

	// Mock lastRun and set eventTimestamp to be before lastRun
	lastRun := now
	eventTimestamp := lastRun.Add(-5 * time.Second)

	// Setup mock pipeline event
	event := &PipelineEvent{
		EventType: "update_progress",
		Id: "event-1",
		Origin: &databricksSdkPipelines.Origin{
			PipelineId:   "12345",
			PipelineName: "Test Pipeline",
			ClusterId:    "fake-cluster-id",
			UpdateId:     "abcde",
		},
		Timestamp: eventTimestamp.Format(RFC_3339_MILLI_LAYOUT),
		Details: &PipelineEventDetails{
			UpdateProgress: &UpdateProgressDetails{
				State: databricksSdkPipelines.UpdateInfoStateCompleted,
			},
		},
	}

	// Setup mock update map
	updates := map[string]*updateData{}

	// Setup old updates
	oldUpdates := map[string]struct{}{}

	// Execute the function under test
	update := processUpdate(
		event,
		event.Origin,
		event.Origin.UpdateId,
		event.Details.UpdateProgress,
		eventTimestamp,
		lastRun,
		updates,
		oldUpdates,
	)

	// Verify result
	assert.Nil(t, update)
	assert.Contains(t, oldUpdates, event.Origin.UpdateId)
}

func TestProcessUpdate_UpdateCompletedAfter(t *testing.T) {
	// Setup now
	now := time.Now()

	// Mock lastRun and set eventTimestamp to be after lastRun
	lastRun := now
	eventTimestamp := lastRun.Add(5 * time.Second)

	// Setup mock pipeline event
	event := &PipelineEvent{
		EventType: "update_progress",
		Id: "event-1",
		Origin: &databricksSdkPipelines.Origin{
			PipelineId:   "12345",
			PipelineName: "Test Pipeline",
			ClusterId:    "fake-cluster-id",
			UpdateId:     "abcde",
		},
		Timestamp: eventTimestamp.Format(RFC_3339_MILLI_LAYOUT),
		Details: &PipelineEventDetails{
			UpdateProgress: &UpdateProgressDetails{
				State: databricksSdkPipelines.UpdateInfoStateCompleted,
			},
		},
	}

	// Setup mock update map
	updates := map[string]*updateData{}

	// Setup old updates
	oldUpdates := map[string]struct{}{}

	// Execute the function under test
	update := processUpdate(
		event,
		event.Origin,
		event.Origin.UpdateId,
		event.Details.UpdateProgress,
		eventTimestamp,
		lastRun,
		updates,
		oldUpdates,
	)

	// Verify result
	assert.NotNil(t, update)
	assert.Equal(t, event.Origin.PipelineId, update.pipelineId)
	assert.Equal(t, event.Origin.PipelineName, update.pipelineName)
	assert.Equal(t, event.Origin.ClusterId, update.clusterId)
	assert.Equal(t, event.Origin.UpdateId, update.updateId)
	assert.True(t, update.creationTime.IsZero())
	assert.True(t, update.waitStartTime.IsZero())
	assert.True(t, update.startTime.IsZero())
	assert.Equal(t, eventTimestamp, update.completionTime)
	assert.Equal(
		t,
		databricksSdkPipelines.UpdateInfoStateCompleted,
		update.state,
	)
}

func TestProcessUpdate_UpdateWaitingForResources(t *testing.T) {
	// Setup now
	now := time.Now()

	// Mock lastRun and set eventTimestamp to be after lastRun
	lastRun := now
	eventTimestamp := lastRun.Add(5 * time.Second)

	// Setup mock pipeline event
	event := &PipelineEvent{
		EventType: "update_progress",
		Id: "event-1",
		Origin: &databricksSdkPipelines.Origin{
			PipelineId:   "12345",
			PipelineName: "Test Pipeline",
			ClusterId:    "fake-cluster-id",
			UpdateId:     "abcde",
		},
		Timestamp: eventTimestamp.Format(RFC_3339_MILLI_LAYOUT),
		Details: &PipelineEventDetails{
			UpdateProgress: &UpdateProgressDetails{
				State: databricksSdkPipelines.UpdateInfoStateWaitingForResources,
			},
		},
	}

	// Setup mock update map
	updates := map[string]*updateData{}

	// Setup old updates
	oldUpdates := map[string]struct{}{}

	// Execute the function under test
	update := processUpdate(
		event,
		event.Origin,
		event.Origin.UpdateId,
		event.Details.UpdateProgress,
		eventTimestamp,
		lastRun,
		updates,
		oldUpdates,
	)

	// Verify result
	assert.NotNil(t, update)
	assert.Equal(t, event.Origin.PipelineId, update.pipelineId)
	assert.Equal(t, event.Origin.PipelineName, update.pipelineName)
	assert.Equal(t, event.Origin.ClusterId, update.clusterId)
	assert.Equal(t, event.Origin.UpdateId, update.updateId)
	assert.True(t, update.creationTime.IsZero())
	assert.Equal(t, eventTimestamp, update.waitStartTime)
	assert.True(t, update.startTime.IsZero())
	assert.True(t, update.completionTime.IsZero())
	assert.Equal(
		t,
		databricksSdkPipelines.UpdateInfoStateWaitingForResources,
		update.state,
	)
}

func TestProcessUpdate_UpdateInitializing(t *testing.T) {
	// Setup now
	now := time.Now()

	// Mock lastRun and set eventTimestamp to be after lastRun
	lastRun := now
	eventTimestamp := lastRun.Add(5 * time.Second)

	// Setup mock pipeline event
	event := &PipelineEvent{
		EventType: "update_progress",
		Id: "event-1",
		Origin: &databricksSdkPipelines.Origin{
			PipelineId:   "12345",
			PipelineName: "Test Pipeline",
			ClusterId:    "fake-cluster-id",
			UpdateId:     "abcde",
		},
		Timestamp: eventTimestamp.Format(RFC_3339_MILLI_LAYOUT),
		Details: &PipelineEventDetails{
			UpdateProgress: &UpdateProgressDetails{
				State: databricksSdkPipelines.UpdateInfoStateInitializing,
			},
		},
	}

	// Setup mock update map
	updates := map[string]*updateData{}

	// Setup old updates
	oldUpdates := map[string]struct{}{}

	// Execute the function under test
	update := processUpdate(
		event,
		event.Origin,
		event.Origin.UpdateId,
		event.Details.UpdateProgress,
		eventTimestamp,
		lastRun,
		updates,
		oldUpdates,
	)

	// Verify result
	assert.NotNil(t, update)
	assert.Equal(t, event.Origin.PipelineId, update.pipelineId)
	assert.Equal(t, event.Origin.PipelineName, update.pipelineName)
	assert.Equal(t, event.Origin.ClusterId, update.clusterId)
	assert.Equal(t, event.Origin.UpdateId, update.updateId)
	assert.True(t, update.creationTime.IsZero())
	assert.True(t, update.waitStartTime.IsZero())
	assert.Equal(t, eventTimestamp, update.startTime)
	assert.True(t, update.completionTime.IsZero())
	assert.Equal(
		t,
		databricksSdkPipelines.UpdateInfoStateInitializing,
		update.state,
	)
}

func TestProcessFlow(t *testing.T) {
	// Setup now
	now := time.Now()

	// Mock lastRun and eventTimestamp
	lastRun := now
	eventTimestamp := now

	// Setup mock pipeline event
	event := &PipelineEvent{
		EventType: "flow_progress",
		Id: "event-1",
		Origin: &databricksSdkPipelines.Origin{
			PipelineId:   "12345",
			PipelineName: "Test Pipeline",
			ClusterId:    "fake-cluster-id",
			UpdateId:     "abcde",
			FlowId:       "flow-1",
			FlowName:     "flow_1",
		},
		Timestamp: eventTimestamp.Format(RFC_3339_MILLI_LAYOUT),
		Details: &PipelineEventDetails{
			FlowProgress: &FlowProgressDetails{
				Status: FlowInfoStatusRunning,
			},
		},
	}

	// Setup mock flow map
	flows := map[string]*flowData{}

	// Setup old flows
	oldFlows := map[string]struct{}{}

	// Execute the function under test
	flow := processFlow(
		event,
		event.Origin,
		event.Origin.UpdateId,
		event.Details.FlowProgress,
		eventTimestamp,
		lastRun,
		flows,
		oldFlows,
	)

	// Verify result
	assert.NotNil(t, flow)
	assert.Equal(t, event.Origin.FlowId, flow.id)
	assert.Equal(t, event.Origin.FlowName, flow.name)
	assert.Equal(t, event.Origin.PipelineId, flow.pipelineId)
	assert.Equal(t, event.Origin.PipelineName, flow.pipelineName)
	assert.Equal(t, event.Origin.ClusterId, flow.clusterId)
	assert.Equal(t, event.Origin.UpdateId, flow.updateId)
	assert.True(t, flow.queueStartTime.IsZero())
	assert.True(t, flow.planStartTime.IsZero())
	assert.True(t, flow.startTime.IsZero())
	assert.True(t, flow.completionTime.IsZero())
	assert.Nil(t, flow.backlogBytes)
	assert.Nil(t, flow.backlogFiles)
	assert.Nil(t, flow.numOutputRows)
	assert.Nil(t, flow.droppedRecords)
	assert.Equal(t, FlowInfoStatusRunning, flow.status)
	assert.Nil(t, flow.expectations)
}

func TestProcessFlow_OldFlow(t *testing.T) {
	// Setup now
	now := time.Now()

	// Setup lastRun and eventTimestamp
	lastRun := now
	eventTimestamp := now

	// Setup mock pipeline event
	event := &PipelineEvent{
		EventType: "flow_progress",
		Id: "event-1",
		Origin: &databricksSdkPipelines.Origin{
			PipelineId:   "12345",
			PipelineName: "Test Pipeline",
			ClusterId:    "fake-cluster-id",
			UpdateId:     "abcde",
			FlowId:       "flow-1",
			FlowName:     "flow_1",
		},
		Timestamp: eventTimestamp.Format(RFC_3339_MILLI_LAYOUT),
		Details: &PipelineEventDetails{
			FlowProgress: &FlowProgressDetails{
				Status: FlowInfoStatusRunning,
			},
		},
	}

	// Setup mock flow map
	flows := map[string]*flowData{}

	// Setup old flows to include flow_1 for update abcde so it will be ignored
	oldFlows := map[string]struct{}{
		"abcde.flow_1": {},
	}

	// Execute the function under test
	flow := processFlow(
		event,
		event.Origin,
		event.Origin.UpdateId,
		event.Details.FlowProgress,
		eventTimestamp,
		lastRun,
		flows,
		oldFlows,
	)

	// Verify result
	assert.Nil(t, flow)
}

func TestProcessFlow_FlowCompletedBefore(t *testing.T) {
	// Setup now
	now := time.Now()

	// Mock lastRun and set eventTimestamp to be before lastRun
	lastRun := now
	eventTimestamp := lastRun.Add(-5 * time.Second)

	// Setup mock pipeline event
	event := &PipelineEvent{
		EventType: "flow_progress",
		Id: "event-1",
		Origin: &databricksSdkPipelines.Origin{
			PipelineId:   "12345",
			PipelineName: "Test Pipeline",
			ClusterId:    "fake-cluster-id",
			UpdateId:     "abcde",
			FlowId:       "flow-1",
			FlowName:     "flow_1",
		},
		Timestamp: eventTimestamp.Format(RFC_3339_MILLI_LAYOUT),
		Details: &PipelineEventDetails{
			FlowProgress: &FlowProgressDetails{
				Status: FlowInfoStatusCompleted,
			},
		},
	}

	// Setup mock flow map
	flows := map[string]*flowData{}

	// Setup old flows
	oldFlows := map[string]struct{}{}

	// Execute the function under test
	flow := processFlow(
		event,
		event.Origin,
		event.Origin.UpdateId,
		event.Details.FlowProgress,
		eventTimestamp,
		lastRun,
		flows,
		oldFlows,
	)

	// Verify result
	assert.Nil(t, flow)
	assert.Contains(t, oldFlows, "abcde.flow_1")
}

func TestProcessFlow_FlowCompletedAfter(t *testing.T) {
	// Setup now
	now := time.Now()

	// Mock lastRun and set eventTimestamp to be after lastRun
	lastRun := now
	eventTimestamp := lastRun.Add(5 * time.Second)

	// Setup mock pipeline event
	event := &PipelineEvent{
		EventType: "flow_progress",
		Id: "event-1",
		Origin: &databricksSdkPipelines.Origin{
			PipelineId:   "12345",
			PipelineName: "Test Pipeline",
			ClusterId:    "fake-cluster-id",
			UpdateId:     "abcde",
			FlowId:       "flow-1",
			FlowName:     "flow_1",
		},
		Timestamp: eventTimestamp.Format(RFC_3339_MILLI_LAYOUT),
		Details: &PipelineEventDetails{
			FlowProgress: &FlowProgressDetails{
				Status: FlowInfoStatusCompleted,
			},
		},
	}

	// Setup mock flow map
	flows := map[string]*flowData{}

	// Setup old flows
	oldFlows := map[string]struct{}{}

	// Execute the function under test
	flow := processFlow(
		event,
		event.Origin,
		event.Origin.UpdateId,
		event.Details.FlowProgress,
		eventTimestamp,
		lastRun,
		flows,
		oldFlows,
	)

	// Verify result
	assert.NotNil(t, flow)
	assert.Equal(t, event.Origin.FlowId, flow.id)
	assert.Equal(t, event.Origin.FlowName, flow.name)
	assert.Equal(t, event.Origin.PipelineId, flow.pipelineId)
	assert.Equal(t, event.Origin.PipelineName, flow.pipelineName)
	assert.Equal(t, event.Origin.ClusterId, flow.clusterId)
	assert.Equal(t, event.Origin.UpdateId, flow.updateId)
	assert.True(t, flow.queueStartTime.IsZero())
	assert.True(t, flow.planStartTime.IsZero())
	assert.True(t, flow.startTime.IsZero())
	assert.Equal(t, eventTimestamp, flow.completionTime)
	assert.Nil(t, flow.backlogBytes)
	assert.Nil(t, flow.backlogFiles)
	assert.Nil(t, flow.numOutputRows)
	assert.Nil(t, flow.droppedRecords)
	assert.Equal(t, FlowInfoStatusCompleted, flow.status)
	assert.Nil(t, flow.expectations)
}

func TestProcessFlow_FlowQueued(t *testing.T) {
	// Setup now
	now := time.Now()

	// Setup lastRun and eventTimestamp
	lastRun := now
	eventTimestamp := now

	// Setup mock pipeline event
	event := &PipelineEvent{
		EventType: "flow_progress",
		Id: "event-1",
		Origin: &databricksSdkPipelines.Origin{
			PipelineId:   "12345",
			PipelineName: "Test Pipeline",
			ClusterId:    "fake-cluster-id",
			UpdateId:     "abcde",
			FlowId:       "flow-1",
			FlowName:     "flow_1",
		},
		Timestamp: eventTimestamp.Format(RFC_3339_MILLI_LAYOUT),
		Details: &PipelineEventDetails{
			FlowProgress: &FlowProgressDetails{
				Status: FlowInfoStatusQueued,
			},
		},
	}

	// Setup mock flow map
	flows := map[string]*flowData{}

	// Setup old flows
	oldFlows := map[string]struct{}{}

	// Execute the function under test
	flow := processFlow(
		event,
		event.Origin,
		event.Origin.UpdateId,
		event.Details.FlowProgress,
		eventTimestamp,
		lastRun,
		flows,
		oldFlows,
	)

	// Verify result
	assert.NotNil(t, flow)
	assert.Equal(t, event.Origin.FlowId, flow.id)
	assert.Equal(t, event.Origin.FlowName, flow.name)
	assert.Equal(t, event.Origin.PipelineId, flow.pipelineId)
	assert.Equal(t, event.Origin.PipelineName, flow.pipelineName)
	assert.Equal(t, event.Origin.ClusterId, flow.clusterId)
	assert.Equal(t, event.Origin.UpdateId, flow.updateId)
	assert.Equal(t, eventTimestamp, flow.queueStartTime)
	assert.True(t, flow.planStartTime.IsZero())
	assert.True(t, flow.startTime.IsZero())
	assert.True(t, flow.completionTime.IsZero())
	assert.Nil(t, flow.backlogBytes)
	assert.Nil(t, flow.backlogFiles)
	assert.Nil(t, flow.numOutputRows)
	assert.Nil(t, flow.droppedRecords)
	assert.Equal(t, FlowInfoStatusQueued, flow.status)
	assert.Nil(t, flow.expectations)
}

func TestProcessFlow_FlowPlanning(t *testing.T) {
	// Setup now
	now := time.Now()

	// Setup lastRun and eventTimestamp
	lastRun := now
	eventTimestamp := now

	// Setup mock pipeline event
	event := &PipelineEvent{
		EventType: "flow_progress",
		Id: "event-1",
		Origin: &databricksSdkPipelines.Origin{
			PipelineId:   "12345",
			PipelineName: "Test Pipeline",
			ClusterId:    "fake-cluster-id",
			UpdateId:     "abcde",
			FlowId:       "flow-1",
			FlowName:     "flow_1",
		},
		Timestamp: eventTimestamp.Format(RFC_3339_MILLI_LAYOUT),
		Details: &PipelineEventDetails{
			FlowProgress: &FlowProgressDetails{
				Status: FlowInfoStatusPlanning,
			},
		},
	}

	// Setup mock flow map
	flows := map[string]*flowData{}

	// Setup old flows
	oldFlows := map[string]struct{}{}

	// Execute the function under test
	flow := processFlow(
		event,
		event.Origin,
		event.Origin.UpdateId,
		event.Details.FlowProgress,
		eventTimestamp,
		lastRun,
		flows,
		oldFlows,
	)

	// Verify result
	assert.NotNil(t, flow)
	assert.Equal(t, event.Origin.FlowId, flow.id)
	assert.Equal(t, event.Origin.FlowName, flow.name)
	assert.Equal(t, event.Origin.PipelineId, flow.pipelineId)
	assert.Equal(t, event.Origin.PipelineName, flow.pipelineName)
	assert.Equal(t, event.Origin.ClusterId, flow.clusterId)
	assert.Equal(t, event.Origin.UpdateId, flow.updateId)
	assert.True(t, flow.queueStartTime.IsZero())
	assert.Equal(t, eventTimestamp, flow.planStartTime)
	assert.True(t, flow.startTime.IsZero())
	assert.True(t, flow.completionTime.IsZero())
	assert.Nil(t, flow.backlogBytes)
	assert.Nil(t, flow.backlogFiles)
	assert.Nil(t, flow.numOutputRows)
	assert.Nil(t, flow.droppedRecords)
	assert.Equal(t, FlowInfoStatusPlanning, flow.status)
	assert.Nil(t, flow.expectations)
}

func TestProcessFlow_FlowStarting(t *testing.T) {
	// Setup now
	now := time.Now()

	// Setup lastRun and eventTimestamp
	lastRun := now
	eventTimestamp := now

	// Setup mock pipeline event
	event := &PipelineEvent{
		EventType: "flow_progress",
		Id: "event-1",
		Origin: &databricksSdkPipelines.Origin{
			PipelineId:   "12345",
			PipelineName: "Test Pipeline",
			ClusterId:    "fake-cluster-id",
			UpdateId:     "abcde",
			FlowId:       "flow-1",
			FlowName:     "flow_1",
		},
		Timestamp: eventTimestamp.Format(RFC_3339_MILLI_LAYOUT),
		Details: &PipelineEventDetails{
			FlowProgress: &FlowProgressDetails{
				Status: FlowInfoStatusStarting,
			},
		},
	}

	// Setup mock flow map
	flows := map[string]*flowData{}

	// Setup old flows
	oldFlows := map[string]struct{}{}

	// Execute the function under test
	flow := processFlow(
		event,
		event.Origin,
		event.Origin.UpdateId,
		event.Details.FlowProgress,
		eventTimestamp,
		lastRun,
		flows,
		oldFlows,
	)

	// Verify result
	assert.NotNil(t, flow)
	assert.Equal(t, event.Origin.FlowId, flow.id)
	assert.Equal(t, event.Origin.FlowName, flow.name)
	assert.Equal(t, event.Origin.PipelineId, flow.pipelineId)
	assert.Equal(t, event.Origin.PipelineName, flow.pipelineName)
	assert.Equal(t, event.Origin.ClusterId, flow.clusterId)
	assert.Equal(t, event.Origin.UpdateId, flow.updateId)
	assert.True(t, flow.queueStartTime.IsZero())
	assert.True(t, flow.planStartTime.IsZero())
	assert.Equal(t, eventTimestamp, flow.startTime)
	assert.True(t, flow.completionTime.IsZero())
	assert.Nil(t, flow.backlogBytes)
	assert.Nil(t, flow.backlogFiles)
	assert.Nil(t, flow.numOutputRows)
	assert.Nil(t, flow.droppedRecords)
	assert.Equal(t, FlowInfoStatusStarting, flow.status)
	assert.Nil(t, flow.expectations)
}

func TestUpdatePipelineCounters(t *testing.T) {
	// Setup mock pipeline counters
	counters := &pipelineCounters{}

	// Setup expected pipeline counters
	expectedCounters := &pipelineCounters{}

	// Setup mock pipeline state info
	stateInfo := databricksSdkPipelines.PipelineStateInfo{}

	tests := []struct {
		name       string
		state      databricksSdkPipelines.PipelineState
		times      int
		update     func(counters *pipelineCounters)
	}{
		{
			name:      "deleted-1",
			state:     databricksSdkPipelines.PipelineStateDeleted,
			times:     2,
			update:    func(counters *pipelineCounters) {
				counters.deleted += 2
			},
		},
		{
			name:      "deploying-1",
			state:     databricksSdkPipelines.PipelineStateDeploying,
			times:     1,
			update:    func(counters *pipelineCounters) {
				counters.deploying += 1
			},
		},
		{
			name:      "failed-1",
			state:     databricksSdkPipelines.PipelineStateFailed,
			times:     5,
			update:    func(counters *pipelineCounters) {
				counters.failed += 5
			},
		},
		{
			name:      "idle-1",
			state:     databricksSdkPipelines.PipelineStateIdle,
			times:     2,
			update:    func(counters *pipelineCounters) { counters.idle += 2 },
		},
		{
			name:      "recovering-1",
			state:     databricksSdkPipelines.PipelineStateRecovering,
			times:     7,
			update:    func(counters *pipelineCounters) {
				counters.recovering += 7
			},
		},
		{
			name:      "resetting-1",
			state:     databricksSdkPipelines.PipelineStateResetting,
			times:     0,
			update:    func(counters *pipelineCounters) {},
		},
		{
			name:      "running-1",
			state:     databricksSdkPipelines.PipelineStateRunning,
			times:     4,
			update:    func(counters *pipelineCounters) {
				counters.running += 4
			},
		},
		{
			name:      "starting-1",
			state:     databricksSdkPipelines.PipelineStateStarting,
			times:     1,
			update:    func(counters *pipelineCounters) {
				counters.starting += 1
			},
		},
		{
			name:      "stopping-1",
			state:     databricksSdkPipelines.PipelineStateStopping,
			times:     3,
			update:    func(counters *pipelineCounters) {
				counters.stopping += 3
			},
		},
		{
			name:      "idle-2",
			state:     databricksSdkPipelines.PipelineStateIdle,
			times:     1,
			update:    func(counters *pipelineCounters) {
				counters.idle += 1
			},
		},
		{
			name:      "failed-2",
			state:     databricksSdkPipelines.PipelineStateFailed,
			times:     2,
			update:    func(counters *pipelineCounters) {
				counters.failed += 2
			},
		},
		{
			name:      "running-1",
			state:     databricksSdkPipelines.PipelineStateRunning,
			times:     4,
			update:    func(counters *pipelineCounters) {
				counters.running += 4
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set state in state info
			stateInfo.State = tt.state

			// Execute the function under test
			for i := 0; i < tt.times; i += 1 {
				updatePipelineCounters(stateInfo, counters)
			}

			// Update the expected counters
			tt.update(expectedCounters)

			// Verify result
			assert.Equal(t, expectedCounters, counters)
		})
	}

	// Verify final counters
	assert.Equal(t, expectedCounters, counters)
}

func TestUpdateUpdateCounters(t *testing.T) {
	// Setup mock update counters
	counters := &updateCounters{}

	// Setup expected update counters
	expectedCounters := &updateCounters{}

	tests := []struct {
		name       string
		data       map[string]*updateData
		update     func(counters *updateCounters)
	}{
		{
			name:      "updates-1",
			data:      map[string]*updateData{
				"update-1": {
					state: databricksSdkPipelines.UpdateInfoStateCreated,
				},
				"update-2": {
					state: databricksSdkPipelines.UpdateInfoStateInitializing,
				},
				"update-3": {
					state: databricksSdkPipelines.UpdateInfoStateQueued,
				},
				"update-4": {
					state: databricksSdkPipelines.UpdateInfoStateResetting,
				},
				"update-5": {
					state: databricksSdkPipelines.UpdateInfoStateRunning,
				},
				"update-6": {
					state: databricksSdkPipelines.UpdateInfoStateSettingUpTables,
				},
				"update-7": {
					state: databricksSdkPipelines.UpdateInfoStateStopping,
				},
				"update-8": {
					state: databricksSdkPipelines.UpdateInfoStateWaitingForResources,
				},
			},
			update:    func(counters *updateCounters) {
				counters.created += 1
				counters.initializing += 1
				counters.queued += 1
				counters.resetting += 1
				counters.running += 1
				counters.settingUpTables += 1
				counters.stopping += 1
				counters.waitingForResources += 1
			},
		},
		{
			name:      "updates-2",
			data:      map[string]*updateData{
				"update-9": {
					state: databricksSdkPipelines.UpdateInfoStateInitializing,
				},
				"update-10": {
					state: databricksSdkPipelines.UpdateInfoStateRunning,
				},
				"update-11": {
					state: databricksSdkPipelines.UpdateInfoStateResetting,
				},
				"update-12": {
					state: databricksSdkPipelines.UpdateInfoStateRunning,
				},
				"update-13": {
					state: databricksSdkPipelines.UpdateInfoStateWaitingForResources,
				},
				"update-14": {
					state: databricksSdkPipelines.UpdateInfoStateWaitingForResources,
				},
				"update-15": {
					state: databricksSdkPipelines.UpdateInfoStateCreated,
				},
			},
			update:    func(counters *updateCounters) {
				counters.created += 1
				counters.initializing += 1
				counters.resetting += 1
				counters.running += 2
				counters.waitingForResources += 2
			},
		},
		{
			name: "updates-3",
			data: map[string]*updateData{
				"update-16": {
					state: databricksSdkPipelines.UpdateInfoStateRunning,
				},
				"update-17": {
					state: databricksSdkPipelines.UpdateInfoStateStopping,
				},
				"update-18": {
					state: databricksSdkPipelines.UpdateInfoStateRunning,
				},
				"update-19": {
					state: databricksSdkPipelines.UpdateInfoStateRunning,
				},
				"update-20": {
					state: databricksSdkPipelines.UpdateInfoStateRunning,
				},
				"update-21": {
					state: databricksSdkPipelines.UpdateInfoStateInitializing,
				},
				"update-22": {
					state: databricksSdkPipelines.UpdateInfoStateSettingUpTables,
				},
			},
			update: func(counters *updateCounters) {
				counters.initializing += 1
				counters.running += 4
				counters.settingUpTables += 1
				counters.stopping += 1
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Execute the function under test
			updateUpdateCounters(tt.data, counters)

			// Update the expected counters
			tt.update(expectedCounters)

			// Verify result
			assert.Equal(t, expectedCounters, counters)
		})
	}

	assert.Equal(t, expectedCounters, counters)
}

func TestUpdateFlowCounters(t *testing.T) {
	// Setup mock flow counters
	counters := &flowCounters{}

	// Setup expected flow counters
	expectedCounters := &flowCounters{}

	tests := []struct {
		name       string
		data       map[string]*flowData
		update     func(counters *flowCounters)
	}{
		{
			name:      "flows-1",
			data:      map[string]*flowData{
				"flow-1": {
					status: FlowInfoStatusIdle,
				},
				"flow-2": {
					status: FlowInfoStatusPlanning,
				},
				"flow-3": {
					status: FlowInfoStatusQueued,
				},
				"flow-4": {
					status: FlowInfoStatusRunning,
				},
				"flow-5": {
					status: FlowInfoStatusStarting,
				},
			},
			update:    func(counters *flowCounters) {
				counters.idle += 1
				counters.planning += 1
				counters.queued += 1
				counters.running += 1
				counters.starting += 1
			},
		},
		{
			name:      "flows-2",
			data:      map[string]*flowData{
				"flow-6": {
					status: FlowInfoStatusPlanning,
				},
				"flow-7": {
					status: FlowInfoStatusRunning,
				},
				"flow-8": {
					status: FlowInfoStatusPlanning,
				},
				"flow-9": {
					status: FlowInfoStatusRunning,
				},
				"flow-10": {
					status: FlowInfoStatusQueued,
				},
			},
			update:    func(counters *flowCounters) {
				counters.planning += 2
				counters.running += 2
				counters.queued += 1
			},
		},
		{
			name:      "flows-3",
			data:      map[string]*flowData{
				"flow-11": {
					status: FlowInfoStatusRunning,
				},
				"flow-12": {
					status: FlowInfoStatusRunning,
				},
				"flow-13": {
					status: FlowInfoStatusQueued,
				},
				"flow-14": {
					status: FlowInfoStatusQueued,
				},
				"flow-15": {
					status: FlowInfoStatusPlanning,
				},
				"flow-16": {
					status: FlowInfoStatusStarting,
				},
			},
			update:    func(counters *flowCounters) {
				counters.planning += 1
				counters.queued += 2
				counters.running += 2
				counters.starting += 1
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Execute the function under test
			updateFlowCounters(tt.data, counters)

			// Update the expected counters
			tt.update(expectedCounters)

			// Verify result
			assert.Equal(t, expectedCounters, counters)
		})
	}

	assert.Equal(t, expectedCounters, counters)
}

func TestMakePipelineBaseAttributes_GetWorkspaceInfoError(t *testing.T) {
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

	// Setup mock eventData
	eventData := &eventData{}

	// Execute the function under test
	baseAttributes, err := makePipelineBaseAttributes(
		context.Background(),
		mockWorkspace,
		eventData,
		tags,
	)

	// Verify the results
	assert.Error(t, err)
	assert.Nil(t, baseAttributes)
	assert.Equal(t, expectedError, err.Error())
}

func TestMakePipelineBaseAttributes_EventDataNil(t *testing.T) {
	// Setup mock workspace and workspace info
	mockWorkspace, teardown := setupMockWorkspaceAndInfo()
	defer teardown()

	// Setup mock tag map
	tags := map[string]string{
		"env": "production",
		"team": "data-engineering",
	}

	// Execute the function under test
	baseAttributes, err := makePipelineBaseAttributes(
		context.Background(),
		mockWorkspace,
		nil,
		tags,
	)

	// Verify the results
	assert.NoError(t, err)

	// Verify attributes
	verifyCommonAttributes(t, baseAttributes)

	// Verify common pipeline attributes are not included
	verifyNoPipelineCommonAttributes(t, baseAttributes)

	// Verify cluster attributes are not included
	verifyNoPipelineClusterAttributes(t, baseAttributes)
}

func TestMakePipelineBaseAttributes_ClusterIdEmpty(t *testing.T) {
	// Setup mock workspace and workspace info
	mockWorkspace, teardown := setupMockWorkspaceAndInfo()
	defer teardown()

	// Setup mock tag map
	tags := map[string]string{
		"env": "production",
		"team": "data-engineering",
	}

	// Setup mock eventData
	eventData := &eventData{
		pipelineId:   "12345",
		pipelineName: "Test Pipeline",
		updateId:     "abcde",
	}

	// Execute the function under test
	baseAttributes, err := makePipelineBaseAttributes(
		context.Background(),
		mockWorkspace,
		eventData,
		tags,
	)

	// Verify the results
	assert.NoError(t, err)

	// Verify attributes
	verifyTestPipelineCommonAttributes(t, baseAttributes)

	// Verify cluster attributes are not included
	verifyNoPipelineClusterAttributes(t, baseAttributes)
}

func TestMakePipelineBaseAttributes_GetClusterInfoByIdError(t *testing.T) {
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

	// Setup mock eventData
	eventData := &eventData{
		clusterId: "fake-cluster-id",
	}

	// Execute the function under test
	baseAttributes, err := makePipelineBaseAttributes(
		context.Background(),
		mockWorkspace,
		eventData,
		tags,
	)

	// Verify the results
	assert.Error(t, err)
	assert.Nil(t, baseAttributes)
	assert.Equal(t, expectedError, err.Error())
}

func TestMakePipelineBaseAttributes_ClusterInfoNil(t *testing.T) {
	// Setup mock workspace and workspace info
	mockWorkspace, teardown := setupMockWorkspaceAndInfo()
	defer teardown()

	// Save original GetClusterInfoById function and setup a defer function
	// to restore after the test
	originalGetClusterInfoById := GetClusterInfoById
	defer func() {
		GetClusterInfoById = originalGetClusterInfoById
	}()

	// Set GetClusterInfoById to return a nil clusterInfo
	GetClusterInfoById = func(
		ctx context.Context,
		w DatabricksWorkspace,
		clusterId string,
	) (
		*ClusterInfo,
		error,
	) {
		return nil, nil
	}

	// Setup mock tag map
	tags := map[string]string{
		"env": "production",
		"team": "data-engineering",
	}

	// Setup mock eventData
	eventData := &eventData{
		pipelineId:   "12345",
		pipelineName: "Test Pipeline",
		clusterId:    "fake-cluster-id",
		updateId:     "abcde",
	}

	// Execute the function under test
	baseAttributes, err := makePipelineBaseAttributes(
		context.Background(),
		mockWorkspace,
		eventData,
		tags,
	)

	// Verify the results
	assert.NoError(t, err)

	// Verify attributes
	verifyTestPipelineCommonAttributes(t, baseAttributes)

	// Verify cluster attributes are not included
	verifyNoPipelineClusterAttributes(t, baseAttributes)
}

func TestMakePipelineBaseAttributes(t *testing.T) {
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

	// Setup mock eventData
	eventData := &eventData{
		pipelineId:   "12345",
		pipelineName: "Test Pipeline",
		clusterId:    "fake-cluster-id",
		updateId:     "abcde",
	}

	// Execute the function under test
	baseAttributes, err := makePipelineBaseAttributes(
		context.Background(),
		mockWorkspace,
		eventData,
		tags,
	)

	// Verify the results
	assert.NoError(t, err)

	// Verify attributes
	verifyTestPipelineCommonAttributes(t, baseAttributes)

	// Verify cluster attributes
	verifyPipelineClusterAttributes(t, baseAttributes)
}

func TestMakePipelineSummaryAttributes_MakePipelineBaseAttributesError(
	t *testing.T,
) {
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
	// makePipelineBaseAttributes will return an error
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

	// Setup mock counters
	pipelineCounters := &pipelineCounters{}
	updateCounters := &updateCounters{}
	flowCounters := &flowCounters{}

	// Execute the function under test
	summaryAttributes, err := makePipelineSummaryAttributes(
		context.Background(),
		mockWorkspace,
		pipelineCounters,
		updateCounters,
		flowCounters,
		tags,
	)

	// Verify the results
	assert.Error(t, err)
	assert.Nil(t, summaryAttributes)
	assert.Equal(t, expectedError, err.Error())
}

func TestMakePipelineSummaryAttributes(t *testing.T) {
	// Setup mock workspace and workspace info
	mockWorkspace, teardown := setupMockWorkspaceAndInfo()
	defer teardown()

	// Setup mock tag map
	tags := map[string]string{
		"env": "production",
		"team": "data-engineering",
	}

	// Setup mock pipeline counters
	pipelineCounters := &pipelineCounters{
		deleted:    1,
		deploying:  0,
		failed:     2,
		idle:       5,
		recovering: 1,
		resetting:  2,
		running:    3,
		starting:   0,
		stopping:   2,
	}

	// Setup mock update counters
	updateCounters := &updateCounters{
		created:             2,
		initializing:        0,
		queued:              3,
		resetting:           2,
		running:             3,
		settingUpTables:     2,
		stopping:            2,
		waitingForResources: 1,
	}

	// Setup mock flow counters
	flowCounters := &flowCounters{
		idle: 0,
		planning: 12,
		queued: 9,
		running: 7,
		starting: 2,
	}

	// Execute the function under test
	pipelineSummaryAttributes, err := makePipelineSummaryAttributes(
		context.Background(),
		mockWorkspace,
		pipelineCounters,
		updateCounters,
		flowCounters,
		tags,
	)

	// Verify result
	assert.NoError(t, err)

	// Verify attributes
	verifyCommonAttributes(t, pipelineSummaryAttributes)

	// Verify counts
	verifyPipelineSummaryAttributes(
		t,
		pipelineCounters,
		updateCounters,
		flowCounters,
		pipelineSummaryAttributes,
	)
}

func TestMakePipelineUpdateBaseAttributes_MakePipelineBaseAttributesError(
	t *testing.T,
) {
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
	// makePipelineBaseAttributes will return an error
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

	// Setup creationTime
	creationTime := time.Now()

	// Setup a mock update
	update := &updateData{
		eventData: eventData{
			pipelineId:   "12345",
			pipelineName: "Test Pipeline",
			clusterId:    "fake-cluster-id",
			updateId:     "abcde",
		},
		creationTime:     creationTime,
		state:            databricksSdkPipelines.UpdateInfoStateRunning,
	}

	// Execute the function under test
	updateAttributes, err := makePipelineUpdateBaseAttributes(
		context.Background(),
		mockWorkspace,
		"start",
		update,
		tags,
	)

	// Verify the results
	assert.Error(t, err)
	assert.Nil(t, updateAttributes)
	assert.Equal(t, expectedError, err.Error())
}

func TestMakePipelineUpdateBaseAttributes(t *testing.T) {
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

	// Setup creationTime
	creationTime := time.Now()

	// Setup a mock update
	update := &updateData{
		eventData: eventData{
			pipelineId:   "12345",
			pipelineName: "Test Pipeline",
			clusterId:    "fake-cluster-id",
			updateId:     "abcde",
		},
		creationTime:     creationTime,
		state:            databricksSdkPipelines.UpdateInfoStateRunning,
	}

	// Execute the function under test
	updateAttributes, err := makePipelineUpdateBaseAttributes(
		context.Background(),
		mockWorkspace,
		"start",
		update,
		tags,
	)

	// Verify the results
	assert.NoError(t, err)

	// Verify attributes
	verifyTestPipelineCommonAttributes(t, updateAttributes)

	// Verify cluster attributes
	verifyPipelineClusterAttributes(t, updateAttributes)

	// Verify event
	assert.Contains(t, updateAttributes, "event")
	assert.Equal(t, "start", updateAttributes["event"])

	// Verify update attributes
	verifyPipelineUpdateBaseAttributes(t, creationTime, updateAttributes)
}

func TestMakePipelineUpdateStartAttributes_MakePipelineUpdateBaseAttributesError(
	t *testing.T,
) {
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
	// makePipelineBaseAttributes will return an error so that
	// makePipelineUpdateBaseAttributes will return an error
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

	// Setup creationTime
	creationTime := time.Now()

	// Setup a mock update
	update := &updateData{
		eventData: eventData{
			pipelineId:   "12345",
			pipelineName: "Test Pipeline",
			clusterId:    "fake-cluster-id",
			updateId:     "abcde",
		},
		creationTime:     creationTime,
		state:            databricksSdkPipelines.UpdateInfoStateRunning,
	}

	// Execute the function under test
	updateAttributes, err := makePipelineUpdateStartAttributes(
		context.Background(),
		mockWorkspace,
		update,
		tags,
	)

	// Verify the results
	assert.Error(t, err)
	assert.Nil(t, updateAttributes)
	assert.Equal(t, expectedError, err.Error())
}

func TestMakePipelineUpdateStartAttributes(t *testing.T) {
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

	// Setup creationTime
	creationTime := time.Now()

	// Setup a mock update
	update := &updateData{
		eventData: eventData{
			pipelineId:   "12345",
			pipelineName: "Test Pipeline",
			clusterId:    "fake-cluster-id",
			updateId:     "abcde",
		},
		creationTime:     creationTime,
		state:            databricksSdkPipelines.UpdateInfoStateRunning,
	}

	// Execute the function under test
	updateAttributes, err := makePipelineUpdateStartAttributes(
		context.Background(),
		mockWorkspace,
		update,
		tags,
	)

	// Verify the results
	assert.NoError(t, err)

	// Verify attributes
	verifyTestPipelineCommonAttributes(t, updateAttributes)

	// Verify cluster attributes
	verifyPipelineClusterAttributes(t, updateAttributes)

	// Verify event
	assert.Contains(t, updateAttributes, "event")
	assert.Equal(t, "start", updateAttributes["event"])

	// Verify update base attributes
	verifyPipelineUpdateBaseAttributes(t, creationTime, updateAttributes)

	// Verify no update end attributes
	verifyNoPipelineUpdateCompleteAttributes(t, updateAttributes)
}

func TestMakePipelineUpdateCompleteAttributes_MakePipelineUpdateBaseAttributesError(
	t *testing.T,
) {
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
	// makePipelineBaseAttributes will return an error so that
	// makePipelineUpdateBaseAttributes will return an error
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

	// Setup times
	creationTime := time.Now()
	waitStartTime := creationTime.Add(5 * time.Second)
	startTime := creationTime.Add(10 * time.Second)
	completionTime := creationTime.Add(15 * time.Second)

	// Setup a mock update
	update := &updateData{
		eventData: eventData{
			pipelineId:   "12345",
			pipelineName: "Test Pipeline",
			clusterId:    "fake-cluster-id",
			updateId:     "abcde",
		},
		creationTime:     creationTime,
		waitStartTime:    waitStartTime,
		startTime:        startTime,
		completionTime:   completionTime,
		state:            databricksSdkPipelines.UpdateInfoStateCompleted,
	}

	// Execute the function under test
	updateAttributes, err := makePipelineUpdateCompleteAttributes(
		context.Background(),
		mockWorkspace,
		update,
		tags,
	)

	// Verify the results
	assert.Error(t, err)
	assert.Nil(t, updateAttributes)
	assert.Equal(t, expectedError, err.Error())
}

func TestMakePipelineUpdateCompleteAttributes_WaitStartTimeZero(t *testing.T) {
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

	// Setup times
	creationTime := time.Now()
	startTime := creationTime.Add(10 * time.Second)
	completionTime := creationTime.Add(15 * time.Second)

	// Setup a mock update
	update := &updateData{
		eventData: eventData{
			pipelineId:   "12345",
			pipelineName: "Test Pipeline",
			clusterId:    "fake-cluster-id",
			updateId:     "abcde",
		},
		creationTime:     creationTime,
		startTime:        startTime,
		completionTime:   completionTime,
		state:            databricksSdkPipelines.UpdateInfoStateCompleted,
	}

	// Execute the function under test
	updateAttributes, err := makePipelineUpdateCompleteAttributes(
		context.Background(),
		mockWorkspace,
		update,
		tags,
	)

	// Verify the results
	assert.NoError(t, err)

	// Verify attributes
	verifyTestPipelineCommonAttributes(t, updateAttributes)

	// Verify cluster attributes
	verifyPipelineClusterAttributes(t, updateAttributes)

	// Verify event
	assert.Contains(t, updateAttributes, "event")
	assert.Equal(t, "complete", updateAttributes["event"])

	// Verify update base attributes
	verifyPipelineUpdateBaseAttributes(t, creationTime, updateAttributes)

	// Verify update complete attributes
	verifyPipelineUpdateCompleteAttributes(
		t,
		creationTime,
		time.Time{},
		startTime,
		completionTime,
		updateAttributes,
	)
}

func TestMakePipelineUpdateCompleteAttributes_StartTimeZero(t *testing.T) {
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

	// Setup times
	creationTime := time.Now()
	waitStartTime := creationTime.Add(5 * time.Second)
	completionTime := creationTime.Add(15 * time.Second)

	// Setup a mock update
	update := &updateData{
		eventData: eventData{
			pipelineId:   "12345",
			pipelineName: "Test Pipeline",
			clusterId:    "fake-cluster-id",
			updateId:     "abcde",
		},
		creationTime:     creationTime,
		waitStartTime:    waitStartTime,
		completionTime:   completionTime,
		state:            databricksSdkPipelines.UpdateInfoStateCompleted,
	}

	// Execute the function under test
	updateAttributes, err := makePipelineUpdateCompleteAttributes(
		context.Background(),
		mockWorkspace,
		update,
		tags,
	)

	// Verify the results
	assert.NoError(t, err)

	// Verify attributes
	verifyTestPipelineCommonAttributes(t, updateAttributes)

	// Verify cluster attributes
	verifyPipelineClusterAttributes(t, updateAttributes)

	// Verify event
	assert.Contains(t, updateAttributes, "event")
	assert.Equal(t, "complete", updateAttributes["event"])

	// Verify update base attributes
	verifyPipelineUpdateBaseAttributes(t, creationTime, updateAttributes)

	// Verify update complete attributes
	verifyPipelineUpdateCompleteAttributes(
		t,
		creationTime,
		waitStartTime,
		time.Time{},
		completionTime,
		updateAttributes,
	)
}

func TestMakePipelineUpdateCompleteAttributes_WaitStartTimeZero_StartTimeZero(
	t *testing.T,
) {
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

	// Setup times
	creationTime := time.Now()
	completionTime := creationTime.Add(15 * time.Second)

	// Setup a mock update
	update := &updateData{
		eventData: eventData{
			pipelineId:   "12345",
			pipelineName: "Test Pipeline",
			clusterId:    "fake-cluster-id",
			updateId:     "abcde",
		},
		creationTime:     creationTime,
		completionTime:   completionTime,
		state:            databricksSdkPipelines.UpdateInfoStateCompleted,
	}

	// Execute the function under test
	updateAttributes, err := makePipelineUpdateCompleteAttributes(
		context.Background(),
		mockWorkspace,
		update,
		tags,
	)

	// Verify the results
	assert.NoError(t, err)

	// Verify attributes
	verifyTestPipelineCommonAttributes(t, updateAttributes)

	// Verify cluster attributes
	verifyPipelineClusterAttributes(t, updateAttributes)

	// Verify event
	assert.Contains(t, updateAttributes, "event")
	assert.Equal(t, "complete", updateAttributes["event"])

	// Verify update base attributes
	verifyPipelineUpdateBaseAttributes(t, creationTime, updateAttributes)

	// Verify update end attributes
	verifyPipelineUpdateCompleteAttributes(
		t,
		creationTime,
		time.Time{},
		time.Time{},
		completionTime,
		updateAttributes,
	)
}

func TestMakePipelineUpdateCompleteAttributes_WithWaitStartTime_WithStartTime(t *testing.T) {
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

	// Setup times
	creationTime := time.Now()
	waitStartTime := creationTime.Add(5 * time.Second)
	startTime := creationTime.Add(10 * time.Second)
	completionTime := creationTime.Add(15 * time.Second)

	// Setup a mock update
	update := &updateData{
		eventData: eventData{
			pipelineId:   "12345",
			pipelineName: "Test Pipeline",
			clusterId:    "fake-cluster-id",
			updateId:     "abcde",
		},
		creationTime:     creationTime,
		waitStartTime:    waitStartTime,
		startTime:        startTime,
		completionTime:   completionTime,
		state:            databricksSdkPipelines.UpdateInfoStateCompleted,
	}

	// Execute the function under test
	updateAttributes, err := makePipelineUpdateCompleteAttributes(
		context.Background(),
		mockWorkspace,
		update,
		tags,
	)

	// Verify the results
	assert.NoError(t, err)

	// Verify attributes
	verifyTestPipelineCommonAttributes(t, updateAttributes)

	// Verify cluster attributes
	verifyPipelineClusterAttributes(t, updateAttributes)

	// Verify event
	assert.Contains(t, updateAttributes, "event")
	assert.Equal(t, "complete", updateAttributes["event"])

	// Verify update base attributes
	verifyPipelineUpdateBaseAttributes(t, creationTime, updateAttributes)

	// Verify update complete attributes
	verifyPipelineUpdateCompleteAttributes(
		t,
		creationTime,
		waitStartTime,
		startTime,
		completionTime,
		updateAttributes,
	)
}

func TestMakePipelineFlowBaseAttributes_MakePipelineBaseAttributesError(
	t *testing.T,
) {
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
	// makePipelineBaseAttributes will return an error
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

	// Setup a mock flow
	flow := &flowData{
		eventData: eventData{
			pipelineId:   "12345",
			pipelineName: "Test Pipeline",
			clusterId:    "fake-cluster-id",
			updateId:     "abcde",
		},
		status:           FlowInfoStatusRunning,
	}

	// Execute the function under test
	flowAttributes, err := makePipelineFlowBaseAttributes(
		context.Background(),
		mockWorkspace,
		"start",
		flow,
		tags,
	)

	// Verify the results
	assert.Error(t, err)
	assert.Nil(t, flowAttributes)
	assert.Equal(t, expectedError, err.Error())
}

func TestMakePipelineFlowBaseAttributes(t *testing.T) {
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

	// Setup queueStartTime
	queueStartTime := time.Now()

	// Setup a mock flow
	flow := &flowData{
		eventData: eventData{
			pipelineId:   "12345",
			pipelineName: "Test Pipeline",
			clusterId:    "fake-cluster-id",
			updateId:     "abcde",
		},
		id:               "flow-1",
		name:             "flow_1",
		queueStartTime:   queueStartTime,
		status:           FlowInfoStatusRunning,
	}

	// Execute the function under test
	flowAttributes, err := makePipelineFlowBaseAttributes(
		context.Background(),
		mockWorkspace,
		"start",
		flow,
		tags,
	)

	// Verify the results
	assert.NoError(t, err)

	// Verify attributes
	verifyTestPipelineCommonAttributes(t, flowAttributes)

	// Verify cluster attributes
	verifyPipelineClusterAttributes(t, flowAttributes)

	// Verify event
	assert.Contains(t, flowAttributes, "event")
	assert.Equal(t, "start", flowAttributes["event"])

	// Verify flow base attributes
	verifyPipelineFlowBaseAttributes(
		t,
		"flow-1",
		"flow_1",
		queueStartTime,
		flowAttributes,
	)

	// Verify no flow complete attributes
	verifyNoPipelineFlowCompleteAttributes(t, flowAttributes)
}

func TestMakePipelineFlowStartAttributes_MakePipelineFlowBaseAttributesError(
	t *testing.T,
) {
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
	// makePipelineBaseAttributes will return an error so that
	// makePipelineFlowBaseAttributes will return an error
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

	// Setup queueStartTime
	queueStartTime := time.Now()

	// Setup a mock flow
	flow := &flowData{
		eventData: eventData{
			pipelineId:   "12345",
			pipelineName: "Test Pipeline",
			clusterId:    "fake-cluster-id",
			updateId:     "abcde",
		},
		queueStartTime:   queueStartTime,
		status:           FlowInfoStatusRunning,
	}

	// Execute the function under test
	flowAttributes, err := makePipelineFlowStartAttributes(
		context.Background(),
		mockWorkspace,
		flow,
		tags,
	)

	// Verify the results
	assert.Error(t, err)
	assert.Nil(t, flowAttributes)
	assert.Equal(t, expectedError, err.Error())
}

func TestMakePipelineFlowStartAttributes(t *testing.T) {
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

	// Setup queueStartTime
	queueStartTime := time.Now()

	// Setup a mock flow
	flow := &flowData{
		eventData: eventData{
			pipelineId:   "12345",
			pipelineName: "Test Pipeline",
			clusterId:    "fake-cluster-id",
			updateId:     "abcde",
		},
		id:               "flow-1",
		name:             "flow_1",
		queueStartTime:   queueStartTime,
		status:           FlowInfoStatusRunning,
	}

	// Execute the function under test
	flowAttributes, err := makePipelineFlowStartAttributes(
		context.Background(),
		mockWorkspace,
		flow,
		tags,
	)

	// Verify the results
	assert.NoError(t, err)

	// Verify attributes
	verifyTestPipelineCommonAttributes(t, flowAttributes)

	// Verify cluster attributes
	verifyPipelineClusterAttributes(t, flowAttributes)

	// Verify event
	assert.Contains(t, flowAttributes, "event")
	assert.Equal(t, "start", flowAttributes["event"])

	// Verify flow base attributes
	verifyPipelineFlowBaseAttributes(
		t,
		"flow-1",
		"flow_1",
		queueStartTime,
		flowAttributes,
	)

	// Verify no flow complete attributes
	verifyNoPipelineFlowCompleteAttributes(t, flowAttributes)
}

func TestMakePipelineFlowCompleteAttributes_MakePipelineFlowBaseAttributesError(
	t *testing.T,
) {
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
	// makePipelineBaseAttributes will return an error so that
	// makePipelineFlowBaseAttributes will return an error
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

	// Setup times
	queueStartTime := time.Now()
	planStartTime := queueStartTime.Add(5 * time.Second)
	startTime := planStartTime.Add(10 * time.Second)
	completionTime := startTime.Add(15 * time.Second)

	// Setup a mock flow
	flow := &flowData{
		eventData: eventData{
			pipelineId:   "12345",
			pipelineName: "Test Pipeline",
			clusterId:    "fake-cluster-id",
			updateId:     "abcde",
		},
		queueStartTime:   queueStartTime,
		planStartTime:    planStartTime,
		startTime:        startTime,
		completionTime:   completionTime,
		status:           FlowInfoStatusCompleted,
	}

	// Execute the function under test
	flowAttributes, err := makePipelineFlowCompleteAttributes(
		context.Background(),
		mockWorkspace,
		flow,
		tags,
	)

	// Verify the results
	assert.Error(t, err)
	assert.Nil(t, flowAttributes)
	assert.Equal(t, expectedError, err.Error())
}

func TestMakePipelineFlowCompleteAttributes_PlanStartTimeZero(t *testing.T) {
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

	// Setup times
	queueStartTime := time.Now()
	startTime := queueStartTime.Add(10 * time.Second)
	completionTime := startTime.Add(15 * time.Second)

	// Setup a mock flow
	flow := &flowData{
		eventData: eventData{
			pipelineId:   "12345",
			pipelineName: "Test Pipeline",
			clusterId:    "fake-cluster-id",
			updateId:     "abcde",
		},
		id:               "flow-1",
		name:             "flow_1",
		queueStartTime:   queueStartTime,
		startTime:        startTime,
		completionTime:   completionTime,
		status:           FlowInfoStatusCompleted,
	}

	// Execute the function under test
	flowAttributes, err := makePipelineFlowCompleteAttributes(
		context.Background(),
		mockWorkspace,
		flow,
		tags,
	)

	// Verify the results
	assert.NoError(t, err)

	// Verify attributes
	verifyTestPipelineCommonAttributes(t, flowAttributes)

	// Verify cluster attributes
	verifyPipelineClusterAttributes(t, flowAttributes)

	// Verify event
	assert.Contains(t, flowAttributes, "event")
	assert.Equal(t, "complete", flowAttributes["event"])

	// Verify flow base attributes
	verifyPipelineFlowBaseAttributes(
		t,
		"flow-1",
		"flow_1",
		queueStartTime,
		flowAttributes,
	)

	// Verify flow complete attributes
	verifyPipelineFlowCompleteAttributes(
		t,
		queueStartTime,
		time.Time{},
		startTime,
		completionTime,
		nil,
		nil,
		nil,
		nil,
		flowAttributes,
	)
}

func TestMakePipelineFlowCompleteAttributes_StartTimeZero(t *testing.T) {
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

	// Setup times
	queueStartTime := time.Now()
	planStartTime := queueStartTime.Add(5 * time.Second)
	completionTime := planStartTime.Add(15 * time.Second)

	// Setup a mock flow
	flow := &flowData{
		eventData: eventData{
			pipelineId:   "12345",
			pipelineName: "Test Pipeline",
			clusterId:    "fake-cluster-id",
			updateId:     "abcde",
		},
		id:               "flow-1",
		name:             "flow_1",
		queueStartTime:   queueStartTime,
		planStartTime:    planStartTime,
		completionTime:   completionTime,
		status:           FlowInfoStatusCompleted,
	}

	// Execute the function under test
	flowAttributes, err := makePipelineFlowCompleteAttributes(
		context.Background(),
		mockWorkspace,
		flow,
		tags,
	)

	// Verify the results
	assert.NoError(t, err)

	// Verify attributes
	verifyTestPipelineCommonAttributes(t, flowAttributes)

	// Verify cluster attributes
	verifyPipelineClusterAttributes(t, flowAttributes)

	// Verify event
	assert.Contains(t, flowAttributes, "event")
	assert.Equal(t, "complete", flowAttributes["event"])

	// Verify flow base attributes
	verifyPipelineFlowBaseAttributes(
		t,
		"flow-1",
		"flow_1",
		queueStartTime,
		flowAttributes,
	)

	// Verify flow complete attributes
	verifyPipelineFlowCompleteAttributes(
		t,
		queueStartTime,
		planStartTime,
		time.Time{},
		completionTime,
		nil,
		nil,
		nil,
		nil,
		flowAttributes,
	)
}

func TestMakePipelineFlowCompleteAttributes_PlanStartTimeZero_StartTimeZero(
	t *testing.T,
) {
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

	// Setup times
	queueStartTime := time.Now()
	completionTime := queueStartTime.Add(15 * time.Second)

	// Setup a mock flow
	flow := &flowData{
		eventData: eventData{
			pipelineId:   "12345",
			pipelineName: "Test Pipeline",
			clusterId:    "fake-cluster-id",
			updateId:     "abcde",
		},
		id:               "flow-1",
		name:             "flow_1",
		queueStartTime:   queueStartTime,
		completionTime:   completionTime,
		status:           FlowInfoStatusCompleted,
	}

	// Execute the function under test
	flowAttributes, err := makePipelineFlowCompleteAttributes(
		context.Background(),
		mockWorkspace,
		flow,
		tags,
	)

	// Verify the results
	assert.NoError(t, err)

	// Verify attributes
	verifyTestPipelineCommonAttributes(t, flowAttributes)

	// Verify cluster attributes
	verifyPipelineClusterAttributes(t, flowAttributes)

	// Verify event
	assert.Contains(t, flowAttributes, "event")
	assert.Equal(t, "complete", flowAttributes["event"])

	// Verify flow base attributes
	verifyPipelineFlowBaseAttributes(
		t,
		"flow-1",
		"flow_1",
		queueStartTime,
		flowAttributes,
	)

	// Verify flow complete attributes
	verifyPipelineFlowCompleteAttributes(
		t,
		queueStartTime,
		time.Time{},
		time.Time{},
		completionTime,
		nil,
		nil,
		nil,
		nil,
		flowAttributes,
	)
}

func TestMakePipelineFlowCompleteAttributes_WithPlanStartTime_WithStartTime(
	t *testing.T,
) {
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

	// Setup times
	queueStartTime := time.Now()
	planStartTime := queueStartTime.Add(5 * time.Second)
	startTime := planStartTime.Add(10 * time.Second)
	completionTime := startTime.Add(15 * time.Second)

	// Setup a mock flow
	flow := &flowData{
		eventData: eventData{
			pipelineId:   "12345",
			pipelineName: "Test Pipeline",
			clusterId:    "fake-cluster-id",
			updateId:     "abcde",
		},
		id:               "flow-1",
		name:             "flow_1",
		queueStartTime:   queueStartTime,
		planStartTime:    planStartTime,
		startTime:        startTime,
		completionTime:   completionTime,
		status:           FlowInfoStatusCompleted,
	}

	// Execute the function under test
	flowAttributes, err := makePipelineFlowCompleteAttributes(
		context.Background(),
		mockWorkspace,
		flow,
		tags,
	)

	// Verify the results
	assert.NoError(t, err)

	// Verify attributes
	verifyTestPipelineCommonAttributes(t, flowAttributes)

	// Verify cluster attributes
	verifyPipelineClusterAttributes(t, flowAttributes)

	// Verify event
	assert.Contains(t, flowAttributes, "event")
	assert.Equal(t, "complete", flowAttributes["event"])

	// Verify flow base attributes
	verifyPipelineFlowBaseAttributes(
		t,
		"flow-1",
		"flow_1",
		queueStartTime,
		flowAttributes,
	)

	// Verify flow complete attributes
	verifyPipelineFlowCompleteAttributes(
		t,
		queueStartTime,
		planStartTime,
		startTime,
		completionTime,
		nil,
		nil,
		nil,
		nil,
		flowAttributes,
	)
}

func TestMakePipelineFlowCompleteAttributes_WithBacklogMetrics(t *testing.T) {
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

	// Setup times
	queueStartTime := time.Now()
	startTime := queueStartTime.Add(10 * time.Second)
	completionTime := startTime.Add(15 * time.Second)

	// Setup backlog metrics
	backlogBytes := float64(1024)
	backlogFiles := float64(10)

	// Setup a mock flow
	flow := &flowData{
		eventData: eventData{
			pipelineId:   "12345",
			pipelineName: "Test Pipeline",
			clusterId:    "fake-cluster-id",
			updateId:     "abcde",
		},
		id:               "flow-1",
		name:             "flow_1",
		queueStartTime:   queueStartTime,
		startTime:        startTime,
		completionTime:   completionTime,
		status:           FlowInfoStatusCompleted,
		backlogBytes:     &backlogBytes,
		backlogFiles:     &backlogFiles,
	}

	// Execute the function under test
	flowAttributes, err := makePipelineFlowCompleteAttributes(
		context.Background(),
		mockWorkspace,
		flow,
		tags,
	)

	// Verify the results
	assert.NoError(t, err)

	// Verify attributes
	verifyTestPipelineCommonAttributes(t, flowAttributes)

	// Verify cluster attributes
	verifyPipelineClusterAttributes(t, flowAttributes)

	// Verify event
	assert.Contains(t, flowAttributes, "event")
	assert.Equal(t, "complete", flowAttributes["event"])

	// Verify flow base attributes
	verifyPipelineFlowBaseAttributes(
		t,
		"flow-1",
		"flow_1",
		queueStartTime,
		flowAttributes,
	)

	// Verify flow complete attributes
	verifyPipelineFlowCompleteAttributes(
		t,
		queueStartTime,
		time.Time{},
		startTime,
		completionTime,
		&backlogBytes,
		&backlogFiles,
		nil,
		nil,
		flowAttributes,
	)
}

func TestMakePipelineFlowCompleteAttributes_WithRowMetrics(t *testing.T) {
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

	// Setup times
	queueStartTime := time.Now()
	startTime := queueStartTime.Add(10 * time.Second)
	completionTime := startTime.Add(15 * time.Second)

	// Setup row metrics
	numOutputRows := float64(100)
	droppedRecords := float64(10)

	// Setup a mock flow
	flow := &flowData{
		eventData: eventData{
			pipelineId:   "12345",
			pipelineName: "Test Pipeline",
			clusterId:    "fake-cluster-id",
			updateId:     "abcde",
		},
		id:               "flow-1",
		name:             "flow_1",
		queueStartTime:   queueStartTime,
		startTime:        startTime,
		completionTime:   completionTime,
		status:           FlowInfoStatusCompleted,
		numOutputRows:    &numOutputRows,
		droppedRecords:   &droppedRecords,
	}

	// Execute the function under test
	flowAttributes, err := makePipelineFlowCompleteAttributes(
		context.Background(),
		mockWorkspace,
		flow,
		tags,
	)

	// Verify the results
	assert.NoError(t, err)

	// Verify attributes
	verifyTestPipelineCommonAttributes(t, flowAttributes)

	// Verify cluster attributes
	verifyPipelineClusterAttributes(t, flowAttributes)

	// Verify event
	assert.Contains(t, flowAttributes, "event")
	assert.Equal(t, "complete", flowAttributes["event"])

	// Verify flow base attributes
	verifyPipelineFlowBaseAttributes(
		t,
		"flow-1",
		"flow_1",
		queueStartTime,
		flowAttributes,
	)

	// Verify flow complete attributes
	verifyPipelineFlowCompleteAttributes(
		t,
		queueStartTime,
		time.Time{},
		startTime,
		completionTime,
		nil,
		nil,
		&numOutputRows,
		&droppedRecords,
		flowAttributes,
	)
}

func TestCalcUpdateWaitDuration_WithStartTime(t *testing.T) {
	// Setup now
	now := time.Now()

	// Setup times
	creationTime := now
	waitStartTime := creationTime.Add(5 * time.Second)
	startTime := waitStartTime.Add(10 * time.Second)
	completionTime := startTime.Add(15 * time.Second)

	// Setup mock update where creationTime, waitStartTime, startTime, and
	// completionTime are all non-zero
	update := &updateData{
		eventData: eventData{
			pipelineId:   "12345",
			pipelineName: "Test Pipeline",
			clusterId:    "fake-cluster-id",
			updateId:     "abcde",
		},
		creationTime:   creationTime,
		waitStartTime:  waitStartTime,
		startTime:      startTime,
		completionTime: completionTime,
		state:          databricksSdkPipelines.UpdateInfoStateCompleted,
	}

	// Execute the function under test
	duration := calcUpdateWaitDuration(update)

	// Verify the results
	assert.Equal(t, int64(10000), duration)
}

func TestCalcUpdateWaitDuration_StartTimeZero(t *testing.T) {
	// Setup now
	now := time.Now()

	// Setup times
	creationTime := now
	waitStartTime := creationTime.Add(5 * time.Second)
	completionTime := waitStartTime.Add(15 * time.Second)

	// Setup mock update where creationTime, waitStartTime, and completionTime
	// are non-zero and startTime is zero
	update := &updateData{
		eventData: eventData{
			pipelineId:   "12345",
			pipelineName: "Test Pipeline",
			clusterId:    "fake-cluster-id",
			updateId:     "abcde",
		},
		creationTime:   creationTime,
		waitStartTime:  waitStartTime,
		completionTime: completionTime,
		state:          databricksSdkPipelines.UpdateInfoStateCompleted,
	}

	// Execute the function under test
	duration := calcUpdateWaitDuration(update)

	// Verify the results
	assert.Equal(t, int64(15000), duration)
}

func TestCalcUpdateRunDuration(t *testing.T) {
	// Setup now
	now := time.Now()

	// Setup times
	creationTime := now
	startTime := creationTime.Add(10 * time.Second)
	completionTime := startTime.Add(15 * time.Second)

	// Setup mock update where creationTime, startTime, and completionTime are
	// non-zero
	update := &updateData{
		eventData: eventData{
			pipelineId:   "12345",
			pipelineName: "Test Pipeline",
			clusterId:    "fake-cluster-id",
			updateId:     "abcde",
		},
		creationTime:   creationTime,
		startTime:      startTime,
		completionTime: completionTime,
		state:          databricksSdkPipelines.UpdateInfoStateCompleted,
	}

	// Execute the function under test
	duration := calcUpdateRunDuration(update)

	// Verify the results
	assert.Equal(t, int64(15000), duration)
}

func TestCalcFlowQueueDuration_WithPlanStartTime(t *testing.T) {
	// Setup now
	now := time.Now()

	// Setup times
	queueStartTime := now
	planStartTime := queueStartTime.Add(5 * time.Second)
	completionTime := planStartTime.Add(15 * time.Second)

	// Setup mock flow where queueStartTime, planStartTime, and completionTime
	// are non-zero and startTime is zero
	flow := &flowData{
		eventData: eventData{
			pipelineId:   "12345",
			pipelineName: "Test Pipeline",
			clusterId:    "fake-cluster-id",
			updateId:     "abcde",
		},
		id:               "flow-1",
		name:             "flow_1",
		status:           FlowInfoStatusCompleted,
		queueStartTime:   queueStartTime,
		planStartTime:    planStartTime,
		completionTime:   completionTime,
	}

	// Execute the function under test
	duration := calcFlowQueueDuration(flow)

	// Verify the results
	assert.Equal(t, int64(5000), duration)
}

func TestCalcFlowQueueDuration_WithStartTime(t *testing.T) {
	// Setup now
	now := time.Now()

	// Setup times
	queueStartTime := now
	startTime := queueStartTime.Add(10 * time.Second)
	completionTime := startTime.Add(15 * time.Second)

	// Setup mock flow where queueStartTime, startTime, and completionTime are
	// non-zero and planStartTime is zero
	flow := &flowData{
		eventData: eventData{
			pipelineId:   "12345",
			pipelineName: "Test Pipeline",
			clusterId:    "fake-cluster-id",
			updateId:     "abcde",
		},
		id:               "flow-1",
		name:             "flow_1",
		status:           FlowInfoStatusCompleted,
		queueStartTime:   queueStartTime,
		startTime:        startTime,
		completionTime:   completionTime,
	}

	// Execute the function under test
	duration := calcFlowQueueDuration(flow)

	// Verify the results
	assert.Equal(t, int64(10000), duration)
}

func TestCalcFlowQueueDuration_PlanStartTimeZero_StartTimeZero(t *testing.T) {
	// Setup now
	now := time.Now()

	// Setup times
	queueStartTime := now
	completionTime := queueStartTime.Add(15 * time.Second)

	// Setup mock update where queueStartTime and completionTime are non-zero
	// and planStartTime and startTime are zero
	flow := &flowData{
		eventData: eventData{
			pipelineId:   "12345",
			pipelineName: "Test Pipeline",
			clusterId:    "fake-cluster-id",
			updateId:     "abcde",
		},
		id:               "flow-1",
		name:             "flow_1",
		status:           FlowInfoStatusCompleted,
		queueStartTime:   queueStartTime,
		completionTime:   completionTime,
	}

	// Execute the function under test
	duration := calcFlowQueueDuration(flow)

	// Verify the results
	assert.Equal(t, int64(15000), duration)
}

func TestCalcFlowPlanDuration_WithStartTime(t *testing.T) {
	// Setup now
	now := time.Now()

	// Setup times
	queueStartTime := now
	planStartTime := queueStartTime.Add(5 * time.Second)
	startTime := planStartTime.Add(10 * time.Second)
	completionTime := startTime.Add(15 * time.Second)

	// Setup mock flow where queueStartTime, planStartTime, completionTime, and
	// startTime are all non-zero
	flow := &flowData{
		eventData: eventData{
			pipelineId:   "12345",
			pipelineName: "Test Pipeline",
			clusterId:    "fake-cluster-id",
			updateId:     "abcde",
		},
		id:               "flow-1",
		name:             "flow_1",
		status:           FlowInfoStatusCompleted,
		queueStartTime:  queueStartTime,
		planStartTime:   planStartTime,
		startTime:       startTime,
		completionTime: completionTime,
	}

	// Execute the function under test
	duration := calcFlowPlanDuration(flow)

	// Verify the results
	assert.Equal(t, int64(10000), duration)
}

func TestCalcFlowPlanDuration_StartTimeZero(t *testing.T) {
	// Setup now
	now := time.Now()

	// Setup times
	queueStartTime := now
	planStartTime := queueStartTime.Add(5 * time.Second)
	completionTime := planStartTime.Add(15 * time.Second)

	// Setup mock flow where queueStartTime, planStartTime, and completionTime
	// are non-zero and startTime is zero
	flow := &flowData{
		eventData: eventData{
			pipelineId:   "12345",
			pipelineName: "Test Pipeline",
			clusterId:    "fake-cluster-id",
			updateId:     "abcde",
		},
		id:               "flow-1",
		name:             "flow_1",
		status:           FlowInfoStatusCompleted,
		queueStartTime:  queueStartTime,
		planStartTime:   planStartTime,
		completionTime:  completionTime,
	}

	// Execute the function under test
	duration := calcFlowPlanDuration(flow)

	// Verify the results
	assert.Equal(t, int64(15000), duration)
}

func TestSetFlowMetrics_NoMetrics(t *testing.T) {
	// Setup now
	now := time.Now()

	// Setup times
	queueStartTime := now
	startTime := queueStartTime.Add(5 * time.Second)
	completionTime := startTime.Add(15 * time.Second)

	// Setup mock flow
	flow := &flowData{
		eventData: eventData{
			pipelineId:   "12345",
			pipelineName: "Test Pipeline",
			clusterId:    "fake-cluster-id",
			updateId:     "abcde",
		},
		id:               "flow-1",
		name:             "flow_1",
		status:           FlowInfoStatusCompleted,
		queueStartTime:   queueStartTime,
		startTime:        startTime,
		completionTime:   completionTime,
	}

	// Setup mock attributes
	attrs := make(map[string]interface{})

	// Execute the function under test
	setFlowMetrics(flow, attrs)

	// Verify the results
	assert.NotContains(t, attrs, "backlogBytes")
	assert.NotContains(t, attrs, "backlogFileCount")
	assert.NotContains(t, attrs, "numOutputRows")
	assert.NotContains(t, attrs, "droppedRecords")
}

func TestSetFlowMetrics_BacklogMetrics(t *testing.T) {
	// Setup now
	now := time.Now()

	// Setup times
	queueStartTime := now
	startTime := queueStartTime.Add(5 * time.Second)
	completionTime := startTime.Add(15 * time.Second)

	// Setup backlog metrics
	backlogBytes := float64(1024)
	backlogFiles := float64(10)

	// Setup mock flow
	flow := &flowData{
		eventData: eventData{
			pipelineId:   "12345",
			pipelineName: "Test Pipeline",
			clusterId:    "fake-cluster-id",
			updateId:     "abcde",
		},
		id:               "flow-1",
		name:             "flow_1",
		status:           FlowInfoStatusCompleted,
		queueStartTime:   queueStartTime,
		startTime:        startTime,
		completionTime:   completionTime,
		backlogBytes:     &backlogBytes,
		backlogFiles:     &backlogFiles,
	}

	// Setup mock attributes
	attrs := make(map[string]interface{})

	// Execute the function under test
	setFlowMetrics(flow, attrs)

	// Verify the results
	assert.Contains(t, attrs, "backlogBytes")
	assert.Equal(t, float64(1024), attrs["backlogBytes"])
	assert.Contains(t, attrs, "backlogFileCount")
	assert.Equal(t, float64(10), attrs["backlogFileCount"])
}

func TestSetFlowMetrics_RowMetrics(t *testing.T) {
	// Setup now
	now := time.Now()

	// Setup times
	queueStartTime := now
	startTime := queueStartTime.Add(5 * time.Second)
	completionTime := startTime.Add(15 * time.Second)

	// Setup row metrics
	numOutputRows := float64(500)
	droppedRecords := float64(50)

	// Setup mock flow
	flow := &flowData{
		eventData: eventData{
			pipelineId:   "12345",
			pipelineName: "Test Pipeline",
			clusterId:    "fake-cluster-id",
			updateId:     "abcde",
		},
		id:               "flow-1",
		name:             "flow_1",
		queueStartTime:   queueStartTime,
		startTime:        startTime,
		completionTime:   completionTime,
		numOutputRows:  &numOutputRows,
		droppedRecords: &droppedRecords,
	}

	// Setup mock attributes
	attrs := make(map[string]interface{})

	// Execute the function under test
	setFlowMetrics(flow, attrs)

	// Verify the results
	assert.Contains(t, attrs, "outputRowCount")
	assert.Equal(t, float64(500), attrs["outputRowCount"])
	assert.Contains(t, attrs, "droppedRecordCount")
	assert.Equal(t, float64(50), attrs["droppedRecordCount"])
}

func TestAddFlowExpectations_MakePipelineBaseAttributesError(t *testing.T) {
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
	// makePipelineBaseAttributes will return an error
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

	// Setup times
	queueStartTime := now
	startTime := queueStartTime.Add(5 * time.Second)
	completionTime := startTime.Add(15 * time.Second)

	// Setup mock flow
	flow := &flowData{
		eventData: eventData{
			pipelineId:   "12345",
			pipelineName: "Test Pipeline",
			clusterId:    "fake-cluster-id",
			updateId:     "abcde",
		},
		id:               "flow-1",
		name:             "flow_1",
		queueStartTime:   queueStartTime,
		startTime:        startTime,
		completionTime:   completionTime,
		status:           FlowInfoStatusCompleted,
		expectations:     []FlowProgressExpectations{
			{
				Name:          "expectation_1",
				Dataset:       "dataset_1",
				PassedRecords: 100,
				FailedRecords: 10,
			},
		},
	}

	// Setup an events channel to receive events
	eventsChan := make(chan model.Event, 500) // Buffer to prevent blocking

	// Execute the function under test
	err := addFlowExpectations(
		context.Background(),
		mockWorkspace,
		flow,
		tags,
		eventsChan,
	)

	// Close the channel to iterate through it
	close(eventsChan)

	// Verify the results
	assert.Error(t, err)
	assert.Equal(t, expectedError, err.Error())
}

func TestAddFlowExpectations_NoExpectations(t *testing.T) {
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

	// Setup times
	queueStartTime := now
	startTime := queueStartTime.Add(5 * time.Second)
	completionTime := startTime.Add(15 * time.Second)

	// Setup mock flow
	flow := &flowData{
		eventData: eventData{
			pipelineId:   "12345",
			pipelineName: "Test Pipeline",
			clusterId:    "fake-cluster-id",
			updateId:     "abcde",
		},
		id:               "flow-1",
		name:             "flow_1",
		queueStartTime:   queueStartTime,
		startTime:        startTime,
		completionTime:   completionTime,
		status:           FlowInfoStatusCompleted,
	}

	// Setup an events channel to receive events
	eventsChan := make(chan model.Event, 500) // Buffer to prevent blocking

	// Execute the function under test
	err := addFlowExpectations(
		context.Background(),
		mockWorkspace,
		flow,
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

	// Verify events
	assert.Equal(
		t,
		0,
		len(events),
		"Expected no events to be produced when there are no flow expectations",
	)
}

func TestAddFlowExpectations_WithExpectations(t *testing.T) {
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

	// Setup times
	queueStartTime := now
	startTime := queueStartTime.Add(5 * time.Second)
	completionTime := startTime.Add(15 * time.Second)

	// Setup mock flow
	flow := &flowData{
		eventData: eventData{
			pipelineId:   "12345",
			pipelineName: "Test Pipeline",
			clusterId:    "fake-cluster-id",
			updateId:     "abcde",
		},
		id:               "flow-1",
		name:             "flow_1",
		queueStartTime:   queueStartTime,
		startTime:        startTime,
		completionTime:   completionTime,
		status:           FlowInfoStatusCompleted,
		expectations:     []FlowProgressExpectations{
			{
				Name:          "expectation_1",
				Dataset:       "dataset_1",
				PassedRecords: 100,
				FailedRecords: 10,
			},
			{
				Name:          "expectation_2",
				Dataset:       "dataset_1",
				PassedRecords: 200,
				FailedRecords: 20,
			},
			{
				Name:          "expectation_1",
				Dataset:       "dataset_2",
				PassedRecords: 300,
				FailedRecords: 30,
			},
		},
	}

	// Setup an events channel to receive events
	eventsChan := make(chan model.Event, 500) // Buffer to prevent blocking

	// Execute the function under test
	err := addFlowExpectations(
		context.Background(),
		mockWorkspace,
		flow,
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

	// Verify events
	assert.Equal(
		t,
		3,
		len(events),
		"Expected three events to be produced when there are three flow expectations",
	)

	// We expect 3 expectations
	foundExpectation1Dataset1 := false
	foundExpectation2Dataset1 := false
	foundExpectation1Dataset2 := false
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
		assert.Equal(
			t,
			"DatabricksPipelineFlowExpectation",
			event.Type,
		)

		// Verify attributes
		assert.NotNil(t, event.Attributes)

		// Verify common attributes
		attrs := event.Attributes
		assert.NotEmpty(t, attrs)

		// Verify attributes
		verifyTestPipelineCommonAttributes(t, attrs)

		// Verify cluster attributes
		verifyPipelineClusterAttributes(t, attrs)

		// Verify flow attributes
		assert.Contains(t, attrs, "databricksPipelineFlowId")
		assert.Equal(t, flow.id, attrs["databricksPipelineFlowId"])
		assert.Contains(t, attrs, "databricksPipelineFlowName")
		assert.Equal(t, flow.name, attrs["databricksPipelineFlowName"])

		// Verify flow expectation attributes
		assert.Contains(t, attrs, "name")
		name := attrs["name"].(string)
		assert.Contains(t, attrs, "dataset")
		dataset := attrs["dataset"].(string)

		if name == "expectation_1" && dataset == "dataset_1" {
			foundExpectation1Dataset1 = true

			assert.Contains(t, attrs, "passedRecordCount")
			assert.Equal(t, float64(100), attrs["passedRecordCount"])
			assert.Contains(t, attrs, "failedRecordCount")
			assert.Equal(t, float64(10), attrs["failedRecordCount"])
		} else if name == "expectation_2" && dataset == "dataset_1" {
			foundExpectation2Dataset1 = true

			assert.Contains(t, attrs, "passedRecordCount")
			assert.Equal(t, float64(200), attrs["passedRecordCount"])
			assert.Contains(t, attrs, "failedRecordCount")
			assert.Equal(t, float64(20), attrs["failedRecordCount"])
		} else if name == "expectation_1" && dataset == "dataset_2" {
			foundExpectation1Dataset2 = true

			assert.Contains(t, attrs, "passedRecordCount")
			assert.Equal(t, float64(300), attrs["passedRecordCount"])
			assert.Contains(t, attrs, "failedRecordCount")
			assert.Equal(t, float64(30), attrs["failedRecordCount"])
		} else {
			foundOther = true
		}
	}

	assert.True(t, foundExpectation1Dataset1)
	assert.True(t, foundExpectation2Dataset1)
	assert.True(t, foundExpectation1Dataset2)
	assert.False(t, foundOther)
}

func TestAddUpdates_OldUpdate(t *testing.T) {
	// Setup mock workspace
	mockWorkspace := setupMockWorkspace()

	// Setup mock tag map
	tags := map[string]string{
		"env": "production",
		"team": "data-engineering",
	}

	// Setup now
	now := time.Now()

	// Setup times to be used to test behavior of event production based on
	// lastRun time.
	lastRun := now
	creationTime := lastRun.Add(-30 * time.Second)
	startTime := creationTime.Add(5 * time.Second)
	completionTime := startTime.Add(10 * time.Second)

	// Setup mock updates
	updates := map[string]*updateData{
		"abcde": {
			eventData: eventData{
				pipelineId:   "12345",
				pipelineName: "Test Pipeline",
				clusterId:    "fake-cluster-id",
				updateId:     "abcde",
			},
			creationTime:     creationTime,
			startTime:        startTime,
			completionTime:   completionTime,
			state:            databricksSdkPipelines.UpdateInfoStateCompleted,
		},
	}

	// Setup old updates and include abcde so it will be ignored
	oldUpdates := map[string]struct{} {
		"abcde": {},
	}

	// Setup an events channel to receive events
	eventsChan := make(chan model.Event, 500) // Buffer to prevent blocking

	// Execute the function under test
	err := addUpdates(
		context.Background(),
		mockWorkspace,
		updates,
		oldUpdates,
		lastRun,
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

	// Verify events
	assert.Equal(
		t,
		0,
		len(events),
		"Expected no events to be produced when there are only old updates",
	)
}

func TestAddUpdates_CreationTimeZero(t *testing.T) {
	// Setup mock workspace
	mockWorkspace := setupMockWorkspace()

	// Setup mock tag map
	tags := map[string]string{
		"env": "production",
		"team": "data-engineering",
	}

	// Setup now
	now := time.Now()

	// Setup times to be used to test behavior of event production based on
	// lastRun time.
	lastRun := now
	startTime := lastRun.Add(5 * time.Second)

	// Setup mock updates with an update with a zero creation time. This can
	// happen if we find update_progress events for an update but do not find
	// the create_update for the update because the create_update occurred prior
	// to the startOffset.
	updates := map[string]*updateData{
		"abcde": {
			eventData: eventData{
				pipelineId:   "12345",
				pipelineName: "Test Pipeline",
				clusterId:    "fake-cluster-id",
				updateId:     "abcde",
			},
			startTime:        startTime,
			state:            databricksSdkPipelines.UpdateInfoStateRunning,
		},
	}

	// Setup old updates
	oldUpdates := map[string]struct{}{}

	// Setup an events channel to receive events
	eventsChan := make(chan model.Event, 500) // Buffer to prevent blocking

	// Execute the function under test
	err := addUpdates(
		context.Background(),
		mockWorkspace,
		updates,
		oldUpdates,
		lastRun,
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

	// Verify events
	assert.Equal(
		t,
		0,
		len(events),
		"Expected no events to be produced when the only update has a creation time of zero",
	)
}

func TestAddUpdates_UpdateCreatedBefore(t *testing.T) {
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
	// Set creationTime to before lastRun
	creationTime := lastRun.Add(-10 * time.Second)
	startTime := creationTime.Add(5 * time.Second)

	// Setup mock updates
	updates := map[string]*updateData{
		"abcde": {
			eventData: eventData{
				pipelineId:   "12345",
				pipelineName: "Test Pipeline",
				clusterId:    "fake-cluster-id",
				updateId:     "abcde",
			},
			creationTime:     creationTime,
			startTime:        startTime,
			state:            databricksSdkPipelines.UpdateInfoStateRunning,
		},
	}

	// Setup old updates
	oldUpdates := map[string]struct{}{}

	// Setup an events channel to receive events
	eventsChan := make(chan model.Event, 500) // Buffer to prevent blocking

	// Execute the function under test
	err := addUpdates(
		context.Background(),
		mockWorkspace,
		updates,
		oldUpdates,
		lastRun,
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

	// Verify events
	assert.Equal(
		t,
		0,
		len(events),
		"Expected no events to be produced when update has a creation time before the lastRun time",
	)
}

func TestAddUpdates_MakePipelineUpdateStartAttributesError(t *testing.T) {
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
	// makePipelineBaseAttributes will return an error so that
	// makePipelineUpdateBaseAttributes will return an error so that
	// makePipelineUpdateStartAttributes will return an error
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
	// Set creationTime to after lastRun to trigger
	// makePipelineUpdateStartAttributes
	creationTime := lastRun.Add(5 * time.Second)
	startTime := creationTime.Add(5 * time.Second)

	// Setup mock updates
	updates := map[string]*updateData{
		"abcde": {
			eventData: eventData{
				pipelineId:   "12345",
				pipelineName: "Test Pipeline",
				clusterId:    "fake-cluster-id",
				updateId:     "abcde",
			},
			creationTime:     creationTime,
			startTime:        startTime,
			state:            databricksSdkPipelines.UpdateInfoStateRunning,
		},
	}

	// Setup old updates
	oldUpdates := map[string]struct{}{}

	// Setup an events channel to receive events
	eventsChan := make(chan model.Event, 500) // Buffer to prevent blocking

	// Execute the function under test
	err := addUpdates(
		context.Background(),
		mockWorkspace,
		updates,
		oldUpdates,
		lastRun,
		tags,
		eventsChan,
	)

	// Close the channel to iterate through it
	close(eventsChan)

	// Verify result
	assert.Error(t, err)
	assert.Equal(t, expectedError, err.Error())

	// Collect all events for verification
	events := make([]model.Event, 0)
	for event := range eventsChan {
		events = append(events, event)
	}

	// Verify events
	assert.Equal(
		t,
		0,
		len(events),
		"Expected no events to be produced when makePipelineUpdateStartAttributes fails",
	)
}

func TestAddUpdates_UpdateCreatedAfter(t *testing.T) {
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
	// Set creationTime to after lastRun to trigger event production
	creationTime := lastRun.Add(5 * time.Second)
	startTime := creationTime.Add(5 * time.Second)

	// Setup mock updates
	updates := map[string]*updateData{
		"abcde": {
			eventData: eventData{
				pipelineId:   "12345",
				pipelineName: "Test Pipeline",
				clusterId:    "fake-cluster-id",
				updateId:     "abcde",
			},
			creationTime:     creationTime,
			startTime:        startTime,
			state:            databricksSdkPipelines.UpdateInfoStateRunning,
		},
	}

	// Setup old updates
	oldUpdates := map[string]struct{}{}

	// Setup an events channel to receive events
	eventsChan := make(chan model.Event, 500) // Buffer to prevent blocking

	// Execute the function under test
	err := addUpdates(
		context.Background(),
		mockWorkspace,
		updates,
		oldUpdates,
		lastRun,
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

	// Verify events
	assert.Equal(
		t,
		1,
		len(events),
		"Expected one event to be produced when update has a creation time after the lastRun time",
	)

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
		"DatabricksPipelineUpdate",
		event.Type,
	)

	// Verify attributes
	assert.NotNil(t, event.Attributes)
	attrs := event.Attributes
	assert.NotEmpty(t, attrs)

	// Verify attributes
	verifyTestPipelineCommonAttributes(t, attrs)

	// Verify cluster attributes
	verifyPipelineClusterAttributes(t, attrs)

	// Verify event
	assert.Contains(t, attrs, "event")
	assert.Equal(t, "start", attrs["event"])

	// Verify update base attributes
	verifyPipelineUpdateBaseAttributes(t, creationTime, attrs)

	// Verify no update complete attributes
	verifyNoPipelineUpdateCompleteAttributes(t, attrs)
}

func TestAddUpdates_UpdateCompletedBefore(t *testing.T) {
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
	creationTime := lastRun.Add(-30 * time.Second)
	startTime := creationTime.Add(5 * time.Second)
	// Set completionTime to before lastRun
	completionTime := startTime.Add(20 * time.Second)

	// Setup mock updates
	updates := map[string]*updateData{
		"abcde": {
			eventData: eventData{
				pipelineId:   "12345",
				pipelineName: "Test Pipeline",
				clusterId:    "fake-cluster-id",
				updateId:     "abcde",
			},
			creationTime:     creationTime,
			startTime:        startTime,
			completionTime:   completionTime,
			state:            databricksSdkPipelines.UpdateInfoStateCompleted,
		},
	}

	// Setup old updates
	oldUpdates := map[string]struct{}{}

	// Setup an events channel to receive events
	eventsChan := make(chan model.Event, 500) // Buffer to prevent blocking

	// Execute the function under test
	err := addUpdates(
		context.Background(),
		mockWorkspace,
		updates,
		oldUpdates,
		lastRun,
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

	// Verify events
	assert.Equal(
		t,
		0,
		len(events),
		"Expected no events to be produced when update has a completion time before the lastRun time",
	)
}

func TestAddUpdates_MakePipelineUpdateCompleteAttributesError(t *testing.T) {
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
	// makePipelineBaseAttributes will return an error so that
	// makePipelineUpdateBaseAttributes will return an error so that
	// makePipelineUpdateCompleteAttributes will return an error
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
	creationTime := lastRun.Add(-30 * time.Second)
	startTime := creationTime.Add(5 * time.Second)
	// Set completionTime to after lastRun to trigger
	// makePipelineUpdateCompleteAttributes
	completionTime := startTime.Add(30 * time.Second)

	// Setup mock updates
	updates := map[string]*updateData{
		"abcde": {
			eventData: eventData{
				pipelineId:   "12345",
				pipelineName: "Test Pipeline",
				clusterId:    "fake-cluster-id",
				updateId:     "abcde",
			},
			creationTime:     creationTime,
			startTime:        startTime,
			completionTime:   completionTime,
			state:            databricksSdkPipelines.UpdateInfoStateCompleted,
		},
	}

	// Setup old updates
	oldUpdates := map[string]struct{}{}

	// Setup an events channel to receive events
	eventsChan := make(chan model.Event, 500) // Buffer to prevent blocking

	// Execute the function under test
	err := addUpdates(
		context.Background(),
		mockWorkspace,
		updates,
		oldUpdates,
		lastRun,
		tags,
		eventsChan,
	)

	// Close the channel to iterate through it
	close(eventsChan)

	// Verify result
	assert.Error(t, err)
	assert.Equal(t, expectedError, err.Error())

	// Collect all events for verification
	events := make([]model.Event, 0)
	for event := range eventsChan {
		events = append(events, event)
	}

	// Verify events
	assert.Equal(
		t,
		0,
		len(events),
		"Expected no events to be produced when makePipelineUpdateCompleteAttributes fails",
	)
}

func TestAddUpdates_UpdateCompletedAfter(t *testing.T) {
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
	creationTime := lastRun.Add(-30 * time.Second)
	startTime := creationTime.Add(5 * time.Second)
	// Set completionTime to after lastRun to trigger event production
	completionTime := startTime.Add(30 * time.Second)

	// Setup mock updates
	updates := map[string]*updateData{
		"abcde": {
			eventData: eventData{
				pipelineId:   "12345",
				pipelineName: "Test Pipeline",
				clusterId:    "fake-cluster-id",
				updateId:     "abcde",
			},
			creationTime:     creationTime,
			startTime:        startTime,
			completionTime:   completionTime,
			state:            databricksSdkPipelines.UpdateInfoStateCompleted,
		},
	}

	// Setup old updates
	oldUpdates := map[string]struct{}{}

	// Setup an events channel to receive events
	eventsChan := make(chan model.Event, 500) // Buffer to prevent blocking

	// Execute the function under test
	err := addUpdates(
		context.Background(),
		mockWorkspace,
		updates,
		oldUpdates,
		lastRun,
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

	// Verify events
	assert.Equal(
		t,
		1,
		len(events),
		"Expected one event to be produced when update has a completion time after the lastRun time",
	)

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
		"DatabricksPipelineUpdate",
		event.Type,
	)

	// Verify attributes
	assert.NotNil(t, event.Attributes)
	attrs := event.Attributes
	assert.NotEmpty(t, attrs)

	// Verify attributes
	verifyTestPipelineCommonAttributes(t, attrs)

	// Verify cluster attributes
	verifyPipelineClusterAttributes(t, attrs)

	// Verify event
	assert.Contains(t, attrs, "event")
	assert.Equal(t, "complete", attrs["event"])

	// Verify update base attributes
	verifyPipelineUpdateBaseAttributes(t, creationTime, attrs)

	// Verify update complete attributes
	verifyPipelineUpdateCompleteAttributes(
		t,
		creationTime,
		time.Time{},
		startTime,
		completionTime,
		attrs,
	)
}

func TestAddFlows_OldFlow(t *testing.T) {
	// Setup mock workspace
	mockWorkspace := setupMockWorkspace()

	// Setup mock tag map
	tags := map[string]string{
		"env": "production",
		"team": "data-engineering",
	}

	// Setup now
	now := time.Now()

	// Setup times to be used to test behavior of event production based on
	// lastRun time.
	lastRun := now
	queueStartTime := lastRun.Add(-30 * time.Second)
	startTime := queueStartTime.Add(5 * time.Second)
	completionTime := startTime.Add(10 * time.Second)

	// Setup mock flows
	flows := map[string]*flowData{
		"flow_1": {
			eventData: eventData{
				pipelineId:   "12345",
				pipelineName: "Test Pipeline",
				clusterId:    "fake-cluster-id",
				updateId:     "abcde",
			},
			id:               "flow-1",
			name:             "flow_1",
			queueStartTime:   queueStartTime,
			startTime:        startTime,
			completionTime:   completionTime,
			status:           FlowInfoStatusCompleted,
		},
	}

	// Setup old flows and include abcde.flow_1 so it will be ignored
	oldFlows := map[string]struct{} {
		"abcde.flow_1": {},
	}

	// Setup an events channel to receive events
	eventsChan := make(chan model.Event, 500) // Buffer to prevent blocking

	// Execute the function under test
	err := addFlows(
		context.Background(),
		mockWorkspace,
		flows,
		oldFlows,
		lastRun,
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

	// Verify events
	assert.Equal(
		t,
		0,
		len(events),
		"Expected no events to be produced when there are only old flows",
	)
}

func TestAddFlows_QueueStartTimeZero(t *testing.T) {
	// Setup mock workspace
	mockWorkspace := setupMockWorkspace()

	// Setup mock tag map
	tags := map[string]string{
		"env": "production",
		"team": "data-engineering",
	}

	// Setup now
	now := time.Now()

	// Setup times to be used to test behavior of event production based on
	// lastRun time.
	lastRun := now
	startTime := lastRun.Add(5 * time.Second)

	// Setup mock flows with a flow with a zero queue start time. This can
	// happen if we find flow_progress events for a flow but do not find the
	// flow_progress QUEUED event for that flow because it occurred prior to the
	// startOffset.
	flows := map[string]*flowData{
		"flow_1": {
			eventData: eventData{
				pipelineId:   "12345",
				pipelineName: "Test Pipeline",
				clusterId:    "fake-cluster-id",
				updateId:     "abcde",
			},
			id:               "flow-1",
			name:             "flow_1",
			startTime:        startTime,
			status:           FlowInfoStatusRunning,
		},
	}

	// Setup old flows
	oldFlows := map[string]struct{}{}

	// Setup an events channel to receive events
	eventsChan := make(chan model.Event, 500) // Buffer to prevent blocking

	// Execute the function under test
	err := addFlows(
		context.Background(),
		mockWorkspace,
		flows,
		oldFlows,
		lastRun,
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

	// Verify events
	assert.Equal(
		t,
		0,
		len(events),
		"Expected no events to be produced when the only flow has a queue start time of zero",
	)
}

func TestAddFlows_FlowStartedBefore(t *testing.T) {
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
	// Set queueStartTime to before last run
	queueStartTime := lastRun.Add(-10 * time.Second)
	startTime := queueStartTime.Add(5 * time.Second)

	// Setup mock flows
	flows := map[string]*flowData{
		"flow_1": {
			eventData: eventData{
				pipelineId:   "12345",
				pipelineName: "Test Pipeline",
				clusterId:    "fake-cluster-id",
				updateId:     "abcde",
			},
			id:               "flow-1",
			name:             "flow_1",
			queueStartTime:   queueStartTime,
			startTime:        startTime,
			status:           FlowInfoStatusRunning,
		},
	}

	// Setup old flows
	oldFlows := map[string]struct{}{}

	// Setup an events channel to receive events
	eventsChan := make(chan model.Event, 500) // Buffer to prevent blocking

	// Execute the function under test
	err := addFlows(
		context.Background(),
		mockWorkspace,
		flows,
		oldFlows,
		lastRun,
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

	// Verify events
	assert.Equal(
		t,
		0,
		len(events),
		"Expected no events to be produced when flow has a queue start time before the lastRun time",
	)
}

func TestAddFlows_MakePipelineFlowStartAttributesError(t *testing.T) {
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
	// makePipelineBaseAttributes will return an error so that
	// makePipelineFlowBaseAttributes will return an error so that
	// makePipelineFlowStartAttributes will return an error
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
	// Set queueStartTime to after last run to trigger
	// makePipelineFlowStartAttributes
	queueStartTime := lastRun.Add(5 * time.Second)
	startTime := queueStartTime.Add(5 * time.Second)

	// Setup mock flows
	flows := map[string]*flowData{
		"flow_1": {
			eventData: eventData{
				pipelineId:   "12345",
				pipelineName: "Test Pipeline",
				clusterId:    "fake-cluster-id",
				updateId:     "abcde",
			},
			id:               "flow-1",
			name:             "flow_1",
			queueStartTime:   queueStartTime,
			startTime:        startTime,
			status:           FlowInfoStatusRunning,
		},
	}

	// Setup old flows
	oldFlows := map[string]struct{}{}

	// Setup an events channel to receive events
	eventsChan := make(chan model.Event, 500) // Buffer to prevent blocking

	// Execute the function under test
	err := addFlows(
		context.Background(),
		mockWorkspace,
		flows,
		oldFlows,
		lastRun,
		tags,
		eventsChan,
	)

	// Close the channel to iterate through it
	close(eventsChan)

	// Verify result
	assert.Error(t, err)
	assert.Equal(t, expectedError, err.Error())

	// Collect all events for verification
	events := make([]model.Event, 0)
	for event := range eventsChan {
		events = append(events, event)
	}

	// Verify events
	assert.Equal(
		t,
		0,
		len(events),
		"Expected no events to be produced when makePipelineFlowStartAttributes fails",
	)
}

func TestAddFlows_FlowStartedAfter(t *testing.T) {
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
	// Set queueStartTime to after last run to trigger event production
	queueStartTime := lastRun.Add(5 * time.Second)
	startTime := queueStartTime.Add(5 * time.Second)

	// Setup mock flows
	flows := map[string]*flowData{
		"flow_1": {
			eventData: eventData{
				pipelineId:   "12345",
				pipelineName: "Test Pipeline",
				clusterId:    "fake-cluster-id",
				updateId:     "abcde",
			},
			id:               "flow-1",
			name:             "flow_1",
			queueStartTime:   queueStartTime,
			startTime:        startTime,
			status:           FlowInfoStatusRunning,
		},
	}

	// Setup old flows
	oldFlows := map[string]struct{}{}

	// Setup an events channel to receive events
	eventsChan := make(chan model.Event, 500) // Buffer to prevent blocking

	// Execute the function under test
	err := addFlows(
		context.Background(),
		mockWorkspace,
		flows,
		oldFlows,
		lastRun,
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

	// Verify events
	assert.Equal(
		t,
		1,
		len(events),
		"Expected one event to be produced when flow has a queue start time after the lastRun time",
	)

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
		"DatabricksPipelineFlow",
		event.Type,
	)

	// Verify attributes
	assert.NotNil(t, event.Attributes)
	attrs := event.Attributes
	assert.NotEmpty(t, attrs)

	// Verify attributes
	verifyTestPipelineCommonAttributes(t, attrs)

	// Verify cluster attributes
	verifyPipelineClusterAttributes(t, attrs)

	// Verify event
	assert.Contains(t, attrs, "event")
	assert.Equal(t, "start", attrs["event"])

	// Verify flow start attributes
	verifyPipelineFlowBaseAttributes(
		t,
		"flow-1",
		"flow_1",
		queueStartTime,
		attrs,
	)

	// Verify no flow complete attributes
	verifyNoPipelineFlowCompleteAttributes(t, attrs)
}

func TestAddFlows_FlowCompletedBefore(t *testing.T) {
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
	queueStartTime := lastRun.Add(-30 * time.Second)
	startTime := queueStartTime.Add(5 * time.Second)
	// Set completionTime to before lastRun
	completionTime := startTime.Add(20 * time.Second)

	// Setup mock flows
	flows := map[string]*flowData{
		"flow_1": {
			eventData: eventData{
				pipelineId:   "12345",
				pipelineName: "Test Pipeline",
				clusterId:    "fake-cluster-id",
				updateId:     "abcde",
			},
			id:               "flow-1",
			name:             "flow_1",
			queueStartTime:   queueStartTime,
			startTime:        startTime,
			completionTime:   completionTime,
			status:           FlowInfoStatusCompleted,
		},
	}

	// Setup old flows
	oldFlows := map[string]struct{}{}

	// Setup an events channel to receive events
	eventsChan := make(chan model.Event, 500) // Buffer to prevent blocking

	// Execute the function under test
	err := addFlows(
		context.Background(),
		mockWorkspace,
		flows,
		oldFlows,
		lastRun,
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

	// Verify events
	assert.Equal(
		t,
		0,
		len(events),
		"Expected no events to be produced when flow has a completion time before the lastRun time",
	)
}

func TestAddFlows_MakePipelineFlowCompleteAttributesError(t *testing.T) {
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
	// makePipelineBaseAttributes will return an error so that
	// makePipelineFlowBaseAttributes will return an error so that
	// makePipelineFlowCompleteAttributes will return an error
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
	queueStartTime := lastRun.Add(-30 * time.Second)
	startTime := queueStartTime.Add(5 * time.Second)
	// Set completionTime to after lastRun to trigger
	// makePipelineFlowCompleteAttributes
	completionTime := startTime.Add(30 * time.Second)

	// Setup backlog metrics
	backlogBytes := float64(1024)
	backlogFiles := float64(10)

	// Setup row metrics
	numOutputRows := float64(500)
	droppedRecords := float64(50)

	// Setup mock flows
	flows := map[string]*flowData{
		"flow_1": {
			eventData: eventData{
				pipelineId:   "12345",
				pipelineName: "Test Pipeline",
				clusterId:    "fake-cluster-id",
				updateId:     "abcde",
			},
			id:               "flow-1",
			name:             "flow_1",
			queueStartTime:   queueStartTime,
			startTime:        startTime,
			completionTime:   completionTime,
			status:           FlowInfoStatusCompleted,
			backlogBytes:     &backlogBytes,
			backlogFiles:     &backlogFiles,
			numOutputRows:    &numOutputRows,
			droppedRecords:   &droppedRecords,
		},
	}

	// Setup old flows
	oldFlows := map[string]struct{}{}

	// Setup an events channel to receive events
	eventsChan := make(chan model.Event, 500) // Buffer to prevent blocking

	// Execute the function under test
	err := addFlows(
		context.Background(),
		mockWorkspace,
		flows,
		oldFlows,
		lastRun,
		tags,
		eventsChan,
	)

	// Close the channel to iterate through it
	close(eventsChan)

	// Verify result
	assert.Error(t, err)
	assert.Equal(t, expectedError, err.Error())

	// Collect all events for verification
	events := make([]model.Event, 0)
	for event := range eventsChan {
		events = append(events, event)
	}

	// Verify events
	assert.Equal(
		t,
		0,
		len(events),
		"Expected no events to be produced when makePipelineFlowCompleteAttributes fails",
	)
}

func TestAddFlows_FlowCompletedAfter(t *testing.T) {
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
	queueStartTime := lastRun.Add(-30 * time.Second)
	startTime := queueStartTime.Add(5 * time.Second)
	// Set completionTime to after lastRun to trigger event production
	completionTime := startTime.Add(30 * time.Second)

	// Setup backlog metrics
	backlogBytes := float64(1024)
	backlogFiles := float64(10)

	// Setup row metrics
	numOutputRows := float64(500)
	droppedRecords := float64(50)

	// Setup mock flows
	flows := map[string]*flowData{
		"flow_1": {
			eventData: eventData{
				pipelineId:   "12345",
				pipelineName: "Test Pipeline",
				clusterId:    "fake-cluster-id",
				updateId:     "abcde",
			},
			id:               "flow-1",
			name:             "flow_1",
			queueStartTime:   queueStartTime,
			startTime:        startTime,
			completionTime:   completionTime,
			status:           FlowInfoStatusCompleted,
			backlogBytes:     &backlogBytes,
			backlogFiles:     &backlogFiles,
			numOutputRows:    &numOutputRows,
			droppedRecords:   &droppedRecords,
			expectations:     []FlowProgressExpectations{
				{
					Name:          "expectation_1",
					Dataset:       "dataset_1",
					PassedRecords: 100,
					FailedRecords: 10,
				},
				{
					Name:          "expectation_2",
					Dataset:       "dataset_1",
					PassedRecords: 200,
					FailedRecords: 20,
				},
				{
					Name:          "expectation_1",
					Dataset:       "dataset_2",
					PassedRecords: 300,
					FailedRecords: 30,
				},
			},
		},
	}

	// Setup old flows
	oldFlows := map[string]struct{}{}

	// Setup an events channel to receive events
	eventsChan := make(chan model.Event, 500) // Buffer to prevent blocking

	// Execute the function under test
	err := addFlows(
		context.Background(),
		mockWorkspace,
		flows,
		oldFlows,
		lastRun,
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

	// Verify the number of events produced
	// We expect:
	// - 1 flow event
	// - 3 flow expectation events
	// This gives us a total of 4 events
	assert.Equal(
		t,
		4,
		len(events),
		"Expected 4 events to be produced when flow has a completion time after the lastRun time and has 3 expectations",
	)

	// Valid event types
	validEventTypes := []string{
		"DatabricksPipelineFlow",
		"DatabricksPipelineFlowExpectation",
	}

	// These should flip to true
	foundFlow := false
	foundExpectation1Dataset1 := false
	foundExpectation2Dataset1 := false
	foundExpectation1Dataset2 := false
	// This should stay false
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

		// Verify attributes
		verifyTestPipelineCommonAttributes(t, attrs)

		// Verify cluster attributes
		verifyPipelineClusterAttributes(t, attrs)

		if event.Type == "DatabricksPipelineFlow" {
			foundFlow = true

			// Verify event
			assert.Contains(t, attrs, "event")
			assert.Equal(t, "complete", attrs["event"])

			// Verify flow base attributes
			verifyPipelineFlowBaseAttributes(
				t,
				"flow-1",
				"flow_1",
				queueStartTime,
				attrs,
			)

			// Verify flow complete attributes
			verifyPipelineFlowCompleteAttributes(
				t,
				queueStartTime,
				time.Time{},
				startTime,
				completionTime,
				&backlogBytes,
				&backlogFiles,
				&numOutputRows,
				&droppedRecords,
				attrs,
			)
		} else if event.Type == "DatabricksPipelineFlowExpectation" {
			flow := flows["flow_1"]

			// Verify flow attributes
			assert.Contains(t, attrs, "databricksPipelineFlowId")
			assert.Equal(t, flow.id, attrs["databricksPipelineFlowId"])
			assert.Contains(t, attrs, "databricksPipelineFlowName")
			assert.Equal(t, flow.name, attrs["databricksPipelineFlowName"])

			// Verify flow expectation attributes
			assert.Contains(t, attrs, "name")
			name := attrs["name"].(string)
			assert.Contains(t, attrs, "dataset")
			dataset := attrs["dataset"].(string)

			if name == "expectation_1" && dataset == "dataset_1" {
				foundExpectation1Dataset1 = true

				assert.Contains(t, attrs, "passedRecordCount")
				assert.Equal(t, float64(100), attrs["passedRecordCount"])
				assert.Contains(t, attrs, "failedRecordCount")
				assert.Equal(t, float64(10), attrs["failedRecordCount"])
			} else if name == "expectation_2" && dataset == "dataset_1" {
				foundExpectation2Dataset1 = true

				assert.Contains(t, attrs, "passedRecordCount")
				assert.Equal(t, float64(200), attrs["passedRecordCount"])
				assert.Contains(t, attrs, "failedRecordCount")
				assert.Equal(t, float64(20), attrs["failedRecordCount"])
			} else if name == "expectation_1" && dataset == "dataset_2" {
				foundExpectation1Dataset2 = true

				assert.Contains(t, attrs, "passedRecordCount")
				assert.Equal(t, float64(300), attrs["passedRecordCount"])
				assert.Contains(t, attrs, "failedRecordCount")
				assert.Equal(t, float64(30), attrs["failedRecordCount"])
			} else {
				foundOther = true
			}
		} else {
			foundOther = true
		}
	}

	assert.True(t, foundFlow)
	assert.True(t, foundExpectation1Dataset1)
	assert.True(t, foundExpectation2Dataset1)
	assert.True(t, foundExpectation1Dataset2)
	assert.False(t, foundOther)
}
