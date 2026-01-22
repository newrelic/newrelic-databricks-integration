package spark

import (
	"context"
	"errors"
	"testing"

	"github.com/newrelic/newrelic-databricks-integration/internal/databricks"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

func setupMockWorkspace() (*databricks.MockWorkspace, func()) {
	mock := &databricks.MockWorkspace{}
	mock.WorkspaceId = 12345
	// Note that in our documentation, we recommend using only the hostname
	// instead of the full URL for the workspaceHost. Internally, the SDK will
	// fix the value to be a valid URL by prepending the "https://" prefix and
	// storing the fixed up URL back in the Host field. Since this is a mock
	// workspace, we won't go through that fixup so we need to set the host
	// to the full URL so our tests can test for both the full URL and the
	// instance name which is stored into the WorkspaceInfo struct.
	mock.Config.Host = "https://foo.fakedomain.local"

	// Set NewDatabricksWorkspace to return a mock workspace
	originalNewDatabricksWorkspace := databricks.NewDatabricksWorkspace
	databricks.NewDatabricksWorkspace = func() (
		w databricks.DatabricksWorkspace,
		err error,
	) {
		return mock, nil
	}

	// Return a teardown function to restore the original NewDatabricksWorkspace
	return mock, func() {
		databricks.NewDatabricksWorkspace = originalNewDatabricksWorkspace
	}
}

func setupMockClusterInfo() func() {
	originalGetClusterInfoById := databricks.GetClusterInfoById
	databricks.GetClusterInfoById = func(
		ctx context.Context,
		w databricks.DatabricksWorkspace,
		clusterId string,
	) (
		*databricks.ClusterInfo,
		error,
	) {
		return &databricks.ClusterInfo{
			Name:           "fake-cluster-name",
			Source:         "fake-cluster-source",
			Creator:        "fake-cluster-creator",
			InstancePoolId: "fake-cluster-instance-pool-id",
			SingleUserName: "fake-cluster-single-user-name",
		}, nil
	}

	// Return a teardown function to restore the original GetClusterInfoById
	return func() {
		databricks.GetClusterInfoById = originalGetClusterInfoById
	}
}

// Helper function to create a new DatabricksSparkEventDecorator with a fake
// cluster ID for tests
func newDatabricksSparkEventDecorator() (
	*DatabricksSparkEventDecorator,
	error,
) {
	return NewDatabricksSparkEventDecorator(
		context.Background(),
		"fake-cluster-id",
	)
}

func TestParseDatabricksSparkJobGroup(t *testing.T) {
	// Reset viper config to ensure clean test state
	viper.Reset()

	// Setup tests
	tests := []struct {
		name     string
		jobGroup string
		want     *databricksSparkJobGroupInfo
	}{
		{
			name:     "valid job group format for job run",
			jobGroup: "spark_job_job-12345-run-67890-action-54321",
			want: &databricksSparkJobGroupInfo{
				jobId:      12345,
				taskRunId:  67890,
				pipelineId: "",
				updateId:   "",
				flowId:     "",
			},
		},
		{
			name:     "valid job group with whitespace",
			jobGroup: "spark_    job  _job-12345-run-67890-action-54321  ",
			want: &databricksSparkJobGroupInfo{
				jobId:      12345,
				taskRunId:  67890,
				pipelineId: "",
				updateId:   "",
				flowId:     "",
			},
		},
		{
			name:     "valid job group format for dlt pipelineId",
			jobGroup: "spark_job_dlt-67890-54321",
			want: &databricksSparkJobGroupInfo{
				jobId:      -1,
				taskRunId:  -1,
				pipelineId: "67890-54321",
				updateId:   "",
				flowId:     "",
			},
		},
		{
			name:     "valid job group format for pipeline flow",
			jobGroup: "12345#67890#54321",
			want: &databricksSparkJobGroupInfo{
				jobId:      -1,
				taskRunId:  -1,
				pipelineId: "12345",
				updateId:   "54321",
				flowId:     "67890",
			},
		},
		{
			name:     "unrecognized format",
			jobGroup: "spark_job_unrecognized-format",
			want:     nil,
		},
		{
			name:     "non-numeric jobId",
			jobGroup: "spark_job_job-abc-run-67890-action-54321",
			want:     nil,
		},
		{
			name:     "non-numeric taskRunId",
			jobGroup: "spark_job_job-12345-run-xyz-action-54321",
			want:     nil,
		},
		{
			name:     "empty job group",
			jobGroup: "",
			want:     nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Execute the function under test
			got := parseDatabricksSparkJobGroup(tt.jobGroup)

			// Verify results
			if tt.want == nil {
				assert.Nil(t, got)
			} else {
				assert.NotNil(t, got)
				assert.Equal(t, tt.want.jobId, got.jobId)
				assert.Equal(t, tt.want.taskRunId, got.taskRunId)
				assert.Equal(t, tt.want.pipelineId, got.pipelineId)
				assert.Equal(t, tt.want.updateId, got.updateId)
				assert.Equal(t, tt.want.flowId, got.flowId)
			}
		})
	}
}

func TestNewDatabricksSparkEventDecorator(t *testing.T) {
	// Reset viper config to ensure clean test state
	viper.Reset()

	// Setup mock workspace
	mock, teardown := setupMockWorkspace()
	defer teardown()

	// Setup mock cluster info
	teardown2 := setupMockClusterInfo()
	defer teardown2()

	// Execute NewDatabricksEventDecorator with default configuration
	decorator, err := NewDatabricksSparkEventDecorator(
		context.Background(),
		"fake-cluster-id",
	)

	// Verify results
	assert.NotNil(t, decorator)
	assert.NoError(t, err)
	assert.Equal(t, decorator.w, mock)
	assert.NotNil(t, decorator.stageIdsToJobs)
	assert.Empty(t, decorator.stageIdsToJobs)
	assert.NotNil(t, decorator.workspaceInfo)
	assert.Equal(t, int64(12345), decorator.workspaceInfo.Id)
	assert.Equal(t, "https://foo.fakedomain.local", decorator.workspaceInfo.Url)
	assert.Equal(t, "foo.fakedomain.local", decorator.workspaceInfo.InstanceName)
	assert.NotNil(t, decorator.clusterInfo)
	assert.Equal(t, "fake-cluster-id", decorator.clusterId)
	assert.Equal(t, "fake-cluster-name", decorator.clusterInfo.Name)
	assert.Equal(t, "fake-cluster-source", decorator.clusterInfo.Source)
	assert.Equal(t, "fake-cluster-creator", decorator.clusterInfo.Creator)
	assert.Equal(
		t,
		"fake-cluster-instance-pool-id",
		decorator.clusterInfo.InstancePoolId,
	)
	assert.Equal(
		t,
		"fake-cluster-single-user-name",
		decorator.clusterInfo.SingleUserName,
	)
}

func TestNewDatabricksSparkEventDecorator_NewDatabricksWorkspaceError(t *testing.T) {
	// Reset viper config to ensure clean test state
	viper.Reset()

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

	// Execute the function under test
	decorator, err := NewDatabricksSparkEventDecorator(
		context.Background(),
		"fake-cluster-id",
	)

	// Verify results
	assert.Error(t, err)
	assert.Nil(t, decorator, "Should return nil when error occurs")
	assert.Equal(t, expectedError, err.Error())
}

func TestNewDatabricksSparkEventDecorator_GetWorkspaceInfoError(t *testing.T) {
	// Reset viper config to ensure clean test state
	viper.Reset()

	// Setup expected error message
	expectedError := "error getting workspace info"

	// Setup mock workspace
	_, teardown := setupMockWorkspace()
	defer teardown()

	// Save original GetWorkspaceInfo function and setup a defer function
	// to restore after the test
	originalGetWorkspaceInfo := databricks.GetWorkspaceInfo
	defer func() {
        databricks.GetWorkspaceInfo = originalGetWorkspaceInfo
    }()


	// Setup GetWorkspaceInfo to return an error
    databricks.GetWorkspaceInfo = func(
		ctx context.Context,
		w databricks.DatabricksWorkspace,
	) (
        *databricks.WorkspaceInfo,
        error,
    ) {
		return nil, errors.New(expectedError)
	}

	// Execute the function under test
	decorator, err := NewDatabricksSparkEventDecorator(
		context.Background(),
		"fake-cluster-id",
	)

	// Verify results
	assert.Error(t, err)
	assert.Nil(t, decorator, "Should return nil when error occurs")
	assert.Equal(t, expectedError, err.Error())
}

func TestNewDatabricksSparkEventDecorator_GetClusterInfoError(t *testing.T) {
	// Reset viper config to ensure clean test state
	viper.Reset()

	// Setup expected error message
	expectedError := "error getting cluster info"

	// Setup mock workspace
	_, teardown := setupMockWorkspace()
	defer teardown()

	// Save original GetClusterInfoById function and setup a defer function
	// to restore after the test
	originalGetClusterInfoById := databricks.GetClusterInfoById
	defer func() {
        databricks.GetClusterInfoById = originalGetClusterInfoById
    }()


	// Setup GetClusterInfoById to return an error
    databricks.GetClusterInfoById = func(
		ctx context.Context,
		w databricks.DatabricksWorkspace,
		clusterId string,
	) (
        *databricks.ClusterInfo,
        error,
    ) {
		return nil, errors.New(expectedError)
	}

	// Execute the function under test
	decorator, err := NewDatabricksSparkEventDecorator(
		context.Background(),
		"fake-cluster-id",
	)

	// Verify results
	assert.Error(t, err)
	assert.Nil(t, decorator, "Should return nil when error occurs")
	assert.Equal(t, expectedError, err.Error())
}

func TestDecorate(t *testing.T) {
	// Reset viper config to ensure clean test state
	viper.Reset()

	// Setup mock workspace
	_, teardown := setupMockWorkspace()
	defer teardown()

	// Setup mock cluster info
	teardown2 := setupMockClusterInfo()
	defer teardown2()

	// Setup the attributes map
	attrs := make(map[string]interface{})

	// Create the decorator instance
	decorator, _ := newDatabricksSparkEventDecorator()

	// Execute the function under test with no job info
	decorator.decorate(nil, attrs)

	// Verify results
	assert.Contains(t, attrs, "databricksWorkspaceId")
	assert.Equal(t, int64(12345), attrs["databricksWorkspaceId"])
	assert.Contains(t, attrs, "databricksWorkspaceName")
	assert.Equal(t, "foo.fakedomain.local", attrs["databricksWorkspaceName"])
	assert.Contains(t, attrs, "databricksWorkspaceUrl")
	assert.Equal(
		t,
		"https://foo.fakedomain.local",
		attrs["databricksWorkspaceUrl"],
	)
	assert.Contains(t, attrs, "databricksClusterId")
	assert.Equal(t, "fake-cluster-id", attrs["databricksClusterId"])
	assert.Contains(t, attrs, "databricksClusterName")
	assert.Equal(
		t,
		"fake-cluster-name",
		attrs["databricksClusterName"],
	)
	assert.Contains(t, attrs, "databricksclustername")
	assert.Equal(
		t,
		"fake-cluster-name",
		attrs["databricksclustername"],
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
	assert.NotContains(t, attrs, "databricksJobId")
	assert.NotContains(t, attrs, "databricksJobRunTaskRunId")
	assert.NotContains(t, attrs, "databricksPipelineId")
	assert.NotContains(t, attrs, "databricksPipelineUpdateId")
	assert.NotContains(t, attrs, "databricksPipelineFlowId")
}

func TestDecorate_Job(t *testing.T) {
	// Reset viper config to ensure clean test state
	viper.Reset()

	// Setup mock workspace
	_, teardown := setupMockWorkspace()
	defer teardown()

	// Setup mock cluster info
	teardown2 := setupMockClusterInfo()
	defer teardown2()

	// Setup the attributes map
	attrs := make(map[string]interface{})

	// Setup a mock job group info with job information
	sparkJobInfo := &databricksSparkJobGroupInfo{
		jobId:      12345,
		taskRunId:  67890,
		pipelineId: "",
		updateId:   "",
		flowId:     "",
	}

	// Create the decorator instance
	decorator, _ := newDatabricksSparkEventDecorator()

	// Execute the function under test with job info
	decorator.decorate(sparkJobInfo, attrs)

	// Verify results
	assert.Contains(t, attrs, "databricksWorkspaceId")
	assert.Equal(t, int64(12345), attrs["databricksWorkspaceId"])
	assert.Contains(t, attrs, "databricksWorkspaceName")
	assert.Equal(t, "foo.fakedomain.local", attrs["databricksWorkspaceName"])
	assert.Contains(t, attrs, "databricksWorkspaceUrl")
	assert.Equal(
		t,
		"https://foo.fakedomain.local",
		attrs["databricksWorkspaceUrl"],
	)
	assert.Contains(t, attrs, "databricksClusterId")
	assert.Equal(t, "fake-cluster-id", attrs["databricksClusterId"])
	assert.Contains(t, attrs, "databricksClusterName")
	assert.Equal(
		t,
		"fake-cluster-name",
		attrs["databricksClusterName"],
	)
	assert.Contains(t, attrs, "databricksclustername")
	assert.Equal(
		t,
		"fake-cluster-name",
		attrs["databricksclustername"],
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
	assert.Contains(t, attrs, "databricksJobId")
	assert.Equal(t, int64(12345), attrs["databricksJobId"])
	assert.Contains(t, attrs, "databricksJobRunTaskRunId")
	assert.Equal(t, int64(67890), attrs["databricksJobRunTaskRunId"])
	assert.NotContains(t, attrs, "databricksPipelineId")
	assert.NotContains(t, attrs, "databricksPipelineUpdateId")
	assert.NotContains(t, attrs, "databricksPipelineFlowId")
}

func TestDecorate_Pipeline(t *testing.T) {
	// Reset viper config to ensure clean test state
	viper.Reset()

	// Setup mock workspace
	_, teardown := setupMockWorkspace()
	defer teardown()

	// Setup mock cluster info
	teardown2 := setupMockClusterInfo()
	defer teardown2()

	// Setup the attributes map
	attrs := make(map[string]interface{})

	// Setup a mock job group info with pipeline ID only
	sparkJobInfo := &databricksSparkJobGroupInfo{
		jobId:      -1,
		taskRunId:  -1,
		pipelineId: "12345",
		updateId:   "",
		flowId:     "",
	}

	// Create the decorator instance
	decorator, _ := newDatabricksSparkEventDecorator()

	// Execute the function under test with pipeline info
	decorator.decorate(sparkJobInfo, attrs)

	// Verify results
	assert.Contains(t, attrs, "databricksWorkspaceId")
	assert.Equal(t, int64(12345), attrs["databricksWorkspaceId"])
	assert.Contains(t, attrs, "databricksWorkspaceName")
	assert.Equal(t, "foo.fakedomain.local", attrs["databricksWorkspaceName"])
	assert.Contains(t, attrs, "databricksWorkspaceUrl")
	assert.Equal(
		t,
		"https://foo.fakedomain.local",
		attrs["databricksWorkspaceUrl"],
	)
	assert.Contains(t, attrs, "databricksClusterId")
	assert.Equal(t, "fake-cluster-id", attrs["databricksClusterId"])
	assert.Contains(t, attrs, "databricksClusterName")
	assert.Equal(
		t,
		"fake-cluster-name",
		attrs["databricksClusterName"],
	)
	assert.Contains(t, attrs, "databricksclustername")
	assert.Equal(
		t,
		"fake-cluster-name",
		attrs["databricksclustername"],
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
	assert.NotContains(t, attrs, "databricksJobId")
	assert.NotContains(t, attrs, "databricksJobRunTaskRunId")
	assert.Contains(t, attrs, "databricksPipelineId")
	assert.Equal(t, "12345", attrs["databricksPipelineId"])
	assert.NotContains(t, attrs, "databricksPipelineUpdateId")
	assert.NotContains(t, attrs, "databricksPipelineFlowId")
}

func TestDecorate_PipelineUpdate(t *testing.T) {
	// Reset viper config to ensure clean test state
	viper.Reset()

	// Setup mock workspace
	_, teardown := setupMockWorkspace()
	defer teardown()

	// Setup mock cluster info
	teardown2 := setupMockClusterInfo()
	defer teardown2()

	// Setup the attributes map
	attrs := make(map[string]interface{})

	// Setup a mock job group info with pipeline update info
	sparkJobInfo := &databricksSparkJobGroupInfo{
		jobId:      -1,
		taskRunId:  -1,
		pipelineId: "12345",
		updateId:   "54321",
		flowId:     "67890",
	}

	// Create the decorator instance
	decorator, _ := newDatabricksSparkEventDecorator()

	// Execute the function under test with pipeline update info
	decorator.decorate(sparkJobInfo, attrs)

	// Verify results
	assert.Contains(t, attrs, "databricksWorkspaceId")
	assert.Equal(t, int64(12345), attrs["databricksWorkspaceId"])
	assert.Contains(t, attrs, "databricksWorkspaceName")
	assert.Equal(t, "foo.fakedomain.local", attrs["databricksWorkspaceName"])
	assert.Contains(t, attrs, "databricksWorkspaceUrl")
	assert.Equal(
		t,
		"https://foo.fakedomain.local",
		attrs["databricksWorkspaceUrl"],
	)
	assert.Contains(t, attrs, "databricksClusterId")
	assert.Equal(t, "fake-cluster-id", attrs["databricksClusterId"])
	assert.Contains(t, attrs, "databricksClusterName")
	assert.Equal(
		t,
		"fake-cluster-name",
		attrs["databricksClusterName"],
	)
	assert.Contains(t, attrs, "databricksclustername")
	assert.Equal(
		t,
		"fake-cluster-name",
		attrs["databricksclustername"],
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
	assert.NotContains(t, attrs, "databricksJobId")
	assert.NotContains(t, attrs, "databricksJobRunTaskRunId")
	assert.Contains(t, attrs, "databricksPipelineId")
	assert.Equal(t, "12345", attrs["databricksPipelineId"])
	assert.Contains(t, attrs, "databricksPipelineUpdateId")
	assert.Equal(t, "54321", attrs["databricksPipelineUpdateId"])
	assert.Contains(t, attrs, "databricksPipelineFlowId")
	assert.Equal(t, "67890", attrs["databricksPipelineFlowId"])
}

func TestDecorateExecutor(t *testing.T) {
	// Reset viper config to ensure clean test state
	viper.Reset()

	// Setup mock workspace
	_, teardown := setupMockWorkspace()
	defer teardown()

	// Setup mock cluster info
	teardown2 := setupMockClusterInfo()
	defer teardown2()

	// Setup the attributes map
	attrs := make(map[string]interface{})

	// Setup a mock executor
	sparkExecutor := &SparkExecutor{}

	// Create the decorator instance
	decorator, _ := newDatabricksSparkEventDecorator()

	// Execute the function under test
	decorator.DecorateExecutor(sparkExecutor, attrs)

	// Verify results
	assert.Contains(t, attrs, "databricksWorkspaceId")
	assert.Equal(t, int64(12345), attrs["databricksWorkspaceId"])
	assert.Contains(t, attrs, "databricksWorkspaceName")
	assert.Equal(t, "foo.fakedomain.local", attrs["databricksWorkspaceName"])
	assert.Contains(t, attrs, "databricksWorkspaceUrl")
	assert.Equal(
		t,
		"https://foo.fakedomain.local",
		attrs["databricksWorkspaceUrl"],
	)
	assert.Contains(t, attrs, "databricksClusterId")
	assert.Equal(t, "fake-cluster-id", attrs["databricksClusterId"])
	assert.Contains(t, attrs, "databricksClusterName")
	assert.Equal(
		t,
		"fake-cluster-name",
		attrs["databricksClusterName"],
	)
	assert.Contains(t, attrs, "databricksclustername")
	assert.Equal(
		t,
		"fake-cluster-name",
		attrs["databricksclustername"],
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
	assert.NotContains(t, attrs, "databricksJobId")
	assert.NotContains(t, attrs, "databricksJobRunTaskRunId")
	assert.NotContains(t, attrs, "databricksPipelineId")
	assert.NotContains(t, attrs, "databricksPipelineUpdateId")
	assert.NotContains(t, attrs, "databricksPipelineFlowId")
}

func TestDecorateJob(t *testing.T) {
	// Reset viper config to ensure clean test state
	viper.Reset()

	// Setup tests
	tests := []struct {
		name             string
		jobGroup         string
		stageIds         []int
		expectDecoration bool
		expectPipelineId bool
	}{
		{
			name:             "valid job group with job run info",
			jobGroup:         "spark_job_job-12345-run-67890-action-54321",
			stageIds:         []int{1, 2, 3},
			expectDecoration: true,
			expectPipelineId: false,
		},
		{
			name:             "valid job group with pipeline only",
			jobGroup:         "spark_job_dlt-67890-54321",
			stageIds:         []int{1, 2, 3},
			expectDecoration: true,
			expectPipelineId: true,
		},
		{
			name:             "invalid job group",
			jobGroup:         "invalid_format",
			stageIds:         []int{1, 2, 3},
			expectDecoration: false,
			expectPipelineId: false,
		},
		{
			name:             "empty stage ids",
			jobGroup:         "spark_job_job-12345-run-67890-action-54321",
			stageIds:         []int{},
			expectDecoration: true,
			expectPipelineId: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup mock workspace
			_, teardown := setupMockWorkspace()
			defer teardown()

			// Setup mock cluster info
			teardown2 := setupMockClusterInfo()
			defer teardown2()

			// Setup the attributes map
			attrs := make(map[string]interface{})

			// Setup a mock job with the specified job group and stage IDs
			sparkJob := &SparkJob{
				JobGroup: tt.jobGroup,
				StageIds: tt.stageIds,
			}

			// Create the decorator instance
			decorator, _ := newDatabricksSparkEventDecorator()

			// Execute the function under test
			decorator.DecorateJob(sparkJob, attrs)

			// Verify results
			assert.Contains(t, attrs, "databricksWorkspaceId")
			assert.Equal(t, int64(12345), attrs["databricksWorkspaceId"])
			assert.Contains(t, attrs, "databricksWorkspaceName")
			assert.Equal(
				t,
				"foo.fakedomain.local",
				attrs["databricksWorkspaceName"],
			)
			assert.Contains(t, attrs, "databricksWorkspaceUrl")
			assert.Equal(
				t,
				"https://foo.fakedomain.local",
				attrs["databricksWorkspaceUrl"],
			)

			assert.Contains(t, attrs, "databricksClusterId")
			assert.Equal(t, "fake-cluster-id", attrs["databricksClusterId"])
			assert.Contains(t, attrs, "databricksClusterName")
			assert.Equal(
				t,
				"fake-cluster-name",
				attrs["databricksClusterName"],
			)
			assert.Contains(t, attrs, "databricksclustername")
			assert.Equal(
				t,
				"fake-cluster-name",
				attrs["databricksclustername"],
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

			if tt.expectDecoration {
				if !tt.expectPipelineId {
					assert.Contains(t, attrs, "databricksJobId")
					assert.Equal(t, int64(12345), attrs["databricksJobId"])
					assert.Contains(t, attrs, "databricksJobRunTaskRunId")
					assert.Equal(
						t,
						int64(67890),
						attrs["databricksJobRunTaskRunId"],
					)
					assert.NotContains(t, attrs, "databricksPipelineId")
					assert.NotContains(t, attrs, "databricksPipelineUpdateId")
					assert.NotContains(t, attrs, "databricksPipelineFlowId")
				} else {
					assert.Contains(t, attrs, "databricksPipelineId")
					assert.Equal(
						t,
						"67890-54321",
						attrs["databricksPipelineId"],
					)
					assert.NotContains(t, attrs, "databricksJobId")
					assert.NotContains(t, attrs, "databricksJobRunTaskRunId")
					assert.NotContains(t, attrs, "databricksPipelineUpdateId")
					assert.NotContains(t, attrs, "databricksPipelineFlowId")
				}

				// Verify stage IDs were mapped correctly
				assert.Equal(t, len(tt.stageIds), len(decorator.stageIdsToJobs))

				for _, stageId := range tt.stageIds {
					jobInfo, exists := decorator.stageIdsToJobs[stageId]
					assert.True(t, exists)
					assert.NotNil(t, jobInfo)

					if !tt.expectPipelineId {
						assert.Equal(t, int64(12345), jobInfo.jobId)
						assert.Equal(t, int64(67890), jobInfo.taskRunId)
						assert.Equal(t, "", jobInfo.pipelineId)
						assert.Equal(t, "", jobInfo.updateId)
						assert.Equal(t, "", jobInfo.flowId)
					} else {
						assert.Equal(t, int64(-1), jobInfo.jobId)
						assert.Equal(t, int64(-1), jobInfo.taskRunId)
						assert.Equal(t, "67890-54321", jobInfo.pipelineId)
						assert.Equal(t, "", jobInfo.updateId)
						assert.Equal(t, "", jobInfo.flowId)
					}
				}
			} else {
				assert.NotContains(t, attrs, "databricksJobId")
				assert.NotContains(t, attrs, "databricksJobRunTaskRunId")
				assert.NotContains(t, attrs, "databricksPipelineId")
				assert.NotContains(t, attrs, "databricksPipelineUpdateId")
				assert.NotContains(t, attrs, "databricksPipelineFlowId")
				assert.Empty(t, decorator.stageIdsToJobs)
			}
		})
	}
}

func TestDecorateStage(t *testing.T) {
	// Reset viper config to ensure clean test state
	viper.Reset()

	// Setup tests
	tests := []struct {
		name             string
		setupStageMap    func(map[int]*databricksSparkJobGroupInfo)
		stageId          int
		expectDecoration bool
		expectPipelineId bool
	}{
		{
			name: "stage id exists in map with job run info",
			setupStageMap: func(m map[int]*databricksSparkJobGroupInfo) {
				m[42] = &databricksSparkJobGroupInfo{
					jobId:      12345,
					taskRunId:  67890,
					pipelineId: "",
					updateId:   "",
					flowId:     "",
				}
			},
			stageId:          42,
			expectDecoration: true,
			expectPipelineId: false,
		},
		{
			name: "stage id exists in map with pipeline only",
			setupStageMap: func(m map[int]*databricksSparkJobGroupInfo) {
				m[42] = &databricksSparkJobGroupInfo{
					jobId:      -1,
					taskRunId:  -1,
					pipelineId: "67890-54321",
					updateId:   "",
					flowId:     "",
				}
			},
			stageId:          42,
			expectDecoration: true,
			expectPipelineId: true,
		},
		{
			name: "stage id doesn't exist in map",
			setupStageMap: func(m map[int]*databricksSparkJobGroupInfo) {
				m[42] = &databricksSparkJobGroupInfo{
					jobId:      12345,
					taskRunId:  67890,
					pipelineId: "",
					updateId:   "",
					flowId:     "",
				}
			},
			stageId:          99,
			expectDecoration: false,
			expectPipelineId: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup mock workspace
			_, teardown := setupMockWorkspace()
			defer teardown()

			// Setup mock cluster info
			teardown2 := setupMockClusterInfo()
			defer teardown2()

			// Setup the attributes map
			attrs := make(map[string]interface{})

			// Setup a mock stage with the specified stage ID
			sparkStage := &SparkStage{
				StageId: tt.stageId,
			}

			// Create the decorator instance
			decorator, _ := newDatabricksSparkEventDecorator()

			// Setup the stage map
			tt.setupStageMap(decorator.stageIdsToJobs)

			// Execute the function under test
			decorator.DecorateStage(sparkStage, attrs)

			// Verify results
			assert.Contains(t, attrs, "databricksWorkspaceId")
			assert.Equal(t, int64(12345), attrs["databricksWorkspaceId"])
			assert.Contains(t, attrs, "databricksWorkspaceName")
			assert.Equal(
				t,
				"foo.fakedomain.local",
				attrs["databricksWorkspaceName"],
			)
			assert.Contains(t, attrs, "databricksWorkspaceUrl")
			assert.Equal(
				t,
				"https://foo.fakedomain.local",
				attrs["databricksWorkspaceUrl"],
			)

			assert.Contains(t, attrs, "databricksClusterId")
			assert.Equal(t, "fake-cluster-id", attrs["databricksClusterId"])
			assert.Contains(t, attrs, "databricksClusterName")
			assert.Equal(
				t,
				"fake-cluster-name",
				attrs["databricksClusterName"],
			)
			assert.Contains(t, attrs, "databricksclustername")
			assert.Equal(
				t,
				"fake-cluster-name",
				attrs["databricksclustername"],
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

			if tt.expectDecoration {
				if !tt.expectPipelineId {
					assert.Contains(t, attrs, "databricksJobId")
					assert.Equal(t, int64(12345), attrs["databricksJobId"])
					assert.Contains(t, attrs, "databricksJobRunTaskRunId")
					assert.Equal(
						t,
						int64(67890),
						attrs["databricksJobRunTaskRunId"],
					)
					assert.NotContains(t, attrs, "databricksPipelineId")
					assert.NotContains(t, attrs, "databricksPipelineUpdateId")
					assert.NotContains(t, attrs, "databricksPipelineFlowId")
				} else {
					assert.Contains(t, attrs, "databricksPipelineId")
					assert.Equal(
						t,
						"67890-54321",
						attrs["databricksPipelineId"],
					)
					assert.NotContains(t, attrs, "databricksJobId")
					assert.NotContains(t, attrs, "databricksJobRunTaskRunId")
					assert.NotContains(t, attrs, "databricksPipelineUpdateId")
					assert.NotContains(t, attrs, "databricksPipelineFlowId")
				}
			} else {
				assert.NotContains(t, attrs, "databricksJobId")
				assert.NotContains(t, attrs, "databricksJobRunTaskRunId")
				assert.NotContains(t, attrs, "databricksPipelineId")
				assert.NotContains(t, attrs, "databricksPipelineUpdateId")
				assert.NotContains(t, attrs, "databricksPipelineFlowId")
			}
		})
	}
}

func TestDecorateTask(t *testing.T) {
	// Reset viper config to ensure clean test state
	viper.Reset()

	// Setup tests
	tests := []struct {
		name             string
		setupStageMap    func(map[int]*databricksSparkJobGroupInfo)
		stageId          int
		expectDecoration bool
		expectPipelineId bool
	}{
		{
			name: "stage id exists in map with job run info",
			setupStageMap: func(m map[int]*databricksSparkJobGroupInfo) {
				m[42] = &databricksSparkJobGroupInfo{
					jobId:      12345,
					taskRunId:  67890,
					pipelineId: "",
					updateId:   "",
					flowId:     "",
				}
			},
			stageId:          42,
			expectDecoration: true,
			expectPipelineId: false,
		},
		{
			name: "stage id exists in map with pipeline only",
			setupStageMap: func(m map[int]*databricksSparkJobGroupInfo) {
				m[42] = &databricksSparkJobGroupInfo{
					jobId:      -1,
					taskRunId:  -1,
					pipelineId: "67890-54321",
					updateId:   "",
					flowId:     "",
				}
			},
			stageId:          42,
			expectDecoration: true,
			expectPipelineId: true,
		},
		{
			name: "stage id doesn't exist in map",
			setupStageMap: func(m map[int]*databricksSparkJobGroupInfo) {
				m[42] = &databricksSparkJobGroupInfo{
					jobId:      12345,
					taskRunId:  67890,
					pipelineId: "",
					updateId:   "",
					flowId:     "",
				}
			},
			stageId:          99,
			expectDecoration: false,
			expectPipelineId: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup mock workspace
			_, teardown := setupMockWorkspace()
			defer teardown()

			// Setup mock cluster info
			teardown2 := setupMockClusterInfo()
			defer teardown2()

			// Setup the attributes map
			attrs := make(map[string]interface{})

			// Setup a mock stage with the specified stage ID
			sparkStage := &SparkStage{
				StageId: tt.stageId,
			}

			// Setup a mock task
			sparkTask := &SparkTask{}

			// Create the decorator instance
			decorator, _ := newDatabricksSparkEventDecorator()

			// Setup the stage map
			tt.setupStageMap(decorator.stageIdsToJobs)

			// Execute the function under test
			decorator.DecorateTask(sparkStage, sparkTask, attrs)

			// Verify results
			assert.Contains(t, attrs, "databricksWorkspaceId")
			assert.Equal(t, int64(12345), attrs["databricksWorkspaceId"])
			assert.Contains(t, attrs, "databricksWorkspaceName")
			assert.Equal(
				t,
				"foo.fakedomain.local",
				attrs["databricksWorkspaceName"],
			)
			assert.Contains(t, attrs, "databricksWorkspaceUrl")
			assert.Equal(
				t,
				"https://foo.fakedomain.local",
				attrs["databricksWorkspaceUrl"],
			)

			assert.Contains(t, attrs, "databricksClusterId")
			assert.Equal(t, "fake-cluster-id", attrs["databricksClusterId"])
			assert.Contains(t, attrs, "databricksClusterName")
			assert.Equal(
				t,
				"fake-cluster-name",
				attrs["databricksClusterName"],
			)
			assert.Contains(t, attrs, "databricksclustername")
			assert.Equal(
				t,
				"fake-cluster-name",
				attrs["databricksclustername"],
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

			// Verify results
			if tt.expectDecoration {
				if !tt.expectPipelineId {
					assert.Contains(t, attrs, "databricksJobId")
					assert.Equal(t, int64(12345), attrs["databricksJobId"])
					assert.Contains(t, attrs, "databricksJobRunTaskRunId")
					assert.Equal(
						t,
						int64(67890),
						attrs["databricksJobRunTaskRunId"],
					)
					assert.NotContains(t, attrs, "databricksPipelineId")
					assert.NotContains(t, attrs, "databricksPipelineUpdateId")
					assert.NotContains(t, attrs, "databricksPipelineFlowId")
				} else {
					assert.Contains(t, attrs, "databricksPipelineId")
					assert.Equal(
						t,
						"67890-54321",
						attrs["databricksPipelineId"],
					)
					assert.NotContains(t, attrs, "databricksJobId")
					assert.NotContains(t, attrs, "databricksJobRunTaskRunId")
					assert.NotContains(t, attrs, "databricksPipelineUpdateId")
					assert.NotContains(t, attrs, "databricksPipelineFlowId")
				}
			} else {
				assert.NotContains(t, attrs, "databricksJobId")
				assert.NotContains(t, attrs, "databricksJobRunTaskRunId")
				assert.NotContains(t, attrs, "databricksPipelineId")
				assert.NotContains(t, attrs, "databricksPipelineUpdateId")
				assert.NotContains(t, attrs, "databricksPipelineFlowId")
			}
		})
	}
}

func TestDecorateRDDs(t *testing.T) {
	// Reset viper config to ensure clean test state
	viper.Reset()

	// Setup mock workspace
	_, teardown := setupMockWorkspace()
	defer teardown()

	// Setup mock cluster info
	teardown2 := setupMockClusterInfo()
	defer teardown2()

	// Setup the attributes map
	attrs := make(map[string]interface{})

	// Setup a mock RDD
	sparkRDD := &SparkRDD{}

	// Create the decorator instance
	decorator, _ := newDatabricksSparkEventDecorator()

	// Execute the function under test
	decorator.DecorateRDD(sparkRDD, attrs)

	// Verify results
	assert.Contains(t, attrs, "databricksWorkspaceId")
	assert.Equal(t, int64(12345), attrs["databricksWorkspaceId"])
	assert.Contains(t, attrs, "databricksWorkspaceName")
	assert.Equal(
		t,
		"foo.fakedomain.local",
		attrs["databricksWorkspaceName"],
	)
	assert.Contains(t, attrs, "databricksWorkspaceUrl")
	assert.Equal(
		t,
		"https://foo.fakedomain.local",
		attrs["databricksWorkspaceUrl"],
	)
	assert.Contains(t, attrs, "databricksClusterId")
	assert.Equal(t, "fake-cluster-id", attrs["databricksClusterId"])
	assert.Contains(t, attrs, "databricksClusterName")
	assert.Equal(
		t,
		"fake-cluster-name",
		attrs["databricksClusterName"],
	)
	assert.Contains(t, attrs, "databricksclustername")
	assert.Equal(
		t,
		"fake-cluster-name",
		attrs["databricksclustername"],
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
}

func TestDecorateEvent(t *testing.T) {
	// Reset viper config to ensure clean test state
	viper.Reset()

	// Setup mock workspace
	_, teardown := setupMockWorkspace()
	defer teardown()

	// Setup mock cluster info
	teardown2 := setupMockClusterInfo()
	defer teardown2()

	// Setup the attributes map
	attrs := make(map[string]interface{})

	// Create the decorator instance
	decorator, _ := newDatabricksSparkEventDecorator()

	// Execute the function under test
	decorator.DecorateEvent(attrs)

	// Verify results
	assert.Contains(t, attrs, "databricksWorkspaceId")
	assert.Equal(t, int64(12345), attrs["databricksWorkspaceId"])
	assert.Contains(t, attrs, "databricksWorkspaceName")
	assert.Equal(
		t,
		"foo.fakedomain.local",
		attrs["databricksWorkspaceName"],
	)
	assert.Contains(t, attrs, "databricksWorkspaceUrl")
	assert.Equal(
		t,
		"https://foo.fakedomain.local",
		attrs["databricksWorkspaceUrl"],
	)
	assert.Contains(t, attrs, "databricksClusterId")
	assert.Equal(t, "fake-cluster-id", attrs["databricksClusterId"])
	assert.Contains(t, attrs, "databricksClusterName")
	assert.Equal(
		t,
		"fake-cluster-name",
		attrs["databricksClusterName"],
	)
	assert.Contains(t, attrs, "databricksclustername")
	assert.Equal(
		t,
		"fake-cluster-name",
		attrs["databricksclustername"],
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
}
