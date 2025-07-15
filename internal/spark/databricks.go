package spark

import (
	"context"
	"regexp"
	"strconv"
	"strings"

	"github.com/newrelic/newrelic-databricks-integration/internal/databricks"
	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/log"
)

var databricksJobGroupRegex = regexp.MustCompile(`^(?:[^_]+_[^_]+_dlt-([a-fA-F0-9\-]+))$|^(?:[^_]+_[^_]+_job-(\d+)-run-(\d+)-action-(\d+))$|^(?:([a-fA-F0-9\-]+)#([a-fA-F0-9\-]+)#([a-fA-F0-9\-]+))$`)

type databricksSparkJobGroupInfo struct {
	// We know from looking at
	// https://pkg.go.dev/github.com/databricks/databricks-sdk-go@v0.60.0/service/jobs#BaseRun
	// and
	// https://pkg.go.dev/github.com/databricks/databricks-sdk-go@v0.60.0/service/jobs#RunTask
	// that these IDs are 64-bit integers
	//
	// NOTE: When the job ID and task run ID are not found, we initialize these
	// ints to -1 so we are assuming that the job ID and task run ID can never
	// be negative which seems safe.
	jobId      int64
	taskRunId  int64

	// We know from looking at
	// https://pkg.go.dev/github.com/databricks/databricks-sdk-go@v0.54.0/service/pipelines#Origin
	// that pipeline/update/flow IDs are strings (and they look like UUIDs when
	// viewed in the UI/JSON).
	pipelineId string
	updateId   string
	flowId     string
}

func parseDatabricksSparkJobGroup(
	jobGroup string,
) *databricksSparkJobGroupInfo {
	log.Debugf(
		"parsing Databricks Spark job group: %s",
		jobGroup,
	)

	// Parse job/pipeline information from the job group name.
	//
	// Job run example: "11111_22222_job-12345-run-56789-action-54321"
	// jobId = 12345
	// taskRunId = 56780
	//
	// For job runs, we don't care about the number after "action-".
	//
	// Pipeline only example: "33333_44444_dlt-abcdef-12345"
	// pipelineId = "abcdef-12345"
	//
	// Pipeline update flow example: "12345#54321#67890"
	// pipelineId = "12345"
	// updateId = "67890" (note that this comes _after_ the flowId)
	// flowId = "54321"
	//
	// Users can set their own job group name so If the job group name does not
	// follow this format, we return nil and the events simply won't be
	// decorated.
	//
	// Note: This implementation assumes that the job group name follows the
	// formats that were discovered by examining the Job Group column in the
	// Spark UI. There is no documentation on this format and it is likely
	// internal to Databricks and could change at any time. If the convention
	// changes, this implementation may need to be updated.

	matches := databricksJobGroupRegex.FindStringSubmatch(
		strings.TrimSpace(jobGroup),
	)
	if matches == nil {
		log.Debugf(
			"unrecognized job group format: %s",
			jobGroup,
		)
		return nil
	}

	if matches[1] != "" {
		// The pipeline only case, "11111_22222_dlt-abcdef-12345".
		log.Debugf(
			"found pipelineId %s in job group name",
			matches[1],
		)
		return &databricksSparkJobGroupInfo{-1, -1, matches[1], "", ""}
	}

	if matches[2] != "" {
		// The job run case, "11111_22222_job-12345-run-56789-action-5432".
		jobId, err := strconv.ParseInt(matches[2], 10, 64)
		if err != nil {
			log.Debugf(
				"invalid jobId in job group name: %s",
				matches[2],
			)
			return nil
		}

		taskRunId, err := strconv.ParseInt(matches[3], 10, 64)
		if err != nil {
			log.Debugf(
				"invalid taskRunId in job group name: %s",
				matches[3],
			)
			return nil
		}

		log.Debugf(
			"found jobId %d and taskRunId %d in job group name",
			jobId,
			taskRunId,
		)

		return &databricksSparkJobGroupInfo{jobId, taskRunId, "", "", ""}
	}

	// The pipeline update flow case, "12345#54321#67890".
	//
	// NOTE: 5, 7, 6 is not a mistake. The update ID comes _after_ the flow ID
	// for some reason.

	log.Debugf(
		"found pipelineId %s and updateId %s and flowId %s in job group name",
		matches[5],
		matches[7],
		matches[6],
	)

	return &databricksSparkJobGroupInfo{
		-1,
		-1,
		matches[5],
		matches[7],
		matches[6],
	}
}

type DatabricksSparkEventDecorator struct {
	w                       databricks.DatabricksWorkspace
	workspaceInfo           *databricks.WorkspaceInfo
	// stageIdsToJobs keeps track of the job group info for each stage's
	// "parent" job so that stages and tasks can be decorated with the same
	// job group info as the "parent" job.
	stageIdsToJobs          map[int]*databricksSparkJobGroupInfo
}

func NewDatabricksSparkEventDecorator(
	ctx context.Context,
) (*DatabricksSparkEventDecorator, error) {
	w, err := databricks.NewDatabricksWorkspace()
	if err != nil {
		return nil, err
	}

	databricks.InitInfoByIdCaches(w)

	workspaceInfo, err := databricks.GetWorkspaceInfo(ctx)
	if err != nil {
		return nil, err
	}

	return &DatabricksSparkEventDecorator{
		w:                       w,
		workspaceInfo:           workspaceInfo,
		stageIdsToJobs:          make(map[int]*databricksSparkJobGroupInfo),
	}, nil
}

func (d *DatabricksSparkEventDecorator) DecorateExecutor(
	_ *SparkExecutor,
	attrs map[string]interface{},
) {
	d.decorate(nil, attrs)
}

func (d *DatabricksSparkEventDecorator) DecorateJob(
	sparkJob *SparkJob,
	attrs map[string]interface{},
) {
    sparkJobGroupInfo := parseDatabricksSparkJobGroup(sparkJob.JobGroup)

	// Always decorate the job even if the job group info is nil so that the
	// workspace info is added to the attributes.
	d.decorate(sparkJobGroupInfo, attrs)

	if sparkJobGroupInfo == nil {
        // No job group info was found so no need to pass job group info to the
		// stages.
		return
	}

	// Store the job group info for all stages for this job so that when
	// DecorateStage is called, the stage can be decorated with the right
	// job group info.
	//
	// NOTE: This implementation assumes that DecorateStage is called after
	// DecorateJob but that is ok because it is guaranteed by the Spark
	// receiver.
	for _, stageId := range sparkJob.StageIds {
		d.stageIdsToJobs[stageId] = sparkJobGroupInfo
	}
}

func (d *DatabricksSparkEventDecorator) DecorateStage(
	sparkStage *SparkStage,
	attrs map[string]interface{},
) {
	// Lookup the job group info for the "parent" job of this stage so that the
	// stage can be decorated with the right job group info.
	//
	// NOTE: This implementation assumes that DecorateStage is called after
	// DecorateJob but that is ok because it is guaranteed by the Spark
	// receiver.
	sparkJobGroupInfo, ok := d.stageIdsToJobs[sparkStage.StageId]
	if ok {
		// If the job group info is found, decorate the stage with it.
		d.decorate(sparkJobGroupInfo, attrs)
		return
	}

	// No job group info was found for this stage but we still need to decorate
	// the stage with the workspace info.
	d.decorate(nil, attrs)
}

func (d *DatabricksSparkEventDecorator) DecorateTask(
	sparkStage *SparkStage,
	_ *SparkTask,
	attrs map[string]interface{},
) {
	// Lookup the job group info for the "parent" stage of this task so that the
	// task can be decorated with the right job group info.
	//
	// NOTE: This implementation assumes that DecorateTask is called after
	// DecorateJob but that is ok because it is guaranteed by the Spark
	// receiver.
	sparkJobGroupInfo, ok := d.stageIdsToJobs[sparkStage.StageId]
	if ok {
		// If the job group info is found, decorate the task with it.
		d.decorate(sparkJobGroupInfo, attrs)
		return
	}

	// No job group info was found for the "parent" stage but we still need to
	// decorate the task with the workspace info.
	d.decorate(nil, attrs)
}

func (d *DatabricksSparkEventDecorator) DecorateRDD(
	_ *SparkRDD,
	attrs map[string]interface{},
) {
	d.decorate(nil, attrs)
}

func (d *DatabricksSparkEventDecorator) DecorateEvent(
	attrs map[string]interface{},
) {
	d.decorate(nil, attrs)
}

func (d *DatabricksSparkEventDecorator) decorate(
	sparkJobGroupInfo *databricksSparkJobGroupInfo,
	attrs map[string]interface{},
) {
	// Always add the workspace info to the attributes
	attrs["databricksWorkspaceId"] = d.workspaceInfo.Id
	attrs["databricksWorkspaceName"] = d.workspaceInfo.InstanceName
	attrs["databricksWorkspaceUrl"] = d.workspaceInfo.Url

	// If no job group info is provided, skip adding additional attributes.
	if sparkJobGroupInfo == nil {
		return
    }

	// When specified, add the job group info to the attributes.
	if sparkJobGroupInfo.jobId != -1 {
		attrs["databricksJobId"] = sparkJobGroupInfo.jobId
		attrs["databricksJobRunTaskRunId"] = sparkJobGroupInfo.taskRunId
	} else if sparkJobGroupInfo.updateId != "" {
		attrs["databricksPipelineId"] = sparkJobGroupInfo.pipelineId
		attrs["databricksPipelineUpdateId"] = sparkJobGroupInfo.updateId
		attrs["databricksPipelineFlowId"] = sparkJobGroupInfo.flowId
	} else if sparkJobGroupInfo.pipelineId != "" {
		attrs["databricksPipelineId"] = sparkJobGroupInfo.pipelineId
	}
}
