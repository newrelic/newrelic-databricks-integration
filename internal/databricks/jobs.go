package databricks

import (
	"context"
	"time"

	databricksSdkJobs "github.com/databricks/databricks-sdk-go/service/jobs"
	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration"
	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/log"
	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/model"
)

type counters struct {
	blocked 		int
	waiting			int
	pending			int
	queued			int
	running			int
	terminating		int
}

type DatabricksJobRunReceiver struct {
	i							*integration.LabsIntegration
	w							DatabricksWorkspace
	startOffset					time.Duration
	tags 						map[string]string
	lastRun                     time.Time
}

func NewDatabricksJobRunReceiver(
	i *integration.LabsIntegration,
	w DatabricksWorkspace,
	startOffset time.Duration,
	tags map[string]string,
) *DatabricksJobRunReceiver {
	InitInfoByIdCaches(w)

	return &DatabricksJobRunReceiver{
		i,
		w,
		startOffset,
		tags,
		Now().UTC(),
	}
}

func (d *DatabricksJobRunReceiver) GetId() string {
	return "databricks-job-run-receiver"
}

func (d *DatabricksJobRunReceiver) PollEvents(
	ctx context.Context,
	writer chan <- model.Event,
) error {
	log.Debugf("listing job runs")
	defer log.Debugf("done listing job runs")

	lastRun := d.lastRun

	// This only works when the integration is run as a standalone application
	// that stays running since it requires state to be maintained. However,
	// this is the only supported way to run the integration. If this ever
	// changes, this approach to saving the last run time needs to be reviewed.
	d.lastRun = Now().UTC()

	lastRunMilli := lastRun.UnixMilli()

	if log.IsDebugEnabled() {
		log.Debugf(
			"lastRun: %s, now: %s",
			lastRun.Format(RFC3339Milli),
			d.lastRun.Format(RFC3339Milli),
		)
	}

	jobRunCounters := counters{}
	taskRunCounters := counters{}

	all := d.w.ListJobRuns(ctx, d.startOffset)

	for ; all.HasNext(ctx);  {
		run, err := all.Next(ctx)
		if err != nil {
			return err
		}

		log.Debugf(
			"processing job run %d (%s) with state %s",
			run.RunId,
			run.RunName,
			run.Status.State,
		)

		state := run.Status.State

		if state == databricksSdkJobs.RunLifecycleStateV2StateTerminated &&
			// this run terminated before the last run so presumably we
			// already picked it up and don't want to report on it twice
			run.EndTime < lastRunMilli {

			if log.IsDebugEnabled() {
				log.Debugf(
					"ignoring job run %s (%d) because the end time (%s) is less than last run (%s)",
					run.RunName,
					run.RunId,
					time.UnixMilli(run.EndTime).Format(RFC3339Milli),
					time.UnixMilli(lastRunMilli).Format(RFC3339Milli),
				)
			}

			continue
		}

		if run.StartTime >= lastRunMilli {
			if log.IsDebugEnabled() {
				log.Debugf(
					"adding job run %s (%d) because the start time (%s) is greater than or equal to last run (%s)",
					run.RunName,
					run.RunId,
					time.UnixMilli(run.StartTime).Format(RFC3339Milli),
					time.UnixMilli(lastRunMilli).Format(RFC3339Milli),
				)
			}

			attrs, err := makeJobRunStartAttributes(ctx, &run, d.tags)
			if err != nil {
				return err
			}

			writer <- model.Event{
				Type: "DatabricksJobRun",
				Timestamp: Now().UnixMilli(),
				Attributes: attrs,
			}
		}

		if state == databricksSdkJobs.RunLifecycleStateV2StateTerminated {
			if log.IsDebugEnabled() {
				log.Debugf(
					"adding terminated job run %s (%d)",
					run.RunName,
					run.RunId,
				)
			}

			attrs, err := makeJobRunCompleteAttributes(ctx, &run, d.tags)
			if err != nil {
				return err
			}

			writer <- model.Event{
				Type: "DatabricksJobRun",
				Timestamp: Now().UnixMilli(),
				Attributes: attrs,
			}
		}

		updateStateCounters(state, &jobRunCounters)

		for _, task := range run.Tasks {
			log.Debugf(
				"processing task run %s (%d) for job run %s (%d)",
				task.TaskKey,
				task.RunId,
				run.RunName,
				task.RunId,
			)

			err := processJobRunTask(
				ctx,
				&run,
				&task,
				lastRunMilli,
				d.tags,
				writer,
			)
			if err != nil {
				return err
			}

			updateStateCounters(task.Status.State, &taskRunCounters)
		}
	}

	attrs, err := makeJobRunSummaryAttributes(
		ctx,
		&jobRunCounters,
		&taskRunCounters,
		d.tags,
	)
	if err != nil {
		return err
	}

	writer <- model.Event{
		Type: "DatabricksJobRunSummary",
		Timestamp: Now().UnixMilli(),
		Attributes: attrs,
	}

	return nil
}

func makeJobRunSummaryAttributes(
	ctx context.Context,
	jobRunCounters *counters,
	taskRunCounters *counters,
	tags map[string]string,
) (map[string]interface{}, error) {
	attrs, err := makeBaseAttributes(ctx, nil, tags)
	if err != nil {
		return nil, err
	}

	attrs["blockedJobRunCount"] = jobRunCounters.blocked
	attrs["waitingJobRunCount"] = jobRunCounters.waiting
	attrs["pendingJobRunCount"] = jobRunCounters.pending
	attrs["queuedJobRunCount"] = jobRunCounters.queued
	attrs["runningJobRunCount"] = jobRunCounters.running
	attrs["terminatingJobRunCount"] = jobRunCounters.terminating
	attrs["blockedTaskRunCount"] = taskRunCounters.blocked
	attrs["waitingTaskRunCount"] = taskRunCounters.waiting
	attrs["pendingTaskRunCount"] = taskRunCounters.pending
	attrs["queuedTaskRunCount"] = taskRunCounters.queued
	attrs["runningTaskRunCount"] = taskRunCounters.running
	attrs["terminatingTaskRunCount"] = taskRunCounters.terminating

	return attrs, nil
}

func makeJobRunBaseAttributes(
	ctx context.Context,
	run *databricksSdkJobs.BaseRun,
	event string,
	tags map[string]string,
) (map[string]interface{}, error) {
	attrs, err := makeBaseAttributes(ctx, run.ClusterInstance, tags)
	if err != nil {
		return nil, err
	}

	attrs["event"] = event
	attrs["jobId"] = run.JobId
	attrs["jobRunId"] = run.RunId
	attrs["jobRunType"] = string(run.RunType)
	attrs["jobRunName"] = run.RunName
	attrs["jobRunStartTime"] = run.StartTime
	attrs["jobRunTrigger"] = string(run.Trigger)
	attrs["originalAttemptRunId"] = run.OriginalAttemptRunId
	attrs["description"] = run.Description
	attrs["attempt"] = run.AttemptNumber
	attrs["isRetry"] = run.OriginalAttemptRunId != run.RunId

	return attrs, nil
}

func makeJobRunStartAttributes(
	ctx context.Context,
	run *databricksSdkJobs.BaseRun,
	tags map[string]string,
) (map[string]interface{}, error) {
	return makeJobRunBaseAttributes(ctx, run, "start", tags)
}

func makeJobRunCompleteAttributes(
	ctx context.Context,
	run *databricksSdkJobs.BaseRun,
	tags map[string]string,
) (map[string]interface{}, error) {
	attrs, err := makeJobRunBaseAttributes(ctx, run, "complete", tags)
	if err != nil {
		return nil, err
	}

	state := run.Status.State
	termDetails := run.Status.TerminationDetails

	attrs["state"] = string(state)
	attrs["terminationCode"] = string(termDetails.Code)
	attrs["terminationType"] = string(termDetails.Type)
	attrs["jobRunEndTime"] = run.EndTime

	tasks := run.Tasks

	if len(tasks) > 1 {
		// For multitask job runs, only RunDuration is set, not setup,
		// execution, and cleanup
		attrs["duration"] = run.RunDuration
		attrs["queueDuration"] = run.QueueDuration

		return attrs, nil
	}

	attrs["duration"] =
		run.SetupDuration + run.ExecutionDuration + run.CleanupDuration
	attrs["queueDuration"] = run.QueueDuration
	attrs["setupDuration"] = run.SetupDuration
	attrs["executionDuration"] = run.ExecutionDuration
	attrs["cleanupDuration"] = run.CleanupDuration

	return attrs, nil
}

func processJobRunTask(
	ctx context.Context,
	run *databricksSdkJobs.BaseRun,
	task *databricksSdkJobs.RunTask,
	lastRunMilli int64,
	tags map[string]string,
	writer chan<- model.Event,
) error {
	state := task.Status.State

	if state == databricksSdkJobs.RunLifecycleStateV2StateTerminated &&
		// this run terminated before the last run so presumably we
		// already picked it up and don't want to report on it twice
		task.EndTime < lastRunMilli {
		if log.IsDebugEnabled() {
			log.Debugf(
				"ignoring task run %s (%d) because the end time (%s) is less than last run (%s)",
				task.TaskKey,
				task.RunId,
				time.UnixMilli(task.EndTime).Format(RFC3339Milli),
				time.UnixMilli(lastRunMilli).Format(RFC3339Milli),
			)
		}

		return nil
	}

	if task.StartTime >= lastRunMilli {
		if log.IsDebugEnabled() {
			log.Debugf(
				"adding task run %s (%d) because the start time (%s) is greater than or equal to last run (%s)",
				task.TaskKey,
				task.RunId,
				time.UnixMilli(task.StartTime).Format(RFC3339Milli),
				time.UnixMilli(lastRunMilli).Format(RFC3339Milli),
			)
		}

		attrs, err := makeJobRunTaskStartAttributes(ctx, run, task, tags)
		if err != nil {
			return err
		}

		writer <- model.Event{
			Type: "DatabricksTaskRun",
			Timestamp: Now().UnixMilli(),
			Attributes: attrs,
		}
	}

	if state == databricksSdkJobs.RunLifecycleStateV2StateTerminated {
		if log.IsDebugEnabled() {
			log.Debugf(
				"adding terminated task run %s (%d)",
				task.TaskKey,
				task.RunId,
			)
		}

		attrs, err := makeJobRunTaskCompleteAttributes(ctx, run, task, tags)
		if err != nil {
			return err
		}

		writer <- model.Event{
			Type: "DatabricksTaskRun",
			Timestamp: Now().UnixMilli(),
			Attributes: attrs,
		}
	}

	return nil
}

func makeJobRunTaskBaseAttributes(
	ctx context.Context,
	run *databricksSdkJobs.BaseRun,
	task *databricksSdkJobs.RunTask,
	event string,
	tags map[string]string,
) (map[string]interface{}, error) {
	clusterInstance := task.ClusterInstance
	if clusterInstance == nil {
		clusterInstance = run.ClusterInstance
	}

	attrs, err := makeBaseAttributes(ctx, clusterInstance, tags)
	if err != nil {
		return nil, err
	}

	attrs["event"] = event
	attrs["jobId"] = run.JobId
	attrs["jobRunId"] = run.RunId
	attrs["jobRunType"] = string(run.RunType)
	attrs["jobRunName"] = run.RunName
	attrs["jobRunStartTime"] = run.StartTime
	attrs["jobRunTrigger"] = string(run.Trigger)
	attrs["taskRunId"] = task.RunId
	attrs["taskName"] = task.TaskKey
	attrs["taskRunStartTime"] = task.StartTime
	attrs["description"] = task.Description
	attrs["attempt"] = task.AttemptNumber
	attrs["isRetry"] = task.AttemptNumber > 0

	return attrs, nil
}

func makeJobRunTaskStartAttributes(
	ctx context.Context,
	run *databricksSdkJobs.BaseRun,
	task *databricksSdkJobs.RunTask,
	tags map[string]string,
) (map[string]interface{}, error) {
	return makeJobRunTaskBaseAttributes(ctx, run, task, "start", tags)
}

func makeJobRunTaskCompleteAttributes(
	ctx context.Context,
	run *databricksSdkJobs.BaseRun,
	task *databricksSdkJobs.RunTask,
	tags map[string]string,
) (map[string]interface{}, error) {
	attrs, err := makeJobRunTaskBaseAttributes(ctx, run, task, "complete", tags)
	if err != nil {
		return nil, err
	}

	state := task.Status.State
	termDetails := task.Status.TerminationDetails

	attrs["state"] = string(state)
	attrs["terminationCode"] = string(termDetails.Code)
	attrs["terminationType"] = string(termDetails.Type)
	attrs["taskRunEndTime"] = task.EndTime

	/* Supposedly, tasks are like jobs in that for multitask jobs, the
		total duration of the task is in RunDuration. But that didn't
		seem to be true in my testing. It could be that the run duration
		is relevant if this task runs other tasks or runs another job
		neither of which I tested. Leaving this commented until we
		understand more about when run duration is actually used.
	if len(tasks) > 1 {
		writeGauge(
			metricPrefix,
			"job.run.task.duration",
			task.RunDuration,
			taskMetricAttrs,
			writer,
		)

		writeGauge(
			metricPrefix,
			"job.run.task.duration.queue",
			task.QueueDuration,
			attrs,
			writer,
		)
	} else {
	*/

	attrs["duration"] =
		task.SetupDuration + task.ExecutionDuration + task.CleanupDuration
	attrs["queueDuration"] = task.QueueDuration
	attrs["setupDuration"] = task.SetupDuration
	attrs["executionDuration"] = task.ExecutionDuration
	attrs["cleanupDuration"] = task.CleanupDuration

	return attrs, nil
}

func makeBaseAttributes(
	ctx context.Context,
	clusterInstance *databricksSdkJobs.ClusterInstance,
	tags map[string]string,
) (map[string]interface{}, error) {
	attrs := makeAttributesMap(tags)

	workspaceInfo, err := GetWorkspaceInfo(ctx)
	if err != nil {
		return nil, err
	}

	attrs["databricksWorkspaceId"] = workspaceInfo.Id
	attrs["databricksWorkspaceName"] = workspaceInfo.InstanceName
	attrs["databricksWorkspaceUrl"] = workspaceInfo.Url

	if clusterInstance != nil {
		clusterInfo, err := GetClusterInfoById(
			ctx,
			clusterInstance.ClusterId,
		)
		if err != nil {
			return nil, err
		}

		attrs["databricksClusterId"] = clusterInstance.ClusterId
		attrs["databricksClusterName"] = clusterInfo.name
		attrs["databricksClusterSource"] = clusterInfo.source
		attrs["databricksClusterInstancePoolId"] = clusterInfo.instancePoolId
		attrs["databricksClusterSparkContextId"] =
			clusterInstance.SparkContextId
	}

	return attrs, nil
}

func updateStateCounters(
	state databricksSdkJobs.RunLifecycleStateV2State,
	counters *counters,
) {
	if state == databricksSdkJobs.RunLifecycleStateV2StateBlocked {
		counters.blocked += 1
	} else if state == databricksSdkJobs.RunLifecycleStateV2StateWaiting {
		counters.waiting += 1
	} else if state == databricksSdkJobs.RunLifecycleStateV2StatePending {
		counters.pending += 1
	} else if state == databricksSdkJobs.RunLifecycleStateV2StateQueued {
		counters.queued += 1
	} else if state == databricksSdkJobs.RunLifecycleStateV2StateRunning {
		counters.running += 1
	} else if
		state == databricksSdkJobs.RunLifecycleStateV2StateTerminating {
		counters.terminating += 1
	}
}
