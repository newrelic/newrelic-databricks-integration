package databricks

import (
	"context"
	"maps"
	"time"

	databricksSdkJobs "github.com/databricks/databricks-sdk-go/service/jobs"
	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration"
	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/log"
	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/model"
)

type counters struct {
	blocked 		int64
	pending			int64
	queued			int64
	running			int64
	terminating		int64
	terminated		int64
}

type DatabricksJobRunReceiver struct {
	i							*integration.LabsIntegration
	w							DatabricksWorkspace
	metricPrefix				string
	startOffset					time.Duration
	includeRunId				bool
	tags 						map[string]string
}

func NewDatabricksJobRunReceiver(
	i *integration.LabsIntegration,
	w DatabricksWorkspace,
	metricPrefix string,
	startOffset time.Duration,
	includeRunId bool,
	tags map[string]string,
) *DatabricksJobRunReceiver {
	return &DatabricksJobRunReceiver{
		i,
		w,
		metricPrefix,
		startOffset,
		includeRunId,
		tags,
	}
}

func (d *DatabricksJobRunReceiver) GetId() string {
	return "databricks-job-run-receiver"
}

func (d *DatabricksJobRunReceiver) PollMetrics(
	ctx context.Context,
	writer chan <- model.Metric,
) error {
	// d.i.UseLastUpdate(ctx, func(lastUpdate time.Time) error {
	log.Debugf("listing job runs")
	defer log.Debugf("done listing job runs")

	jobCounters := counters{}
	taskCounters := counters{}

	/*
	lastRun := lastUpdate
	if lastRun.IsZero() {
		lastRun = time.Now().Add(-time.Duration(600) * time.Second)
	}
	lastRunMilli := lastRun.UnixMilli()
	*/
	lastRunMilli := time.Now().Add(-d.i.Interval * time.Second).UnixMilli()

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
			continue
		}

		attrs := makeAttributesMap(d.tags)
		attrs["databricksJobId"] = run.JobId

		// This one could really increase cardinality but is also really
		// useful
		if d.includeRunId {
			attrs["databricksJobRunId"] = run.RunId
		}

		attrs["databricksJobRunName"] = run.RunName
		attrs["databricksJobRunState"] = string(state)
		attrs["databricksJobRunAttemptNumber"] = run.AttemptNumber
		attrs["databricksJobRunIsRetry"] = (
			run.OriginalAttemptRunId != run.RunId)

		if state == databricksSdkJobs.RunLifecycleStateV2StateTerminated {
			jobCounters.terminated += 1

			writeJobTerminatedMetrics(
				&run,
				d.metricPrefix,
				attrs,
				writer,
				lastRunMilli,
				&taskCounters,
			)

			continue
		}

		// Calculate the wall clock time of the job run duration by subtracting
		// the job start time from now. This duration could include setup,
		// queue, execution, and cleanup durations

		// For BLOCKED tasks, the StartTime is zero. This may be the case for
		// BLOCKED jobs as well. When this happens, the calculated run duration
		// ends up being the current millis which could significantly throw off
		// aggregations. Adding another data point with a value of 0 instead
		// could equally throw off aggregations. Just in case there are other
		// cases where StartTime can be zero, only calculate and record the run
		// duration if StartTime > 0 (as opposed to just when state is BLOCKED).
		// Even if StartTime == 0, we still need to increment our job counters
		// and call write task metrics since we also want to increment our task
		// counters. We don't need to worry about blocked tasks being recorded
		// because writeTaskMetrics has the same StartTime check.

		if run.StartTime > 0 {
			writeGauge(
				d.metricPrefix,
				"job.run.duration",
				time.Now().UnixMilli() - run.StartTime,
				attrs,
				writer,
			)
		}

		writeTaskMetrics(
			run.Tasks,
			d.metricPrefix,
			attrs,
			writer,
			lastRunMilli,
			&taskCounters,
		)

		if state == databricksSdkJobs.RunLifecycleStateV2StateBlocked {
			jobCounters.blocked += 1
		} else if state == databricksSdkJobs.RunLifecycleStateV2StatePending {
			jobCounters.pending += 1
		} else if state == databricksSdkJobs.RunLifecycleStateV2StateQueued {
			jobCounters.queued += 1
		} else if state == databricksSdkJobs.RunLifecycleStateV2StateRunning {
			jobCounters.running += 1
		} else if
			state == databricksSdkJobs.RunLifecycleStateV2StateTerminating {
			jobCounters.terminating += 1
		}
	}


	writeCounters(
		d.metricPrefix,
		"job.runs",
		&jobCounters,
		"databricksJobRunState",
		d.tags,
		writer,
	)

	writeCounters(
		d.metricPrefix,
		"job.tasks",
		&taskCounters,
		"databricksJobRunTaskState",
		d.tags,
		writer,
	)

	return nil
	//}, "databricks-jobs-receiver")

	//return nil
}

func writeJobTerminatedMetrics(
	run *databricksSdkJobs.BaseRun,
	metricPrefix string,
	attrs map[string]interface{},
	writer chan <- model.Metric,
	lastRunMilli int64,
	taskCounters *counters,
) {
	termDetails := run.Status.TerminationDetails

	attrs["databricksJobRunTerminationCode"] = string(termDetails.Code)
	attrs["databricksJobRunTerminationType"] = string(termDetails.Type)

	tasks := run.Tasks

	if len(tasks) > 1 {
		// For multitask job runs, only RunDuration is set, not setup,
		// execution, and cleanup
		writeGauge(
			metricPrefix,
			"job.run.duration",
			run.RunDuration,
			attrs,
			writer,
		)
		// Queue duration seems to always be set
		writeGauge(
			metricPrefix,
			"job.run.duration.queue",
			run.QueueDuration,
			attrs,
			writer,
		)
	} else {
		writeGauge(
			metricPrefix,
			"job.run.duration",
			run.SetupDuration + run.ExecutionDuration + run.CleanupDuration,
			attrs,
			writer,
		)
		writeGauge(
			metricPrefix,
			"job.run.duration.queue",
			run.QueueDuration,
			attrs,
			writer,
		)
		writeGauge(
			metricPrefix,
			"job.run.duration.execution",
			run.ExecutionDuration,
			attrs,
			writer,
		)
		writeGauge(
			metricPrefix,
			"job.run.duration.setup",
			run.SetupDuration,
			attrs,
			writer,
		)
		writeGauge(
			metricPrefix,
			"job.run.duration.cleanup",
			run.CleanupDuration,
			attrs,
			writer,
		)
	}

	writeTaskMetrics(
		tasks,
		metricPrefix,
		attrs,
		writer,
		lastRunMilli,
		taskCounters,
	)
}

func writeTaskMetrics(
	tasks []databricksSdkJobs.RunTask,
	metricPrefix string,
	attrs map[string]interface{},
	writer chan <- model.Metric,
	lastRunMilli int64,
	taskCounters *counters,
) {
	taskMetricAttrs := maps.Clone(attrs)

	for _, task := range tasks {
		log.Debugf(
			"processing task %s for task run ID %d",
			task.TaskKey,
			task.RunId,
		)

		state := task.Status.State

		if state == databricksSdkJobs.RunLifecycleStateV2StateTerminated &&
			// this run terminated before the last run so presumably we
			// already picked it up and don't want to report on it twice
			task.EndTime < lastRunMilli {
			continue
		}

		taskMetricAttrs["databricksJobRunTaskName"] = task.TaskKey
		taskMetricAttrs["databricksJobRunTaskState"] = string(state)
		taskMetricAttrs["databricksJobRunTaskAttemptNumber"] = (
			task.AttemptNumber)
		taskMetricAttrs["databricksJobRunTaskIsRetry"] = task.AttemptNumber > 0

		if state == databricksSdkJobs.RunLifecycleStateV2StateTerminated {
			taskCounters.terminated += 1

			writeTaskTerminatedMetrics(
				&task,
				metricPrefix,
				taskMetricAttrs,
				writer,
			)

			continue
		}

		// Calculate the wall clock time of the task run duration by subtracting
		// the task start time from now. This duration could include setup,
		// queue, execution, and cleanup durations

		// For BLOCKED tasks, the StartTime is zero. When this happens, the
		// calculated run duration ends up being the current millis which could
		// significantly throw off aggregations. Adding another data point with
		// a value of 0 instead could equally throw off aggregations. Just in
		// case there are other cases where StartTime can be zero, only
		// calculate and record the run duration if StartTime > 0 (as opposed to
		// just when state is BLOCKED). Even if StartTime == 0, we still
		// need to increment our task counters.

		if task.StartTime > 0 {
			writeGauge(
				metricPrefix,
				"job.run.task.duration",
				time.Now().UnixMilli() - task.StartTime,
				taskMetricAttrs,
				writer,
			)
		}

		if state == databricksSdkJobs.RunLifecycleStateV2StateBlocked {
			taskCounters.blocked += 1
		} else if state == databricksSdkJobs.RunLifecycleStateV2StatePending {
			taskCounters.pending += 1
		} else if state == databricksSdkJobs.RunLifecycleStateV2StateQueued {
			taskCounters.queued += 1
		} else if state == databricksSdkJobs.RunLifecycleStateV2StateRunning {
			taskCounters.running += 1
		} else if
			state == databricksSdkJobs.RunLifecycleStateV2StateTerminating {
			taskCounters.terminating += 1
		}
	}
}

func writeTaskTerminatedMetrics(
	task *databricksSdkJobs.RunTask,
	metricPrefix string,
	attrs map[string]interface{},
	writer chan <- model.Metric,
) {
	termDetails := task.Status.TerminationDetails

	attrs["databricksJobRunTaskTerminationCode"] = string(termDetails.Code)
	attrs["databricksJobRunTaskTerminationType"] = string(termDetails.Type)

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
		writeGauge(
			metricPrefix,
			"job.run.task.duration",
			task.SetupDuration + task.ExecutionDuration + task.CleanupDuration,
			attrs,
			writer,
		)
		writeGauge(
			metricPrefix,
			"job.run.task.duration.queue",
			task.QueueDuration,
			attrs,
			writer,
		)
		writeGauge(
			metricPrefix,
			"job.run.task.duration.execution",
			task.ExecutionDuration,
			attrs,
			writer,
		)
		writeGauge(
			metricPrefix,
			"job.run.task.duration.setup",
			task.SetupDuration,
			attrs,
			writer,
		)
		writeGauge(
			metricPrefix,
			"job.run.task.duration.cleanup",
			task.CleanupDuration,
			attrs,
			writer,
		)
	/* See note above
	}
	*/
}

func writeCounters(
	metricPrefix string,
	metricName string,
	counters *counters,
	attrName string,
	tags map[string]string,
	writer chan <- model.Metric,
) {
	attrs := makeAttributesMap(tags)

	attrs[attrName] = string(databricksSdkJobs.RunLifecycleStateV2StateBlocked)

	writeGauge(
		metricPrefix,
		metricName,
		counters.blocked,
		attrs,
		writer,
	)

	attrs[attrName] = string(databricksSdkJobs.RunLifecycleStateV2StatePending)

	writeGauge(
		metricPrefix,
		metricName,
		counters.pending,
		attrs,
		writer,
	)

	attrs[attrName] = string(databricksSdkJobs.RunLifecycleStateV2StateQueued)

	writeGauge(
		metricPrefix,
		metricName,
		counters.queued,
		attrs,
		writer,
	)

	attrs[attrName] = string(databricksSdkJobs.RunLifecycleStateV2StateRunning)

	writeGauge(
		metricPrefix,
		metricName,
		counters.running,
		attrs,
		writer,
	)

	attrs[attrName] = (
		string(databricksSdkJobs.RunLifecycleStateV2StateTerminating))

	writeGauge(
		metricPrefix,
		metricName,
		counters.terminating,
		attrs,
		writer,
	)

	attrs[attrName] = (
		string(databricksSdkJobs.RunLifecycleStateV2StateTerminated))

	writeGauge(
		metricPrefix,
		metricName,
		counters.terminated,
		attrs,
		writer,
	)
}
