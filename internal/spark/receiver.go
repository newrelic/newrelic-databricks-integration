package spark

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration"
	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/log"
	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/model"
	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/pipeline"
	"github.com/spf13/viper"
)

type ClusterManagerType string

const (
	ClusterManagerTypeStandalone ClusterManagerType = "standalone"
	ClusterManagerTypeDatabricks ClusterManagerType = "databricks"
)

var (
	// NewSparkMetricsReceiver is exposed like this for dependency injection
	// purposes to enable mocking of the receiver in tests.
	NewSparkMetricsReceiver = newSparkMetricsReceiver
)

func getClusterManager() (ClusterManagerType, error) {
	// Set the default cluster manager to standalone
	viper.SetDefault(
		"spark.clusterManager",
		string(ClusterManagerTypeStandalone),
	)

	// Get the cluster manager type from the config
	clusterManagerType := ClusterManagerType(
		strings.ToLower(viper.GetString("spark.clusterManager")),
	)

	switch clusterManagerType {
	case ClusterManagerTypeStandalone, ClusterManagerTypeDatabricks:
		return clusterManagerType, nil
	default:
		return "", errors.New("invalid clusterManager type")
	}
}

type SparkEventDecorator interface {
	DecorateExecutor(
		sparkExecutor *SparkExecutor,
		attrs map[string]interface{},
	)
	DecorateJob(sparkJob *SparkJob, attrs map[string]interface{})
	DecorateStage(sparkStage *SparkStage, attrs map[string]interface{})
	DecorateTask(
		sparkStage *SparkStage,
		sparkTask *SparkTask,
		attrs map[string]interface{},
	)
	DecorateRDD(sparkRDD *SparkRDD, attrs map[string]interface{})
	DecorateEvent(attrs map[string]interface{})
}

type SparkMetricsReceiver struct {
	i               *integration.LabsIntegration
	client          SparkApiClient
	clusterManager  ClusterManagerType
	eventDecorator  SparkEventDecorator
	tags            map[string]string
	lastRun         time.Time
}

func newSparkMetricsReceiver(
	ctx context.Context,
	i *integration.LabsIntegration,
	client SparkApiClient,
	originalTags map[string]string,
	tags map[string]string,
) (pipeline.EventsReceiver, error) {
	clusterManager, err := getClusterManager()
	if err != nil {
		return nil, err
	}

	var eventDecorator SparkEventDecorator

	switch clusterManager {
	case ClusterManagerTypeDatabricks:
		log.Debugf("using Databricks metric decorator")

		// We pass the originalTags to support the case where the customer is
		// using an older version of the init script that doesn't have the
		// databricks.clusterId set but does have the legacy databricks* tags
		// set. This allows use to provide dynamic cluster attributes even if an
		// older init script is in use and still allow the legacy tags to be
		// removed from the actual tags added to all events emitted by the
		// receiver.
		eventDecorator, err = NewDatabricksSparkEventDecorator(
			ctx,
			getClusterId(originalTags),
		)
		if err != nil {
			return nil, err
		}
	}

	r := &SparkMetricsReceiver{
		i,
		client,
		clusterManager,
		eventDecorator,
		tags,
		Now().UTC(),
	}

	return r, nil
}

func (s *SparkMetricsReceiver) GetId() string {
	return "spark-metrics-receiver"
}

func (s *SparkMetricsReceiver) PollEvents(
	ctx context.Context,
	writer chan<- model.Event,
) error {
	lastRun := s.lastRun

	// This only works when the integration is run as a standalone application
	// that stays running since it requires state to be maintained. However,
	// this is the only supported way to run the integration. If this ever
	// changes, this approach to saving the last run time needs to be reviewed.
	s.lastRun = Now().UTC()

	sparkApps, err := s.client.GetApplications(ctx)
	if err != nil {
		return err
	}

	wg := sync.WaitGroup{}
	errs := []error{}

	for _, sparkApp := range sparkApps {
		wg.Add(1)
		go func(app *SparkApplication) {
			defer wg.Done()

			err := collectSparkAppExecutorMetrics(
				ctx,
				s.client,
				app,
				s.eventDecorator,
				s.tags,
				writer,
			)
			if err != nil {
				errs = append(errs, err)
			}

			completedJobs, err := collectSparkAppJobMetrics(
				ctx,
				s.client,
				app,
				s.eventDecorator,
				lastRun,
				s.tags,
				writer,
			)
			if err != nil {
				errs = append(errs, err)
			}

			err = collectSparkAppStageMetrics(
				ctx,
				s.client,
				app,
				completedJobs,
				s.eventDecorator,
				lastRun,
				s.tags,
				writer,
			)
			if err != nil {
				errs = append(errs, err)
			}

			err = collectSparkAppRDDMetrics(
				ctx,
				s.client,
				app,
				s.eventDecorator,
				s.tags,
				writer,
			)
			if err != nil {
				errs = append(errs, err)
			}
		}(&sparkApp)
	}

	wg.Wait()

	return errors.Join(errs...)
}

func collectSparkAppExecutorMetrics(
	ctx context.Context,
	client SparkApiClient,
	sparkApp *SparkApplication,
	eventDecorator SparkEventDecorator,
	tags map[string]string,
	writer chan<- model.Event,
) error {
	executors, err := client.GetApplicationExecutors(ctx, sparkApp)
	if err != nil {
		return err
	}

	for _, executor := range executors {
		log.Debugf("processing executor %s", executor.Id)

		attrs := makeAppAttributesMap(
			sparkApp,
			tags,
		)

		attrs["executorId"] = executor.Id
		attrs["isActive"] = executor.IsActive
		attrs["isExcluded"] = executor.IsExcluded
		attrs["isBlacklisted"] = executor.IsBlacklisted

		addTime, err := time.Parse(RFC3339Milli, executor.AddTime)
		if err != nil {
			log.Warnf(
				"ignoring invalid add time for executor %s: %s",
				executor.Id,
				executor.AddTime,
			)
		} else {
			attrs["addTime"] = addTime.UnixMilli()
		}

		if eventDecorator != nil {
			eventDecorator.DecorateExecutor(&executor, attrs)
		}

		attrs["rddBlockCount"] = executor.RddBlocks
		attrs["memoryUsedBytes"] = executor.MemoryUsed
		attrs["diskUsedBytes"] = executor.DiskUsed
		attrs["coreCount"] = executor.TotalCores
		attrs["maxTasks"] = executor.MaxTasks
		attrs["activeTaskCount"] = executor.ActiveTasks
		attrs["failedTaskCount"] = executor.FailedTasks
		attrs["completedTaskCount"] = executor.CompletedTasks
		attrs["taskCount"] = executor.TotalTasks
		attrs["taskDuration"] = executor.TotalDuration
		attrs["gcDuration"] = executor.TotalGCTime
		attrs["inputBytes"] = executor.TotalInputBytes
		attrs["shuffleReadBytes"] = executor.TotalShuffleRead
		attrs["shuffleWriteBytes"] = executor.TotalShuffleWrite
		attrs["memoryTotalBytes"] = executor.MaxMemory

		addMemoryMetrics(
			&executor.MemoryMetrics,
			attrs,
		)

		addPeakMemoryMetrics(
			&executor.PeakMemoryMetrics,
			attrs,
		)

		writer <- model.Event{
			Timestamp:  Now().UnixMilli(),
            Type:       "SparkExecutorSample",
            Attributes: attrs,
        }
	}

	return nil
}

func collectSparkAppJobMetrics(
	ctx context.Context,
	client SparkApiClient,
	sparkApp *SparkApplication,
	eventDecorator SparkEventDecorator,
	lastRun time.Time,
	tags map[string]string,
	writer chan<- model.Event,
) ([]SparkJob, error) {
	jobs, err := client.GetApplicationJobs(ctx, sparkApp)
	if err != nil {
		return nil, err
	}

	completedJobs := []SparkJob{}

	for _, job := range jobs {
		log.Debugf("processing job %d (%s)", job.JobId, job.Name)

		submissionTime, err := time.Parse(RFC3339Milli, job.SubmissionTime)
		if err != nil {
			log.Warnf(
				"skipping job %d (%s) because it has an invalid submission time: %s",
				job.JobId,
				job.Name,
				job.SubmissionTime,
			)
			continue
		}

		var completionTime time.Time

		if job.CompletionTime != "" {
			completionTime, err = time.Parse(RFC3339Milli, job.CompletionTime)
			if err != nil {
				log.Warnf(
					"skipping job %d (%s) because it has an invalid completion time: %s",
					job.JobId,
					job.Name,
					job.CompletionTime,
				)
				continue
			}
		}

		if submissionTime.After(lastRun) {
			// Job was submitted since the last run so we need to add a submit
			// event for it
			writer <- model.Event{
				Timestamp:  Now().UnixMilli(),
				Type:       "SparkJob",
				Attributes: makeJobStartAttributes(
					sparkApp,
                    &job,
                    eventDecorator,
					submissionTime,
                    tags,
				),
			}
		}

		if completionTime.After(lastRun) {
			// Job completed since the last run so we need to add a complete
			// event for it
			writer <- model.Event{
				Timestamp:  Now().UnixMilli(),
				Type:       "SparkJob",
				Attributes: makeJobCompleteAttributes(
					sparkApp,
                    &job,
                    eventDecorator,
					submissionTime,
					completionTime,
                    tags,
				),
			}

			// Add the job to the list of completed jobs
			completedJobs = append(completedJobs, job)
        }
	}

	return completedJobs, nil
}

func makeJobBaseAttributes(
	sparkApp *SparkApplication,
	job *SparkJob,
	eventDecorator SparkEventDecorator,
	event string,
	submissionTime time.Time,
	tags map[string]string,
) map[string]interface{} {
	attrs := makeAppAttributesMap(
		sparkApp,
		tags,
	)

	attrs["event"] = event

	attrs["jobId"] = job.JobId
	attrs["jobName"] = job.Name
	attrs["jobGroup"] = job.JobGroup
	attrs["jobTags"] = strings.Join(job.JobTags, ",")

	attrs["description"] = job.Description

	if eventDecorator != nil {
		eventDecorator.DecorateJob(job, attrs)
	}

	attrs["submissionTime"] = submissionTime.UnixMilli()

	return attrs
}

func makeJobStartAttributes(
	sparkApp *SparkApplication,
    job *SparkJob,
    eventDecorator SparkEventDecorator,
	submissionTime time.Time,
    tags map[string]string,
) map[string]interface{} {
	return makeJobBaseAttributes(
		sparkApp,
		job,
		eventDecorator,
		"start",
		submissionTime,
		tags,
	)
}

func makeJobCompleteAttributes(
	sparkApp *SparkApplication,
    job *SparkJob,
    eventDecorator SparkEventDecorator,
	submissionTime time.Time,
	completionTime time.Time,
    tags map[string]string,
) map[string]interface{} {
	// We want the complete event to have everything in the start event
	attrs := makeJobStartAttributes(
		sparkApp,
		job,
		eventDecorator,
		submissionTime,
		tags,
	)

	// Except the event type should be set to "complete"
	attrs["event"] = "complete"

	// And it should have the following additional attributes
	attrs["status"] = strings.ToLower(job.Status)
	attrs["completionTime"] = completionTime.UnixMilli()
	attrs["duration"] = completionTime.Sub(submissionTime).Milliseconds()

	attrs["completedIndexCount"] = job.NumCompletedIndices
	attrs["activeStageCount"] = job.NumActiveStages
	attrs["completedStageCount"] = job.NumCompletedStages
	attrs["skippedStageCount"] = job.NumSkippedStages
	attrs["failedStageCount"] = job.NumFailedStages
	attrs["taskCount"] = job.NumTasks
	attrs["activeTaskCount"] = job.NumActiveTasks
	attrs["completedTaskCount"] = job.NumCompletedTasks
	attrs["skippedTaskCount"] = job.NumSkippedTasks
	attrs["failedTaskCount"] = job.NumFailedTasks
	attrs["killedTaskCount"] = job.NumKilledTasks

	return attrs
}

func collectSparkAppStageMetrics(
	ctx context.Context,
	client SparkApiClient,
	sparkApp *SparkApplication,
	completedJobs []SparkJob,
	eventDecorator SparkEventDecorator,
	lastRun time.Time,
	tags map[string]string,
	writer chan<- model.Event,
) error {
	stages, err := client.GetApplicationStages(ctx, sparkApp)
	if err != nil {
		return err
	}

	for _, stage := range stages {
		if log.IsDebugEnabled() {
			log.Debugf(
				"processing stage %d (%s), status: %s, firstTaskLaunchedTime: %s, completionTime: %s, lastRun: %s",
				stage.StageId,
				stage.Name,
				stage.Status,
				stage.FirstTaskLaunchedTime,
				stage.CompletionTime,
				lastRun.Format(RFC3339Milli),
			)
		}

		// First, handle skipped stages as special cases.
		if strings.ToLower(stage.Status) == "skipped" {
			if shouldReportSkippedStage(completedJobs, &stage) {
				writer <- model.Event{
					Timestamp:  Now().UnixMilli(),
					Type:       "SparkStage",
					Attributes: makeStageSkippedAttributes(
						sparkApp,
						&stage,
						eventDecorator,
						tags,
					),
				}
			}

			continue
		}

		if stage.FirstTaskLaunchedTime == "" {
			// There are legitimate cases where a stage does not have a first
			// task launched time. For example, in testing, when the status was
			// "PENDING" or "SKIPPED", the first task launched time, along with
			// the submission time and completion time, were not included on the
			// response. We handle "SKIPPED" as a special case above. For other
			// cases, I don't believe the lack of a first launched time is an
			// error condition so don't produce a warning, just log it and skip
			// the stage.

			log.Debugf(
				"skipping stage %d (%s) because it has no first task launched time",
				stage.StageId,
				stage.Name,
			)
			continue
		}

		firstTaskLaunchedTime, err := time.Parse(
			RFC3339Milli,
			stage.FirstTaskLaunchedTime,
		)
		if err != nil {
			log.Warnf(
				"skipping stage %d (%s) because it has an invalid first task launched time: %s",
				stage.StageId,
				stage.Name,
				stage.FirstTaskLaunchedTime,
			)
			continue
		}

		var completionTime time.Time

		if stage.CompletionTime != "" {
			completionTime, err = time.Parse(RFC3339Milli, stage.CompletionTime)
			if err != nil {
				log.Warnf(
					"skipping stage %d (%s) because it has an invalid completion time: %s",
					stage.StageId,
					stage.Name,
					stage.CompletionTime,
				)
				continue
			}
		}

		if !completionTime.IsZero() && completionTime.Before(lastRun) {
			// Stage completed before the last run so we don't need to do
			// anything with it.
			log.Debugf(
				"skipping stage %d (%s) because it completed before the last run",
				stage.StageId,
				stage.Name,
			)
			continue
		}

		if firstTaskLaunchedTime.After(lastRun) {
			// Stage was started since the last run so we need to add a start
			// event for it
			writer <- model.Event{
				Timestamp:  Now().UnixMilli(),
				Type:       "SparkStage",
				Attributes: makeStageStartAttributes(
					sparkApp,
                    &stage,
                    eventDecorator,
					firstTaskLaunchedTime,
                    tags,
				),
			}
		}

		if completionTime.After(lastRun) {
			// Stage completed since the last run so we need to add a complete
			// event for it
			writer <- model.Event{
				Timestamp:  Now().UnixMilli(),
				Type:       "SparkStage",
				Attributes: makeStageCompleteAttributes(
					sparkApp,
					&stage,
					eventDecorator,
					firstTaskLaunchedTime,
					completionTime,
					tags,
				),
			}
		}

		// Now process the tasks in the stage
		for _, task := range stage.Tasks {
			if log.IsDebugEnabled() {
				log.Debugf(
					"processing task %d, status: %s, launchTime: %s, duration: %d, lastRun: %s",
					task.TaskId,
					task.Status,
					task.LaunchTime,
					task.Duration,
					lastRun.Format(RFC3339Milli),
				)
			}

			processSparkAppStageTask(
				sparkApp,
				&stage,
				&task,
				eventDecorator,
				lastRun,
				tags,
				writer,
			)
		}
	}

	return nil
}

func makeStageBaseAttributes(
	sparkApp *SparkApplication,
	stage *SparkStage,
	eventDecorator SparkEventDecorator,
	event string,
	tags map[string]string,
) map[string]interface{} {
	attrs := makeAppAttributesMap(sparkApp, tags)

	attrs["event"] = event

	attrs["stageId"] = stage.StageId
	attrs["jobDescription"] = stage.Description
	attrs["stageName"] = stage.Name
	attrs["details"] = stage.Details[:min(len(stage.Details), 4096)]
	attrs["attemptId"] = stage.AttemptId
	// @TODO: is this an id or a name or something else?
	attrs["schedulingPool"] = stage.SchedulingPool
	attrs["resourceProfileId"] = stage.ResourceProfileId

	if eventDecorator != nil {
		eventDecorator.DecorateStage(stage, attrs)
	}

	return attrs
}

func makeStageSkippedAttributes(
	sparkApp *SparkApplication,
	stage *SparkStage,
	eventDecorator SparkEventDecorator,
	tags map[string]string,
) map[string]interface{} {
	attrs := makeStageBaseAttributes(
		sparkApp,
		stage,
		eventDecorator,
		"complete",
		tags,
	)

	attrs["status"] = strings.ToLower(stage.Status)
	// @TODO: double check if submission time is set
	attrs["taskCount"] = stage.NumTasks

	return attrs
}

func makeStageStartAttributes(
	sparkApp *SparkApplication,
    stage *SparkStage,
    eventDecorator SparkEventDecorator,
	firstTaskLaunchedTime time.Time,
    tags map[string]string,
) map[string]interface{} {
	attrs := makeStageBaseAttributes(
		sparkApp,
		stage,
		eventDecorator,
		"start",
		tags,
	)

	attrs["firstTaskLaunchedTime"] = firstTaskLaunchedTime.UnixMilli()

	submissionTime, err := time.Parse(RFC3339Milli, stage.SubmissionTime)
	if err != nil {
		log.Warnf(
			"ignoring invalid submission time for stage %d (%s): %s",
			stage.StageId,
			stage.Name,
			stage.SubmissionTime,
		)
	} else {
		attrs["submissionTime"] = submissionTime.UnixMilli()
	}

	return attrs
}

func makeStageCompleteAttributes(
	sparkApp *SparkApplication,
    stage *SparkStage,
    eventDecorator SparkEventDecorator,
	firstTaskLaunchedTime time.Time,
	completionTime time.Time,
    tags map[string]string,
) map[string]interface{} {
	// We want the complete event to have everything in the start event
	attrs := makeStageStartAttributes(
		sparkApp,
		stage,
		eventDecorator,
		firstTaskLaunchedTime,
		tags,
	)

	// Except the event type should be set to "complete"
	attrs["event"] = "complete"

	// And it should have the following additional attributes
	attrs["status"] = strings.ToLower(stage.Status)
	attrs["completionTime"] = completionTime.UnixMilli()
	attrs["duration"] = completionTime.Sub(firstTaskLaunchedTime).Milliseconds()

	attrs["taskCount"] = stage.NumTasks
	attrs["activeTaskCount"] = stage.NumActiveTasks
	attrs["completedTaskCount"] = stage.NumCompleteTasks
	attrs["failedTaskCount"] = stage.NumFailedTasks
	attrs["killedTaskCount"] = stage.NumKilledTasks
	attrs["completedIndexCount"] = stage.NumCompletedIndices
	attrs["executorDeserializeDuration"] = stage.ExecutorDeserializeTime
	attrs["executorDeserializeCpuDuration"] =
		stage.ExecutorDeserializeCpuTime
	attrs["executorRunDuration"] = stage.ExecutorRunTime
	attrs["executorCpuDuration"] = stage.ExecutorCpuTime
	attrs["resultSizeBytes"] = stage.ResultSize
	attrs["gcDuration"] = stage.JvmGcTime
	attrs["resultSerializationDuration"] = stage.ResultSerializationTime
	attrs["memorySpilledBytes"] = stage.MemoryBytesSpilled
	attrs["diskSpilledBytes"] = stage.DiskBytesSpilled
	attrs["peakExecutionMemoryUsedBytes"] = stage.PeakExecutionMemory
	attrs["inputBytes"] = stage.InputBytes
	attrs["inputRecords"] = stage.InputRecords
	attrs["outputBytes"] = stage.OutputBytes
	attrs["outputRecords"] = stage.OutputRecords
	attrs["shuffleRemoteFetchedBlockCount"] =
		stage.ShuffleRemoteBlocksFetched
	attrs["shuffleLocalFetchedBlockCount"] = stage.ShuffleLocalBlocksFetched
	attrs["shuffleFetchWaitDuration"] = stage.ShuffleFetchWaitTime
	attrs["shuffleRemoteReadBytes"] = stage.ShuffleRemoteBytesRead
	attrs["shuffleRemoteReadToDiskBytes"] =
		stage.ShuffleRemoteBytesReadToDisk
	attrs["shuffleLocalReadBytes"] = stage.ShuffleLocalBytesRead
	attrs["shuffleReadBytes"] = stage.ShuffleReadBytes
	attrs["shuffleReadRecords"] = stage.ShuffleReadRecords
	attrs["shuffleCorruptMergedBlockChunkCount"] =
		stage.ShuffleCorruptMergedBlockChunks
	attrs["shuffleMergedFetchFallbackCount"] =
		stage.ShuffleMergedFetchFallbackCount
	attrs["shuffleRemoteMergedFetchedBlockCount"] =
		stage.ShuffleMergedRemoteBlocksFetched
	attrs["shuffleLocalMergedFetchedBlockCount"] =
		stage.ShuffleMergedLocalBlocksFetched
	attrs["shuffleRemoteMergedFetchedChunkCount"] =
		stage.ShuffleMergedRemoteChunksFetched
	attrs["shuffleLocalMergedFetchedChunkCount"] =
		stage.ShuffleMergedLocalChunksFetched
	attrs["shuffleRemoteMergedReadBytes"] =
		stage.ShuffleMergedRemoteBytesRead
	attrs["shuffleLocalMergedReadBytes"] = stage.ShuffleMergedLocalBytesRead
	attrs["shuffleRemoteReqsDuration"] = stage.ShuffleRemoteReqsDuration
	attrs["shuffleRemoteMergedReqsDuration"] =
		stage.ShuffleMergedRemoteReqsDuration
	attrs["shuffleWriteBytes"] = stage.ShuffleWriteBytes
	attrs["shuffleWriteDuration"] = stage.ShuffleWriteTime
	attrs["shuffleWriteRecords"] = stage.ShuffleWriteRecords
	attrs["shuffleMergersCount"] = stage.ShuffleMergersCount

	// The next 4 are Databricks only attributes, not present in the regular
	// Spark metrics. I don't know what they are for, whether used or free or
	// total or bytes or something else so I'm leaving the names as the same
	// as we get them in the API call. Prefixing them with "stage." to
	// differentiate them from the same named peak memory metrics added in
	// addPeakMemoryMetrics.
	attrs["stage.peakNettyDirectMemory"] = stage.PeakNettyDirectMemory
	attrs["stage.peakJvmDirectMemory"] = stage.PeakJvmDirectMemory
	attrs["stage.peakSparkDirectMemoryOverLimit"] =
		stage.PeakSparkDirectMemoryOverLimit
	attrs["stage.peakTotalOffHeapMemory"] = stage.PeakTotalOffHeapMemory

	// Add the peak memory metrics
	addPeakMemoryMetrics(
		&stage.PeakExecutorMetrics,
		attrs,
	)

	return attrs
}

func processSparkAppStageTask(
	sparkApp *SparkApplication,
	stage *SparkStage,
	task *SparkTask,
	eventDecorator SparkEventDecorator,
	lastRun time.Time,
	tags map[string]string,
	writer chan<- model.Event,
) {
	if task.LaunchTime == "" {
		// It's possible there are legitimate cases where a task does not
		// have a launch time. For example, when the status is "PENDING"
		// it is conceivable that the launch time would be empty based on the
		// fact that we know this to be the case for stages. I was not able to
		// confirm this like I was for stages but for now we'll assume empty
		// launch times are legitimate cases and not error conditions, so don't
		// produce a warning, just log it and skip the task.
		log.Debugf(
			"skipping task %d because it has no launch time",
			task.TaskId,
		)
		return
	}

	launchTime, err := time.Parse(RFC3339Milli, task.LaunchTime)
	if err != nil {
		log.Warnf(
			"skipping task %d because it has an invalid launch time: %s",
			task.TaskId,
			task.LaunchTime,
		)
		return
	}

	status := strings.ToLower(task.Status)

	var completionTime time.Time

	if status == "success" || status == "failed" || status == "killed" {
		// If the task is in a completion state, we can use the launch time and
		// the duration to calculate the completion time.
		completionTime = launchTime.Add(
			time.Duration(task.Duration) * time.Millisecond,
		)

		if log.IsDebugEnabled() {
			log.Debugf(
				"calculated completion time for task %d: %s",
				task.TaskId,
				completionTime.Format(RFC3339Milli),
			)
		}
	}

	if launchTime.After(lastRun) {
		// Task was launched since the last run so we need to add a start
		// event for it
		writer <- model.Event{
			Timestamp:  Now().UnixMilli(),
			Type:       "SparkTask",
			Attributes: makeTaskStartAttributes(
				sparkApp,
				stage,
				task,
				eventDecorator,
				launchTime,
				tags,
			),
		}
	}

	if completionTime.After(lastRun) {
		// Task completed since the last run so we need to add a complete
		// event for it
		writer <- model.Event{
			Timestamp:  Now().UnixMilli(),
			Type:       "SparkTask",
			Attributes: makeTaskCompleteAttributes(
				sparkApp,
				stage,
				task,
				eventDecorator,
				launchTime,
				completionTime,
				tags,
			),
		}
	}
}

func makeTaskBaseAttributes(
	sparkApp *SparkApplication,
	stage *SparkStage,
	task *SparkTask,
	eventDecorator SparkEventDecorator,
	event string,
	launchTime time.Time,
	tags map[string]string,
) map[string]interface{} {
	attrs := makeAppAttributesMap(sparkApp, tags)

	attrs["event"] = event

	attrs["stageId"] = stage.StageId
	attrs["jobDescription"] = stage.Description
	attrs["stageName"] = stage.Name
	attrs["stageStatus"] = strings.ToLower(stage.Status)
	attrs["stageAttemptId"] = stage.AttemptId
	attrs["taskId"] = task.TaskId
	attrs["index"] = task.Index
	attrs["attemptId"] = task.Attempt
	attrs["executorId"] = task.ExecutorId
	attrs["locality"] = task.TaskLocality
	attrs["speculative"] = task.Speculative
	attrs["partitionId"] = task.PartitionId

	if eventDecorator != nil {
		eventDecorator.DecorateTask(stage, task, attrs)
	}

	attrs["launchTime"] = launchTime.UnixMilli()

	return attrs
}

func makeTaskStartAttributes(
	sparkApp *SparkApplication,
	stage *SparkStage,
	task *SparkTask,
	eventDecorator SparkEventDecorator,
	launchTime time.Time,
	tags map[string]string,
) map[string]interface{} {
	return makeTaskBaseAttributes(
		sparkApp,
		stage,
		task,
		eventDecorator,
		"start",
		launchTime,
		tags,
	)
}

func makeTaskCompleteAttributes(
	sparkApp *SparkApplication,
	stage *SparkStage,
	task *SparkTask,
	eventDecorator SparkEventDecorator,
	launchTime time.Time,
	completionTime time.Time,
	tags map[string]string,
) map[string]interface{} {
	// We want the complete event to have everything in the start event
	attrs := makeTaskStartAttributes(
		sparkApp,
		stage,
		task,
		eventDecorator,
		launchTime,
		tags,
	)

	// Except the event type should be set to "complete"
	attrs["event"] = "complete"

	// And it should have the following additional attributes
	attrs["status"] = strings.ToLower(task.Status)
	attrs["completionTime"] = completionTime.UnixMilli()
	attrs["duration"] = task.Duration
	attrs["schedulerDelay"] = task.SchedulerDelay
	attrs["gettingResultDuration"] = task.GettingResultTime

	taskMetrics := task.TaskMetrics

	attrs["executorDeserializeDuration"] = taskMetrics.ExecutorDeserializeTime
	attrs["executorDeserializeCpuDuration"] =
		taskMetrics.ExecutorDeserializeCpuTime
	attrs["executorRunDuration"] = taskMetrics.ExecutorRunTime
	attrs["executorCpuDuration"] = taskMetrics.ExecutorCpuTime
	attrs["resultSizeBytes"] = taskMetrics.ResultSize
	attrs["gcDuration"] = taskMetrics.JvmGcTime
	attrs["resultSerializationDuration"] = taskMetrics.ResultSerializationTime
	attrs["memorySpilledBytes"] = taskMetrics.MemoryBytesSpilled
	attrs["diskSpilledBytes"] = taskMetrics.DiskBytesSpilled
	attrs["peakExecutionMemoryUsedBytes"] = taskMetrics.PeakExecutionMemory
	attrs["inputReadBytes"] = taskMetrics.InputMetrics.BytesRead
	attrs["inputReadRecords"] = taskMetrics.InputMetrics.RecordsRead
	attrs["outputWriteBytes"] = taskMetrics.OutputMetrics.BytesWritten
	attrs["outputWriteRecords"] = taskMetrics.OutputMetrics.RecordsWritten
	attrs["shuffleReadRemoteFetchedBlockCount"] =
		taskMetrics.ShuffleReadMetrics.RemoteBlocksFetched
	attrs["shuffleReadLocalFetchedBlockCount"] =
		taskMetrics.ShuffleReadMetrics.LocalBlocksFetched
	attrs["shuffleReadFetchWaitDuration"] =
		taskMetrics.ShuffleReadMetrics.FetchWaitTime
	attrs["shuffleReadRemoteReadBytes"] =
		taskMetrics.ShuffleReadMetrics.RemoteBytesRead
	attrs["shuffleReadRemoteReadToDiskBytes"] =
		taskMetrics.ShuffleReadMetrics.RemoteBytesReadToDisk
	attrs["shuffleReadLocalReadBytes"] =
		taskMetrics.ShuffleReadMetrics.LocalBytesRead
	attrs["shuffleReadReadRecords"] = taskMetrics.ShuffleReadMetrics.RecordsRead
	attrs["shuffleReadRemoteReqsDuration"] =
		taskMetrics.ShuffleReadMetrics.RemoteReqsDuration
	attrs["shufflePushReadCorruptMergedBlockChunkCount"] =
		taskMetrics.ShuffleReadMetrics.SufflePushReadMetrics.CorruptMergedBlockChunks
	attrs["shufflePushReadMergedFetchFallbackCount"] =
		taskMetrics.ShuffleReadMetrics.SufflePushReadMetrics.MergedFetchFallbackCount
	attrs["shufflePushReadRemoteMergedFetchedBlockCount"] =
		taskMetrics.ShuffleReadMetrics.SufflePushReadMetrics.RemoteMergedBlocksFetched
	attrs["shufflePushReadLocalMergedFetchedBlockCount"] =
		taskMetrics.ShuffleReadMetrics.SufflePushReadMetrics.LocalMergedBlocksFetched
	attrs["shufflePushReadRemoteMergedFetchedChunkCount"] =
		taskMetrics.ShuffleReadMetrics.SufflePushReadMetrics.RemoteMergedChunksFetched
	attrs["shufflePushReadLocalMergedFetchedChunkCount"] =
		taskMetrics.ShuffleReadMetrics.SufflePushReadMetrics.LocalMergedChunksFetched
	attrs["shufflePushReadRemoteMergedReadBytes"] =
		taskMetrics.ShuffleReadMetrics.SufflePushReadMetrics.RemoteMergedBytesRead
	attrs["shufflePushReadLocalMergedReadBytes"] =
		taskMetrics.ShuffleReadMetrics.SufflePushReadMetrics.LocalMergedBytesRead
	attrs["shufflePushReadRemoteMergedReqsDuration"] =
		taskMetrics.ShuffleReadMetrics.SufflePushReadMetrics.RemoteMergedReqsDuration
	attrs["shuffleWriteWriteBytes"] =
		taskMetrics.ShuffleWriteMetrics.BytesWritten
	attrs["shuffleWriteWriteDuration"] =
		taskMetrics.ShuffleWriteMetrics.WriteTime
	attrs["shuffleWriteWriteRecords"] =
		taskMetrics.ShuffleWriteMetrics.RecordsWritten

	// The next 4 are Databricks only attributes, not present in the regular
	// Spark metrics. I don't know what they are for, whether used or free or
	// total or bytes or something else so I'm leaving the names as the same
	// as we get them in the API call.
	attrs["photonOffHeapMinMemorySize"] =
		taskMetrics.PhotonMemoryMetrics.OffHeapMinMemorySize
	attrs["photonOffHeapMaxMemorySize"] =
		taskMetrics.PhotonMemoryMetrics.OffHeapMaxMemorySize
	attrs["photonBufferPoolMinMemorySize"] =
		taskMetrics.PhotonMemoryMetrics.PhotonBufferPoolMinMemorySize
	attrs["photonBufferPoolMaxMemorySize"] =
		taskMetrics.PhotonMemoryMetrics.PhotonBufferPoolMaxMemorySize
	attrs["photonizedTaskTimeNs"] = taskMetrics.PhotonizedTaskTimeNs
	attrs["snapStartedTaskCount"] = taskMetrics.SnapStartedTaskCount

	return attrs
}

func collectSparkAppRDDMetrics(
	ctx context.Context,
	client SparkApiClient,
	sparkApp *SparkApplication,
	eventDecorator SparkEventDecorator,
	tags map[string]string,
	writer chan<- model.Event,
) error {
	rdds, err := client.GetApplicationRDDs(ctx, sparkApp)
	if err != nil {
		return err
	}

	for _, rdd := range rdds {
		log.Debugf("processing rdd %d", rdd.Id)

		attrs := makeAppAttributesMap(
			sparkApp,
			tags,
		)

		attrs["rddId"] = rdd.Id
		attrs["rddName"] = rdd.Name
		attrs["partitionCount"] = rdd.NumPartitions
		attrs["cachedPartitionCount"] = rdd.NumCachedPartitions
		attrs["storageLevel"] = rdd.StorageLevel
		attrs["memoryUsedBytes"] = rdd.MemoryUsed
		attrs["diskUsedBytes"] = rdd.DiskUsed

		if eventDecorator != nil {
			eventDecorator.DecorateRDD(&rdd, attrs)
		}

		writer <- model.Event{
			Timestamp:  Now().UnixMilli(),
            Type:       "SparkRDDSample",
            Attributes: attrs,
        }

		for index, distribution := range rdd.DataDistribution {
			distributionAttrs := makeAppAttributesMap(sparkApp, tags)

			if eventDecorator != nil {
				eventDecorator.DecorateEvent(distributionAttrs)
			}

			distributionAttrs["rddId"] = rdd.Id
			distributionAttrs["rddName"] = rdd.Name
			distributionAttrs["distributionIndex"] = index
			distributionAttrs["memoryUsedBytes"] = distribution.MemoryUsed
			distributionAttrs["memoryFreeBytes"] = distribution.MemoryRemaining
			distributionAttrs["diskUsedBytes"] = distribution.DiskUsed
			distributionAttrs["onHeapMemoryUsedBytes"] =
				distribution.OnHeapMemoryUsed
			distributionAttrs["onHeapMemoryFreeBytes"] =
				distribution.OnHeapMemoryRemaining
			distributionAttrs["offHeapMemoryUsedBytes"] =
				distribution.OffHeapMemoryUsed
			distributionAttrs["offHeapMemoryFreeBytes"] =
				distribution.OffHeapMemoryRemaining

			writer <- model.Event{
				Timestamp:  Now().UnixMilli(),
				Type:       "SparkRDDDistributionSample",
				Attributes: distributionAttrs,
			}
		}

		for _, partition := range rdd.Partitions {
			partitionAttrs := makeAppAttributesMap(sparkApp, tags)

			if eventDecorator != nil {
				eventDecorator.DecorateEvent(partitionAttrs)
			}

			partitionAttrs["rddId"] = rdd.Id
			partitionAttrs["rddName"] = rdd.Name
			partitionAttrs["blockName"] = partition.BlockName
			partitionAttrs["storageLevel"] = partition.StorageLevel
			partitionAttrs["memoryUsedBytes"] = partition.MemoryUsed
			partitionAttrs["diskUsedBytes"] = partition.DiskUsed
			partitionAttrs["executorIds"] =
				strings.Join(partition.Executors, ",")

			writer <- model.Event{
                Timestamp:  Now().UnixMilli(),
                Type:       "SparkRDDPartitionSample",
                Attributes: partitionAttrs,
            }
		}
	}

	return nil
}

func addMemoryMetrics(
	memoryMetrics *SparkExecutorMemoryMetrics,
	attrs map[string]interface{},
) {
	attrs["onHeapMemoryUsedBytes"] = memoryMetrics.UsedOnHeapStorageMemory
	attrs["offHeapMemoryUsedBytes"] = memoryMetrics.UsedOffHeapStorageMemory
	attrs["onHeapMemoryTotalBytes"] = memoryMetrics.TotalOnHeapStorageMemory
	attrs["offHeapMemoryTotalBytes"] = memoryMetrics.TotalOffHeapStorageMemory
}

func addPeakMemoryMetrics(
	peakMemoryMetrics *SparkExecutorPeakMemoryMetrics,
	attrs map[string]interface{},
) {
	attrs["peakJvmHeapMemoryUsedBytes"] = peakMemoryMetrics.JVMHeapMemory
	attrs["peakJvmOffHeapMemoryUsedBytes"] = peakMemoryMetrics.JVMOffHeapMemory
	attrs["peakOnHeapExecutionMemoryUsedBytes"] =
		peakMemoryMetrics.OnHeapExecutionMemory
	attrs["peakOffHeapExecutionMemoryUsedBytes"] =
		peakMemoryMetrics.OffHeapExecutionMemory
	attrs["peakOnHeapStorageMemoryUsedBytes"] =
		peakMemoryMetrics.OnHeapStorageMemory
	attrs["peakOffHeapStorageMemoryUsedBytes"] =
		peakMemoryMetrics.OffHeapStorageMemory
	attrs["peakOnHeapUnifiedMemoryUsedBytes"] =
		peakMemoryMetrics.OnHeapUnifiedMemory
	attrs["peakOffHeapUnifiedMemoryUsedBytes"] =
		peakMemoryMetrics.OffHeapUnifiedMemory
	attrs["peakDirectPoolMemoryUsedBytes"] = peakMemoryMetrics.DirectPoolMemory
	attrs["peakMappedPoolMemoryUsedBytes"] = peakMemoryMetrics.MappedPoolMemory
	attrs["peakProcessTreeJvmVirtualBytes"] =
		peakMemoryMetrics.ProcessTreeJVMVMemory
	attrs["peakProcessTreeJvmRSS"] = peakMemoryMetrics.ProcessTreeJVMRSSMemory
	attrs["peakProcessTreePythonVirtualBytes"] =
		peakMemoryMetrics.ProcessTreePythonVMemory
	attrs["peakProcessTreePythonRSS"] =
		peakMemoryMetrics.ProcessTreePythonRSSMemory
	attrs["peakProcessTreeOtherVirtualBytes"] =
		peakMemoryMetrics.ProcessTreeOtherVMemory
	attrs["peakProcessTreeOtherRSS"] =
		peakMemoryMetrics.ProcessTreeOtherRSSMemory
	attrs["peakMinorGCCount"] = peakMemoryMetrics.MinorGCCount
	attrs["peakMinorGCDuration"] = peakMemoryMetrics.MinorGCTime
	attrs["peakMajorGCCount"] = peakMemoryMetrics.MajorGCCount
	attrs["peakMajorGCDuration"] = peakMemoryMetrics.MajorGCTime
	attrs["peakTotalGCDuration"] = peakMemoryMetrics.TotalGCTime

	// The next 4 are Databricks only attributes, not present in the regular
	// Spark metrics. I don't know what they are for, whether used or free or
	// total or bytes or something else so I'm leaving the names as the same
	// as we get them in the API call.
	attrs["peakNettyDirectMemory"] = peakMemoryMetrics.NettyDirectMemory
	attrs["peakJvmDirectMemory"] = peakMemoryMetrics.JvmDirectMemory
	attrs["peakSparkDirectMemoryOverLimit"] =
		peakMemoryMetrics.SparkDirectMemoryOverLimit
	attrs["peakTotalOffHeapMemory"] = peakMemoryMetrics.TotalOffHeapMemory
}

// Skipped stages don't have submission time, first task launched time, or
// completion time on them so we can't use those to tell if we've already
// recorded the skipped stage or if this stage was skipped since the last run.
// To compensate, we keep track of the completed jobs we've seen since the last
// run and when collecting stage metrics, if we see a skipped stage that is
// part of a job that completed since the last run, we report a complete event
// (but no start event) for the skipped stage. Using this logic, we will only
// record skipped stages once since we only record completed jobs once.
func shouldReportSkippedStage(
	completedJobs []SparkJob,
	stage *SparkStage,
) bool {
	// See if the stage is part of a completed job
	for _, job := range completedJobs {
		for _, stageId := range job.StageIds {
			if stageId == stage.StageId {
				return true
			}
		}
	}

	return false
}

func makeAppAttributesMap(
	sparkApp *SparkApplication,
	tags map[string]string,
) map[string]interface{} {
	attrs := make(map[string]interface{})

	for k, v := range tags {
		attrs[k] = v
	}

	attrs["sparkAppId"] = sparkApp.Id
	attrs["sparkAppName"] = sparkApp.Name

	return attrs
}

// Get the Databricks cluster ID either from the configuration parameter
// spark.databricks.clusterId or from the tags map under the key
// databricksclusterid. The latter check allows us to support the case where the
// customer is using an older version of the init script that doesn't have the
// databricks.clusterId configuration parameter set but does have the legacy
// databricks* tags set. This allows use to provide dynamic cluster attributes
// even if an older init script is in use.
func getClusterId(originalTags map[string]string) string {
	clusterId := viper.GetString("spark.databricks.clusterId")

	if clusterId == "" {
		var ok bool

		clusterId, ok = originalTags["databricksclusterid"]
		if ok {
			log.Debugf("got Databricks cluster ID from tags")
		}
	}

	return clusterId
}
