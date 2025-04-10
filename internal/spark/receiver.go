package spark

import (
	"context"
	"errors"
	"maps"
	"strings"
	"sync"
	"time"

	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration"
	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/log"
	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/model"
	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/pipeline"
)

type SparkMetricsReceiver struct {
	i 					*integration.LabsIntegration
	client 				*SparkApiClient
	metricPrefix		string
	tags				map[string]string
}

func NewSparkMetricsReceiver(
	i *integration.LabsIntegration,
	client *SparkApiClient,
	metricPrefix string,
	tags map[string]string,
) pipeline.MetricsReceiver {
	r := &SparkMetricsReceiver{
		i,
		client,
		metricPrefix,
		tags,
	}

	return r
}

func (s *SparkMetricsReceiver) GetId() string {
	return "spark-metrics-receiver"
}

func (s *SparkMetricsReceiver) PollMetrics(
	ctx context.Context,
	writer chan <- model.Metric,
) error {
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
				s.metricPrefix,
				s.tags,
				writer,
			)
			if err != nil {
				errs = append(errs, err)
			}

			err = collectSparkAppJobMetrics(
				ctx,
				s.client,
				app,
				s.metricPrefix,
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
				s.metricPrefix,
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
				s.metricPrefix,
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
	client *SparkApiClient,
	sparkApp *SparkApplication,
	metricPrefix string,
	tags map[string]string,
	writer chan <- model.Metric,
) error {
	executors, err := client.GetApplicationExecutors(ctx, sparkApp)
	if err !=  nil {
		return err
	}

	for _, executor := range executors {
		log.Debugf("processing executor %s", executor.Id)

		attrs := makeAppAttributesMap(
			sparkApp,
			tags,
		)

		attrs["sparkAppExecutorId"] = executor.Id

		writeGauge(
			metricPrefix,
			"app.executor.rddBlocks",
			executor.RddBlocks,
			attrs,
			writer,
		)

		writeGauge(
			metricPrefix,
			"app.executor.memoryUsed",
			executor.MemoryUsed,
			attrs,
			writer,
		)

		writeGauge(
			metricPrefix,
			"app.executor.diskUsed",
			executor.DiskUsed,
			attrs,
			writer,
		)

		writeGauge(
			metricPrefix,
			"app.executor.totalCores",
			executor.TotalCores,
			attrs,
			writer,
		)

		writeGauge(
			metricPrefix,
			"app.executor.maxTasks",
			executor.MaxTasks,
			attrs,
			writer,
		)

		writeGauge(
			metricPrefix,
			"app.executor.activeTasks",
			executor.ActiveTasks,
			attrs,
			writer,
		)

		writeGauge(
			metricPrefix,
			"app.executor.failedTasks",
			executor.FailedTasks,
			attrs,
			writer,
		)

		writeGauge(
			metricPrefix,
			"app.executor.completedTasks",
			executor.CompletedTasks,
			attrs,
			writer,
		)

		writeGauge(
			metricPrefix,
			"app.executor.totalTasks",
			executor.TotalTasks,
			attrs,
			writer,
		)

		writeGauge(
			metricPrefix,
			"app.executor.totalDuration",
			executor.TotalDuration,
			attrs,
			writer,
		)

		writeGauge(
			metricPrefix,
			"app.executor.totalGCTime",
			executor.TotalGCTime,
			attrs,
			writer,
		)

		writeGauge(
			metricPrefix,
			"app.executor.totalInputBytes",
			executor.TotalInputBytes,
			attrs,
			writer,
		)

		writeGauge(
			metricPrefix,
			"app.executor.totalShuffleRead",
			executor.TotalShuffleRead,
			attrs,
			writer,
		)

		writeGauge(
			metricPrefix,
			"app.executor.totalShuffleWrite",
			executor.TotalShuffleWrite,
			attrs,
			writer,
		)

		writeGauge(
			metricPrefix,
			"app.executor.maxMemory",
			executor.MaxMemory,
			attrs,
			writer,
		)

		writeMemoryMetrics(
			metricPrefix + "app.executor.memory.",
			&executor.MemoryMetrics,
			attrs,
			writer,
		)

		writePeakMemoryMetrics(
			metricPrefix + "app.executor.memory.peak.",
			&executor.PeakMemoryMetrics,
			attrs,
			writer,
		)
	}

	return nil
}

func collectSparkAppJobMetrics(
	ctx context.Context,
	client *SparkApiClient,
	sparkApp *SparkApplication,
	metricPrefix string,
	tags map[string]string,
	writer chan <- model.Metric,
) error {
	jobs, err := client.GetApplicationJobs(ctx, sparkApp)
	if err !=  nil {
		return err
	}

	jobsRunning := 0
	jobsLost := 0
	jobsSucceeded := 0
	jobsFailed := 0

	for _, job := range jobs {
		log.Debugf("processing job %d (%s)", job.JobId, job.Name)

		attrs := makeAppAttributesMap(
			sparkApp,
			tags,
		)

		attrs["sparkAppJobId"] = job.JobId
		// The job name and job group cause very high cardinality for Databricks
		// Notebook runs.
		//attrs["sparkAppJobName"] = job.Name
		//attrs["sparkAppJobGroup"] = job.JobGroup
		attrs["sparkAppJobStatus"] = job.Status

		jobStatus := strings.ToLower(job.Status)

		if jobStatus == "running" {
			jobsRunning += 1
		} else if jobStatus == "unknown" {
			jobsLost += 1
		} else if jobStatus == "succeeded" {
			jobsSucceeded += 1
		} else if jobStatus == "failed" {
			jobsFailed += 1
		}

		// Write all the things.

		if job.CompletionTime != "" {
			jobDuration, err := calcDateDifferenceMillis(
				job.SubmissionTime,
				job.CompletionTime,
			)
			if err != nil {
				log.Warnf("could not calculate job duration: %v", err)

				continue
			}

			writeGauge(
				metricPrefix,
				"app.job.duration",
				jobDuration,
				attrs,
				writer,
			)
		}

		writeGauge(
			metricPrefix,
			"app.job.indices.completed",
			job.NumCompletedIndices,
			attrs,
			writer,
		)

		attrs["sparkAppStageStatus"] = "active"

		writeGauge(
			metricPrefix,
			"app.job.stages",
			job.NumActiveStages,
			attrs,
			writer,
		)

		attrs["sparkAppStageStatus"] = "complete"

		writeGauge(
			metricPrefix,
			"app.job.stages",
			job.NumCompletedStages,
			attrs,
			writer,
		)

		attrs["sparkAppStageStatus"] = "skipped"

		writeGauge(
			metricPrefix,
			"app.job.stages",
			job.NumSkippedStages,
			attrs,
			writer,
		)

		attrs["sparkAppStageStatus"] = "failed"

		writeGauge(
			metricPrefix,
			"app.job.stages",
			job.NumFailedStages,
			attrs,
			writer,
		)

		delete(attrs, "sparkAppStageStatus")

		attrs["sparkAppTaskStatus"] = "active"

		writeGauge(
			metricPrefix,
			"app.job.tasks",
			job.NumActiveTasks,
			attrs,
			writer,
		)

		attrs["sparkAppTaskStatus"] = "complete"

		writeGauge(
			metricPrefix,
			"app.job.tasks",
			job.NumCompletedTasks,
			attrs,
			writer,
		)

		attrs["sparkAppTaskStatus"] = "skipped"

		writeGauge(
			metricPrefix,
			"app.job.tasks",
			job.NumSkippedTasks,
			attrs,
			writer,
		)

		attrs["sparkAppTaskStatus"] = "failed"

		writeGauge(
			metricPrefix,
			"app.job.tasks",
			job.NumFailedTasks,
			attrs,
			writer,
		)

		attrs["sparkAppTaskStatus"] = "killed"

		writeGauge(
			metricPrefix,
			"app.job.tasks",
			job.NumKilledTasks,
			attrs,
			writer,
		)

		delete(attrs, "sparkAppTaskStatus")
	}

	attrs := makeAppAttributesMap(
		sparkApp,
		tags,
	)

	attrs["sparkAppJobStatus"] = "running"

	writeGauge(
		metricPrefix,
		"app.jobs",
		jobsRunning,
		attrs,
		writer,
	)

	attrs["sparkAppJobStatus"] = "lost"

	writeGauge(
		metricPrefix,
		"app.jobs",
		jobsLost,
		attrs,
		writer,
	)

	attrs["sparkAppJobStatus"] = "succeeded"

	writeGauge(
		metricPrefix,
		"app.jobs",
		jobsSucceeded,
		attrs,
		writer,
	)

	attrs["sparkAppJobStatus"] = "failed"

	writeGauge(
		metricPrefix,
		"app.jobs",
		jobsFailed,
		attrs,
		writer,
	)

	return nil
}

func collectSparkAppStageMetrics(
	ctx context.Context,
	client *SparkApiClient,
	sparkApp *SparkApplication,
	metricPrefix string,
	tags map[string]string,
	writer chan <- model.Metric,
) error {
	stages, err := client.GetApplicationStages(ctx, sparkApp)
	if err != nil {
		return err
	}

	stagesActive := 0
	stagesPending := 0
	stagesComplete := 0
	stagesFailed := 0
	stagesSkipped := 0

	for _, stage := range stages {
		log.Debugf("processing stage %d (%s)", stage.StageId, stage.Name)

		stageStatus := strings.ToLower(stage.Status)

		attrs := makeAppAttributesMap(
			sparkApp,
			tags,
		)

		attrs["sparkAppStageName"] = stage.Name
		attrs["sparkAppStageStatus"] = stageStatus
		// @TODO: The attributes below may cause high cardinality. Further
		// investigation is needed.
		attrs["sparkAppStageId"] = stage.StageId
		attrs["sparkAppStageAttemptId"] = stage.AttemptId
		//attrs["sparkAppStageSchedulingPool"] = stage.SchedulingPool
		//attrs["sparkAppStageResourceProfileId"] = stage.ResourceProfileId

		if stageStatus == "active" {
			stagesActive += 1
		} else if stageStatus == "pending" {
			stagesPending += 1
		} else if stageStatus == "complete" {
			stagesComplete += 1
		} else if stageStatus == "failed" {
			stagesFailed += 1
		} else if stageStatus == "skipped" {
			stagesSkipped += 1
		}

		// Write all the things.

		if stage.CompletionTime != "" {
			stageDuration, err := calcDateDifferenceMillis(
				stage.FirstTaskLaunchedTime,
				stage.CompletionTime,
			)
			if err != nil {
				log.Warnf("could not calculate stage duration: %v", err)

				continue
			}

			writeGauge(
				metricPrefix,
				"app.stage.duration",
				stageDuration,
				attrs,
				writer,
			)
		}

		writeGauge(
			metricPrefix,
			"app.stage.peakNettyDirectMemory",
			stage.PeakNettyDirectMemory,
			attrs,
			writer,
		)
		writeGauge(
			metricPrefix,
			"app.stage.peakJvmDirectMemory",
			stage.PeakJvmDirectMemory,
			attrs,
			writer,
		)
		writeGauge(
			metricPrefix,
			"app.stage.peakSparkDirectMemoryOverLimit",
			stage.PeakSparkDirectMemoryOverLimit,
			attrs,
			writer,
		)
		writeGauge(
			metricPrefix,
			"app.stage.peakTotalOffHeapMemory",
			stage.PeakTotalOffHeapMemory,
			attrs,
			writer,
		)
		writeGauge(
			metricPrefix,
			"app.stage.executor.deserializeTime",
			stage.ExecutorDeserializeTime,
			attrs,
			writer,
		)
		writeGauge(
			metricPrefix,
			"app.stage.executor.deserializeCpuTime",
			stage.ExecutorDeserializeCpuTime,
			attrs,
			writer,
		)
		writeGauge(
			metricPrefix,
			"app.stage.executor.runTime",
			stage.ExecutorRunTime,
			attrs,
			writer,
		)
		writeGauge(
			metricPrefix,
			"app.stage.executor.cpuTime",
			stage.ExecutorCpuTime,
			attrs,
			writer,
		)
		writeGauge(
			metricPrefix,
			"app.stage.resultSize",
			stage.ResultSize,
			attrs,
			writer,
		)
		writeGauge(
			metricPrefix,
			"app.stage.jvmGcTime",
			stage.JvmGcTime,
			attrs,
			writer,
		)
		writeGauge(
			metricPrefix,
			"app.stage.resultSerializationTime",
			stage.ResultSerializationTime,
			attrs,
			writer,
		)
		writeGauge(
			metricPrefix,
			"app.stage.memoryBytesSpilled",
			stage.MemoryBytesSpilled,
			attrs,
			writer,
		)
		writeGauge(
			metricPrefix,
			"app.stage.diskBytesSpilled",
			stage.DiskBytesSpilled,
			attrs,
			writer,
		)
		writeGauge(
			metricPrefix,
			"app.stage.peakExecutionMemory",
			stage.PeakExecutionMemory,
			attrs,
			writer,
		)
		writeGauge(
			metricPrefix,
			"app.stage.inputBytes",
			stage.InputBytes,
			attrs,
			writer,
		)
		writeGauge(
			metricPrefix,
			"app.stage.inputRecords",
			stage.InputRecords,
			attrs,
			writer,
		)
		writeGauge(
			metricPrefix,
			"app.stage.outputBytes",
			stage.OutputBytes,
			attrs,
			writer,
		)
		writeGauge(
			metricPrefix,
			"app.stage.outputRecords",
			stage.OutputRecords,
			attrs,
			writer,
		)
		writeGauge(
			metricPrefix,
			"app.stage.shuffle.remoteBlocksFetched",
			stage.ShuffleRemoteBlocksFetched,
			attrs,
			writer,
		)
		writeGauge(
			metricPrefix,
			"app.stage.shuffle.localBlocksFetched",
			stage.ShuffleLocalBlocksFetched,
			attrs,
			writer,
		)
		writeGauge(
			metricPrefix,
			"app.stage.shuffle.fetchWaitTime",
			stage.ShuffleFetchWaitTime,
			attrs,
			writer,
		)
		writeGauge(
			metricPrefix,
			"app.stage.shuffle.remoteBytesRead",
			stage.ShuffleRemoteBytesRead,
			attrs,
			writer,
		)
		writeGauge(
			metricPrefix,
			"app.stage.shuffle.remoteBytesReadToDisk",
			stage.ShuffleRemoteBytesReadToDisk,
			attrs,
			writer,
		)
		writeGauge(
			metricPrefix,
			"app.stage.shuffle.localBytesRead",
			stage.ShuffleLocalBytesRead,
			attrs,
			writer,
		)
		writeGauge(
			metricPrefix,
			"app.stage.shuffle.readBytes",
			stage.ShuffleReadBytes,
			attrs,
			writer,
		)
		writeGauge(
			metricPrefix,
			"app.stage.shuffle.readRecords",
			stage.ShuffleReadRecords,
			attrs,
			writer,
		)
		writeGauge(
			metricPrefix,
			"app.stage.shuffle.corruptMergedClockChunks",
			stage.ShuffleCorruptMergedBlockChunks,
			attrs,
			writer,
		)
		writeGauge(
			metricPrefix,
			"app.stage.shuffle.mergedFetchFallbackCount",
			stage.ShuffleMergedFetchFallbackCount,
			attrs,
			writer,
		)
		writeGauge(
			metricPrefix,
			"app.stage.shuffle.mergedRemoteBlocksFetched",
			stage.ShuffleMergedRemoteBlocksFetched,
			attrs,
			writer,
		)
		writeGauge(
			metricPrefix,
			"app.stage.shuffle.mergedLocalBlocksFetched",
			stage.ShuffleMergedLocalBlocksFetched,
			attrs,
			writer,
		)
		writeGauge(
			metricPrefix,
			"app.stage.shuffle.mergedRemoteChunksFetched",
			stage.ShuffleMergedRemoteChunksFetched,
			attrs,
			writer,
		)
		writeGauge(
			metricPrefix,
			"app.stage.shuffle.mergedLocalChunksFetched",
			stage.ShuffleMergedLocalChunksFetched,
			attrs,
			writer,
		)
		writeGauge(
			metricPrefix,
			"app.stage.shuffle.mergedRemoteBytesRead",
			stage.ShuffleMergedRemoteBytesRead,
			attrs,
			writer,
		)
		writeGauge(
			metricPrefix,
			"app.stage.shuffle.mergedLocalBytesRead",
			stage.ShuffleMergedLocalBytesRead,
			attrs,
			writer,
		)
		writeGauge(
			metricPrefix,
			"app.stage.shuffle.remoteReqsDuration",
			stage.ShuffleRemoteReqsDuration,
			attrs,
			writer,
		)
		writeGauge(
			metricPrefix,
			"app.stage.shuffle.mergedRemoteReqsDuration",
			stage.ShuffleMergedRemoteReqsDuration,
			attrs,
			writer,
		)
		writeGauge(
			metricPrefix,
			"app.stage.shuffle.writeBytes",
			stage.ShuffleWriteBytes,
			attrs,
			writer,
		)
		writeGauge(
			metricPrefix,
			"app.stage.shuffle.writeTime",
			stage.ShuffleWriteTime,
			attrs,
			writer,
		)
		writeGauge(
			metricPrefix,
			"app.stage.shuffle.writeRecords",
			stage.ShuffleWriteRecords,
			attrs,
			writer,
		)
		writeGauge(
			metricPrefix,
			"app.stage.shuffle.mergersCount",
			stage.ShuffleMergersCount,
			attrs,
			writer,
		)

		for _, task := range stage.Tasks {
			writeStageTaskMetrics(
				metricPrefix + "app.stage.task.",
				&task,
				attrs,
				writer,
			)
		}

		writePeakMemoryMetrics(
			metricPrefix + "app.stage.executor.memory.peak.",
			&stage.PeakExecutorMetrics,
			attrs,
			writer,
		)

		writeGauge(
			metricPrefix,
			"app.stage.tasks.total",
			stage.NumTasks,
			attrs,
			writer,
		)

		attrs["sparkAppTaskStatus"] = "active"

		writeGauge(
			metricPrefix,
			"app.stage.tasks",
			stage.NumActiveTasks,
			attrs,
			writer,
		)

		attrs["sparkAppTaskStatus"] = "complete"

		writeGauge(
			metricPrefix,
			"app.stage.tasks",
			stage.NumCompleteTasks,
			attrs,
			writer,
		)

		attrs["sparkAppTaskStatus"] = "failed"

		writeGauge(
			metricPrefix,
			"app.stage.tasks",
			stage.NumFailedTasks,
			attrs,
			writer,
		)

		attrs["sparkAppTaskStatus"] = "killed"

		writeGauge(
			metricPrefix,
			"app.stage.tasks",
			stage.NumKilledTasks,
			attrs,
			writer,
		)

		delete(attrs, "sparkAppTaskStatus")

		writeGauge(
			metricPrefix,
			"app.stage.indices.completed",
			stage.NumCompletedIndices,
			attrs,
			writer,
		)
	}

	attrs := makeAppAttributesMap(
		sparkApp,
		tags,
	)

	attrs["sparkAppStageStatus"] = "active"

	writeGauge(
		metricPrefix,
		"app.stages",
		stagesActive,
		attrs,
		writer,
	)

	attrs["sparkAppStageStatus"] = "pending"

	writeGauge(
		metricPrefix,
		"app.stages",
		stagesPending,
		attrs,
		writer,
	)

	attrs["sparkAppStageStatus"] = "complete"

	writeGauge(
		metricPrefix,
		"app.stages",
		stagesComplete,
		attrs,
		writer,
	)

	attrs["sparkAppStageStatus"] = "failed"

	writeGauge(
		metricPrefix,
		"app.stages",
		stagesFailed,
		attrs,
		writer,
	)

	attrs["sparkAppStageStatus"] = "skipped"

	writeGauge(
		metricPrefix,
		"app.stages",
		stagesSkipped,
		attrs,
		writer,
	)

	return nil
}

func writeStageTaskMetrics(
	metricPrefix string,
	task *SparkTask,
	attrs map[string]interface{},
	writer chan <- model.Metric,
) {
	log.Debugf("processing task %d", task.TaskId)

	taskStatus := strings.ToLower(task.Status)

	taskMetricAttrs := maps.Clone(attrs)

	taskMetricAttrs["sparkAppTaskExecutorId"] = task.ExecutorId
	taskMetricAttrs["sparkAppTaskStatus"] = taskStatus
	taskMetricAttrs["sparkAppTaskLocality"] = task.TaskLocality
	taskMetricAttrs["sparkAppTaskSpeculative"] = task.Speculative
	// @TODO: The attributes below may cause high cardinality. Further
	// investigation is needed.
	taskMetricAttrs["sparkAppTaskId"] = task.TaskId
	taskMetricAttrs["sparkAppTaskAttempt"] = task.Attempt
	//attrs["sparkAppTaskPartitionId"] = task.PartitionId

	writeGauge(
		metricPrefix,
		"duration",
		task.Duration,
		taskMetricAttrs,
		writer,
	)

	taskMetrics := task.TaskMetrics

	writeGauge(
		metricPrefix,
		"executorDeserializeTime",
		taskMetrics.ExecutorDeserializeTime,
		taskMetricAttrs,
		writer,
	)
	writeGauge(
		metricPrefix,
		"executorDeserializeCpuTime",
		taskMetrics.ExecutorDeserializeCpuTime,
		taskMetricAttrs,
		writer,
	)
	writeGauge(
		metricPrefix,
		"executorRunTime",
		taskMetrics.ExecutorRunTime,
		taskMetricAttrs,
		writer,
	)
	writeGauge(
		metricPrefix,
		"executorCpuTime",
		taskMetrics.ExecutorCpuTime,
		taskMetricAttrs,
		writer,
	)
	writeGauge(
		metricPrefix,
		"resultSize",
		taskMetrics.ResultSize,
		taskMetricAttrs,
		writer,
	)
	writeGauge(
		metricPrefix,
		"jvmGcTime",
		taskMetrics.JvmGcTime,
		taskMetricAttrs,
		writer,
	)
	writeGauge(
		metricPrefix,
		"resultSerializationTime",
		taskMetrics.ResultSerializationTime,
		taskMetricAttrs,
		writer,
	)
	writeGauge(
		metricPrefix,
		"memoryBytesSpilled",
		taskMetrics.MemoryBytesSpilled,
		taskMetricAttrs,
		writer,
	)
	writeGauge(
		metricPrefix,
		"diskBytesSpilled",
		taskMetrics.DiskBytesSpilled,
		taskMetricAttrs,
		writer,
	)
	writeGauge(
		metricPrefix,
		"peakExecutionMemory",
		taskMetrics.PeakExecutionMemory,
		taskMetricAttrs,
		writer,
	)
	writeGauge(
		metricPrefix,
		"input.bytesRead",
		taskMetrics.InputMetrics.BytesRead,
		taskMetricAttrs,
		writer,
	)
	writeGauge(
		metricPrefix,
		"input.recordsRead",
		taskMetrics.InputMetrics.RecordsRead,
		taskMetricAttrs,
		writer,
	)
	writeGauge(
		metricPrefix,
		"output.bytesWritten",
		taskMetrics.OutputMetrics.BytesWritten,
		taskMetricAttrs,
		writer,
	)
	writeGauge(
		metricPrefix,
		"output.recordsWritten",
		taskMetrics.OutputMetrics.RecordsWritten,
		taskMetricAttrs,
		writer,
	)
	writeGauge(
		metricPrefix,
		"shuffle.read.remoteBlocksFetched",
		taskMetrics.ShuffleReadMetrics.RemoteBlocksFetched,
		taskMetricAttrs,
		writer,
	)
	writeGauge(
		metricPrefix,
		"shuffle.read.localBlocksFetched",
		taskMetrics.ShuffleReadMetrics.LocalBlocksFetched,
		taskMetricAttrs,
		writer,
	)
	writeGauge(
		metricPrefix,
		"shuffle.read.fetchWaitTime",
		taskMetrics.ShuffleReadMetrics.FetchWaitTime,
		taskMetricAttrs,
		writer,
	)
	writeGauge(
		metricPrefix,
		"shuffle.read.remoteBytesRead",
		taskMetrics.ShuffleReadMetrics.RemoteBytesRead,
		taskMetricAttrs,
		writer,
	)
	writeGauge(
		metricPrefix,
		"shuffle.read.remoteBytesReadToDisk",
		taskMetrics.ShuffleReadMetrics.RemoteBytesReadToDisk,
		taskMetricAttrs,
		writer,
	)
	writeGauge(
		metricPrefix,
		"shuffle.read.localBytesRead",
		taskMetrics.ShuffleReadMetrics.LocalBytesRead,
		taskMetricAttrs,
		writer,
	)
	writeGauge(
		metricPrefix,
		"shuffle.read.recordsRead",
		taskMetrics.ShuffleReadMetrics.RecordsRead,
		taskMetricAttrs,
		writer,
	)
	writeGauge(
		metricPrefix,
		"shuffle.read.remoteReqsDuration",
		taskMetrics.ShuffleReadMetrics.RemoteReqsDuration,
		taskMetricAttrs,
		writer,
	)
	writeGauge(
		metricPrefix,
		"shuffle.read.push.corruptMergedBlockChunks",
		taskMetrics.ShuffleReadMetrics.SufflePushReadMetrics.CorruptMergedBlockChunks,
		taskMetricAttrs,
		writer,
	)
	writeGauge(
		metricPrefix,
		"shuffle.read.push.mergedFetchFallbackCount",
		taskMetrics.ShuffleReadMetrics.SufflePushReadMetrics.MergedFetchFallbackCount,
		taskMetricAttrs,
		writer,
	)
	writeGauge(
		metricPrefix,
		"shuffle.read.push.remoteMergedBlocksFetched",
		taskMetrics.ShuffleReadMetrics.SufflePushReadMetrics.RemoteMergedBlocksFetched,
		taskMetricAttrs,
		writer,
	)
	writeGauge(
		metricPrefix,
		"shuffle.read.push.localMergedBlocksFetched",
		taskMetrics.ShuffleReadMetrics.SufflePushReadMetrics.LocalMergedBlocksFetched,
		taskMetricAttrs,
		writer,
	)
	writeGauge(
		metricPrefix,
		"shuffle.read.push.remoteMergedChunksFetched",
		taskMetrics.ShuffleReadMetrics.SufflePushReadMetrics.RemoteMergedChunksFetched,
		taskMetricAttrs,
		writer,
	)
	writeGauge(
		metricPrefix,
		"shuffle.read.push.localMergedChunksFetched",
		taskMetrics.ShuffleReadMetrics.SufflePushReadMetrics.LocalMergedChunksFetched,
		taskMetricAttrs,
		writer,
	)
	writeGauge(
		metricPrefix,
		"shuffle.read.push.remoteMergedBytesRead",
		taskMetrics.ShuffleReadMetrics.SufflePushReadMetrics.RemoteMergedBytesRead,
		taskMetricAttrs,
		writer,
	)
	writeGauge(
		metricPrefix,
		"shuffle.read.push.localMergedBytesRead",
		taskMetrics.ShuffleReadMetrics.SufflePushReadMetrics.LocalMergedBytesRead,
		taskMetricAttrs,
		writer,
	)
	writeGauge(
		metricPrefix,
		"shuffle.read.push.remoteMergedReqsDuration",
		taskMetrics.ShuffleReadMetrics.SufflePushReadMetrics.RemoteMergedReqsDuration,
		taskMetricAttrs,
		writer,
	)
	writeGauge(
		metricPrefix,
		"shuffle.write.bytesWritten",
		taskMetrics.ShuffleWriteMetrics.BytesWritten,
		taskMetricAttrs,
		writer,
	)
	writeGauge(
		metricPrefix,
		"shuffle.write.writeTime",
		taskMetrics.ShuffleWriteMetrics.WriteTime,
		taskMetricAttrs,
		writer,
	)
	writeGauge(
		metricPrefix,
		"shuffle.write.recordsWritten",
		taskMetrics.ShuffleWriteMetrics.RecordsWritten,
		taskMetricAttrs,
		writer,
	)
	writeGauge(
		metricPrefix,
		"photon.offHeapMinMemorySize",
		taskMetrics.PhotonMemoryMetrics.OffHeapMinMemorySize,
		taskMetricAttrs,
		writer,
	)
	writeGauge(
		metricPrefix,
		"photon.offHeapMaxMemorySize",
		taskMetrics.PhotonMemoryMetrics.OffHeapMaxMemorySize,
		taskMetricAttrs,
		writer,
	)
	writeGauge(
		metricPrefix,
		"photon.photonBufferPoolMinMemorySize",
		taskMetrics.PhotonMemoryMetrics.PhotonBufferPoolMinMemorySize,
		taskMetricAttrs,
		writer,
	)
	writeGauge(
		metricPrefix,
		"photon.photonBufferPoolMaxMemorySize",
		taskMetrics.PhotonMemoryMetrics.PhotonBufferPoolMaxMemorySize,
		taskMetricAttrs,
		writer,
	)
	writeGauge(
		metricPrefix,
		"photon.photonizedTaskTimeNs",
		taskMetrics.PhotonizedTaskTimeNs,
		taskMetricAttrs,
		writer,
	)
}

func collectSparkAppRDDMetrics(
	ctx context.Context,
	client *SparkApiClient,
	sparkApp *SparkApplication,
	metricPrefix string,
	tags map[string]string,
	writer chan <- model.Metric,
) error {
	rdds, err := client.GetApplicationRDDs(ctx, sparkApp)
	if err !=  nil {
		return err
	}

	for _, rdd := range rdds {
		log.Debugf("processing rdd %d", rdd.Id)

		attrs := makeAppAttributesMap(
			sparkApp,
			tags,
		)

		attrs["sparkAppRDDId"] = rdd.Id
		attrs["sparkAppRDDName"] = rdd.Name

		writeGauge(
			metricPrefix,
			"app.storage.rdd.partitions",
			rdd.NumPartitions,
			attrs,
			writer,
		)
		writeGauge(
			metricPrefix,
			"app.storage.rdd.cachedPartitions",
			rdd.NumCachedPartitions,
			attrs,
			writer,
		)
		writeGauge(
			metricPrefix,
			"app.storage.rdd.memory.used",
			rdd.MemoryUsed,
			attrs,
			writer,
		)
		writeGauge(
			metricPrefix,
			"app.storage.rdd.disk.used",
			rdd.DiskUsed,
			attrs,
			writer,
		)

		for index, distribution := range rdd.DataDistribution {
			rddDistributionAttrs := maps.Clone(attrs)

			rddDistributionAttrs["sparkAppRddDistributionIndex"] = index

			writeGauge(
				metricPrefix,
				"app.storage.rdd.distribution.memory.used" ,
				distribution.MemoryUsed,
				rddDistributionAttrs,
				writer,
			)
			writeGauge(
				metricPrefix,
				"app.storage.rdd.distribution.memory.remaining" ,
				distribution.MemoryRemaining,
				rddDistributionAttrs,
				writer,
			)
			writeGauge(
				metricPrefix,
				"app.storage.rdd.distribution.disk.used" ,
				distribution.DiskUsed,
				rddDistributionAttrs,
				writer,
			)
			writeGauge(
				metricPrefix,
				"app.storage.rdd.distribution.memory.usedOnHeap" ,
				distribution.OnHeapMemoryUsed,
				rddDistributionAttrs,
				writer,
			)
			writeGauge(
				metricPrefix,
				"app.storage.rdd.distribution.memory.usedOffHeap" ,
				distribution.OffHeapMemoryUsed,
				rddDistributionAttrs,
				writer,
			)
			writeGauge(
				metricPrefix,
				"app.storage.rdd.distribution.memory.remainingOnHeap" ,
				distribution.OnHeapMemoryRemaining,
				rddDistributionAttrs,
				writer,
			)
			writeGauge(
				metricPrefix,
				"app.storage.rdd.distribution.memory.remainingOffHeap" ,
				distribution.OffHeapMemoryRemaining,
				rddDistributionAttrs,
				writer,
			)
		}

		for _, partition := range rdd.Partitions {
			rddPartitionAttrs := maps.Clone(attrs)

			rddPartitionAttrs["sparkAppRddPartitionBlockName"] =
				partition.BlockName

			writeGauge(
				metricPrefix,
				"app.storage.rdd.partition.memory.used" ,
				partition.MemoryUsed,
				rddPartitionAttrs,
				writer,
			)
			writeGauge(
				metricPrefix,
				"app.storage.rdd.partition.disk.used" ,
				partition.DiskUsed,
				rddPartitionAttrs,
				writer,
			)
		}
	}

	return nil
}

func writeMemoryMetrics(
	metricPrefix string,
	memoryMetrics *SparkExecutorMemoryMetrics,
	attrs map[string]interface{},
	writer chan <- model.Metric,
) {
	writeGauge(
		metricPrefix,
		"usedOnHeapStorage",
		memoryMetrics.UsedOnHeapStorageMemory,
		attrs,
		writer,
	)

	writeGauge(
		metricPrefix,
		"usedOffHeapStorage",
		memoryMetrics.UsedOffHeapStorageMemory,
		attrs,
		writer,
	)

	writeGauge(
		metricPrefix,
		"totalOnHeapStorage",
		memoryMetrics.TotalOnHeapStorageMemory,
		attrs,
		writer,
	)

	writeGauge(
		metricPrefix,
		"totalOffHeapStorage",
		memoryMetrics.TotalOffHeapStorageMemory,
		attrs,
		writer,
	)
}

func writePeakMemoryMetrics(
	metricPrefix string,
	peakMemoryMetrics *SparkExecutorPeakMemoryMetrics,
	attrs map[string]interface{},
	writer chan <- model.Metric,
) {
	writeGauge(
		metricPrefix,
		"jvmHeap",
		peakMemoryMetrics.JVMHeapMemory,
		attrs,
		writer,
	)
	writeGauge(
		metricPrefix,
		"jvmOffHeap",
		peakMemoryMetrics.JVMOffHeapMemory,
		attrs,
		writer,
	)
	writeGauge(
		metricPrefix,
		"onHeapExecution",
		peakMemoryMetrics.OnHeapExecutionMemory,
		attrs,
		writer,
	)
	writeGauge(
		metricPrefix,
		"offHeapExecution",
		peakMemoryMetrics.OffHeapExecutionMemory,
		attrs,
		writer,
	)
	writeGauge(
		metricPrefix,
		"onHeapStorage",
		peakMemoryMetrics.OnHeapStorageMemory,
		attrs,
		writer,
	)
	writeGauge(
		metricPrefix,
		"offHeapStorage",
		peakMemoryMetrics.OffHeapStorageMemory,
		attrs,
		writer,
	)
	writeGauge(
		metricPrefix,
		"onHeapUnified",
		peakMemoryMetrics.OnHeapUnifiedMemory,
		attrs,
		writer,
	)
	writeGauge(
		metricPrefix,
		"offHeapUnified",
		peakMemoryMetrics.OffHeapUnifiedMemory,
		attrs,
		writer,
	)
	writeGauge(
		metricPrefix,
		"directPool",
		peakMemoryMetrics.DirectPoolMemory,
		attrs,
		writer,
	)
	writeGauge(
		metricPrefix,
		"mappedPool",
		peakMemoryMetrics.MappedPoolMemory,
		attrs,
		writer,
	)
	writeGauge(
		metricPrefix,
		"nettyDirect",
		peakMemoryMetrics.NettyDirectMemory,
		attrs,
		writer,
	)
	writeGauge(
		metricPrefix,
		"jvmDirect",
		peakMemoryMetrics.JvmDirectMemory,
		attrs,
		writer,
	)
	writeGauge(
		metricPrefix,
		"sparkDirectMemoryOverLimit",
		peakMemoryMetrics.SparkDirectMemoryOverLimit,
		attrs,
		writer,
	)
	writeGauge(
		metricPrefix,
		"totalOffHeap",
		peakMemoryMetrics.TotalOffHeapMemory,
		attrs,
		writer,
	)
	writeGauge(
		metricPrefix,
		"processTreeJvmVirtual",
		peakMemoryMetrics.ProcessTreeJVMVMemory,
		attrs,
		writer,
	)
	writeGauge(
		metricPrefix,
		"processTreeJvmRSS",
		peakMemoryMetrics.ProcessTreeJVMRSSMemory,
		attrs,
		writer,
	)
	writeGauge(
		metricPrefix,
		"processTreePythonVirtual",
		peakMemoryMetrics.ProcessTreePythonVMemory,
		attrs,
		writer,
	)
	writeGauge(
		metricPrefix,
		"processTreePythonRSS",
		peakMemoryMetrics.ProcessTreePythonRSSMemory,
		attrs,
		writer,
	)
	writeGauge(
		metricPrefix,
		"processTreeOtherVirtual",
		peakMemoryMetrics.ProcessTreeOtherVMemory,
		attrs,
		writer,
	)
	writeGauge(
		metricPrefix,
		"processTreeOtherRSS",
		peakMemoryMetrics.ProcessTreeOtherRSSMemory,
		attrs,
		writer,
	)
	writeGauge(
		metricPrefix,
		"minorGCCount",
		peakMemoryMetrics.MinorGCCount,
		attrs,
		writer,
	)
	writeGauge(
		metricPrefix,
		"minorGCTime",
		peakMemoryMetrics.MinorGCTime,
		attrs,
		writer,
	)
	writeGauge(
		metricPrefix,
		"majorGCCount",
		peakMemoryMetrics.MajorGCCount,
		attrs,
		writer,
	)
	writeGauge(
		metricPrefix,
		"majorGCTime",
		peakMemoryMetrics.MajorGCTime,
		attrs,
		writer,
	)
	writeGauge(
		metricPrefix,
		"totalGCTime",
		peakMemoryMetrics.TotalGCTime,
		attrs,
		writer,
	)
}

func writeGauge(
	prefix string,
	metricName string,
	metricValue any,
	attrs map[string]interface{},
	writer chan <- model.Metric,
) {
	metric := model.NewGaugeMetric(
		prefix + metricName,
		model.MakeNumeric(metricValue),
		time.Now(),
	)

	for k, v := range attrs {
		metric.Attributes[k] = v
	}

	writer <- metric
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
