package spark

import (
	"context"

	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration"
	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/exporters"
	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/log"
	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/pipeline"
	"github.com/spf13/viper"
)

type SparkApplication struct {
	Id       	string `json:"id"`
	Name    	string `json:"name"`
}

type SparkExecutorMemoryMetrics struct {
	UsedOnHeapStorageMemory   	int `json:"usedOnHeapStorageMemory"`
	UsedOffHeapStorageMemory  	int `json:"usedOffHeapStorageMemory"`
	TotalOnHeapStorageMemory  	int `json:"totalOnHeapStorageMemory"`
	TotalOffHeapStorageMemory 	int `json:"totalOffHeapStorageMemory"`
}

type SparkExecutorPeakMemoryMetrics struct {
	JVMHeapMemory              	int	`json:"JVMHeapMemory"`
	JVMOffHeapMemory           	int `json:"JVMOffHeapMemory"`
	OnHeapExecutionMemory      	int `json:"OnHeapExecutionMemory"`
	OffHeapExecutionMemory     	int `json:"OffHeapExecutionMemory"`
	OnHeapStorageMemory        	int `json:"OnHeapStorageMemory"`
	OffHeapStorageMemory       	int `json:"OffHeapStorageMemory"`
	OnHeapUnifiedMemory        	int `json:"OnHeapUnifiedMemory"`
	OffHeapUnifiedMemory       	int `json:"OffHeapUnifiedMemory"`
	DirectPoolMemory           	int `json:"DirectPoolMemory"`
	MappedPoolMemory           	int `json:"MappedPoolMemory"`
	NettyDirectMemory          	int `json:"NettyDirectMemory"`
	JvmDirectMemory            	int `json:"JvmDirectMemory"`
	SparkDirectMemoryOverLimit 	int `json:"SparkDirectMemoryOverLimit"`
	TotalOffHeapMemory         	int `json:"TotalOffHeapMemory"`
	ProcessTreeJVMVMemory      	int `json:"ProcessTreeJVMVMemory"`
	ProcessTreeJVMRSSMemory    	int `json:"ProcessTreeJVMRSSMemory"`
	ProcessTreePythonVMemory   	int `json:"ProcessTreePythonVMemory"`
	ProcessTreePythonRSSMemory 	int `json:"ProcessTreePythonRSSMemory"`
	ProcessTreeOtherVMemory    	int `json:"ProcessTreeOtherVMemory"`
	ProcessTreeOtherRSSMemory  	int `json:"ProcessTreeOtherRSSMemory"`
	MinorGCCount               	int `json:"MinorGCCount"`
	MinorGCTime                	int `json:"MinorGCTime"`
	MajorGCCount               	int `json:"MajorGCCount"`
	MajorGCTime                	int `json:"MajorGCTime"`
	TotalGCTime                	int `json:"TotalGCTime"`
}

type SparkExecutor struct {
	Id                	string 	`json:"id"`
	RddBlocks         	int    	`json:"rddBlocks"`
	MemoryUsed        	int    	`json:"memoryUsed"`
	DiskUsed          	int    	`json:"diskUsed"`
	TotalCores        	int    	`json:"totalCores"`
	MaxTasks          	int    	`json:"maxTasks"`
	ActiveTasks       	int    	`json:"activeTasks"`
	FailedTasks       	int    	`json:"failedTasks"`
	CompletedTasks    	int    	`json:"completedTasks"`
	TotalTasks        	int    	`json:"totalTasks"`
	TotalDuration     	int    	`json:"totalDuration"`
	TotalGCTime       	int    	`json:"totalGCTime"`
	TotalInputBytes   	int    	`json:"totalInputBytes"`
	TotalShuffleRead  	int    	`json:"totalShuffleRead"`
	TotalShuffleWrite 	int    	`json:"totalShuffleWrite"`
	MaxMemory         	int    	`json:"maxMemory"`
	MemoryMetrics 	  	SparkExecutorMemoryMetrics    	`json:"memoryMetrics"`
	PeakMemoryMetrics 	SparkExecutorPeakMemoryMetrics 	`json:"peakMemoryMetrics"`
}

type SparkExecutorSummary struct {
	TaskTime			int		`json:"taskTime"`
	FailedTasks			int		`json:"failedTasks"`
	SucceededTasks		int		`json:"succeededTasks"`
	KilledTasks			int		`json:"killedTasks"`
	InputBytes			int		`json:"inputBytes"`
	InputRecords		int		`json:"inputRecords"`
	OutputBytes			int		`json:"outputBytes"`
	OutputRecords		int		`json:"outputRecords"`
	ShuffleRead			int		`json:"shuffleRead"`
	ShuffleReadRecords	int		`json:"shuffleReadRecords"`
	ShuffleWrite		int		`json:"shuffleWrite"`
	ShuffleWriteRecords	int		`json:"shuffleWriteRecords"`
	MemoryBytesSpilled	int		`json:"memoryBytesSpilled"`
	DiskBytesSpilled	int		`json:"diskBytesSpilled"`
	PeakMemoryMetrics 	SparkExecutorPeakMemoryMetrics 	`json:"peakMemoryMetrics"`
}

type SparkJob struct {
	JobId               int           `json:"jobId"`
	Name                string        `json:"name"`
	SubmissionTime		string		  `json:"submissionTime"`
	CompletionTime		string		  `json:"completionTime"`
	JobGroup            string        `json:"jobGroup"`
	/* status=[running|succeeded|failed|unknown] */
	Status              string        `json:"status"`
	NumTasks            int           `json:"numTasks"`
	NumActiveTasks      int           `json:"numActiveTasks"`
	NumCompletedTasks   int           `json:"numCompletedTasks"`
	NumSkippedTasks     int           `json:"numSkippedTasks"`
	NumFailedTasks      int           `json:"numFailedTasks"`
	NumKilledTasks      int           `json:"numKilledTasks"`
	NumCompletedIndices int           `json:"numCompletedIndices"`
	NumActiveStages     int           `json:"numActiveStages"`
	NumCompletedStages  int           `json:"numCompletedStages"`
	NumSkippedStages    int           `json:"numSkippedStages"`
	NumFailedStages     int           `json:"numFailedStages"`
}

type SparkTaskMetrics struct {
	ExecutorDeserializeTime         int           	`json:"executorDeserializeTime"`
	ExecutorDeserializeCpuTime      int           	`json:"executorDeserializeCpuTime"`
	ExecutorRunTime                 int           	`json:"executorRunTime"`
	ExecutorCpuTime                 int           	`json:"executorCpuTime"`
	ResultSize                      int           	`json:"resultSize"`
	JvmGcTime                       int           	`json:"jvmGcTime"`
	ResultSerializationTime         int           	`json:"resultSerializationTime"`
	MemoryBytesSpilled              int           	`json:"memoryBytesSpilled"`
	DiskBytesSpilled                int           	`json:"diskBytesSpilled"`
	PeakExecutionMemory             int        		`json:"peakExecutionMemory"`
	InputMetrics					struct {
		BytesRead						int			`json:"bytesRead"`
		RecordsRead						int			`json:"recordsRead"`
	} 	`json:"inputMetrics"`
	OutputMetrics					struct {
		BytesWritten					int			`json:"bytesWritten"`
		RecordsWritten					int			`json:"recordsWritten"`
	} 	`json:"outputMetrics"`
	ShuffleReadMetrics				struct {
		RemoteBlocksFetched       		int         `json:"remoteBlocksFetched"`
		LocalBlocksFetched        		int         `json:"localBlocksFetched"`
		FetchWaitTime             		int         `json:"fetchWaitTime"`
		RemoteBytesRead           		int         `json:"remoteBytesRead"`
		RemoteBytesReadToDisk     		int         `json:"remoteBytesReadToDisk"`
		LocalBytesRead            		int         `json:"localBytesRead"`
		RecordsRead						int			`json:"recordsRead"`
		RemoteReqsDuration        		int         `json:"remoteReqsDuration"`
		SufflePushReadMetrics		struct {
			CorruptMergedBlockChunks	int			`json:"corruptMergedBlockChunks"`
			MergedFetchFallbackCount	int			`json:"mergedFetchFallbackCount"`
			RemoteMergedBlocksFetched	int			`json:"remoteMergedBlocksFetched"`
			LocalMergedBlocksFetched	int			`json:"localMergedBlocksFetched"`
			RemoteMergedChunksFetched	int			`json:"remoteMergedChunksFetched"`
			LocalMergedChunksFetched	int			`json:"localMergedChunksFetched"`
			RemoteMergedBytesRead		int			`json:"remoteMergedBytesRead"`
			LocalMergedBytesRead		int			`json:"localMergedBytesRead"`
			RemoteMergedReqsDuration	int			`json:"remoteMergedReqsDuration"`
		}	`json:"shufflePushReadMetrics"`
	}	`json:"shuffleReadMetrics"`
	ShuffleWriteMetrics				struct {
		BytesWritten					int			`json:"bytesWritten"`
		WriteTime						int			`json:"writeTime"`
		RecordsWritten					int			`json:"recordsWritten"`
	}	`json:"shuffleWriteMetrics"`
	PhotonMemoryMetrics				struct {
		OffHeapMinMemorySize			int			`json:"offHeapMinMemorySize"`
		OffHeapMaxMemorySize			int			`json:"offHeapMaxMemorySize"`
		PhotonBufferPoolMinMemorySize	int			`json:"photonBufferPoolMinMemorySize"`
		PhotonBufferPoolMaxMemorySize	int			`json:"photonBufferPoolMaxMemorySize"`
	}	`json:"photonMemoryMetrics"`
	PhotonizedTaskTimeNs				int			`json:"photonizedTaskTimeNs"`
}

type SparkTask struct {
	TaskId						int					`json:"taskId"`
	Index						int					`json:"index"`
	Attempt						int					`json:"attempt"`
	PartitionId					int					`json:"partitionId"`
	LaunchTime					string				`json:"launchTime"`
	Duration					int					`json:"duration"`
	ExecutorId					string				`json:"executorId"`
	/*
	taskStatus=[RUNNING|SUCCESS|FAILED|KILLED|PENDING]
	*/
	Status						string				`json:"status"`
	TaskLocality				string				`json:"taskLocality"`
	Speculative					bool				`json:"speculative"`
	TaskMetrics					SparkTaskMetrics	`json:"taskMetrics"`
	SchedulerDelay				int					`json:"schedulerDelay"`
	GettingResultTime			int					`json:"gettingResultTime"`
}

type SparkStage struct {
	/*
	enum StageStatus {
		STAGE_STATUS_UNSPECIFIED = 0;
		STAGE_STATUS_ACTIVE = 1;
		STAGE_STATUS_COMPLETE = 2;
		STAGE_STATUS_FAILED = 3;
		STAGE_STATUS_PENDING = 4;
		STAGE_STATUS_SKIPPED = 5;
	}
	*/
	Status                           	string        	`json:"status"`
	StageId                          	int           	`json:"stageId"`
	AttemptId                        	int           	`json:"attemptId"`
	NumTasks                         	int           	`json:"numTasks"`
	NumActiveTasks                   	int           	`json:"numActiveTasks"`
	NumCompleteTasks                 	int           	`json:"numCompleteTasks"`
	NumFailedTasks                   	int           	`json:"numFailedTasks"`
	NumKilledTasks                   	int           	`json:"numKilledTasks"`
	NumCompletedIndices              	int           	`json:"numCompletedIndices"`
	PeakNettyDirectMemory            	int           	`json:"peakNettyDirectMemory"`
	PeakJvmDirectMemory              	int           	`json:"peakJvmDirectMemory"`
	PeakSparkDirectMemoryOverLimit   	int           	`json:"peakSparkDirectMemoryOverLimit"`
	PeakTotalOffHeapMemory           	int           	`json:"peakTotalOffHeapMemory"`
	SubmissionTime                   	string        	`json:"submissionTime"`
	FirstTaskLaunchedTime            	string        	`json:"firstTaskLaunchedTime"`
	CompletionTime                   	string        	`json:"completionTime"`
	ExecutorDeserializeTime          	int           	`json:"executorDeserializeTime"`
	ExecutorDeserializeCpuTime       	int           	`json:"executorDeserializeCpuTime"`
	ExecutorRunTime                  	int           	`json:"executorRunTime"`
	ExecutorCpuTime                  	int           	`json:"executorCpuTime"`
	ResultSize                       	int           	`json:"resultSize"`
	JvmGcTime                        	int           	`json:"jvmGcTime"`
	ResultSerializationTime          	int           	`json:"resultSerializationTime"`
	MemoryBytesSpilled               	int           	`json:"memoryBytesSpilled"`
	DiskBytesSpilled                 	int           	`json:"diskBytesSpilled"`
	PeakExecutionMemory              	int           	`json:"peakExecutionMemory"`
	InputBytes                       	int           	`json:"inputBytes"`
	InputRecords                     	int           	`json:"inputRecords"`
	OutputBytes                      	int           	`json:"outputBytes"`
	OutputRecords                    	int           	`json:"outputRecords"`
	ShuffleRemoteBlocksFetched       	int           	`json:"shuffleRemoteBlocksFetched"`
	ShuffleLocalBlocksFetched        	int           	`json:"shuffleLocalBlocksFetched"`
	ShuffleFetchWaitTime             	int           	`json:"shuffleFetchWaitTime"`
	ShuffleRemoteBytesRead           	int           	`json:"shuffleRemoteBytesRead"`
	ShuffleRemoteBytesReadToDisk     	int           	`json:"shuffleRemoteBytesReadToDisk"`
	ShuffleLocalBytesRead            	int           	`json:"shuffleLocalBytesRead"`
	ShuffleReadBytes                 	int           	`json:"shuffleReadBytes"`
	ShuffleReadRecords               	int           	`json:"shuffleReadRecords"`
	ShuffleCorruptMergedBlockChunks  	int           	`json:"shuffleCorruptMergedBlockChunks"`
	ShuffleMergedFetchFallbackCount  	int           	`json:"shuffleMergedFetchFallbackCount"`
	ShuffleMergedRemoteBlocksFetched 	int           	`json:"shuffleMergedRemoteBlocksFetched"`
	ShuffleMergedLocalBlocksFetched  	int           	`json:"shuffleMergedLocalBlocksFetched"`
	ShuffleMergedRemoteChunksFetched 	int           	`json:"shuffleMergedRemoteChunksFetched"`
	ShuffleMergedLocalChunksFetched  	int           	`json:"shuffleMergedLocalChunksFetched"`
	ShuffleMergedRemoteBytesRead     	int           	`json:"shuffleMergedRemoteBytesRead"`
	ShuffleMergedLocalBytesRead      	int           	`json:"shuffleMergedLocalBytesRead"`
	ShuffleRemoteReqsDuration        	int           	`json:"shuffleRemoteReqsDuration"`
	ShuffleMergedRemoteReqsDuration  	int           	`json:"shuffleMergedRemoteReqsDuration"`
	ShuffleWriteBytes                	int           	`json:"shuffleWriteBytes"`
	ShuffleWriteTime                 	int           	`json:"shuffleWriteTime"`
	ShuffleWriteRecords              	int           	`json:"shuffleWriteRecords"`
	Name                             	string        	`json:"name"`
	SchedulingPool                   	string        	`json:"schedulingPool"`
	Tasks								map[string]SparkTask `json:"tasks"`
	ExecutorSummary						map[string]SparkExecutorSummary `json:"executorSummary"`
	ResourceProfileId   				int 			`json:"resourceProfileId"`
	PeakExecutorMetrics SparkExecutorPeakMemoryMetrics 	`json:"peakExecutorMetrics"`
	ShuffleMergersCount  				int  			`json:"shuffleMergersCount"`
}

type SparkRDD struct {
	Id 						int						`json:"id"`
	Name					string					`json:"name"`
	NumPartitions			int						`json:"numPartitions"`
	NumCachedPartitions		int						`json:"numCachedPartitions"`
	MemoryUsed				int						`json:"memoryUsed"`
	DiskUsed				int						`json:"diskUsed"`
	DataDistribution		[]struct {
		MemoryUsed				int					`json:"memoryUsed"`
		MemoryRemaining			int					`json:"memoryRemaining"`
		DiskUsed				int					`json:"diskUsed"`
		OnHeapMemoryUsed		int					`json:"onHeapMemoryUsed"`
		OffHeapMemoryUsed		int					`json:"offHeapMemoryUsed"`
		OnHeapMemoryRemaining	int					`json:"onHeapMemoryRemaining"`
		OffHeapMemoryRemaining	int					`json:"offHeapMemoryRemaining"`
	} `json:"dataDistribution"`
	Partitions				[]struct {
		BlockName				string				`json:"blockName"`
		MemoryUsed				int					`json:"memoryUsed"`
		DiskUsed				int					`json:"diskUsed"`
		Executors				[]string			`json:"executors"`
	} `json:"partitions"`
}

type SparkStreamingStatistics struct {
	StartTime					string			`json:"startTime"`
	BatchDuration				int				`json:"batchDuration"`
	NumReceivers				int				`json:"numReceivers"`
	NumActiveReceivers			int				`json:"numActiveReceivers"`
	NumInactiveReceivers		int				`json:"numInactiveReceivers"`
	NumTotalCompletedBatches	int				`json:"numTotalCompletedBatches"`
	NumRetainedCompletedBatches	int				`json:"numRetainedCompletedBatches"`
	NumActiveBatches			int				`json:"numActiveBatches"`
	NumProcessedRecords			int				`json:"numProcessedRecords"`
	NumReceivedRecords			int				`json:"numReceivedRecords"`
	AvgInputRate				float64			`json:"avgInputRate"`
	AvgSchedulingDelay			float64			`json:"avgSchedulingDelay"`
	AvgProcessingTime			float64			`json:"avgProcessingTime"`
	AvgTotalDelay				float64			`json:"avgTotalDelay"`
}

type SparkStreamingReceiver struct {
	StreamId					int				`json:"streamId"`
	StreamName					string			`json:"streamName"`
	ExecutorId					string			`json:"executorId"`
	ExecutorHost				string			`json:"executorHost"`
	AvgEventRate				float64			`json:"avgEventRate"`
}

func InitPipelines(
	ctx context.Context,
	i *integration.LabsIntegration,
	tags map[string]string,
) error {
	// Get the web UI URL
	webUiUrl := viper.GetString("spark.webUiUrl")
	if webUiUrl == "" {
		// default to localhost:4040
		webUiUrl = "http://localhost:4040"
	}

	// @TODO: support authentication?

	sparkApiClient := NewSparkApiClient(
		webUiUrl,
		nil,
	)

	// Initialize spark pipelines
	log.Debugf("initializing Spark pipeline with spark web UI URL %s", webUiUrl)

	// Create the newrelic exporter
	newRelicExporter := exporters.NewNewRelicExporter(
		"newrelic-api",
		i.Name,
		i.Id,
		i.NrClient,
		i.GetLicenseKey(),
		i.GetRegion(),
		i.DryRun,
	)

	// Create a metrics pipeline
	mp := pipeline.NewMetricsPipeline("spark-metrics-pipeline")
	mp.AddExporter(newRelicExporter)

	// Create the Spark receiver
	sparkReceiver := NewSparkMetricsReceiver(
		i,
		sparkApiClient,
		viper.GetString("spark.metricPrefix"),
		tags,
	)

	mp.AddReceiver(sparkReceiver)

	i.AddComponent(mp)

	return nil
}
