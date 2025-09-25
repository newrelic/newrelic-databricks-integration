package databricks

import (
	"context"
	"time"

	databricksSdkSql "github.com/databricks/databricks-sdk-go/service/sql"
	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration"
	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/log"
	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/model"
)

const (
	DATABRICKS_QUERY_EVENT_TYPE = "DatabricksQuery"
	DEFAULT_QUERY_HISTORY_START_OFFSET = 10 * 60
	DEFAULT_QUERY_HISTORY_INTERVAL_OFFSET = 5
	DEFAULT_QUERY_HISTORY_MAX_RESULTS = 100
)

type DatabricksQueryMetricsReceiver struct {
	i							*integration.LabsIntegration
	w							DatabricksWorkspace
	startOffset					time.Duration
	intervalOffset				time.Duration
	maxResults					int
	includeIdentityMetadata		bool
	tags 						map[string]string
	lastRun						time.Time
}

func NewDatabricksQueryMetricsReceiver(
	i *integration.LabsIntegration,
	w DatabricksWorkspace,
	startOffset time.Duration,
	intervalOffset time.Duration,
	maxResults int,
	includeIdentityMetadata bool,
	tags map[string]string,
) (*DatabricksQueryMetricsReceiver, error) {
	return &DatabricksQueryMetricsReceiver{
		i,
		w,
		startOffset,
		intervalOffset,
		maxResults,
		includeIdentityMetadata,
		tags,
		// lastRun is initialized to now. Note that because of this, the window
		// used on the first run will always be 0 or very close to 0 so it is
		// very unlikely that queries will be picked up on the first run. This
		// is fine because we would only pick up events in the past and
		// generally integrations don't pick up past data. However, on the
		// second run, because of the intervalOffset, it is possible to pickup a
		// query that ended in the intervalOffset seconds before the integration
		// started but this shouldn't be a problem.
		time.Now(),
	}, nil
}

func (d *DatabricksQueryMetricsReceiver) GetId() string {
	return "databricks-query-metrics-receiver"
}

func (d *DatabricksQueryMetricsReceiver) PollEvents(
	ctx context.Context,
	writer chan <- model.Event,
) error {
	lastRun := d.lastRun
	now := time.Now()

	// This only works when the integration is run as a standalone application
	// that stays running since it requires state to be maintained. However,
	// this is the only supported way to run the integration. If this ever
	// changes, this approach to saving the last run time needs to be reviewed.
	// Note that on the first run, the lastRun and d.lastRun will be the same.
	d.lastRun = now

	log.Debugf("listing queries...")
	defer log.Debugf("done listing queries")

	nextPageToken := ""
	done := false

	// The query history API returns queries sorted in descending order by
	// _start time_. This means that unless the time range used on the API
	// call includes the time when a query started, it will not be returned.
	// Were the integration to only request the query history since the last
	// execution of the integration, telemetry for queries that started prior to
	// the last execution but completed since the last execution of the
	// integration (since we only record queries when they complete) would never
	// be recorded. To account for this, we offset the start time of the API
	// call by the startOffset. This allows us to process queries that started
	// prior to the last run but completed since the last run.
	startTimeMs := now.Add(-d.startOffset).UnixMilli()

	// We only record queries when they terminate. Furthermore, we only record
	// queries that terminated since the last run of the integration so that we
	// only record queries once. To account for any lag in the setting of the
	// query end time, we offset the window used for the termination check from
	// [lastRun, now) to [lastRun-intervalOffset, now-intervalOffset). This
	// handles the case where a query terminated just prior to the end of one
	// run of the integration but the end time is not updated until the start
	// of the next run of the integration. If we used [lastRun, now) as the
	// window, such queries would never be recorded. By sliding the window by
	// intervalOffset, we give Databricks extra time to set the end time field
	// of the query.
	effectiveLastRunMs := lastRun.Add(-d.intervalOffset).UnixMilli()
	endTimeMs := now.Add(-d.intervalOffset).UnixMilli()

	for ;!done; {
		queries, hasNpt, npt, err := d.w.ListQueries(
			ctx,
			d.maxResults,
			startTimeMs,
			endTimeMs,
			nextPageToken,
		)
		if err != nil {
			return err
		}

		for _, query := range queries {
			// Note that the values passed for startWindow (effectiveLastRunMs)
			// and endWindow (endTimeMs) will evaluate to the same thing (or
			// within milliseconds) on the first run it is very unlikely for
			// anything to be captured on the first run.
			processQuery(
				ctx,
				d.w,
				&query,
				d.includeIdentityMetadata,
				effectiveLastRunMs,
				endTimeMs,
				d.tags,
				writer,
			)
		}

		if hasNpt {
			nextPageToken = npt
		} else {
			done = true
		}
	}

	return nil
}

func processQuery(
	ctx context.Context,
	w DatabricksWorkspace,
	query *databricksSdkSql.QueryInfo,
	includeIdentityMetadata bool,
	windowStartMs int64,
	windowEndMs int64,
	tags map[string]string,
	writer chan <- model.Event,
) {
	if log.IsDebugEnabled() {
		log.Debugf(
			"processing query with ID %s of type %s on warehouse ID %s with status %s",
			query.QueryId,
			query.StatementType,
			query.WarehouseId,
			query.Status,
		)
		defer log.Debugf(
			"done processing query with ID %s of type %s on warehouse ID %s with status %s",
			query.QueryId,
			query.StatementType,
			query.WarehouseId,
			query.Status,
		)
	}

	status := query.Status

	if status != databricksSdkSql.QueryStatusCanceled &&
		status != databricksSdkSql.QueryStatusFailed &&
		status != databricksSdkSql.QueryStatusFinished {
		// This query has not terminated yet so we won't report on it.
		log.Debugf(
			"query with ID %s of type %s on warehouse ID %s with status %s has not terminated, ignoring",
			query.QueryId,
			query.StatementType,
			query.WarehouseId,
			query.Status,
		)
		return
	}

	queryEndMs := query.QueryEndTimeMs

	// Here we check if the query ended within the current time window. The
	// passed window start time is the last run time minus the interval offset.
	// The passed window end time is the current time minus the interval offset.
	// By checking if the query ended within the current time window, we ensure
	// that we only report on terminated queries once because a query can't
	// end within multiple windows.
	if windowStartMs <= queryEndMs && queryEndMs < windowEndMs {
		// This query finished/failed/canceled in the current time window we
		// are considering so report on it.

		writer <- model.Event{
			Type: DATABRICKS_QUERY_EVENT_TYPE,
			Timestamp: time.Now().UnixMilli(),
			Attributes: makeQueryEventAttrs(
				ctx,
				w,
				query,
				includeIdentityMetadata,
				tags,
			),
		}

		return
	}

	// This query finished/failed/canceled terminated outside the current time
	// window we are considering and it can be ignored. This way we only report
	// on terminated updates once.
	if log.IsDebugEnabled() {
		log.Debugf(
			"query with ID %s of type %s on warehouse ID %s terminated outside the current window [%s, %s), ignoring it",
			query.QueryId,
			query.StatementType,
			query.WarehouseId,
			time.UnixMilli(windowStartMs).UTC().Format(RFC_3339_MILLI_LAYOUT),
			time.UnixMilli(windowEndMs).UTC().Format(RFC_3339_MILLI_LAYOUT),
		)
	}
}

func makeQueryEventAttrs(
	ctx context.Context,
	w DatabricksWorkspace,
	query *databricksSdkSql.QueryInfo,
	includeIdentityMetadata bool,
	tags map[string]string,
) map[string]interface{} {
	attrs := makeAttributesMap(tags)

	attrs["id"] = query.QueryId
	attrs["type"] = query.StatementType
	attrs["status"] = query.Status
	attrs["warehouseId"] = query.WarehouseId

	warehouseInfo, err := GetWarehouseInfoById(ctx, w, query.WarehouseId)
	if err != nil {
		log.Warnf(
			"could not resolve warehouse ID %s to warehouse info while processing result for query %s: %s",
			query.WarehouseId,
			query.QueryId,
			err,
		)
	} else if warehouseInfo == nil {
		log.Warnf(
			"could not resolve warehouse ID %s to warehouse info while processing result for query %s: warehouse ID not found",
			query.WarehouseId,
			query.QueryId,
		)
	} else {
		// @TODO remove else here by adding a function to decorate tags
		// directly with an early return if err != nil. Maybe in cache.go after
		// it is refactored?
		attrs["warehouseName"] = warehouseInfo.Name
		if includeIdentityMetadata {
			attrs["warehouseCreator"] = warehouseInfo.Creator
		}
	}

	workspaceInfo, err := GetWorkspaceInfo(ctx, w)
	if err != nil {
		log.Warnf(
			"could not resolve workspace info while processing result for query %s: %s",
			query.QueryId,
			err,
		)
	} else {
		// @TODO remove else here by adding a function to decorate tags
		// directly with an early return if err != nil. Maybe in cache.go after
		// it is refactored?
		attrs["workspaceId"] = workspaceInfo.Id
		attrs["workspaceName"] = workspaceInfo.InstanceName
		attrs["workspaceUrl"] = workspaceInfo.Url
	}

	attrs["duration"] = query.Duration

	// We truncate the query text to a maximum of 4096 characters since New
	// Relic event attributes have a maximum length of 4096 characters.
	attrs["query"] = query.QueryText[0:min(len(query.QueryText), 4096)]

	attrs["plansState"] = query.PlansState

	queryFailed := query.Status == databricksSdkSql.QueryStatusFailed

	attrs["error"] = queryFailed
	if queryFailed {
		attrs["errorMessage"] = query.ErrorMessage
	}

	if includeIdentityMetadata {
		attrs["userId"] = query.UserId
		attrs["userName"] = query.UserName
		attrs["runAsUserId"] = query.ExecutedAsUserId
		attrs["runAsUserName"] = query.ExecutedAsUserName
	}

	attrs["executionEndTime"] = query.ExecutionEndTimeMs
	attrs["startTime"] = query.QueryStartTimeMs
	attrs["endTime"] = query.QueryEndTimeMs
	attrs["final"] = query.IsFinal

	metrics := query.Metrics

	attrs["compilationTime"] = metrics.CompilationTimeMs
	attrs["executionTime"] = metrics.ExecutionTimeMs
	attrs["fetchTime"] = metrics.ResultFetchTimeMs
	attrs["totalTime"] = metrics.TotalTimeMs
	attrs["totalTaskTime"] = metrics.TaskTotalTimeMs
	attrs["totalPhotonTime"] = metrics.PhotonTotalTimeMs
	attrs["overloadingQueueStartTime"] = metrics.OverloadingQueueStartTimestamp
	attrs["provisioningQueueStartTime"] =
		metrics.ProvisioningQueueStartTimestamp
	attrs["compilationStartTime"] = metrics.QueryCompilationStartTimestamp
	attrs["fromCache"] = metrics.ResultFromCache

	attrs["bytesRead"] = metrics.ReadBytes
	attrs["cacheBytesRead"] = metrics.ReadCacheBytes
	attrs["filesRead"] = metrics.ReadFilesCount
	attrs["partitionsRead"] = metrics.ReadPartitionsCount
	attrs["remoteBytesRead"] = metrics.ReadRemoteBytes
	attrs["rowsRead"] = metrics.RowsReadCount
	attrs["rowsReturned"] = metrics.RowsProducedCount
	attrs["bytesPruned"] = metrics.PrunedBytes
	attrs["filesPruned"] = metrics.PrunedFilesCount
	attrs["remoteBytesWritten"] = metrics.WriteRemoteBytes
	attrs["diskBytesSpilled"] = metrics.SpillToDiskBytes
	attrs["networkBytesSent"] = metrics.NetworkSentBytes

	return attrs
}
