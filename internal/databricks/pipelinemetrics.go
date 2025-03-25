package databricks

import (
	"context"
	"fmt"
	"maps"
	"net/http"
	"strings"
	"time"

	databricksSdk "github.com/databricks/databricks-sdk-go"
	databricksSdkClient "github.com/databricks/databricks-sdk-go/client"
	"github.com/databricks/databricks-sdk-go/listing"
	"github.com/databricks/databricks-sdk-go/marshal"
	databricksSdkPipelines "github.com/databricks/databricks-sdk-go/service/pipelines"
	"github.com/databricks/databricks-sdk-go/useragent"
	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration"
	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/log"
	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/model"
)

type pipelineEventClient struct {
	client *databricksSdkClient.DatabricksClient
}

func newPipelineEventClient(w *databricksSdk.WorkspaceClient) (*pipelineEventClient, error) {
	cfg := w.Config

	// The following 10 lines are taken from the WorkspaceClient code at
	// https://github.com/databricks/databricks-sdk-go/blob/v0.60.0/workspace_client.go#L1161
	// We do it ourselves here since we don't have access to either the
	// apiClient or databricksClient on the WorkspaceClient.
	apiClient, err := cfg.NewApiClient()
	if err != nil {
		return nil, err
	}

	databricksClient, err := databricksSdkClient.NewWithClient(cfg, apiClient)
	if err != nil {
		return nil, err
	}

	return &pipelineEventClient{
		databricksClient,
	}, nil
}

type FlowProgressMetrics struct {
	BacklogBytes *float64 `json:"backlog_bytes,omitempty"`
	BacklogFiles *float64 `json:"backlog_files,omitempty"`
	NumOutputRows *float64 `json:"num_output_rows,omitempty"`
}

type FlowProgressExpectations struct {
	Name string `json:"name,omitempty"`
	Dataset string `json:"dataset,omitempty"`
	PassedRecords float64 `json:"passed_records,omitempty"`
	FailedRecords float64 `json:"failed_records,omitempty"`
}

type FlowProgressDataQuality struct {
	DroppedRecords *float64 `json:"dropped_records,omitempty"`
	Expectations []FlowProgressExpectations `json:"expectations,omitempty"`
}

type FlowProgressDetails struct {
	Status string `json:"status,omitempty"`
	Metrics *FlowProgressMetrics `json:"metrics,omitempty"`
	DataQuality *FlowProgressDataQuality `json:"data_quality,omitempty"`
}

type UpdateProgressDetails struct {
	State databricksSdkPipelines.UpdateInfoState `json:"state,omitempty"`
}

type PipelineEventDetails struct {
	FlowProgress *FlowProgressDetails `json:"flow_progress,omitempty"`
	UpdateProgress *UpdateProgressDetails `json:"update_progress,omitempty"`
}

// Below copied from
// https://github.com/databricks/databricks-sdk-go/blob/v0.60.0/service/pipelines/model.go#L1021
// in order to support pipeline events with details field until SDK supports it

type PipelineEvent struct {
	// Information about an error captured by the event.
	Error *databricksSdkPipelines.ErrorDetail `json:"error,omitempty"`
	// The event type. Should always correspond to the details
	EventType string `json:"event_type,omitempty"`
	// A time-based, globally unique id.
	Id string `json:"id,omitempty"`
	// The severity level of the event.
	Level databricksSdkPipelines.EventLevel `json:"level,omitempty"`
	// Maturity level for event_type.
	MaturityLevel databricksSdkPipelines.MaturityLevel `json:"maturity_level,omitempty"`
	// The display message associated with the event.
	Message string `json:"message,omitempty"`
	// Describes where the event originates from.
	Origin *databricksSdkPipelines.Origin `json:"origin,omitempty"`
	// A sequencing object to identify and order events.
	Sequence *databricksSdkPipelines.Sequencing `json:"sequence,omitempty"`
	// The time of the event.
	Timestamp string `json:"timestamp,omitempty"`

	// The details of the event.
	// This is the field we add that is not supported in the SDK but is part
	// of the ReST API.
	Details *PipelineEventDetails `json:"details,omitempty"`

	ForceSendFields []string `json:"-" url:"-"`
}

// Below copied from
// https://github.com/databricks/databricks-sdk-go/blob/v0.60.0/service/pipelines/model.go#L582
// in order to support pipeline events with details field until SDK supports it

type ListPipelineEventsResponse struct {
	// The list of events matching the request criteria.
	Events []PipelineEvent `json:"events,omitempty"`
	// If present, a token to fetch the next page of events.
	NextPageToken string `json:"next_page_token,omitempty"`
	// If present, a token to fetch the previous page of events.
	PrevPageToken string `json:"prev_page_token,omitempty"`

	ForceSendFields []string `json:"-" url:"-"`
}

func (s *ListPipelineEventsResponse) UnmarshalJSON(b []byte) error {
	return marshal.Unmarshal(b, s)
}

func (s ListPipelineEventsResponse) MarshalJSON() ([]byte, error) {
	return marshal.Marshal(s)
}

// Below copied from
// https://github.com/databricks/databricks-sdk-go/blob/v0.60.0/service/pipelines/impl.go#L117
// in order to support pipeline events with details field until SDK supports it

func (a *pipelineEventClient) internalListPipelineEvents(ctx context.Context, request databricksSdkPipelines.ListPipelineEventsRequest) (*ListPipelineEventsResponse, error) {
	var listPipelineEventsResponse ListPipelineEventsResponse
	path := fmt.Sprintf("/api/2.0/pipelines/%v/events", request.PipelineId)
	queryParams := make(map[string]any)
	headers := make(map[string]string)
	headers["Accept"] = "application/json"
	err := a.client.Do(ctx, http.MethodGet, path, headers, queryParams, request, &listPipelineEventsResponse)
	return &listPipelineEventsResponse, err
}

// Below temporarily copied from
// https://github.com/databricks/databricks-sdk-go/blob/v0.60.0/service/pipelines/impl.go#L84
// in order to support pipeline events with details field until SDK supports it

func (a *pipelineEventClient) listPipelineEvents(_ context.Context, request databricksSdkPipelines.ListPipelineEventsRequest) listing.Iterator[PipelineEvent] {

	getNextPage := func(ctx context.Context, req databricksSdkPipelines.ListPipelineEventsRequest) (*ListPipelineEventsResponse, error) {
		ctx = useragent.InContext(ctx, "sdk-feature", "pagination")
		return a.internalListPipelineEvents(ctx, req)
	}
	getItems := func(resp *ListPipelineEventsResponse) []PipelineEvent {
		return resp.Events
	}
	getNextReq := func(resp *ListPipelineEventsResponse) *databricksSdkPipelines.ListPipelineEventsRequest {
		if resp.NextPageToken == "" {
			return nil
		}
		request.PageToken = resp.NextPageToken
		return &request
	}
	iterator := listing.NewIterator(
		&request,
		getNextPage,
		getItems,
		getNextReq)
	return iterator
}

type Set[T comparable] map[T]struct{}

func (s Set[T]) IsMember(u T) bool {
	_, ok := s[u]
	return ok
}

func (s Set[T]) Add(u T) {
	s[u] = struct{}{}
}

type eventData struct {
	pipelineId string
	pipelineName string
	clusterId string
	updateId string
}

const (
	FlowInfoStatusCompleted = "COMPLETED"
	FlowInfoStatusExcluded = "EXCLUDED"
	FlowInfoStatusFailed = "FAILED"
	FlowInfoStatusIdle = "IDLE"
	FlowInfoStatusPlanning = "PLANNING"
	FlowInfoStatusQueued = "QUEUED"
	FlowInfoStatusRunning = "RUNNING"
	FlowInfoStatusSkipped = "SKIPPED"
	FlowInfoStatusStarting = "STARTING"
	FlowInfoStatusStopped = "STOPPED"
)

func isFlowTerminated(status string) bool {
	// We assume the status has already been uppercased. Since this is an
	// internal function and we guarantee this in the caller, this is OK.
	return status == FlowInfoStatusCompleted ||
		status == FlowInfoStatusStopped ||
		status == FlowInfoStatusSkipped ||
		status == FlowInfoStatusFailed ||
		status == FlowInfoStatusExcluded
}

type flowData struct {
	eventData

	id string
	name string
	queuedTime time.Time
	planningTime time.Time
	startTime time.Time
	completionTime time.Time
	status string
	backlogBytes *float64
	backlogFiles *float64
	numOutputRows *float64
	droppedRecords *float64
	expectations []FlowProgressExpectations
}

func newFlowData(origin *databricksSdkPipelines.Origin) *flowData {
	return &flowData{
		eventData: eventData{
			pipelineId: origin.PipelineId,
			pipelineName: origin.PipelineName,
			clusterId: origin.ClusterId,
			updateId: origin.UpdateId,
		},
		id: origin.FlowId,
		name: origin.FlowName,
	}
}

func getOrCreateFlowData(
	flows map[string]*flowData,
	flowProgress *FlowProgressDetails,
	origin *databricksSdkPipelines.Origin,
	completionTime time.Time,
) *flowData {
	// OK to use flow name here instead of flow ID because flow names are unique
	// within a pipeline.
	// see: https://docs.databricks.com/aws/en/delta-live-tables/flows
	// Also not all flow_process events have an ID in the origin!
	flow, ok := flows[origin.FlowName]
	if !ok {
		flow = newFlowData(origin)
	}

	// Only look for the additional metrics on the log event if the flow has
	// already terminated. This way we don't record these metrics more than
	// once and we can use normal aggregation functions to do things like
	// calculate average backlogBytes. This logic depends on seeing the flow
	// termination event first which is technically guaranteed by the API (events
	// are returned in descending order by timestamp). Note that we have to
	// check the passed completionTime in addition to the completionTime on the
	// flow because some of the metrics are on the COMPLETED log event and the
	// completionTime on the flow is not set until after this function is
	// called.
	if !flow.completionTime.IsZero() || !completionTime.IsZero() {
		processFlowData(flowProgress, flow)
	}

	return flow
}

func processFlowData(
	flowProgress *FlowProgressDetails,
	flow *flowData,
) {
	if flowProgress.Metrics != nil {
		if flowProgress.Metrics.BacklogBytes != nil {
			flow.backlogBytes = flowProgress.Metrics.BacklogBytes
		}
		if flowProgress.Metrics.BacklogFiles != nil {
			flow.backlogFiles = flowProgress.Metrics.BacklogFiles
		}
		if flowProgress.Metrics.NumOutputRows != nil {
			flow.numOutputRows = flowProgress.Metrics.NumOutputRows
		}
	}

	if flowProgress.DataQuality != nil {
		if flowProgress.DataQuality.DroppedRecords != nil {
			flow.droppedRecords = flowProgress.DataQuality.DroppedRecords
		}
		flow.expectations = flowProgress.DataQuality.Expectations
	}
}

func isUpdateTerminated(state databricksSdkPipelines.UpdateInfoState) bool {
	return state == databricksSdkPipelines.UpdateInfoStateCompleted ||
		state == databricksSdkPipelines.UpdateInfoStateCanceled ||
		state == databricksSdkPipelines.UpdateInfoStateFailed
}

type updateData struct {
	eventData

	startTime time.Time
	waitingTime time.Time
	startRunTime time.Time
	completionTime time.Time
	state databricksSdkPipelines.UpdateInfoState
}

func newUpdateData(origin *databricksSdkPipelines.Origin) *updateData {
	return &updateData{
		eventData: eventData{
			pipelineId: origin.PipelineId,
			pipelineName: origin.PipelineName,
			clusterId: origin.ClusterId,
			updateId: origin.UpdateId,
		},
	}
}

func getOrCreateUpdateData(
	updates map[string]*updateData,
	origin *databricksSdkPipelines.Origin,
) *updateData {
	update, ok := updates[origin.UpdateId]
	if ok {
		return update
	}

	return newUpdateData(origin)
}

// see https://github.com/databricks/databricks-sdk-go/blob/v0.52.0/service/pipelines/model.go#L1182
type pipelineCounters struct {
	deleted 		int64
	deploying		int64
	failed			int64
	idle			int64
	recovering		int64
	resetting		int64
	running 		int64
	starting 		int64
	stopping 		int64
}

// see https://github.com/databricks/databricks-sdk-go/blob/v0.52.0/service/pipelines/model.go#L1715
type updateCounters struct {
	canceled				int64
	completed				int64
	created					int64
	failed					int64
	initializing			int64
	queued					int64
	resetting				int64
	running					int64
	settingUpTables			int64
	stopping				int64
	waitingForResources		int64
}

// determined valid states by looking at pipeline log flow_progress events and
// looking at the status dropdown in the list view for a pipeline in the
// Pipelines UI
type flowCounters struct {
	completed		int64
	excluded		int64
	failed			int64
	idle			int64
	planning		int64
	queued			int64
	running			int64
	skipped			int64
	starting		int64
	stopped			int64
}

type DatabricksPipelineMetricsReceiver struct {
	i							*integration.LabsIntegration
	w							*databricksSdk.WorkspaceClient
	c							*pipelineEventClient
	metricPrefix				string
	startOffset					time.Duration
	intervalOffset				time.Duration
	includeUpdateId				bool
	tags 						map[string]string
	lastRun						time.Time
}

func NewDatabricksPipelineMetricsReceiver(
	i *integration.LabsIntegration,
	w *databricksSdk.WorkspaceClient,
	metricPrefix string,
	startOffset time.Duration,
	intervalOffset time.Duration,
	includeUpdateId bool,
	tags map[string]string,
) (*DatabricksPipelineMetricsReceiver, error) {
	client, err := newPipelineEventClient(w)
	if err != nil {
		return nil, err
	}

	return &DatabricksPipelineMetricsReceiver{
		i,
		w,
		client,
		metricPrefix,
		startOffset,
		intervalOffset,
		includeUpdateId,
		tags,
		// lastRun is initialized to now minus the collection interval
		time.Now().Add(-i.Interval * time.Second),
	}, nil
}

func (d *DatabricksPipelineMetricsReceiver) GetId() string {
	return "databricks-pipeline-metrics-receiver"
}

func (d *DatabricksPipelineMetricsReceiver) PollMetrics(
	ctx context.Context,
	writer chan <- model.Metric,
) error {
	lastRun := d.lastRun

	// This only works when the integration is run as a standalone application
	// that stays running since it requires state to be maintained. However,
	// this is the only supported way to run the integration. If this ever
	// changes, this approach to saving the last run time needs to be reviewed.
	d.lastRun = time.Now()

	log.Debugf("listing pipelines...")
	defer log.Debugf("done listing pipelines")

	pipelineCounters := pipelineCounters{}
	updateCounters := updateCounters{}
	flowCounters := flowCounters{}

	all := d.w.Pipelines.ListPipelines(
		ctx,
		databricksSdkPipelines.ListPipelinesRequest{},
	)

	for ; all.HasNext(ctx);  {
		pipelineStateInfo, err := all.Next(ctx)
		if err != nil {
			return err
		}

		updatePipelineCounters(pipelineStateInfo, &pipelineCounters)

		d.processPipelineEvents(
			ctx,
			pipelineStateInfo,
			lastRun,
			&updateCounters,
			&flowCounters,
			writer,
		)
	}

	writePipelineCounters(
		d.metricPrefix,
		&pipelineCounters,
		d.tags,
		writer,
	)

	writeUpdateCounters(
		d.metricPrefix,
		&updateCounters,
		d.tags,
		writer,
	)

	writeFlowCounters(
		d.metricPrefix,
		&flowCounters,
		d.tags,
		writer,
	)

	return nil
}

func (d *DatabricksPipelineMetricsReceiver) processPipelineEvents(
	ctx context.Context,
	pipelineStateInfo databricksSdkPipelines.PipelineStateInfo,
	lastRun time.Time,
	updateCounters *updateCounters,
	flowCounters *flowCounters,
	writer chan <- model.Metric,
) {
	log.Debugf(
		"processing pipeline events for pipeline %s (%s) with state %s",
		pipelineStateInfo.Name,
		pipelineStateInfo.PipelineId,
		pipelineStateInfo.State,
	)
	defer log.Debugf(
		"done processing pipeline events for pipeline %s (%s) with state %s",
		pipelineStateInfo.Name,
		pipelineStateInfo.PipelineId,
		pipelineStateInfo.State,
	)

	allEvents := d.c.listPipelineEvents(
		ctx,
		databricksSdkPipelines.ListPipelineEventsRequest{
			Filter: "timestamp >= '" +
				time.Now().Add(-d.startOffset).UTC().Format(
					RFC_3339_MILLI_LAYOUT,
				) +
				"'",
			PipelineId: pipelineStateInfo.PipelineId,
		},
	)

	updates := map[string]*updateData{}
	oldUpdates := Set[string]{}
	flows := map[string]*flowData{}
	oldFlows := Set[string]{}

	// Due to very slight lags in the API, it is possible to receive an event
	// that occurred at N - 1 at N + 1 where N is the time we poll for events.
	// This means we won't see the event when we poll at N. This is not a
	// problem except for termination events because we always check termination
	// events against the last run of the integration so that we only count
	// termination events once. If the event is a termination event, when we see
	// it on the next polling cycle after N, we will think that we already saw
	// the termination event and so we will ignore it. The result is we never
	// process the termination event for the associated update or flow so we
	// won't post count or duration events with the termination status for that
	// update or flow. To account for the lag and reduce the risk of this type
	// of situation, we can offset the last run time so that our termination
	// checkpoint lags behind the actual last run time. The effectiveLastRun
	// time is the actual last run time minus the lag (called the
	// intervalOffset).
	effectiveLastRun := lastRun.UTC().Add(-d.intervalOffset)

	// Prior to discovering the lag, we were simply processing all events
	// starting with the first event retrieved (so the most recent since they
	// are returned in descending timestamp order). Because we now offset the
	// termination checkpoint, it seemed prudent to also lag the processing of
	// events by the same amount. In other words, we define a threshold for the
	// maximum timestamp of events to process equal to the time at which we poll
	// minus the same offset. It turned out that using now - offset to come up
	// with the threshold time caused some duplication of metrics in some edge
	// cases so instead we simply add the polling interval to the effective last
	// run since effectiveLastRun + interval equals lastRun + interval - offset
	// equals now - offset only without using the moving target that is now.
	endTime := effectiveLastRun.Add(d.i.Interval * time.Second)

	if log.IsDebugEnabled() {
		log.Debugf(
			"last run: %s; effectiveLastRun: %s; endTime: %s",
			lastRun.Format(RFC_3339_MILLI_LAYOUT),
			effectiveLastRun.Format(RFC_3339_MILLI_LAYOUT),
			endTime.Format(RFC_3339_MILLI_LAYOUT),
		)
	}

	for ; allEvents.HasNext(ctx);  {
		pipelineEvent, err := allEvents.Next(ctx)
		if err != nil {
			log.Warnf(
				"unexpected no more items error or failed to fetch next page of pipeline events for pipeline %s (%s): %v",
				pipelineStateInfo.Name,
				pipelineStateInfo.PipelineId,
				err,
			)
			return
		}

		eventType := pipelineEvent.EventType

		// Short circuit early if this is not an event type we care about.
		if eventType != "create_update" &&
			eventType != "update_progress" &&
			eventType != "flow_progress" {
			continue
		}

		// There isn't much we can do without an origin since the origin
		// data is used to decorate the metrics with things like update ID,
		// flow name, flow ID, and so on.
		if pipelineEvent.Origin == nil {
			log.Warnf(
				"ignoring event with type %s with ID %s for pipeline %s (%s) since it has no origin data",
				eventType,
				pipelineEvent.Id,
				pipelineStateInfo.Name,
				pipelineStateInfo.PipelineId,
			)
			continue
		}

		origin := pipelineEvent.Origin

		// There isn't much we can do without details either since all the
		// data we need to record the metrics is in the details.
		if pipelineEvent.Details == nil {
			log.Warnf(
				"ignoring event with type %s with ID %s for update %s for pipeline %s (%s) since it has no details data",
				eventType,
				pipelineEvent.Id,
				origin.UpdateId,
				pipelineStateInfo.Name,
				pipelineStateInfo.PipelineId,
			)
			continue
		}

		// Parse the event timestamp.
		eventTimestamp, err := time.Parse(
			RFC_3339_MILLI_LAYOUT,
			pipelineEvent.Timestamp,
		)
		if err != nil {
			log.Warnf(
				"ignoring event with type %s with ID %s with invalid timestamp %s for update %s for pipeline %s (%s): %v",
				eventType,
				pipelineEvent.Id,
				pipelineEvent.Timestamp,
				origin.UpdateId,
				pipelineStateInfo.Name,
				pipelineStateInfo.PipelineId,
				err,
			)
			continue
		}

		if eventTimestamp.After(endTime) {
			log.Warnf(
				"ignoring event with type %s with ID %s with timestamp %s for update %s for pipeline %s (%s) since it occurred after the calculated end time",
				eventType,
				pipelineEvent.Id,
				pipelineEvent.Timestamp,
				origin.UpdateId,
				pipelineStateInfo.Name,
				pipelineStateInfo.PipelineId,
			)
			continue
		}

		details := pipelineEvent.Details
		updateId := origin.UpdateId

		if oldUpdates.IsMember(updateId) {
			// Ignore this update progress event because it is associated with
			// an update that completed before the last run.
			if log.IsDebugEnabled() {
				log.Debugf(
					"ignoring event with type %s with ID %s with timestamp %s for update %s for pipeline %s (%s) because the update already terminated",
					eventType,
					pipelineEvent.Id,
					pipelineEvent.Timestamp,
					updateId,
					pipelineStateInfo.Name,
					pipelineStateInfo.PipelineId,
				)
			}

			continue
		}

		if eventType == "create_update" {
			update := processCreateUpdate(
				&pipelineEvent,
				origin,
				updateId,
				eventTimestamp,
				updates,
			)

			if update != nil {
				updates[updateId] = update
			}
		} else if eventType == "update_progress" &&
			details.UpdateProgress != nil {
			update := processUpdate(
				&pipelineEvent,
				origin,
				updateId,
				details.UpdateProgress,
				eventTimestamp,
				effectiveLastRun,
				updates,
				oldUpdates,
			)

			if update != nil {
				updates[updateId] = update
			}
		} else if eventType == "flow_progress" &&
			details.FlowProgress != nil {

			flow := processFlow(
				&pipelineEvent,
				origin,
				updateId,
				details.FlowProgress,
				eventTimestamp,
				effectiveLastRun,
				flows,
				oldFlows,
			)

			if flow != nil {
				flows[origin.FlowName] = flow
			}
		}
	}

	writeUpdateMetrics(
		d.metricPrefix,
		updates,
		oldUpdates,
		d.includeUpdateId,
		d.tags,
		writer,
	)

	updateUpdateCounters(updates, updateCounters)

	writeFlowMetrics(
		d.metricPrefix,
		flows,
		oldFlows,
		d.includeUpdateId,
		d.tags,
		writer,
	)

	updateFlowCounters(flows, flowCounters)
}

func processCreateUpdate(
	pipelineEvent *PipelineEvent,
	origin *databricksSdkPipelines.Origin,
	updateId string,
	eventTimestamp time.Time,
	updates map[string]*updateData,
) (*updateData) {
	if log.IsDebugEnabled() {
		log.Debugf(
			"processing create_update event at time %s for update %s",
			pipelineEvent.Timestamp,
			updateId,
		)
	}

	update := getOrCreateUpdateData(updates, origin)

	update.startTime = eventTimestamp

	return update
}

func processUpdate(
	pipelineEvent *PipelineEvent,
	origin *databricksSdkPipelines.Origin,
	updateId string,
	updateProgress *UpdateProgressDetails,
	eventTimestamp time.Time,
	lastRun time.Time,
	updates map[string]*updateData,
	oldUpdates Set[string],
) (*updateData) {
	state := updateProgress.State

	var (
		waitingTime time.Time
		startRunTime time.Time
		completionTime time.Time
	)

	if isUpdateTerminated(state) {
		if eventTimestamp.Before(lastRun) {
			// This update completed/canceled/failed before the last run so we
			// should already have reported on it and it can be ignored. This
			// way we only report on terminated updates once and normal
			// aggregation functions can be used with the update counters for
			// these states. Also add the updateId to the old updates set so
			// we ignore all other event log messages for this update.
			if log.IsDebugEnabled() {
				log.Debugf(
					"update %s terminated (%s at %s) before last run (%s), adding it to the ignore set",
					updateId,
					state,
					eventTimestamp.Format(RFC_3339_MILLI_LAYOUT),
					lastRun.Format(RFC_3339_MILLI_LAYOUT),
				)
			}

			oldUpdates.Add(updateId)
			return nil
		}

		completionTime = eventTimestamp
	} else if state ==
		databricksSdkPipelines.UpdateInfoStateWaitingForResources {
		waitingTime = eventTimestamp
	} else if state == databricksSdkPipelines.UpdateInfoStateInitializing {
		// The Databricks update details UI tracks the overall update duration
		// and the update "Run time" separately. It starts tracking the run
		// time when the update INITIALIZING event is emitted.
		startRunTime = eventTimestamp
	}

	if log.IsDebugEnabled() {
		log.Debugf(
			"processing update_progress event at time %s for update %s with status %s",
			pipelineEvent.Timestamp,
			updateId,
			state,
		)
	}

	update := getOrCreateUpdateData(updates, origin)

	if !completionTime.IsZero() {
		update.completionTime = completionTime
	}

	if !waitingTime.IsZero() {
		update.waitingTime = waitingTime
	}

	if !startRunTime.IsZero() {
		update.startRunTime = startRunTime
	}

	// If we have not yet seen a state for this update, save the state from the
	// current event as the current state of the update. This logic depends on
	// seeing the latest event first which is technically guaranteed by the API
	// (events are returned in descending order by timestamp).
	if update.state == "" {
		update.state = state
	}

	return update
}

func processFlow(
	pipelineEvent *PipelineEvent,
	origin *databricksSdkPipelines.Origin,
	updateId string,
	flowProgress *FlowProgressDetails,
	eventTimestamp time.Time,
	lastRun time.Time,
	flows map[string]*flowData,
	oldFlows Set[string],
) *flowData {
	if oldFlows.IsMember(origin.FlowName) {
		// Ignore this flow progress event because it is associated with a flow
		// that completed before the last run.
		if log.IsDebugEnabled() {
			log.Debugf(
				"ignoring flow %s (%s) for update ID %s because the flow already terminated",
				origin.FlowName,
				origin.FlowId,
				updateId,
			)
		}

		return nil
	}

	status := strings.ToUpper(flowProgress.Status)

	var (
		queuedTime time.Time
		planningTime time.Time
		startTime time.Time
		completionTime time.Time
	)

	if isFlowTerminated(status) {
		if eventTimestamp.Before(lastRun) {
			// This update completed/failed/was stopped/was skipped/was excluded
			// before the last run so we should already have reported on it and
			// it can be ignored. This way we only report on terminated flows
			// once and normal aggregation functions can be used with the flow
			// counters for these states. Also add the flowId to the old flows
			// set we ignore all other event log messages for this flow.
			if log.IsDebugEnabled() {
				log.Debugf(
					"flow %s (%s) for update ID %s terminated (%s at %s) before last run (%s), adding it to the ignore set",
					origin.FlowName,
					origin.FlowId,
					updateId,
					status,
					eventTimestamp.Format(RFC_3339_MILLI_LAYOUT),
					lastRun.Format(RFC_3339_MILLI_LAYOUT),
				)
			}

			oldFlows.Add(origin.FlowName)
			return nil
		}

		completionTime = eventTimestamp
	} else if status == FlowInfoStatusQueued {
		queuedTime = eventTimestamp
	} else if status == FlowInfoStatusPlanning {
		planningTime = eventTimestamp
	} else if status == FlowInfoStatusStarting {
		startTime = eventTimestamp
	}

	if log.IsDebugEnabled() {
		log.Debugf(
			"processing flow_progress event at time %s for update %s and flow %s (%s) with status %s",
			pipelineEvent.Timestamp,
			updateId,
			origin.FlowName,
			origin.FlowId,
			status,
		)
	}

	flow := getOrCreateFlowData(flows, flowProgress, origin, completionTime)

	if !completionTime.IsZero() {
		flow.completionTime = completionTime
	}

	if !queuedTime.IsZero() {
		flow.queuedTime = queuedTime
	}

	if !planningTime.IsZero() {
		flow.planningTime = planningTime
	}

	if !startTime.IsZero() {
		flow.startTime = startTime
	}

	// If we have not yet seen a state for this flow, save the state from the
	// current event as the current state of the flow. This logic depends on
	// seeing the latest event first which is technically guaranteed by the API
	// (events are returned in descending order by timestamp).
	if flow.status == "" {
		flow.status = status
	}

	return flow
}

func updatePipelineCounters(
	pipelineStateInfo databricksSdkPipelines.PipelineStateInfo,
	counters *pipelineCounters,
) {
	state := pipelineStateInfo.State

	if state == databricksSdkPipelines.PipelineStateDeleted {
		// @TODO do we want to know total deleted or deleted since last run?
		counters.deleted += 1
	} else if state == databricksSdkPipelines.PipelineStateDeploying {
		counters.deploying += 1
	} else if state == databricksSdkPipelines.PipelineStateFailed {
		// @TODO do we want to know total failed or failed since last run?
		counters.failed += 1
	} else if state == databricksSdkPipelines.PipelineStateIdle {
		counters.idle += 1
	} else if state == databricksSdkPipelines.PipelineStateRecovering {
		counters.recovering += 1
	} else if state == databricksSdkPipelines.PipelineStateResetting {
		counters.resetting += 1
	} else if state == databricksSdkPipelines.PipelineStateRunning {
		counters.running += 1
	} else if state == databricksSdkPipelines.PipelineStateStarting {
		counters.starting += 1
	} else if state == databricksSdkPipelines.PipelineStateStopping {
		counters.stopping += 1
	}
}

func updateUpdateCounters(
	updates map[string]*updateData,
	counters *updateCounters,
) {
	for _, update := range updates {
		state := update.state

		if state == databricksSdkPipelines.UpdateInfoStateCanceled {
			counters.canceled += 1
		} else if state == databricksSdkPipelines.UpdateInfoStateCompleted {
			counters.completed += 1
		} else if state == databricksSdkPipelines.UpdateInfoStateCreated {
			// @TODO do we want to know total created or created since last run?
			counters.created += 1
		} else if state == databricksSdkPipelines.UpdateInfoStateFailed {
			counters.failed += 1
		} else if state == databricksSdkPipelines.UpdateInfoStateInitializing {
			counters.initializing += 1
		} else if state == databricksSdkPipelines.UpdateInfoStateQueued {
			counters.queued += 1
		} else if state == databricksSdkPipelines.UpdateInfoStateResetting {
			counters.resetting += 1
		} else if state == databricksSdkPipelines.UpdateInfoStateRunning {
			counters.running += 1
		} else if state ==
			databricksSdkPipelines.UpdateInfoStateSettingUpTables {
			counters.settingUpTables += 1
		} else if state == databricksSdkPipelines.UpdateInfoStateStopping {
			counters.stopping += 1
		} else if state ==
			databricksSdkPipelines.UpdateInfoStateWaitingForResources {
			counters.waitingForResources += 1
		}
	}
}

func updateFlowCounters(
	flows map[string]*flowData,
	counters *flowCounters,
) {
	for _, flow := range flows {
		// flow.status is already uppercase because it was uppercased before it
		// was set into the flow.
		status := flow.status

		if status == FlowInfoStatusCompleted {
			counters.completed += 1
		} else if status == FlowInfoStatusExcluded {
			counters.excluded += 1
		} else if status == FlowInfoStatusFailed {
			counters.failed += 1
		} else if status == FlowInfoStatusIdle {
			counters.idle += 1
		} else if status == FlowInfoStatusPlanning {
			counters.planning += 1
		} else if status == FlowInfoStatusQueued {
			counters.queued += 1
		} else if status == FlowInfoStatusRunning {
			counters.running += 1
		} else if status == FlowInfoStatusSkipped {
			counters.skipped += 1
		} else if status == FlowInfoStatusStarting {
			counters.starting += 1
		} else if status == FlowInfoStatusStopped {
			counters.stopped += 1
		}
	}
}

func makeEventAttrs(
	event *eventData,
	includeUpdateId bool,
	tags map[string]string,
) map[string]interface{} {
	attrs := makeAttributesMap(tags)

	attrs["databricksPipelineId"] = event.pipelineId
	attrs["databricksPipelineName"] = event.pipelineName
	attrs["databricksClusterId"] = event.clusterId

	if includeUpdateId {
		attrs["databricksPipelineUpdateId"] = event.updateId
	}

	return attrs
}

func writeUpdateMetrics(
	metricPrefix string,
	updates map[string]*updateData,
	oldUpdates map[string]struct{},
	includeUpdateId bool,
	tags map[string]string,
	writer chan <- model.Metric,
) {
	for _, update := range updates {
		// Double check that we aren't ignoring this update. This handles cases
		// where update events may have come in out of order. For example, we
		// got an INITIALIZING event before a termination event. This shouldn't
		// happen since the API returns events in descending order by timestamp
		// but just in case, this prevents duplicate data.
		_, ok := oldUpdates[update.updateId]
		if ok {
			continue
		}

		// If the update hasn't been created yet, the start time (set when the
		// create_update event is processed) will be zero. Nothing to report in
		// this case.
		if update.startTime.IsZero() {
			continue
		}

		attrs := makeEventAttrs(&update.eventData, includeUpdateId, tags)

		attrs["databricksPipelineUpdateStatus"] = update.state

		// Always write the waiting duration.
		writeUpdateWaitingDuration(metricPrefix, update, attrs, writer)

		// If completionTime is zero, we haven't seen a termination event yet
		// which means the update hasn't completed, record the _wall clock_
		// duration only. We know the update was at least created because we
		// passed the startTime check above.
		if update.completionTime.IsZero() {
			// Record the duration as now - startTime. We know startTime is
			// non-zero since we passed the IsZero() check above.
			writeGauge(
				metricPrefix,
				"pipeline.update.duration",
				int64(time.Now().UTC().Sub(update.startTime) /
					time.Millisecond),
				attrs,
				writer,
			)

			// If we've seen an INITIALIZING event, startRunTime will be
			// non-zero. Record the run time as now - startRunTime.
			if !update.startRunTime.IsZero() {
				writeGauge(
					metricPrefix,
					"pipeline.update.runTime",
					int64(time.Now().UTC().Sub(update.startRunTime) /
						time.Millisecond),
					attrs,
					writer,
				)
			}

			continue
		}

		// If we get to here it means we have a valid start and completion time.
		// This means the update terminated in some way. See
		// isUpdateTerminated() for the possible termination statuses.

		// Record the actual update duration as completionTime - startTime.
		writeGauge(
			metricPrefix,
			"pipeline.update.duration",
			int64(update.completionTime.Sub(update.startTime) /
				time.Millisecond),
			attrs,
			writer,
		)

		// Check for a non-zero startRunTime. If there is no startRunTime, we
		// did not see an INITIALIZING event so the flow went from being created
		// or from WAITING_FOR_RESOURCES straight to a terminal state. This can
		// happen if the update FAILED or was CANCELED. In this case, it should
		// have no duration. Record nothing. Otherwise, we saw an INITIALIZING
		// event, so record the actual runTime duration as
		// completionTime - startRunTime.
		if !update.startRunTime.IsZero() {
			writeGauge(
				metricPrefix,
				"pipeline.update.runTime",
				int64(update.completionTime.Sub(update.startRunTime) /
					time.Millisecond),
				attrs,
				writer,
			)
		}
	}
}

func writeUpdateWaitingDuration(
	metricPrefix string,
	update *updateData,
	attrs map[string]interface{},
	writer chan <- model.Metric,
) {
	// waitingTime will be non-zero if we saw a WAITING_FOR_RESOURCES event.
	if !update.waitingTime.IsZero() {
		// If we are still in the WAITING_FOR_RESOURCES state, record the
		// _wall clock_ duration. Otherwise record the actual waiting duration
		// as startRunTime - waitingTime if startRunTime is non-zero or
		// completionTime - waitingTime if completionTime is non-zero. The
		// latter case should handle transitions from WAITING_FOR_RESOURCES to a
		// terminal state like FAILED or CANCELED.
		if update.state ==
			databricksSdkPipelines.UpdateInfoStateWaitingForResources {
			// The update is still in WAITING_FOR_RESOURCES. Record waiting
			// duration as now - waitingTime.
			writeGauge(
				metricPrefix,
				"pipeline.update.duration.wait",
				int64(time.Now().UTC().Sub(update.waitingTime) /
					time.Millisecond),
				attrs,
				writer,
			)
		} else if !update.startRunTime.IsZero() {
			// If startRunTime is non-zero, the update made it to INITIALING
			// after WAITING_FOR_RESOURCES. Record waiting duration as
			// startRunTime - waitingTime.
			writeGauge(
				metricPrefix,
				"pipeline.update.duration.wait",
				int64(update.startRunTime.Sub(update.waitingTime) /
					time.Millisecond),
				attrs,
				writer,
			)
		} else if !update.completionTime.IsZero() {
			// If completionTime is non-zero, the update went from
			// WAITING_FOR_RESOURCES to a terminal state like FAILED or
			// CANCELED. Record waiting duration as
			// completionTime - waitingTime.
			writeGauge(
				metricPrefix,
				"pipeline.update.duration.wait",
				int64(update.completionTime.Sub(update.waitingTime) /
					time.Millisecond),
				attrs,
				writer,
			)
		}

		// Not sure what it would mean if none of the above are true so record
		// nothing.
	}
}

func writeFlowMetrics(
	metricPrefix string,
	flows map[string]*flowData,
	oldFlows map[string]struct{},
	includeUpdateId bool,
	tags map[string]string,
	writer chan <- model.Metric,
) {
	for _, flow := range flows {
		// Double check that we aren't ignoring this flow. This handles cases
		// where flow events may have come in out of order. For example, we got
		// a RUNNING event before a termination event. This shouldn't happen
		// since the API returns events in descending order by timestamp but
		// just in case, this prevents duplicate data.
		_, ok := oldFlows[flow.name]
		if ok {
			continue
		}

		// If the flow hasn't even been queued yet, the queued time (set when
		// the flow_progress QUEUED event is processed) will be zero. Nothing to
		// report in this case.
		if flow.queuedTime.IsZero() {
			continue
		}

		attrs := makeEventAttrs(&flow.eventData, includeUpdateId, tags)

		attrs["databricksPipelineFlowId"] = flow.id
		attrs["databricksPipelineFlowName"] = flow.name
		attrs["databricksPipelineFlowStatus"] = flow.status

		// Always write the queue and planning durations.

		writeFlowQueueDuration(metricPrefix, flow, attrs, writer)
		writeFlowPlanningDuration(metricPrefix, flow, attrs, writer)

		// If completionTime is zero, we haven't seen a termination event yet
		// which means the flow hasn't completed, record the _wall clock_
		// duration only. We know the flow was at least QUEUED because we passed
		// the queueTime check above.
		if flow.completionTime.IsZero() {
			// If startTime is non-zero, we saw the STARTING event. Since
			// completionTime is zero, the flow hasn't terminated yet so record
			// the total flow duration as now - startTime. If the startTime is
			// zero, we did not see a STARTING event so the flow went from
			// QUEUED to some other state and never started running. In this
			// case it should have no duration. Record nothing.
			if !flow.startTime.IsZero() {
				writeGauge(
					metricPrefix,
					"pipeline.flow.duration",
					int64(time.Now().UTC().Sub(flow.startTime) /
						time.Millisecond),
					attrs,
					writer,
				)
			}

			continue
		}

		// If we get to here it means we have a valid queued and completion
		// time. This means the flow terminated in some way. See
		// isFlowTerminated() for the possible termination statuses.

		// Check for a non-zero start time. If there is no startTime, we did not
		// see a STARTING event so the flow went from QUEUED straight to a
		// terminal state. This can happen if the flow stopped/failed/was
		// skipped/was excluded/completed after it was QUEUED. In this case, it
		// should have no duration. Record nothing. Otherwise we saw a STARTING
		// event, so record the actual run time as completion time - startTime.
		if !flow.startTime.IsZero() {
			writeGauge(
				metricPrefix,
				"pipeline.flow.duration",
				int64(flow.completionTime.Sub(flow.startTime) /
					time.Millisecond),
				attrs,
				writer,
			)
		}

		// Record any flow metrics we found. These will only be non-nil if we
		// saw a termination event (see getOrCreateFlowData()) AND we saw the
		// corresponding metric on the flow_progress event. Since we only
		// process termination events once, we will only record these metrics
		// once and can use regular aggregation functions to do things like
		// calculate average backlog bytes.

		if flow.backlogBytes != nil {
			writeGauge(
				metricPrefix,
				"pipeline.flow.backlogBytes",
				*flow.backlogBytes,
				attrs,
				writer,
			)
		}

		if flow.backlogFiles != nil {
			writeGauge(
				metricPrefix,
				"pipeline.flow.backlogFiles",
				*flow.backlogFiles,
				attrs,
				writer,
			)
		}

		if flow.numOutputRows != nil {
			writeGauge(
				metricPrefix,
				"pipeline.flow.rowsWritten",
				*flow.numOutputRows,
				attrs,
				writer,
			)
		}

		if flow.droppedRecords != nil {
			writeGauge(
				metricPrefix,
				"pipeline.flow.recordsDropped",
				*flow.droppedRecords,
				attrs,
				writer,
			)
		}

		// If there were expectations for the flow, record expectation metrics.

		if len(flow.expectations) > 0 {
			for _, e := range flow.expectations {
				expectationAttrs := maps.Clone(attrs)
				expectationAttrs["databricksPipelineFlowExpectationName"] =
					e.Name
				expectationAttrs["databricksPipelineDatasetName"] = e.Dataset

				writeGauge(
					metricPrefix,
					"pipeline.flow.expectation.recordsPassed",
					e.PassedRecords,
					expectationAttrs,
					writer,
				)
				writeGauge(
					metricPrefix,
					"pipeline.flow.expectation.recordsFailed",
					e.FailedRecords,
					expectationAttrs,
					writer,
				)
			}
		}
	}
}

func writeFlowQueueDuration(
	metricPrefix string,
	flow *flowData,
	attrs map[string]interface{},
	writer chan <- model.Metric,
) {
	// queuedTime will be non-zero if we saw a QUEUED event.
	if !flow.queuedTime.IsZero() {
		// If we are still in the QUEUED state, record the _wall clock_
		// duration. Otherwise record the actual queue duration as
		// planningTime - queuedTime if planningTime is non-zero, or
		// startTime - queuedTime if the startTime is non-zero, or
		// completionTime - queuedTime if completionTime is non-zero. The latter
		// case should handle transitions from QUEUED to a termination state
		// like SKIPPED or STOPPED.
		if flow.status == FlowInfoStatusQueued {
			// The flow is still in QUEUED. Record queue duration as
			// now - queuedTime.
			writeGauge(
				metricPrefix,
				"pipeline.flow.duration.queue",
				int64(time.Now().UTC().Sub(flow.queuedTime) /
					time.Millisecond),
				attrs,
				writer,
			)
		} else if !flow.planningTime.IsZero() {
			// If planningTime is non-zero, the flow made it to PLANNING after
			// QUEUED. Record queue duration as planningTime - queuedTime.
			writeGauge(
				metricPrefix,
				"pipeline.flow.duration.queue",
				int64(flow.planningTime.Sub(flow.queuedTime) /
					time.Millisecond),
				attrs,
				writer,
			)
		} else if !flow.startTime.IsZero() {
			// If startTime is non-zero, the flow went from QUEUED to STARTING
			// (the flow did not do any PLANNING). Record queue duration as
			// startTime - queuedTime.
			writeGauge(
				metricPrefix,
				"pipeline.flow.duration.queue",
				int64(flow.startTime.Sub(flow.queuedTime) /
					time.Millisecond),
				attrs,
				writer,
			)
		} else if !flow.completionTime.IsZero() {
			// If completionTime is non-zero, the flow went from QUEUED to a
			// termination state like SKIPPED or STOPPED. Record queue duration
			// was completionTime - queuedTime.
			writeGauge(
				metricPrefix,
				"pipeline.flow.duration.queue",
				int64(flow.completionTime.Sub(flow.queuedTime) /
					time.Millisecond),
				attrs,
				writer,
			)
		}

		// Not sure what it would mean if none of the above are true so record
		// nothing
	}
}

func writeFlowPlanningDuration(
	metricPrefix string,
	flow *flowData,
	attrs map[string]interface{},
	writer chan <- model.Metric,
) {
	// planningTime will be non-zero if we saw a PLANNING event.
	if !flow.planningTime.IsZero() {
		// If we are still in the PLANNING state, record the _wall clock_
		// duration. Otherwise record the actual planning duration as
		// startTime - planningTime if the startTime is non-zero or
		// completionTime - planningTime if completionTime is non-zero. The
		// latter case should handle transitions from PLANNING to a termination
		// state like FAILED or STOPPED.
		if flow.status == FlowInfoStatusPlanning {
			// The flow is still in PLANNING. Record planning duration as
			// now - planningTime.
			writeGauge(
				metricPrefix,
				"pipeline.flow.duration.plan",
				int64(time.Now().UTC().Sub(flow.planningTime) /
					time.Millisecond),
				attrs,
				writer,
			)
		} else if !flow.startTime.IsZero() {
			// If startTime is non-zero, the flow made it to STARTING after
			// PLANNING. Record planing duration as startTime - planningTime.
			writeGauge(
				metricPrefix,
				"pipeline.flow.duration.plan",
				int64(flow.startTime.Sub(flow.planningTime) /
					time.Millisecond),
				attrs,
				writer,
			)
		} else if !flow.completionTime.IsZero() {
			// If completionTime is non-zero, the flow went from PLANNING to a
			// termination state like FAILED or STOPPED. Record planning
			// duration as completionTime - planningTime.
			writeGauge(
				metricPrefix,
				"pipeline.flow.duration.plan",
				int64(flow.completionTime.Sub(flow.planningTime) /
					time.Millisecond),
				attrs,
				writer,
			)
		}

		// Not sure what it would mean if none of the above are true so record
		// nothing
	}
}

func writePipelineCounters(
	metricPrefix string,
	counters *pipelineCounters,
	tags map[string]string,
	writer chan <- model.Metric,
) {
	attrs := makeAttributesMap(tags)

	attrs["databricksPipelineState"] =
		string(databricksSdkPipelines.PipelineStateDeleted)

	writeGauge(
		metricPrefix,
		"pipeline.pipelines",
		counters.deleted,
		attrs,
		writer,
	)

	attrs["databricksPipelineState"] =
		string(databricksSdkPipelines.PipelineStateDeploying)

	writeGauge(
		metricPrefix,
		"pipeline.pipelines",
		counters.deploying,
		attrs,
		writer,
	)

	attrs["databricksPipelineState"] =
		string(databricksSdkPipelines.PipelineStateFailed)

	writeGauge(
		metricPrefix,
		"pipeline.pipelines",
		counters.failed,
		attrs,
		writer,
	)

	attrs["databricksPipelineState"] =
		string(databricksSdkPipelines.PipelineStateIdle)

	writeGauge(
		metricPrefix,
		"pipeline.pipelines",
		counters.idle,
		attrs,
		writer,
	)

	attrs["databricksPipelineState"] =
		string(databricksSdkPipelines.PipelineStateRecovering)

	writeGauge(
		metricPrefix,
		"pipeline.pipelines",
		counters.recovering,
		attrs,
		writer,
	)

	attrs["databricksPipelineState"] =
		string(databricksSdkPipelines.PipelineStateResetting)

	writeGauge(
		metricPrefix,
		"pipeline.pipelines",
		counters.resetting,
		attrs,
		writer,
	)

	attrs["databricksPipelineState"] =
		string(databricksSdkPipelines.PipelineStateRunning)

	writeGauge(
		metricPrefix,
		"pipeline.pipelines",
		counters.running,
		attrs,
		writer,
	)

	attrs["databricksPipelineState"] =
		string(databricksSdkPipelines.PipelineStateStarting)

	writeGauge(
		metricPrefix,
		"pipeline.pipelines",
		counters.starting,
		attrs,
		writer,
	)

	attrs["databricksPipelineState"] =
		string(databricksSdkPipelines.PipelineStateStopping)

	writeGauge(
		metricPrefix,
		"pipeline.pipelines",
		counters.stopping,
		attrs,
		writer,
	)
}

func writeUpdateCounters(
	metricPrefix string,
	counters *updateCounters,
	tags map[string]string,
	writer chan <- model.Metric,
) {
	attrs := makeAttributesMap(tags)

	attrs["databricksPipelineUpdateStatus"] =
		string(databricksSdkPipelines.UpdateInfoStateCanceled)

	writeGauge(
		metricPrefix,
		"pipeline.updates",
		counters.canceled,
		attrs,
		writer,
	)

	attrs["databricksPipelineUpdateStatus"] =
		string(databricksSdkPipelines.UpdateInfoStateCompleted)

	writeGauge(
		metricPrefix,
		"pipeline.updates",
		counters.completed,
		attrs,
		writer,
	)

	attrs["databricksPipelineUpdateStatus"] =
		string(databricksSdkPipelines.UpdateInfoStateCreated)

	writeGauge(
		metricPrefix,
		"pipeline.updates",
		counters.created,
		attrs,
		writer,
	)

	attrs["databricksPipelineUpdateStatus"] =
		string(databricksSdkPipelines.UpdateInfoStateFailed)

	writeGauge(
		metricPrefix,
		"pipeline.updates",
		counters.failed,
		attrs,
		writer,
	)

	attrs["databricksPipelineUpdateStatus"] =
		string(databricksSdkPipelines.UpdateInfoStateInitializing)

	writeGauge(
		metricPrefix,
		"pipeline.updates",
		counters.initializing,
		attrs,
		writer,
	)

	attrs["databricksPipelineUpdateStatus"] =
		string(databricksSdkPipelines.UpdateInfoStateQueued)

	writeGauge(
		metricPrefix,
		"pipeline.updates",
		counters.queued,
		attrs,
		writer,
	)

	attrs["databricksPipelineUpdateStatus"] =
		string(databricksSdkPipelines.UpdateInfoStateResetting)

	writeGauge(
		metricPrefix,
		"pipeline.updates",
		counters.resetting,
		attrs,
		writer,
	)

	attrs["databricksPipelineUpdateStatus"] =
		string(databricksSdkPipelines.UpdateInfoStateRunning)

	writeGauge(
		metricPrefix,
		"pipeline.updates",
		counters.running,
		attrs,
		writer,
	)

	attrs["databricksPipelineUpdateStatus"] =
		string(databricksSdkPipelines.UpdateInfoStateSettingUpTables)

	writeGauge(
		metricPrefix,
		"pipeline.updates",
		counters.settingUpTables,
		attrs,
		writer,
	)

	attrs["databricksPipelineUpdateStatus"] =
		string(databricksSdkPipelines.UpdateInfoStateStopping)

	writeGauge(
		metricPrefix,
		"pipeline.updates",
		counters.stopping,
		attrs,
		writer,
	)

	attrs["databricksPipelineUpdateStatus"] =
		string(databricksSdkPipelines.UpdateInfoStateWaitingForResources)

	writeGauge(
		metricPrefix,
		"pipeline.updates",
		counters.waitingForResources,
		attrs,
		writer,
	)
}

func writeFlowCounters(
	metricPrefix string,
	counters *flowCounters,
	tags map[string]string,
	writer chan <- model.Metric,
) {
	attrs := makeAttributesMap(tags)

	attrs["databricksPipelineFlowStatus"] = FlowInfoStatusCompleted

	writeGauge(
		metricPrefix,
		"pipeline.flows",
		counters.completed,
		attrs,
		writer,
	)

	attrs["databricksPipelineFlowStatus"] = FlowInfoStatusExcluded

	writeGauge(
		metricPrefix,
		"pipeline.flows",
		counters.excluded,
		attrs,
		writer,
	)

	attrs["databricksPipelineFlowStatus"] = FlowInfoStatusFailed

	writeGauge(
		metricPrefix,
		"pipeline.flows",
		counters.failed,
		attrs,
		writer,
	)

	attrs["databricksPipelineFlowStatus"] = FlowInfoStatusIdle

	writeGauge(
		metricPrefix,
		"pipeline.flows",
		counters.idle,
		attrs,
		writer,
	)

	attrs["databricksPipelineFlowStatus"] = FlowInfoStatusPlanning

	writeGauge(
		metricPrefix,
		"pipeline.flows",
		counters.planning,
		attrs,
		writer,
	)

	attrs["databricksPipelineFlowStatus"] = FlowInfoStatusQueued

	writeGauge(
		metricPrefix,
		"pipeline.flows",
		counters.queued,
		attrs,
		writer,
	)

	attrs["databricksPipelineFlowStatus"] = FlowInfoStatusRunning

	writeGauge(
		metricPrefix,
		"pipeline.flows",
		counters.running,
		attrs,
		writer,
	)

	attrs["databricksPipelineFlowStatus"] = FlowInfoStatusSkipped

	writeGauge(
		metricPrefix,
		"pipeline.flows",
		counters.skipped,
		attrs,
		writer,
	)

	attrs["databricksPipelineFlowStatus"] = FlowInfoStatusStarting

	writeGauge(
		metricPrefix,
		"pipeline.flows",
		counters.starting,
		attrs,
		writer,
	)

	attrs["databricksPipelineFlowStatus"] = FlowInfoStatusStopped

	writeGauge(
		metricPrefix,
		"pipeline.flows",
		counters.stopped,
		attrs,
		writer,
	)
}
