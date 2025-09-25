package databricks

import (
	"context"
	"strings"
	"time"

	databricksSdkPipelines "github.com/databricks/databricks-sdk-go/service/pipelines"
	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration"
	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/log"
	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/model"
)

type Set[T comparable] map[T]struct{}

func (s Set[T]) IsMember(u T) bool {
	_, ok := s[u]
	return ok
}

func (s Set[T]) Add(u T) {
	s[u] = struct{}{}
}

type eventData struct {
	pipelineId   string
	pipelineName string
	clusterId    string
	updateId     string
}

const (
	FlowInfoStatusCompleted = "COMPLETED"
	FlowInfoStatusExcluded  = "EXCLUDED"
	FlowInfoStatusFailed    = "FAILED"
	FlowInfoStatusIdle      = "IDLE"
	FlowInfoStatusPlanning  = "PLANNING"
	FlowInfoStatusQueued    = "QUEUED"
	FlowInfoStatusRunning   = "RUNNING"
	FlowInfoStatusSkipped   = "SKIPPED"
	FlowInfoStatusStarting  = "STARTING"
	FlowInfoStatusStopped   = "STOPPED"
)

type flowData struct {
	eventData

	id             string
	name           string
	queueStartTime time.Time
	planStartTime  time.Time
	startTime      time.Time
	completionTime time.Time
	status         string
	backlogBytes   *float64
	backlogFiles   *float64
	numOutputRows  *float64
	droppedRecords *float64
	expectations   []FlowProgressExpectations
}

func newFlowData(origin *databricksSdkPipelines.Origin) *flowData {
	return &flowData{
		eventData: eventData{
			pipelineId:   origin.PipelineId,
			pipelineName: origin.PipelineName,
			clusterId:    origin.ClusterId,
			updateId:     origin.UpdateId,
		},
		id:   origin.FlowId,
		name: origin.FlowName,
	}
}

func getOrCreateFlowData(
	flows map[string]*flowData,
	flowProgress *FlowProgressDetails,
	origin *databricksSdkPipelines.Origin,
	updateId string,
	queueStartTime time.Time,
	planStartTime time.Time,
	startTime time.Time,
	completionTime time.Time,
	status string,
) *flowData {
	// OK to use flow name here instead of flow ID because flow names are unique
	// within a pipeline.
	// see: https://docs.databricks.com/aws/en/delta-live-tables/flows
	// Also not all flow_process events have an ID in the origin!
	flow, ok := flows[flowName(updateId, origin.FlowName)]
	if !ok {
		flow = newFlowData(origin)
	}

	if !queueStartTime.IsZero() {
		flow.queueStartTime = queueStartTime
	}

	if !planStartTime.IsZero() {
		flow.planStartTime = planStartTime
	}

	if !startTime.IsZero() {
		flow.startTime = startTime
	}

	if !completionTime.IsZero() {
		flow.completionTime = completionTime
	}

	// If we have not yet seen a state for this flow, save the state from the
	// current event as the current state of the flow. This logic depends on
	// seeing the latest event first which is technically guaranteed by the API
	// (events are returned in descending order by timestamp).
	if flow.status == "" {
		flow.status = status
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
	//
	// @TODO: I believe the second completionTime check is no longer needed now
	// that we set the flow's completion time above.
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

func isFlowTerminated(status string) bool {
	// We assume the status has already been uppercased. Since this is an
	// internal function and we guarantee this in the caller, this is OK.
	return status == FlowInfoStatusCompleted ||
		status == FlowInfoStatusStopped ||
		status == FlowInfoStatusSkipped ||
		status == FlowInfoStatusFailed ||
		status == FlowInfoStatusExcluded
}

// This method is used to return the unique name of a flow within a given
// update. This is done by prefixing the flow name with the update ID. This is
// necessary when looking up flows in the oldFlows set to determine if a given
// flow is one that already terminated. This could be done without having to
// prefix the flow name by using a map[string]Set[string] instead. But this has
// two issues. First, in order to lookup a flow in the oldFlows set, we have to
// first lookup the right set by update ID and if no set exists for an update
// ID, do we consider that an error or consider that the flow is not an old
// flow? And once all updates are processed, this should really never be the
// case but we would probably want to check it anyway just to be safe. Second,
// it means that when we add a flow to the oldFlows set, we have to first check
// if the set for that update exists and that has to be done on every insert.
// Rather than do all that, although the string concatenation does incur
// additional performance overhead, it isn't much and makes the code simpler
// and easier to understand.
func flowName(updateId string, flowName string) string {
	return updateId + "." + flowName
}

type updateData struct {
	eventData

	creationTime   time.Time
	waitStartTime  time.Time
	startTime      time.Time
	completionTime time.Time
	state          databricksSdkPipelines.UpdateInfoState
}

func newUpdateData(origin *databricksSdkPipelines.Origin) *updateData {
	return &updateData{
		eventData: eventData{
			pipelineId:   origin.PipelineId,
			pipelineName: origin.PipelineName,
			clusterId:    origin.ClusterId,
			updateId:     origin.UpdateId,
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

// We need a separate function to create update data for update_progress events
// than for create_update events since create_update events do not have a status
// and so none of the time fields are set so there is no need to perform the
// same checks as we do for update_progress events.
func getOrCreateUpdateDataForUpdateProgress(
	updates map[string]*updateData,
	origin *databricksSdkPipelines.Origin,
	waitStartTime time.Time,
	startTime time.Time,
	completionTime time.Time,
	state databricksSdkPipelines.UpdateInfoState,
) *updateData {
	update := getOrCreateUpdateData(updates, origin)

	if !waitStartTime.IsZero() {
		update.waitStartTime = waitStartTime
	}

	if !startTime.IsZero() {
		update.startTime = startTime
	}

	if !completionTime.IsZero() {
		update.completionTime = completionTime
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

func isUpdateTerminated(state databricksSdkPipelines.UpdateInfoState) bool {
	return state == databricksSdkPipelines.UpdateInfoStateCompleted ||
		state == databricksSdkPipelines.UpdateInfoStateCanceled ||
		state == databricksSdkPipelines.UpdateInfoStateFailed
}

func isTargetEventType(eventType string) bool {
	return eventType == "create_update" ||
		eventType == "update_progress" ||
		eventType == "flow_progress"
}

// see https://github.com/databricks/databricks-sdk-go/blob/v0.52.0/service/pipelines/model.go#L1182
type pipelineCounters struct {
	deleted    int64
	deploying  int64
	failed     int64
	idle       int64
	recovering int64
	resetting  int64
	running    int64
	starting   int64
	stopping   int64
}

// see https://github.com/databricks/databricks-sdk-go/blob/v0.52.0/service/pipelines/model.go#L1715
// Terminated states are not tracked because they can be counted using count(*)
// on DatabricksPipelineUpdate complete events
type updateCounters struct {
	created             int64
	initializing        int64
	queued              int64
	resetting           int64
	running             int64
	settingUpTables     int64
	stopping            int64
	waitingForResources int64
}

// determined valid states by looking at pipeline log flow_progress events and
// looking at the status dropdown in the list view for a pipeline in the
// Pipelines UI
// Terminated states are not tracked because they can be counted using count(*)
// on DatabricksPipelineFlow complete events
type flowCounters struct {
	idle      int64
	planning  int64
	queued    int64
	running   int64
	starting  int64
}

type DatabricksPipelineMetricsReceiver struct {
	i               *integration.LabsIntegration
	w               DatabricksWorkspace
	startOffset     time.Duration
	intervalOffset  time.Duration
	tags            map[string]string
	lastRun         time.Time
}

func NewDatabricksPipelineMetricsReceiver(
	i *integration.LabsIntegration,
	w DatabricksWorkspace,
	startOffset time.Duration,
	intervalOffset time.Duration,
	tags map[string]string,
) *DatabricksPipelineMetricsReceiver {
	return &DatabricksPipelineMetricsReceiver{
		i,
		w,
		startOffset,
		intervalOffset,
		tags,
		// lastRun is initialized to now minus the collection interval
		Now().UTC().Add(-i.Interval * time.Second),
	}
}

func (d *DatabricksPipelineMetricsReceiver) GetId() string {
	return "databricks-pipeline-metrics-receiver"
}

func (d *DatabricksPipelineMetricsReceiver) PollEvents(
	ctx context.Context,
	writer chan<- model.Event,
) error {
	lastRun := d.lastRun

	// This only works when the integration is run as a standalone application
	// that stays running since it requires state to be maintained. However,
	// this is the only supported way to run the integration. If this ever
	// changes, this approach to saving the last run time needs to be reviewed.
	d.lastRun = Now().UTC()

	log.Debugf("listing pipelines...")
	defer log.Debugf("done listing pipelines")

	pipelineCounters := pipelineCounters{}
	updateCounters := updateCounters{}
	flowCounters := flowCounters{}

	all := d.w.ListPipelines(ctx)

	for all.HasNext(ctx) {
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

	attrs, err := makePipelineSummaryAttributes(
		ctx,
		d.w,
		&pipelineCounters,
		&updateCounters,
		&flowCounters,
		d.tags,
	)
	if err != nil {
		return err
	}

	writer <- model.Event{
		Type:       "DatabricksPipelineSummary",
		Timestamp:  Now().UnixMilli(),
		Attributes: attrs,
	}

	return nil
}

func (d *DatabricksPipelineMetricsReceiver) processPipelineEvents(
	ctx context.Context,
	pipelineStateInfo databricksSdkPipelines.PipelineStateInfo,
	lastRun time.Time,
	updateCounters *updateCounters,
	flowCounters *flowCounters,
	writer chan<- model.Event,
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

	allEvents := d.w.ListPipelineEventsWithDetails(
		ctx,
		d.startOffset,
		pipelineStateInfo.PipelineId,
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

	for allEvents.HasNext(ctx) {
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

		eventType := strings.ToLower(pipelineEvent.EventType)

		eventTimestamp, ok := shouldProcessEvent(
			&pipelineEvent,
			&pipelineStateInfo,
			eventType,
			endTime,
			oldUpdates,
		)
		if !ok {
			continue
		}

		origin := pipelineEvent.Origin
		updateId := origin.UpdateId
		details := pipelineEvent.Details

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
				flows[flowName(updateId, origin.FlowName)] = flow
			}
		}
	}

	addUpdates(
		ctx,
		d.w,
		updates,
		oldUpdates,
		effectiveLastRun,
		d.tags,
		writer,
	)

	updateUpdateCounters(updates, updateCounters)

	addFlows(
		ctx,
		d.w,
		flows,
		oldFlows,
		effectiveLastRun,
		d.tags,
		writer,
	)

	updateFlowCounters(flows, flowCounters)
}

func shouldProcessEvent(
	pipelineEvent *PipelineEvent,
	pipelineStateInfo *databricksSdkPipelines.PipelineStateInfo,
	eventType string,
	endTime time.Time,
	oldUpdates Set[string],
) (time.Time, bool) {
	// Short circuit early if this is not an event type we care about.
	if !isTargetEventType(eventType) {
		return time.Time{}, false
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
		return time.Time{}, false
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
		return time.Time{}, false
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
		return time.Time{}, false
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
		return time.Time{}, false
	}

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

		return time.Time{}, false
	}

	return eventTimestamp, true
}

func processCreateUpdate(
	pipelineEvent *PipelineEvent,
	origin *databricksSdkPipelines.Origin,
	updateId string,
	eventTimestamp time.Time,
	updates map[string]*updateData,
) *updateData {
	if log.IsDebugEnabled() {
		log.Debugf(
			"processing create_update event at time %s for update %s",
			pipelineEvent.Timestamp,
			updateId,
		)
	}

	update := getOrCreateUpdateData(updates, origin)

	update.creationTime = eventTimestamp

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
) *updateData {
	state := updateProgress.State

	var (
		waitStartTime    time.Time
		startTime        time.Time
		completionTime   time.Time
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
		waitStartTime = eventTimestamp
	} else if state == databricksSdkPipelines.UpdateInfoStateInitializing {
		// The Databricks update details UI tracks the overall update duration
		// and the update "Run time" separately. It starts tracking the run
		// time when the update INITIALIZING event is emitted.
		startTime = eventTimestamp
	}

	if log.IsDebugEnabled() {
		log.Debugf(
			"processing update_progress event at time %s for update %s with status %s",
			pipelineEvent.Timestamp,
			updateId,
			state,
		)
	}

	return getOrCreateUpdateDataForUpdateProgress(
		updates,
		origin,
		waitStartTime,
		startTime,
		completionTime,
		state,
	)
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
	if oldFlows.IsMember(flowName(origin.UpdateId, origin.FlowName)) {
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
		queueStartTime     time.Time
		planStartTime      time.Time
		startTime          time.Time
		completionTime     time.Time
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

			oldFlows.Add(flowName(origin.UpdateId, origin.FlowName))
			return nil
		}

		completionTime = eventTimestamp
	} else if status == FlowInfoStatusQueued {
		queueStartTime = eventTimestamp
	} else if status == FlowInfoStatusPlanning {
		planStartTime = eventTimestamp
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

	return getOrCreateFlowData(
		flows,
		flowProgress,
		origin,
		updateId,
		queueStartTime,
		planStartTime,
		startTime,
		completionTime,
		status,
	)
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

		if state == databricksSdkPipelines.UpdateInfoStateCreated {
			// Technically we never see this state because it is not on the
			// create_update event
			counters.created += 1
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

		if status == FlowInfoStatusIdle {
			counters.idle += 1
		} else if status == FlowInfoStatusPlanning {
			counters.planning += 1
		} else if status == FlowInfoStatusQueued {
			counters.queued += 1
		} else if status == FlowInfoStatusRunning {
			counters.running += 1
		}else if status == FlowInfoStatusStarting {
			counters.starting += 1
		}
	}
}

func makePipelineBaseAttributes(
	ctx context.Context,
	w DatabricksWorkspace,
	eventData *eventData,
	tags map[string]string,
) (map[string]interface{}, error) {
	attrs := makeAttributesMap(tags)

	workspaceInfo, err := GetWorkspaceInfo(ctx, w)
	if err != nil {
		return nil, err
	}

	// We store most of the attributes in this function using the databricks*
	// convention to enable facet linking from Spark metrics or other metrics
	// which use the databricks* naming convention.

	attrs["databricksWorkspaceId"] = workspaceInfo.Id
	attrs["databricksWorkspaceName"] = workspaceInfo.InstanceName
	attrs["databricksWorkspaceUrl"] = workspaceInfo.Url

	if eventData == nil {
		return attrs, nil
	}

	if eventData.clusterId != "" {
		clusterInfo, err := GetClusterInfoById(
			ctx,
			w,
			eventData.clusterId,
		)
		if err != nil {
			return nil, err
		}

		if clusterInfo != nil {
			attrs["databricksClusterId"] = eventData.clusterId
			attrs["databricksClusterName"] = clusterInfo.Name
			attrs["databricksClusterSource"] = clusterInfo.Source
			attrs["databricksClusterInstancePoolId"] =
				clusterInfo.InstancePoolId
		}
	}

	attrs["databricksPipelineId"] = eventData.pipelineId
	attrs["databricksPipelineUpdateId"] = eventData.updateId

	attrs["pipelineName"] = eventData.pipelineName

	return attrs, nil
}

func makePipelineSummaryAttributes(
	ctx context.Context,
	w DatabricksWorkspace,
	pipelineCounters *pipelineCounters,
	updateCounters *updateCounters,
	flowCounters *flowCounters,
	tags map[string]string,
) (map[string]interface{}, error) {
	attrs, err := makePipelineBaseAttributes(ctx, w, nil, tags)
	if err != nil {
		return nil, err
	}

	// Add pipeline counts
	attrs["deletedPipelineCount"] = pipelineCounters.deleted
	attrs["deployingPipelineCount"] = pipelineCounters.deploying
	attrs["failedPipelineCount"] = pipelineCounters.failed
	attrs["idlePipelineCount"] = pipelineCounters.idle
	attrs["recoveringPipelineCount"] = pipelineCounters.recovering
	attrs["resettingPipelineCount"] = pipelineCounters.resetting
	attrs["runningPipelineCount"] = pipelineCounters.running
	attrs["startingPipelineCount"] = pipelineCounters.starting
	attrs["stoppingPipelineCount"] = pipelineCounters.stopping

	// Add update counts
	attrs["createdUpdateCount"] = updateCounters.created
	attrs["initializingUpdateCount"] = updateCounters.initializing
	attrs["queuedUpdateCount"] = updateCounters.queued
	attrs["resettingUpdateCount"] = updateCounters.resetting
	attrs["runningUpdateCount"] = updateCounters.running
	attrs["settingUpTablesUpdateCount"] = updateCounters.settingUpTables
	attrs["stoppingUpdateCount"] = updateCounters.stopping
	attrs["waitingForResourcesUpdateCount"] = updateCounters.waitingForResources

	// Add flow counts
	attrs["idleFlowCount"] = flowCounters.idle
	attrs["planningFlowCount"] = flowCounters.planning
	attrs["queuedFlowCount"] = flowCounters.queued
	attrs["runningFlowCount"] = flowCounters.running
	attrs["startingFlowCount"] = flowCounters.starting

	return attrs, nil
}

func makePipelineUpdateBaseAttributes(
	ctx context.Context,
	w DatabricksWorkspace,
	event string,
	update *updateData,
	tags map[string]string,
) (map[string]interface{}, error) {
	attrs, err := makePipelineBaseAttributes(ctx, w, &update.eventData, tags)
	if err != nil {
		return nil, err
	}

	attrs["event"] = event

	// creationTime is guaranteed to be set when this function is called by both
	// makePipelineUpdateStartAttributes and
	// makePipelineUpdateCompleteAttributes because they are only called by
	// addUpdates when creationTime is non-zero.
	attrs["creationTime"] = update.creationTime.UnixMilli()

	return attrs, nil
}

func makePipelineUpdateStartAttributes(
	ctx context.Context,
	w DatabricksWorkspace,
	update *updateData,
	tags map[string]string,
) (map[string]interface{}, error) {
	return makePipelineUpdateBaseAttributes(ctx, w, "start", update, tags)
}

func makePipelineUpdateCompleteAttributes(
	ctx context.Context,
	w DatabricksWorkspace,
	update *updateData,
	tags map[string]string,
) (map[string]interface{}, error) {
	attrs, err := makePipelineUpdateBaseAttributes(
		ctx,
		w,
		"complete",
		update,
		tags,
	)
	if err != nil {
		return nil, err
	}

	attrs["status"] = string(update.state)

	// A note on times and durations:
	//
	// Updates always have a creation and completion time and both are
	// guaranteed to be set when this function is called because it is only
	// called by addUpdates when creationTime is non-zero and completionTime is
	// greater than last run. The update duration is always completionTime -
	// creationTime. Note that the creationTime attribute is set in
	// makePipelineUpdateBaseAttributes.
	//
	// Updates include a wait start time only if a WAITING_FOR_RESOURCES event
	// was seen. If it was seen, the duration is startTime - waitStartTime if an
	// INITIALIZING event was seen or completionTime - waitStartTime if not. In
	// the latter case, the update must have gone straight from
	// WAITING_FOR_RESOURCES to to a terminal state like FAILED or CANCELLED.
	// If no WAITING_FOR_RESOURCES event was seen at all, the update didn't have
	// to wait and neither the wait start time nor the wait duration attribute
	// are set.
	//
	// Updates include a start time only if an INITIALIZING event was seen. If
	// it was seen, the run duration is completionTime - startTime. Otherwise,
	// neither the start time nor the run duration attribute are set.

	attrs["completionTime"] = update.completionTime.UnixMilli()
	attrs["duration"] = int64(
		update.completionTime.Sub(update.creationTime) / time.Millisecond)

	if !update.waitStartTime.IsZero() {
		// waitStartTime is non-zero so we saw a WAITING_FOR_RESOURCES event
		attrs["waitStartTime"] = update.waitStartTime.UnixMilli()
		// waitDuration is set to startTime - waitStartTime or
		// completionTime - waitStartTime
		attrs["waitDuration"] = calcUpdateWaitDuration(update)
	}

	if !update.startTime.IsZero() {
		// startTime is non-zero so we saw an INITIALIZING event
		attrs["startTime"] = update.startTime.UnixMilli()
		// runDuration is set to completionTime - startTime
		attrs["runDuration"] = calcUpdateRunDuration(update)
	}

	return attrs, nil
}

func makePipelineFlowBaseAttributes(
	ctx context.Context,
	w DatabricksWorkspace,
	event string,
	flow *flowData,
	tags map[string]string,
) (map[string]interface{}, error) {
	attrs, err := makePipelineBaseAttributes(ctx, w, &flow.eventData, tags)
	if err != nil {
		return nil, err
	}

	attrs["event"] = event

	// We store the flow ID and flow name using the databricks* convention
	// to enable facet linking from Spark metrics or other metrics which use the
	// databricks* naming convention.
	attrs["databricksPipelineFlowId"] = flow.id
	attrs["databricksPipelineFlowName"] = flow.name

	attrs["queueStartTime"] = flow.queueStartTime.UnixMilli()

	return attrs, nil
}

func makePipelineFlowStartAttributes(
	ctx context.Context,
	w DatabricksWorkspace,
	flow *flowData,
	tags map[string]string,
) (map[string]interface{}, error) {
	return makePipelineFlowBaseAttributes(ctx, w, "start", flow, tags)
}

func makePipelineFlowCompleteAttributes(
	ctx context.Context,
	w DatabricksWorkspace,
	flow *flowData,
	tags map[string]string,
) (map[string]interface{}, error) {
	attrs, err := makePipelineFlowBaseAttributes(
		ctx,
		w,
		"complete",
		flow,
		tags,
	)
	if err != nil {
		return nil, err
	}

	attrs["status"] = string(flow.status)

	// A note on times and durations:
	//
	// Flows always have a queueStartTime and completionTime and both are
	// guaranteed to be set when this function is called because it is only
	// called by addFlows when queueStartTime is non-zero and completionTime is
	// greater than last run.
	//
	// The flow duration is set to completionTime - startTime and the flow start
	// time is set _only_ if a STARTING event was seen. If a STARTING event was
	// not seen, the flow must have gone straight from QUEUED or PLANNING to a
	// terminal state like FAILED or SKIPPED. In this case, neither the flow
	// start time nor the flow duration are set since it never started running.
	//
	// A queue duration is always set. It is set to
	// planStartTime - queueStartTime if a PLANNING event was seen,
	// startTime - queueStartTime if a STARTING event was seen, or
	// completionTime - queueStartTime if neither the STARTING nor PLANNING
	// events were seen. Note that the queueStartTime attribute is set in
	// makePipelineFlowBaseAttributes.
	//
	// Flows have a planStartTime if a PLANNING event was seen. If it was seen,
	// the planning duration is startTime - planStartTime if a STARTING event
	// was seen or completionTime - planStartTime if not. In the latter case,
	// the flow must have gone straight from PLANNING to to a terminal state
	// like FAILED or SKIPPED. If no PLANNING event was seen at all, the flow
	// didn't have to plan so neither the plan start time nor the plan duration
	// are set.

	attrs["completionTime"] = flow.completionTime.UnixMilli()

	// queueDuration is set to planStartTime - queueStartTime,
	// startTime - queueStartTime, or completionTime - queueStartTime
	attrs["queueDuration"] = calcFlowQueueDuration(flow)

	if !flow.planStartTime.IsZero() {
		// planStartTime is non-zero so we saw a PLANNING event
		attrs["planStartTime"] = flow.planStartTime.UnixMilli()
		// planDuration is set to startTime - planStartTime if a STARTING event
		// was seen or completionTime - planStartTime if not.
		attrs["planDuration"] = calcFlowPlanDuration(flow)
	}

	if !flow.startTime.IsZero() {
		// startTime is non-zero so we saw a STARTING event
		attrs["startTime"] = flow.startTime.UnixMilli()
		// duration is set to completionTime - startTime
		attrs["duration"] = int64(
			flow.completionTime.Sub(flow.startTime) / time.Millisecond)
	}

	// Record flow metrics
	setFlowMetrics(flow, attrs)

	return attrs, nil
}

func calcUpdateWaitDuration(update *updateData) int64 {
	// waitStartTime is guaranteed to be non-zero when this function is called
	// because it is only called by makePipelineUpdateCompleteAttributes when
	// waitStartTime is non-zero.

	if !update.startTime.IsZero() {
		// If startTime is non-zero, the update made it to INITIALIZING
		// after WAITING_FOR_RESOURCES. Record waiting duration as
		// startTime - waitStartTime.
		return int64(
			update.startTime.Sub(update.waitStartTime) /
				time.Millisecond)
	}

	// completionTime is guaranteed to be non-zero when this function is called
	// because it is only called from makePipelineUpdateCompleteAttributes which
	// is only called when a completionTime occurs after the last run time.
	//
	// If startTime is zero, the update went from WAITING_FOR_RESOURCES to a
	// terminal state like FAILED or CANCELED.
	//
	// Return waiting duration as completionTime - waitStartTime.
	return int64(
		update.completionTime.Sub(update.waitStartTime) /
			time.Millisecond)
}

func calcUpdateRunDuration(update *updateData) int64 {
	// startTime is guaranteed to be non-zero when this function is called
	// because it is only called by makePipelineUpdateCompleteAttributes when
	// startTime is non-zero.

	// completionTime is guaranteed to be non-zero when this function is called
	// because it is only called from makePipelineUpdateCompleteAttributes which
	// is only called when a completionTime occurs after the last run time.
	//
	// Record run duration as completionTime - startTime.
	return int64(
		update.completionTime.Sub(update.startTime) /
			time.Millisecond)
}

func calcFlowQueueDuration(flow *flowData) int64 {
	// queueStartTime is guaranteed to be non-zero when this function is called
	// because it is only called by makePipelineFlowCompleteAttributes which
	// is only called by addFlows when queueStartTime is non-zero.
	if !flow.planStartTime.IsZero() {
		// If planningTime is non-zero, the flow made it to PLANNING after
		// QUEUED. Record queue duration as planStartTime - queueStartTime.
		return int64(flow.planStartTime.Sub(flow.queueStartTime) /
			time.Millisecond)
	} else if !flow.startTime.IsZero() {
		// If startTime is non-zero, the flow went from QUEUED to STARTING
		// (the flow did not do any PLANNING). Record queue duration as
		// startTime - queueStartTime.
		return int64(flow.startTime.Sub(flow.queueStartTime) /
			time.Millisecond)
	}

	// completionTime is guaranteed to be non-zero when this function is called
	// because it is only called from makePipelineFlowCompleteAttributes which
	// is only called when a completionTime occurs after the last run time.
	//
	// Record run duration as completionTime - queueStartTime.
	return int64(flow.completionTime.Sub(flow.queueStartTime) /
		time.Millisecond)
}

func calcFlowPlanDuration(flow *flowData) int64 {
	// planStartTime is guaranteed to be non-zero when this function is called
	// because it is only called by makePipelineFlowCompleteAttributes when
	// planStartTime is non-zero.
	if !flow.startTime.IsZero() {
		// If startTime is non-zero, the flow made it to STARTING after
		// PLANNING. Record plan duration as startTime - planStartTime.
		return int64(flow.startTime.Sub(flow.planStartTime) /
			time.Millisecond)
	}

	// completionTime is guaranteed to be non-zero when this function is called
	// because it is only called from makePipelineFlowCompleteAttributes which
	// is only called when a completionTime occurs after the last run time.
	//
	// Record run duration as completionTime - planStartTime.
	return int64(flow.completionTime.Sub(flow.planStartTime) /
		time.Millisecond)
}

func setFlowMetrics(
	flow *flowData,
	attrs map[string]interface{},
) {
	// Record any flow metrics we found. These will only be non-nil if we
	// saw a flow_progress termination event AND we saw the corresponding
	// metrics on that event.

	if flow.backlogBytes != nil {
		attrs["backlogBytes"] = *flow.backlogBytes
	}

	if flow.backlogFiles != nil {
		attrs["backlogFileCount"] = *flow.backlogFiles
	}

	if flow.numOutputRows != nil {
		attrs["outputRowCount"] = *flow.numOutputRows
	}

	if flow.droppedRecords != nil {
		attrs["droppedRecordCount"] = *flow.droppedRecords
	}
}

func addFlowExpectations(
	ctx context.Context,
	w DatabricksWorkspace,
	flow *flowData,
	tags map[string]string,
	writer chan<- model.Event,
) error {
	for _, expectation := range flow.expectations {
		attrs, err := makePipelineBaseAttributes(
			ctx,
			w,
			&flow.eventData,
			tags,
		)
		if err != nil {
			return err
		}

		attrs["databricksPipelineFlowId"] = flow.id
		attrs["databricksPipelineFlowName"] = flow.name

		attrs["name"] = expectation.Name
		attrs["dataset"] = expectation.Dataset
		attrs["passedRecordCount"] = expectation.PassedRecords
		attrs["failedRecordCount"] = expectation.FailedRecords

		writer <- model.Event{
			Type:       "DatabricksPipelineFlowExpectation",
			Timestamp:  Now().UnixMilli(),
			Attributes: attrs,
		}
	}

	return nil
}

func addUpdates(
	ctx context.Context,
	w DatabricksWorkspace,
	updates map[string]*updateData,
	oldUpdates map[string]struct{},
	lastRun time.Time,
	tags map[string]string,
	writer chan<- model.Event,
) error {
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

		// If the update startTime is zero, we never saw the create_update event
		// so report nothing since we would not be able to properly calculate
		// the update duration and also means we didn't see the entire update.
		// This probably means the create_update event occurred before the start
		// of the time range used to query the pipeline event logs.
		if update.creationTime.IsZero() {
			continue
		}

		// If the start time is greater than the last run time, this update
		// started since the last run of the integration so write out a start
		// event
		if update.creationTime.After(lastRun) {
			attrs, err := makePipelineUpdateStartAttributes(
				ctx,
				w,
				update,
				tags,
			)
			if err != nil {
				return err
			}

			writer <- model.Event{
				Type:       "DatabricksPipelineUpdate",
				Timestamp:  Now().UnixMilli(),
				Attributes: attrs,
			}
		}

		// If the completion time is greater than the last run time, this update
		// completed since the last run of the integration so write out a
		// complete event
		if update.completionTime.After(lastRun) {
			attrs, err := makePipelineUpdateCompleteAttributes(
				ctx,
				w,
				update,
				tags,
			)
			if err != nil {
				return err
			}

			writer <- model.Event{
				Type:       "DatabricksPipelineUpdate",
				Timestamp:  Now().UnixMilli(),
				Attributes: attrs,
			}
		}
	}

	return nil
}

func addFlows(
	ctx context.Context,
	w DatabricksWorkspace,
	flows map[string]*flowData,
	oldFlows map[string]struct{},
	lastRun time.Time,
	tags map[string]string,
	writer chan<- model.Event,
) error {
	for _, flow := range flows {
		// Double check that we aren't ignoring this flow. This handles cases
		// where flow events may have come in out of order. For example, we got
		// a RUNNING event before a termination event. This shouldn't happen
		// since the API returns events in descending order by timestamp but
		// just in case, this prevents duplicate data.
		_, ok := oldFlows[flowName(flow.eventData.updateId, flow.name)]
		if ok {
			continue
		}

		// If the update startTime is zero, we never saw the QUEUED
		// flow_progress event so report nothing since we would not be able to
		// properly calculate the flow duration and also means we didn't see
		// the entire flow. This probably means the QUEUED flow_progress event
		// occurred before the start of the time range used to query the
		// pipeline event logs.
		if flow.queueStartTime.IsZero() {
			continue
		}

		// If the queue start time is greater than the last run time, this flow
		// was queued since the last run of the integration so write out a start
		// event
		if flow.queueStartTime.After(lastRun) {
			attrs, err := makePipelineFlowStartAttributes(ctx, w, flow, tags)
			if err != nil {
				return err
			}

			writer <- model.Event{
				Type:       "DatabricksPipelineFlow",
				Timestamp:  Now().UnixMilli(),
				Attributes: attrs,
			}
		}

		// If the completion time is greater than the last run time, this update
		// completed since the last run of the integration so write out a
		// complete event
		if flow.completionTime.After(lastRun) {
			attrs, err := makePipelineFlowCompleteAttributes(
				ctx,
				w,
				flow,
				tags,
			)
			if err != nil {
				return err
			}

			writer <- model.Event{
				Type:       "DatabricksPipelineFlow",
				Timestamp:  Now().UnixMilli(),
				Attributes: attrs,
			}

			// If there were expectations for the flow, record expectation
			// metrics
			err = addFlowExpectations(ctx, w, flow, tags, writer)
			if err != nil {
				return err
			}
		}
	}

	return nil
}
