package databricks

import (
	"context"
	"fmt"
	"time"

	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration"
	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/log"
	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/model"
)

const (
	RFC_3339_MILLI_LAYOUT = "2006-01-02T15:04:05.000Z07:00"
)

type DatabricksPipelineEventsReceiver struct {
	i							*integration.LabsIntegration
	w							DatabricksWorkspace
	tags 						map[string]string
}

func NewDatabricksPipelineEventsReceiver(
	i *integration.LabsIntegration,
	w DatabricksWorkspace,
	tags map[string]string,
) *DatabricksPipelineEventsReceiver {
	return &DatabricksPipelineEventsReceiver{
		i,
		w,
		tags,
	}
}

func (d *DatabricksPipelineEventsReceiver) GetId() string {
	return "databricks-pipeline-events-receiver"
}

func (d *DatabricksPipelineEventsReceiver) PollLogs(
	ctx context.Context,
	writer chan <- model.Log,
) error {
	lastRun := time.Now().Add(-d.i.Interval * time.Second)

	all := d.w.ListPipelines(ctx)

	LOOP:

	for ; all.HasNext(ctx);  {
		pipelineStateInfo, err := all.Next(ctx)
		if err != nil {
			return err
		}

		log.Debugf(
			"processing pipeline events for pipeline %s (%s) with state %s",
			pipelineStateInfo.PipelineId,
			pipelineStateInfo.Name,
			pipelineStateInfo.State,
		)

		allEvents := d.w.ListPipelineEvents(
			ctx,
			lastRun,
			pipelineStateInfo.PipelineId,
		)

		count := 0

		for ; allEvents.HasNext(ctx);  {
			pipelineEvent, err := allEvents.Next(ctx)
			if err != nil {
				log.Warnf(
					"unexpected no more items error or failed to fetch next page of pipeline events for pipeline %s (%s): %v",
					pipelineStateInfo.PipelineId,
					pipelineStateInfo.Name,
					err,
				)
				continue LOOP
			}

			// eventTimestamp is not used until the end of the loop but by
			// parsing here we can abort if an error occurs and skip allocating
			// and populating the attributes map.

			eventTimestamp, err := time.Parse(
				RFC_3339_MILLI_LAYOUT,
				pipelineEvent.Timestamp,
			)
			if err != nil {
				log.Warnf(
					"ignoring event with ID %s with invalid timestamp %s for pipeline %s (%s): %v",
					pipelineEvent.Id,
					pipelineEvent.Timestamp,
					pipelineStateInfo.PipelineId,
					pipelineStateInfo.Name,
					err,
				)
				continue
			}

			attrs := makeAttributesMap(d.tags)

			attrs["databricksPipelineEventId"] = pipelineEvent.Id
			attrs["databricksPipelineEventType"] = pipelineEvent.EventType
			attrs["level"] = pipelineEvent.Level
			attrs["databricksPipelineEventLevel"] = pipelineEvent.Level
			attrs["databricksPipelineEventMaturityLevel"] =
				pipelineEvent.MaturityLevel

			isError := pipelineEvent.Error != nil

			attrs["databricksPipelineEventError"] = isError

			if isError {
				attrs["databricksPipelineEventErrorFatal"] =
					pipelineEvent.Error.Fatal

				if len(pipelineEvent.Error.Exceptions) > 0 {
					for i, e := range pipelineEvent.Error.Exceptions {
						attrName := fmt.Sprintf(
							"databricksPipelineEventErrorException%dClassName",
							i + 1,
						)
						attrs[attrName] = e.ClassName

						attrName = fmt.Sprintf(
							"databricksPipelineEventErrorException%dMessage",
							i + 1,
						)
						attrs[attrName] = e.Message
					}
				}
			}

			if pipelineEvent.Origin != nil {
				origin := pipelineEvent.Origin

				attrs["databricksPipelineFlowBatchId"] = origin.BatchId
				attrs["databricksCloud"] = origin.Cloud
				attrs["databricksClusterId"] = origin.ClusterId
				attrs["databricksPipelineDatasetName"] = origin.DatasetName
				attrs["databricksPipelineFlowId"] = origin.FlowId
				attrs["databricksPipelineFlowName"] = origin.FlowName
				attrs["databricksPipelineEventHost"] = origin.Host
				attrs["databricksPipelineMaintenanceId"] = origin.MaintenanceId
				attrs["databricksPipelineMaterializationName"] =
					origin.MaterializationName
				attrs["databricksOrgId"] = origin.OrgId
				attrs["databricksPipelineId"] = origin.PipelineId
				attrs["databricksPipelineName"] = origin.PipelineName
				attrs["databricksCloudRegion"] = origin.Region
				attrs["databricksPipelineRequestId"] = origin.RequestId
				attrs["databricksDeltaTableId"] = origin.TableId
				attrs["databricksUcResourceId"] = origin.UcResourceId
				attrs["databricksPipelineUpdateId"] = origin.UpdateId
			}

			writer <- model.NewLog(
				pipelineEvent.Message,
				attrs,
				eventTimestamp,
			)

			count += 1
		}

		log.Debugf("received %d pipeline events", count)
	}

	return nil
}
