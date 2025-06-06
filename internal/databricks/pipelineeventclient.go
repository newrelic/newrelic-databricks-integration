package databricks

import (
	"context"
	"fmt"
	"net/http"

	databricksSdk "github.com/databricks/databricks-sdk-go"
	databricksSdkClient "github.com/databricks/databricks-sdk-go/client"
	databricksSdkListing "github.com/databricks/databricks-sdk-go/listing"
	databricksSdkMarshal "github.com/databricks/databricks-sdk-go/marshal"
	databricksSdkPipelines "github.com/databricks/databricks-sdk-go/service/pipelines"
	databricksSdkUserAgent "github.com/databricks/databricks-sdk-go/useragent"
)

// This file contains a workaround for the lack of pipeline events with the
// details field in the Databricks SDK. The details field is available in the
// API but is not exposed in the PipelineEvent struct. We workaround this by
// recreating the PipelineEvent struct to include the details field and
// recreating several of the functions that are used in the pipelines service
// package.
type pipelineEventClient struct {
	client *databricksSdkClient.DatabricksClient
}

func newPipelineEventClient(w *databricksSdk.WorkspaceClient) (*pipelineEventClient, error) {
	cfg := w.Config

	// In order to support pipeline events with the details field (until the SDK
	// supports it), we need a DatabricksClient that can be used by our copy of
	// internalListPipelineEvents (see below).
	//
	// Since the WorkspaceClient does not expose the DatabricksClient that it
	// uses, we need to create our own DatabricksClient.
	//
	// We do so using the following 10 lines which are taken from the
	// WorkspaceClient code at https://github.com/databricks/databricks-sdk-go/blob/v0.60.0/workspace_client.go#L1161
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

// Below temporarily copied from
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
	return databricksSdkMarshal.Unmarshal(b, s)
}

func (s ListPipelineEventsResponse) MarshalJSON() ([]byte, error) {
	return databricksSdkMarshal.Marshal(s)
}

// Below temporarily copied from
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

func (a *pipelineEventClient) listPipelineEvents(_ context.Context, request databricksSdkPipelines.ListPipelineEventsRequest) databricksSdkListing.Iterator[PipelineEvent] {
	getNextPage := func(ctx context.Context, req databricksSdkPipelines.ListPipelineEventsRequest) (*ListPipelineEventsResponse, error) {
		ctx = databricksSdkUserAgent.InContext(ctx, "sdk-feature", "pagination")
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
	iterator := databricksSdkListing.NewIterator(
		&request,
		getNextPage,
		getItems,
		getNextReq)
	return iterator
}
