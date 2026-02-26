package databricks

import (
	"context"
	"net/url"
	"time"

	databricksSdk "github.com/databricks/databricks-sdk-go"
	databricksSdkConfig "github.com/databricks/databricks-sdk-go/config"
	databricksSdkListing "github.com/databricks/databricks-sdk-go/listing"
	databricksSdkCompute "github.com/databricks/databricks-sdk-go/service/compute"
	databricksSdkJobs "github.com/databricks/databricks-sdk-go/service/jobs"
	databricksSdkPipelines "github.com/databricks/databricks-sdk-go/service/pipelines"
	databricksSdkSql "github.com/databricks/databricks-sdk-go/service/sql"
)

var (
    // NewDatabricksWorkspace is exposed like this for dependency injection
	// purposes to enable mocking of the Databricks workspace in tests.
    NewDatabricksWorkspace = newDatabricksWorkspaceImpl
)

type DatabricksWorkspace interface {
	GetConfig() *databricksSdkConfig.Config

    GetCurrentWorkspaceId(ctx context.Context) (int64, error)

    GetClusterById(
        ctx context.Context,
        clusterId string,
    ) (*databricksSdkCompute.ClusterDetails, error)

    GetWarehouseById(
        ctx context.Context,
        warehouseId string,
    ) (*databricksSdkSql.GetWarehouseResponse, error)

    ListClusters(ctx context.Context) (
        databricksSdkListing.Iterator[databricksSdkCompute.ClusterDetails],
    )

    ListWarehouses(ctx context.Context) (
        databricksSdkListing.Iterator[databricksSdkSql.EndpointInfo],
    )

    ListJobRuns(ctx context.Context, startOffset time.Duration) (
        databricksSdkListing.Iterator[databricksSdkJobs.BaseRun],
    )

    ListPipelines(ctx context.Context) (
        databricksSdkListing.Iterator[databricksSdkPipelines.PipelineStateInfo],
    )

    ListPipelineEvents(
        ctx context.Context,
        lastRun time.Time,
        pipelineId string,
    ) (
        databricksSdkListing.Iterator[databricksSdkPipelines.PipelineEvent],
    )

    ListPipelineEventsWithDetails(
        ctx context.Context,
        startOffset time.Duration,
        pipelineId string,
    ) (
        databricksSdkListing.Iterator[PipelineEvent],
    )

    ListQueries(
        ctx context.Context,
        maxResults int,
        startTimeMs int64,
        endTimeMs int64,
        nextPageToken string,
    ) ([]databricksSdkSql.QueryInfo, bool, string, error)

    ExecuteStatement(
        ctx context.Context,
        warehouseId string,
        defaultCatalog string,
        defaultSchema string,
        stmt string,
        params []databricksSdkSql.StatementParameterListItem,
    ) (*databricksSdkSql.StatementResponse, error)

    GetStatementByStatementId(ctx context.Context, statementId string) (
        *databricksSdkSql.StatementResponse,
        error,
    )

    GetStatementResultChunkNByStatementIdAndChunkIndex(
        ctx context.Context,
        statementId string,
        chunkIndex int,
    ) (*databricksSdkSql.ResultData, error)

    CancelExecution(ctx context.Context, statementId string) error
}

type databricksWorkspaceImpl struct {
    w *databricksSdk.WorkspaceClient
    pipelineEventClient *pipelineEventClient
}

func newDatabricksWorkspaceImpl() (DatabricksWorkspace, error) {
    w, err := databricksSdk.NewWorkspaceClient(newDatabricksSdkConfig())
	if err != nil {
		return nil, err
	}

    pipelineEventClient, err := newPipelineEventClient(w)
	if err != nil {
		return nil, err
	}

    return &databricksWorkspaceImpl{
        w: w,
        pipelineEventClient: pipelineEventClient,
    }, nil
}

func (d *databricksWorkspaceImpl) GetConfig() *databricksSdkConfig.Config {
    return d.w.Config
}

func (d *databricksWorkspaceImpl) GetCurrentWorkspaceId(
    ctx context.Context,
) (int64, error) {
    return d.w.CurrentWorkspaceID(ctx)
}

func (d *databricksWorkspaceImpl) GetClusterById(
    ctx context.Context,
    clusterId string,
) (*databricksSdkCompute.ClusterDetails, error) {
    return d.w.Clusters.GetByClusterId(ctx, clusterId)
}

func (d *databricksWorkspaceImpl) GetWarehouseById(
    ctx context.Context,
    warehouseId string,
) (*databricksSdkSql.GetWarehouseResponse, error) {
    return d.w.Warehouses.GetById(ctx, warehouseId)
}

func (d *databricksWorkspaceImpl) ListClusters(ctx context.Context) (
    databricksSdkListing.Iterator[databricksSdkCompute.ClusterDetails],
) {
    return d.w.Clusters.List(
		ctx,
		databricksSdkCompute.ListClustersRequest{ PageSize: 100 },
	)
}

func (d *databricksWorkspaceImpl) ListWarehouses(ctx context.Context) (
    databricksSdkListing.Iterator[databricksSdkSql.EndpointInfo],
) {
    return d.w.Warehouses.List(
		ctx,
		databricksSdkSql.ListWarehousesRequest{},
	)
}

func (d *databricksWorkspaceImpl) ListJobRuns(
    ctx context.Context,
    startOffset time.Duration,
) (
    databricksSdkListing.Iterator[databricksSdkJobs.BaseRun],
) {
    listRunsRequest := databricksSdkJobs.ListRunsRequest{
		StartTimeFrom: time.Now().Add(-startOffset).UnixMilli(),
		ExpandTasks: true,
	}

	return d.w.Jobs.ListRuns(ctx, listRunsRequest)
}


func (d *databricksWorkspaceImpl) ListPipelines(ctx context.Context) (
    databricksSdkListing.Iterator[databricksSdkPipelines.PipelineStateInfo],
) {
    return d.w.Pipelines.ListPipelines(
        ctx,
        databricksSdkPipelines.ListPipelinesRequest{},
    )
}


func (d *databricksWorkspaceImpl) ListPipelineEvents(
    ctx context.Context,
    lastRun time.Time,
    pipelineId string,
) (
    databricksSdkListing.Iterator[databricksSdkPipelines.PipelineEvent],
) {
    return d.w.Pipelines.ListPipelineEvents(
			ctx,
			databricksSdkPipelines.ListPipelineEventsRequest{
				Filter: "timestamp >= '" +
					lastRun.UTC().Format(RFC_3339_MILLI_LAYOUT) +
					"'",
				PipelineId: pipelineId,
			},
		)
}

func (d *databricksWorkspaceImpl) ListPipelineEventsWithDetails(
        ctx context.Context,
        startOffset time.Duration,
        pipelineId string,
) (
    databricksSdkListing.Iterator[PipelineEvent],
) {
    return d.pipelineEventClient.listPipelineEvents(
		ctx,
		databricksSdkPipelines.ListPipelineEventsRequest{
			Filter: "timestamp >= '" +
				time.Now().Add(-startOffset).UTC().Format(
					RFC_3339_MILLI_LAYOUT,
				) +
				"'",
			PipelineId: pipelineId,
		},
	)
}

func (d *databricksWorkspaceImpl) ListQueries(
    ctx context.Context,
    maxResults int,
    startTimeMs int64,
    endTimeMs int64,
    nextPageToken string,
) ([]databricksSdkSql.QueryInfo, bool, string, error) {
    request := databricksSdkSql.ListQueryHistoryRequest{
        MaxResults: maxResults,
        IncludeMetrics: true,
    }

    if nextPageToken != "" {
        // It's unclear if the time range needs to be added when using
        // the page token. However, it doesn't seem like it would be
        // necessary so we'll leave it out for now.
        request.PageToken = url.QueryEscape(nextPageToken)
    } else {
        request.FilterBy = &databricksSdkSql.QueryFilter{
            QueryStartTimeRange: &databricksSdkSql.TimeRange{
                StartTimeMs: startTimeMs,
                EndTimeMs: endTimeMs,
            },
        }
    }

    response, err := d.w.QueryHistory.List(ctx, request)
    if err != nil {
        return nil, false, "", err
    }

    return response.Res, response.HasNextPage, response.NextPageToken, nil
}

func (d *databricksWorkspaceImpl) ExecuteStatement(
    ctx context.Context,
    warehouseId string,
    defaultCatalog string,
    defaultSchema string,
    stmt string,
    params []databricksSdkSql.StatementParameterListItem,
) (*databricksSdkSql.StatementResponse, error) {
    req := databricksSdkSql.ExecuteStatementRequest{
		WarehouseId: warehouseId,
		Catalog: defaultCatalog,
		Schema: defaultSchema,
		Statement: stmt,
		WaitTimeout: "0s",
		Parameters: params,
	}

	return d.w.StatementExecution.ExecuteStatement(ctx, req)
}

func (d *databricksWorkspaceImpl) GetStatementByStatementId(
    ctx context.Context,
    statementId string,
) (*databricksSdkSql.StatementResponse, error) {
    return d.w.StatementExecution.GetStatementByStatementId(
		ctx,
		statementId,
	)
}

func (d *databricksWorkspaceImpl) GetStatementResultChunkNByStatementIdAndChunkIndex(
    ctx context.Context,
    statementId string,
    chunkIndex int,
) (*databricksSdkSql.ResultData, error) {
    return d.w.StatementExecution.GetStatementResultChunkNByStatementIdAndChunkIndex(
        ctx,
        statementId,
        chunkIndex,
    )
}

func (d *databricksWorkspaceImpl) CancelExecution(
    ctx context.Context,
    statementId string,
) error {
    return d.w.StatementExecution.CancelExecution(
        ctx,
        databricksSdkSql.CancelExecutionRequest{
            StatementId: statementId,
        },
    )
}
