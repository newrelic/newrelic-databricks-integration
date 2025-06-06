package databricks

import (
	"context"
	"time"

	databricksSdkConfig "github.com/databricks/databricks-sdk-go/config"
	databricksSdkListing "github.com/databricks/databricks-sdk-go/listing"
	databricksSdkCompute "github.com/databricks/databricks-sdk-go/service/compute"
	databricksSdkJobs "github.com/databricks/databricks-sdk-go/service/jobs"
	databricksSdkPipelines "github.com/databricks/databricks-sdk-go/service/pipelines"
	databricksSdkSql "github.com/databricks/databricks-sdk-go/service/sql"
)

type MockWorkspace struct {
    Config                                                 databricksSdkConfig.Config
    WorkspaceId                                            int64
    GetConfigFunc                                          func() (
        *databricksSdkConfig.Config,
    )
    GetCurrentWorkspaceIdFunc                              func(
        ctx context.Context,
    ) (int64, error)
    ListClustersFunc                                       func(
        ctx context.Context,
    ) databricksSdkListing.Iterator[databricksSdkCompute.ClusterDetails]
    ListWarehousesFunc                                     func(
        ctx context.Context,
    ) databricksSdkListing.Iterator[databricksSdkSql.EndpointInfo]
    ListJobRunsFunc                                        func(
        ctx context.Context,
        startOffset time.Duration,
    ) databricksSdkListing.Iterator[databricksSdkJobs.BaseRun]
    ListPipelinesFunc                                      func(
        ctx context.Context,
    ) databricksSdkListing.Iterator[databricksSdkPipelines.PipelineStateInfo]
    ListPipelineEventsFunc                                 func(
        ctx context.Context,
        lastRun time.Time,
        pipelineId string,
    ) databricksSdkListing.Iterator[databricksSdkPipelines.PipelineEvent]
    ListPipelineEventsWithDetailsFunc                      func(
        ctx context.Context,
        startOffset time.Duration,
        pipelineId string,
    ) databricksSdkListing.Iterator[PipelineEvent]
    ListQueriesFunc                                        func(
        ctx context.Context,
        maxResults int,
        startTimeMs int64,
        endTimeMs int64,
        nextPageToken string,
    ) ([]databricksSdkSql.QueryInfo, bool, string, error)
    ExecuteStatementFunc                                   func(
        ctx context.Context,
        warehouseId string,
        defaultCatalog string,
        defaultSchema string,
        stmt string,
        params []databricksSdkSql.StatementParameterListItem,
    ) (*databricksSdkSql.StatementResponse, error)
    GetStatementByStatementIdFunc                          func(
        ctx context.Context,
        statementId string,
    ) (*databricksSdkSql.StatementResponse, error)
    GetStatementResultChunkNByStatementIdAndChunkIndexFunc func(
        ctx context.Context,
        statementId string,
        chunkIndex int,
    ) (*databricksSdkSql.ResultData, error)
    CancelExecutionFunc                                    func(
        ctx context.Context,
        statementId string,
    ) error
}

func (d *MockWorkspace) GetConfig() *databricksSdkConfig.Config {
    if d.GetConfigFunc != nil {
        return d.GetConfigFunc()
    }

    return &d.Config
}

func (d *MockWorkspace) GetCurrentWorkspaceId(
    ctx context.Context,
) (int64, error) {
    if d.GetCurrentWorkspaceIdFunc != nil {
        return d.GetCurrentWorkspaceIdFunc(ctx)
    }

    return d.WorkspaceId, nil
}

func (d *MockWorkspace) ListClusters(ctx context.Context) (
    databricksSdkListing.Iterator[databricksSdkCompute.ClusterDetails],
) {
    if d.ListClustersFunc != nil {
        return d.ListClustersFunc(ctx)
    }

    return nil
}

func (d *MockWorkspace) ListWarehouses(ctx context.Context) (
    databricksSdkListing.Iterator[databricksSdkSql.EndpointInfo],
) {
    if d.ListWarehousesFunc != nil {
        return d.ListWarehousesFunc(ctx)
    }

    return nil
}

func (d *MockWorkspace) ListJobRuns(
    ctx context.Context,
    startOffset time.Duration,
) (
    databricksSdkListing.Iterator[databricksSdkJobs.BaseRun],
) {
    if d.ListJobRunsFunc != nil {
        return d.ListJobRunsFunc(ctx, startOffset)
    }

    return nil
}


func (d *MockWorkspace) ListPipelines(ctx context.Context) (
    databricksSdkListing.Iterator[databricksSdkPipelines.PipelineStateInfo],
) {
    if d.ListPipelinesFunc != nil {
        return d.ListPipelinesFunc(ctx)
    }

    return nil
}

func (d *MockWorkspace) ListPipelineEvents(
    ctx context.Context,
    lastRun time.Time,
    pipelineId string,
) (
    databricksSdkListing.Iterator[databricksSdkPipelines.PipelineEvent],
) {
    if d.ListPipelineEventsFunc != nil {
        return d.ListPipelineEventsFunc(ctx, lastRun, pipelineId)
    }

    return nil
}

func (d *MockWorkspace) ListPipelineEventsWithDetails(
    ctx context.Context,
    startOffset time.Duration,
    pipelineId string,
) (
    databricksSdkListing.Iterator[PipelineEvent],
) {
    if d.ListPipelineEventsWithDetailsFunc != nil {
        return d.ListPipelineEventsWithDetailsFunc(
            ctx,
            startOffset,
            pipelineId,
        )
    }

    return nil
}

func (d *MockWorkspace) ListQueries(
    ctx context.Context,
    maxResults int,
    startTimeMs int64,
    endTimeMs int64,
    nextPageToken string,
) ([]databricksSdkSql.QueryInfo, bool, string, error) {
    if d.ListQueriesFunc != nil {
        return d.ListQueriesFunc(
            ctx,
            maxResults,
            startTimeMs,
            endTimeMs,
            nextPageToken,
        )
    }

    return nil, false, "", nil
}

func (d *MockWorkspace) ExecuteStatement(
    ctx context.Context,
    warehouseId string,
    defaultCatalog string,
    defaultSchema string,
    stmt string,
    params []databricksSdkSql.StatementParameterListItem,
) (*databricksSdkSql.StatementResponse, error) {
    if d.ExecuteStatementFunc != nil {
        return d.ExecuteStatementFunc(
            ctx,
            warehouseId,
            defaultCatalog,
            defaultSchema,
            stmt,
            params,
        )
    }

	return nil, nil
}

func (d *MockWorkspace) GetStatementByStatementId(
    ctx context.Context,
    statementId string,
) (*databricksSdkSql.StatementResponse, error) {
    if d.GetStatementByStatementIdFunc != nil {
        return d.GetStatementByStatementIdFunc(ctx, statementId)
    }

    return nil, nil
}

func (d *MockWorkspace) GetStatementResultChunkNByStatementIdAndChunkIndex(
    ctx context.Context,
    statementId string,
    chunkIndex int,
) (*databricksSdkSql.ResultData, error) {
    if d.GetStatementResultChunkNByStatementIdAndChunkIndexFunc != nil {
        return d.GetStatementResultChunkNByStatementIdAndChunkIndexFunc(
            ctx,
            statementId,
            chunkIndex,
        )
    }

    return nil, nil
}

func (d *MockWorkspace) CancelExecution(
    ctx context.Context,
    statementId string,
) error {
    if d.CancelExecutionFunc != nil {
        return d.CancelExecutionFunc(ctx, statementId)
    }

    return nil
}
