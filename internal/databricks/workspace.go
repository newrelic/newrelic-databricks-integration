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
	"github.com/spf13/viper"
)

var (
    // NewDatabricksWorkspace is exposed like this for dependency injection
	// purposes to enable mocking of the Databricks workspace in tests.
    NewDatabricksWorkspace = newDatabricksWorkspaceImpl
)

type DatabricksWorkspace interface {
	GetConfig() *databricksSdkConfig.Config

    GetCurrentWorkspaceId(ctx context.Context) (int64, error)

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

func configureWorkspaceAuth(config *databricksSdk.Config) error {
	/*
	 * Any of the variables below can be specified in any of the ways that
	 * are supported by the Databricks SDK so if we don't explicitly find one
	 * in the config file, it's not an error.  We assume the user has used one
	 * of the SDK mechanisms and if they haven't the SDK will return an error at
	 * config time or when a request fails.
	 */

	// Prefer OAuth by looking for client ID in our config first
	databricksOAuthClientId := viper.GetString("databricks.oauthClientId")
	if databricksOAuthClientId != "" {
		/*
		 * If an OAuth client ID was in our config we will at this point tell
		 * the SDK to use OAuth M2M authentication. The secret may come from our
		 * config but can still come from any of the supported SDK mechanisms.
		 * So if we don't find the secret in our config file, it's not an error.
		 * Note that because we are forcing OAuth M2M authentication now, the
		 * SDK will not try other mechanisms if OAuth M2M authentication is
		 * unsuccessful.
		 */
		config.ClientID = databricksOAuthClientId
		config.Credentials = databricksSdkConfig.M2mCredentials{}

		databricksOAuthClientSecret := viper.GetString(
			"databricks.oauthClientSecret",
		)
		if databricksOAuthClientSecret != "" {
			config.ClientSecret = databricksOAuthClientSecret
		}

		return nil
	}

	// Check for a PAT in our config next
	databricksAccessToken := viper.GetString("databricks.accessToken")
	if databricksAccessToken != "" {
		/*
		* If the user didn't specify an OAuth client ID but does specify a PAT,
		* we will at this point tell the SDK to use PAT authentication. Note
		* that because we are forcing PAT authentication now, the SDK will not
		* try other mechanisms if PAT authentication is unsuccessful.
		*/
		config.Token = databricksAccessToken
		config.Credentials = databricksSdkConfig.PatCredentials{}

		return nil
	}

	/*
	 * At this point, it's up to the user to specify authentication via an
	 * SDK-supported mechanism. This does not preclude the user from using OAuth
	 * M2M authentication or PAT authentication. The user can still use these
	 * authentication types via SDK-supported mechanisms or any other
	 * SDK-supported authentication types via the corresponding SDK-supported
	 * mechanisms.
	 */

	return nil
}

type databricksWorkspaceImpl struct {
    w *databricksSdk.WorkspaceClient
    pipelineEventClient *pipelineEventClient
}

func newDatabricksWorkspaceImpl() (DatabricksWorkspace, error) {
	databricksConfig := &databricksSdk.Config{}

	/*
	 * If the user explicitly specifies a host in the config, use that.
	 * Otherwise the user can specify using an SDK-supported mechanism.
	 */
	databricksWorkspaceHost := viper.GetString("databricks.workspaceHost")
	if databricksWorkspaceHost != "" {
		databricksConfig.Host = databricksWorkspaceHost
	}

	// Configure authentication
	err := configureWorkspaceAuth(databricksConfig)
	if err != nil {
		return nil, err
	}

    w, err := databricksSdk.NewWorkspaceClient(databricksConfig)
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
