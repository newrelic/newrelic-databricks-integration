package databricks

import (
	"context"
	"net/http"

	databricksSdk "github.com/databricks/databricks-sdk-go"
	databricksSdkClient "github.com/databricks/databricks-sdk-go/client"
	"github.com/newrelic/newrelic-databricks-integration/internal/spark"
	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/connectors"
)

type DatabricksSparkApiClient struct {
	sparkContextUiPath		string
	client					*databricksSdkClient.DatabricksClient
}

func NewDatabricksSparkApiClient(
	sparkContextUiPath string,
	w *databricksSdk.WorkspaceClient,
) (*DatabricksSparkApiClient, error) {
	cfg := w.Config

	// The following 10 lines are taken from the WorkspaceClient code at
	// https://github.com/databricks/databricks-sdk-go/blob/main/workspace_client.go#L1100
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

	return &DatabricksSparkApiClient{
		sparkContextUiPath,
		databricksClient,
	}, nil
}

func (s *DatabricksSparkApiClient) GetApplications(
	ctx context.Context,
) ([]spark.SparkApplication, error) {
	sparkApps := []spark.SparkApplication{}

	// Here and throughout we follow the pattern at
	// https://github.com/databricks/databricks-sdk-go/blob/main/service/apps/impl.go#L18
	// using our own DatabricksClient. Again, because we don't have access to
	// the databricksClient on the WorkspaceClient

	path := s.sparkContextUiPath + "/api/v1/applications"
	headers := makeHeaders()

	err := s.client.Do(ctx, http.MethodGet, path, headers, nil, &sparkApps)
	if err != nil {
		return nil, err
	}

	return sparkApps, nil
}

func (s *DatabricksSparkApiClient) GetApplicationExecutors(
	ctx context.Context,
	app *spark.SparkApplication,
) ([]spark.SparkExecutor, error) {
	executors := []spark.SparkExecutor{}

	path := s.sparkContextUiPath + "/api/v1/applications/" + app.Id + "/executors"
	headers := makeHeaders()

	err := s.client.Do(ctx, http.MethodGet, path, headers, nil, &executors)
	if err != nil {
		return nil, err
	}

	return executors, nil
}

func (s *DatabricksSparkApiClient) GetApplicationJobs(
	ctx context.Context,
	app *spark.SparkApplication,
) ([]spark.SparkJob, error) {
	jobs := []spark.SparkJob{}

	path := s.sparkContextUiPath + "/api/v1/applications/" + app.Id + "/jobs"
	headers := makeHeaders()

	err := s.client.Do(ctx, http.MethodGet, path, headers, nil, &jobs)
	if err != nil {
		return nil, err
	}

	return jobs, nil
}

func (s *DatabricksSparkApiClient) GetApplicationStages(
	ctx context.Context,
	app *spark.SparkApplication,
) ([]spark.SparkStage, error) {
	stages := []spark.SparkStage{}

	path := s.sparkContextUiPath + "/api/v1/applications/" + app.Id + "/stages?details=true"
	headers := makeHeaders()

	err := s.client.Do(ctx, http.MethodGet, path, headers, nil, &stages)
	if err != nil {
		return nil, err
	}

	return stages, nil
}

func (s *DatabricksSparkApiClient) GetApplicationRDDs(
	ctx context.Context,
	app *spark.SparkApplication,
) ([]spark.SparkRDD, error) {
	rdds := []spark.SparkRDD{}

	path := s.sparkContextUiPath + "/api/v1/applications/" + app.Id + "/storage/rdd"
	headers := makeHeaders()

	err := s.client.Do(ctx, http.MethodGet, path, headers, nil, &rdds)
	if err != nil {
		return nil, err
	}

	return rdds, nil
}

func makeHeaders() map[string]string {
	headers := make(map[string]string)

	headers["Accept"] = "application/json"
	headers["Content-Type"] = "application/json"
	headers["User-Agent"] = connectors.GetUserAgent()

	return headers
}
