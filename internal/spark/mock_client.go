package spark

import (
	"context"
)

// MockSparkApiClient is a mock implementation of SparkApiClient for testing
type MockSparkApiClient struct {
	GetApplicationsFunc         func(
		ctx context.Context,
	) ([]SparkApplication, error)
	GetApplicationExecutorsFunc func(
		ctx context.Context,
		app *SparkApplication,
	) ([]SparkExecutor, error)
	GetApplicationJobsFunc      func(
		ctx context.Context,
		app *SparkApplication,
	) ([]SparkJob, error)
	GetApplicationStagesFunc    func(
		ctx context.Context,
		app *SparkApplication,
	) ([]SparkStage, error)
	GetApplicationRDDsFunc      func(
		ctx context.Context,
		app *SparkApplication,
	) ([]SparkRDD, error)
}

// GetApplications implements the SparkApiClient interface
func (m *MockSparkApiClient) GetApplications(
	ctx context.Context,
) ([]SparkApplication, error) {
	if m.GetApplicationsFunc != nil {
		return m.GetApplicationsFunc(ctx)
	}

	return nil, nil
}

// GetApplicationExecutors implements the SparkApiClient interface
func (m *MockSparkApiClient) GetApplicationExecutors(
	ctx context.Context,
	app *SparkApplication,
) ([]SparkExecutor, error) {
	if m.GetApplicationExecutorsFunc != nil {
		return m.GetApplicationExecutorsFunc(ctx, app)
	}

	return nil, nil
}

// GetApplicationJobs implements the SparkApiClient interface
func (m *MockSparkApiClient) GetApplicationJobs(
	ctx context.Context,
	app *SparkApplication,
) ([]SparkJob, error) {
	if m.GetApplicationJobsFunc != nil {
		return m.GetApplicationJobsFunc(ctx, app)
	}

	return nil, nil
}

// GetApplicationStages implements the SparkApiClient interface
func (m *MockSparkApiClient) GetApplicationStages(
	ctx context.Context,
	app *SparkApplication,
) ([]SparkStage, error) {
	if m.GetApplicationStagesFunc != nil {
		return m.GetApplicationStagesFunc(ctx, app)
	}

	return nil, nil
}

// GetApplicationRDDs implements the SparkApiClient interface
func (m *MockSparkApiClient) GetApplicationRDDs(
	ctx context.Context,
	app *SparkApplication,
) ([]SparkRDD, error) {
	if m.GetApplicationRDDsFunc != nil {
		return m.GetApplicationRDDsFunc(ctx, app)
	}

	return nil, nil
}
