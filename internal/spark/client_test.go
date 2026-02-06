package spark

import (
	"context"
	"errors"
	"testing"

	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/connectors"
	"github.com/stretchr/testify/assert"
)

func TestNewSparkApiClient(t *testing.T) {
	// Setup test inputs
	sparkContextUiUrl := "http://spark.example.com:4040"
	mockAuthenticator := &MockHttpAuthenticator{}

	// Execute the function under test
	client := newSparkApiClient(sparkContextUiUrl, mockAuthenticator)

	// Verify results
	assert.NotNil(t, client, "Should return a non-nil client")
	assert.IsType(
		t,
		&sparkApiClientImpl{},
		client,
		"Should return a *sparkApiClientImpl",
	)
}

func TestSparkApiClientImpl_GetApplications(t *testing.T) {
	// Setup mock applications response
	mockApps := `[
		{
			"id":   "app-123456",
			"name": "Test Spark App 1"
		},
		{
			"id":   "app-789012",
			"name": "Test Spark App 2"
		}
	]`

	// Create a mock for the newHttpGetConnector function
	originalNewHttpGetConnector := newHttpGetConnector
	defer func() { newHttpGetConnector = originalNewHttpGetConnector }()

	var connector *mockHttpGetConnector

	newHttpGetConnector = func(url string) httpGetConnector {
		// Save the mock connector for verification purposes
		connector = newMockHttpGetConnector(url, mockApps)
		return connector
	}

	// Create the client instance
	client := newSparkApiClient(
		"http://spark.example.com:4040",
		nil,
	)

	// Execute the function under test
	applications, err := client.GetApplications(context.Background())

	// Verify results
	assert.NoError(t, err)
	assert.NotNil(t, applications, "Should return a non-nil slice")
	assert.Equal(t, 2, len(applications))
	assert.Equal(t, "app-123456", applications[0].Id)
	assert.Equal(t, "Test Spark App 1", applications[0].Name)
	assert.Equal(t, "app-789012", applications[1].Id)
	assert.Equal(t, "Test Spark App 2", applications[1].Name)
	assert.NotNil(t, connector, "Connector should have been non-nil")
	assert.Equal(
		t,
		"http://spark.example.com:4040/api/v1/applications",
		connector.url,
	)
	assert.Nil(
		t,
		connector.authenticator,
		"Authenticator should have been nil",
	)
}

func TestSparkApiClientImpl_GetApplications_EmptyList(t *testing.T) {
	// Setup mock applications response
	mockApps := "[]"

	// Create a mock for the newHttpGetConnector function
	originalNewHttpGetConnector := newHttpGetConnector
	defer func() { newHttpGetConnector = originalNewHttpGetConnector }()

	var connector *mockHttpGetConnector

	newHttpGetConnector = func(url string) httpGetConnector {
		// Save the mock connector for verification purposes
		connector = newMockHttpGetConnector(url, mockApps)
		return connector
	}

	// Create the client instance
	client := newSparkApiClient(
		"http://spark.example.com:4040",
		nil,
	)

	// Execute the function under test
	applications, err := client.GetApplications(context.Background())

	// Verify results
	assert.NoError(t, err)
	assert.NotNil(t, applications, "Should return a non-nil slice")
	assert.Empty(
		t,
		applications,
		"Should return an empty slice when no applications are found",
	)
	assert.Equal(
		t,
		"http://spark.example.com:4040/api/v1/applications",
		connector.url,
	)
	assert.Nil(
		t,
		connector.authenticator,
		"Authenticator should have been nil",
	)
}

func TestSparkApiClientImpl_GetApplications_Error(t *testing.T) {
	// Setup expected error message
	expectedError := "mock error from Request()"

	// Create a mock for the newHttpGetConnector function
	originalNewHttpGetConnector := newHttpGetConnector
	defer func() { newHttpGetConnector = originalNewHttpGetConnector }()

	var connector *mockHttpGetConnector

	newHttpGetConnector = func(url string) httpGetConnector {
		// Save the mock connector for verification purposes
		connector = newMockHttpGetConnectorWithRequestError(
			url,
			"",
			errors.New(expectedError),
		)
		return connector
	}

	// Create the client instance
	client := newSparkApiClient(
		"http://spark.example.com:4040",
		nil,
	)

	// Execute the function under test
	applications, err := client.GetApplications(context.Background())

	// Verify results
	assert.Error(t, err)
	assert.Nil(t, applications, "Should return nil when request error occurs")
	assert.Equal(
		t,
		"http://spark.example.com:4040/api/v1/applications",
		connector.url,
	)
	assert.Nil(
		t,
		connector.authenticator,
		"Authenticator should have been nil",
	)
	assert.Equal(t, expectedError, err.Error())
}

func TestSparkApiClientImpl_GetApplicationExecutors(t *testing.T) {
	// Setup mock application
	mockApp := SparkApplication{
		Id:   "app-123456",
		Name: "Test Spark App 1",
	}

	// Setup mock executors response
	mockExecutors := `[
		{
			"id": "exec-1"
		},
		{
			"id": "exec-2"
		}
	]`

	// Create a mock for the newHttpGetConnector function
	originalNewHttpGetConnector := newHttpGetConnector
	defer func() { newHttpGetConnector = originalNewHttpGetConnector }()

	var connector *mockHttpGetConnector

	newHttpGetConnector = func(url string) httpGetConnector {
		// Save the mock connector for verification purposes
		connector = newMockHttpGetConnector(url, mockExecutors)
		return connector
	}

	// Create the client instance
	client := newSparkApiClient(
		"http://spark.example.com:4040",
		nil,
	)

	// Execute the function under test
	executors, err := client.GetApplicationExecutors(
		context.Background(),
		&mockApp,
	)

	// Verify results
	assert.NoError(t, err)
	assert.NotNil(t, executors, "Should return a non-nil slice")
	assert.Equal(t, 2, len(executors))
	assert.Equal(t, "exec-1", executors[0].Id)
	assert.Equal(t, "exec-2", executors[1].Id)
	// Verify correct URL is used
	assert.Equal(
		t,
		"http://spark.example.com:4040/api/v1/applications/app-123456/executors",
		connector.url,
		"URL should have been passed correctly",
	)
	// Verify authenticator is nil
	assert.Nil(
		t,
		connector.authenticator,
		"Authenticator should have been nil",
	)
}

func TestSparkApiClientImpl_GetApplicationExecutors_EmptyList(t *testing.T) {
	// Setup mock application
	mockApp := SparkApplication{
		Id:   "app-123456",
		Name: "Test Spark App 1",
	}

	// Setup mock executors response
	mockExecutors := "[]"

	// Create a mock for the newHttpGetConnector function
	originalNewHttpGetConnector := newHttpGetConnector
	defer func() { newHttpGetConnector = originalNewHttpGetConnector }()

	var connector *mockHttpGetConnector

	newHttpGetConnector = func(url string) httpGetConnector {
		// Save the mock connector for verification purposes
		connector = newMockHttpGetConnector(url, mockExecutors)
		return connector
	}

	// Create the client instance
	client := newSparkApiClient(
		"http://spark.example.com:4040",
		nil,
	)

	// Execute the function under test
	executors, err := client.GetApplicationExecutors(
		context.Background(),
		&mockApp,
	)

	// Verify results
	assert.NoError(t, err)
	assert.NotNil(t, executors, "Should return a non-nil slice")
	assert.Empty(t,
		executors,
		"Should return an empty slice when no executors are found",
	)
	// Verify correct URL is used
	assert.Equal(
		t,
		"http://spark.example.com:4040/api/v1/applications/app-123456/executors",
		connector.url,
		"URL should have been passed correctly",
	)
	// Verify authenticator is nil
	assert.Nil(
		t,
		connector.authenticator,
		"Authenticator should have been nil",
	)
}

func TestSparkApiClientImpl_GetApplicationExecutors_Error(t *testing.T) {
	// Setup mock application
	mockApp := SparkApplication{
		Id:   "app-123456",
		Name: "Test Spark App 1",
	}

	// Setup expected error message
	expectedError := "mock error from Request()"

	// Create a mock for the newHttpGetConnector function
	originalNewHttpGetConnector := newHttpGetConnector
	defer func() { newHttpGetConnector = originalNewHttpGetConnector }()

	var connector *mockHttpGetConnector

	newHttpGetConnector = func(url string) httpGetConnector {
		// Save the mock connector for verification purposes
		connector = newMockHttpGetConnectorWithRequestError(
			url,
			"",
			errors.New(expectedError),
		)
		return connector
	}

	// Create the client instance
	client := newSparkApiClient(
		"http://spark.example.com:4040",
		nil,
	)

	// Execute the function under test
	executors, err := client.GetApplicationExecutors(
		context.Background(),
		&mockApp,
	)

	// Verify results
	assert.Error(t, err)
	assert.Nil(t, executors, "Should return nil when error occurs")
	// Verify correct URL is used
	assert.Equal(
		t,
		"http://spark.example.com:4040/api/v1/applications/app-123456/executors",
		connector.url,
		"URL should have been passed correctly",
	)
	// Verify authenticator is nil
	assert.Nil(
		t,
		connector.authenticator,
		"Authenticator should have been nil",
	)
	assert.Equal(t, expectedError, err.Error())
}

func TestSparkApiClientImpl_GetApplicationJobs(t *testing.T) {
	// Setup mock application
	mockApp := SparkApplication{
		Id:   "app-123456",
		Name: "Test Spark App 1",
	}

	// Setup mock jobs response
	mockJobs := `[
		{
			"jobId":   1,
			"name":   "Job 1",
			"status": "RUNNING"
		},
		{
			"jobId":   2,
			"name":   "Job 2",
			"status": "FAILED"
		}
	]`

	// Create a mock for the newHttpGetConnector function
	originalNewHttpGetConnector := newHttpGetConnector
	defer func() { newHttpGetConnector = originalNewHttpGetConnector }()

	var connector *mockHttpGetConnector

	newHttpGetConnector = func(url string) httpGetConnector {
		// Save the mock connector for verification purposes
		connector = newMockHttpGetConnector(url, mockJobs)
		return connector
	}

	// Create the client instance
	client := newSparkApiClient(
		"http://spark.example.com:4040",
		nil,
	)

	// Execute the function under test
	jobs, err := client.GetApplicationJobs(
		context.Background(),
		&mockApp,
	)

	// Verify results
	assert.NoError(t, err)
	assert.NotNil(t, jobs, "Should return a non-nil slice")
	assert.Equal(t, 2, len(jobs))
	assert.Equal(t, 1, jobs[0].JobId)
	assert.Equal(t, "Job 1", jobs[0].Name)
	assert.Equal(t, "RUNNING", jobs[0].Status)
	assert.Equal(t, 2, jobs[1].JobId)
	assert.Equal(t, "Job 2", jobs[1].Name)
	assert.Equal(t, "FAILED", jobs[1].Status)
	// Verify correct URL is used
	assert.Equal(
		t,
		"http://spark.example.com:4040/api/v1/applications/app-123456/jobs",
		connector.url,
		"URL should have been passed correctly",
	)
	// Verify authenticator is nil
	assert.Nil(
		t,
		connector.authenticator,
		"Authenticator should have been nil",
	)
}

func TestSparkApiClientImpl_GetApplicationJobs_EmptyList(t *testing.T) {
	// Setup mock application
	mockApp := SparkApplication{
		Id:   "app-123456",
		Name: "Test Spark App 1",
	}

	// Setup mock jobs response
	mockJobs := "[]"

	// Create a mock for the newHttpGetConnector function
	originalNewHttpGetConnector := newHttpGetConnector
	defer func() { newHttpGetConnector = originalNewHttpGetConnector }()

	var connector *mockHttpGetConnector

	newHttpGetConnector = func(url string) httpGetConnector {
		// Save the mock connector for verification purposes
		connector = newMockHttpGetConnector(url, mockJobs)
		return connector
	}

	// Create the client instance
	client := newSparkApiClient(
		"http://spark.example.com:4040",
		nil,
	)

	// Execute the function under test
	jobs, err := client.GetApplicationJobs(
		context.Background(),
		&mockApp,
	)

	// Verify results
	assert.NoError(t, err)
	assert.NotNil(t, jobs, "Should return a non-nil slice")
	assert.Empty(t, jobs, "Should return an empty slice when no jobs are found")
	// Verify correct URL is used
	assert.Equal(
		t,
		"http://spark.example.com:4040/api/v1/applications/app-123456/jobs",
		connector.url,
		"URL should have been passed correctly",
	)
	// Verify authenticator is nil
	assert.Nil(
		t,
		connector.authenticator,
		"Authenticator should have been nil",
	)
}

func TestSparkApiClientImpl_GetApplicationJobs_Error(t *testing.T) {
	// Setup mock application
	mockApp := SparkApplication{
		Id:   "app-123456",
		Name: "Test Spark App 1",
	}

	// Setup expected error message
	expectedError := "mock error from Request()"

	// Create a mock for the newHttpGetConnector function
	originalNewHttpGetConnector := newHttpGetConnector
	defer func() { newHttpGetConnector = originalNewHttpGetConnector }()

	var connector *mockHttpGetConnector

	newHttpGetConnector = func(url string) httpGetConnector {
		// Save the mock connector for verification purposes
		connector = newMockHttpGetConnectorWithRequestError(
			url,
			"",
			errors.New(expectedError),
		)
		return connector
	}

	// Create the client instance
	client := newSparkApiClient(
		"http://spark.example.com:4040",
		nil,
	)

	// Execute the function under test
	jobs, err := client.GetApplicationJobs(
		context.Background(),
		&mockApp,
	)

	// Verify results
	assert.Error(t, err)
	assert.Nil(t, jobs, "Should return nil when error occurs")
	// Verify correct URL is used
	assert.Equal(
		t,
		"http://spark.example.com:4040/api/v1/applications/app-123456/jobs",
		connector.url,
		"URL should have been passed correctly",
	)
	// Verify authenticator is nil
	assert.Nil(
		t,
		connector.authenticator,
		"Authenticator should have been nil",
	)
	assert.Equal(t, expectedError, err.Error())
}

func TestSparkApiClientImpl_GetApplicationStages(t *testing.T) {
	// Setup mock application
	mockApp := SparkApplication{
		Id:   "app-123456",
		Name: "Test Spark App 1",
	}

	// Setup mock stages response
	mockStages := `[
		{
			"stageId": 1,
			"name":   "Stage 1",
			"status": "ACTIVE"
		},
		{
			"stageId": 2,
			"name":   "Stage 2",
			"status": "FAILED"
		}
	]`

	// Create a mock for the newHttpGetConnector function
	originalNewHttpGetConnector := newHttpGetConnector
	defer func() { newHttpGetConnector = originalNewHttpGetConnector }()

	var connector *mockHttpGetConnector

	newHttpGetConnector = func(url string) httpGetConnector {
		// Save the mock connector for verification purposes
		connector = newMockHttpGetConnector(url, mockStages)
		return connector
	}

	// Create the client instance
	client := newSparkApiClient(
		"http://spark.example.com:4040",
		nil,
	)

	// Execute the function under test
	stages, err := client.GetApplicationStages(
		context.Background(),
		&mockApp,
	)

	// Verify results
	assert.NoError(t, err)
	assert.Equal(t, 2, len(stages))
	assert.Equal(t, 1, stages[0].StageId)
	assert.Equal(t, "Stage 1", stages[0].Name)
	assert.Equal(t, "ACTIVE", stages[0].Status)
	assert.Equal(t, 2, stages[1].StageId)
	assert.Equal(t, "Stage 2", stages[1].Name)
	assert.Equal(t, "FAILED", stages[1].Status)
	// Verify correct URL is used
	assert.Equal(
		t,
		"http://spark.example.com:4040/api/v1/applications/app-123456/stages?details=true",
		connector.url,
		"URL should have been passed correctly",
	)
	// Verify authenticator is nil
	assert.Nil(
		t,
		connector.authenticator,
		"Authenticator should have been nil",
	)
}

func TestSparkApiClientImpl_GetApplicationStages_EmptyList(t *testing.T) {
	// Setup mock application
	mockApp := SparkApplication{
		Id:   "app-123456",
		Name: "Test Spark App 1",
	}

	// Setup mock stages response
	mockStages := "[]"

	// Create a mock for the newHttpGetConnector function
	originalNewHttpGetConnector := newHttpGetConnector
	defer func() { newHttpGetConnector = originalNewHttpGetConnector }()

	var connector *mockHttpGetConnector

	newHttpGetConnector = func(url string) httpGetConnector {
		// Save the mock connector for verification purposes
		connector = newMockHttpGetConnector(url, mockStages)
		return connector
	}

	// Create the client instance
	client := newSparkApiClient(
		"http://spark.example.com:4040",
		nil,
	)

	// Execute the function under test
	stages, err := client.GetApplicationStages(
		context.Background(),
		&mockApp,
	)

	// Verify results
	assert.NoError(t, err)
	assert.NotNil(t, stages, "Should return a non-nil slice")
	assert.Empty(
		t,
		stages,
		"Should return an empty slice when no stages are found",
	)
	// Verify correct URL is used
	assert.Equal(
		t,
		"http://spark.example.com:4040/api/v1/applications/app-123456/stages?details=true",
		connector.url,
		"URL should have been passed correctly",
	)
	// Verify authenticator is nil
	assert.Nil(
		t,
		connector.authenticator,
		"Authenticator should have been nil",
	)
}

func TestSparkApiClientImpl_GetApplicationStages_Error(t *testing.T) {
	// Setup mock application
	mockApp := SparkApplication{
		Id:   "app-123456",
		Name: "Test Spark App 1",
	}

	// Setup expected error message
	expectedError := "mock error from Request()"

	// Create a mock for the newHttpGetConnector function
	originalNewHttpGetConnector := newHttpGetConnector
	defer func() { newHttpGetConnector = originalNewHttpGetConnector }()

	var connector *mockHttpGetConnector

	newHttpGetConnector = func(url string) httpGetConnector {
		// Save the mock connector for verification purposes
		connector = newMockHttpGetConnectorWithRequestError(
			url,
			"",
			errors.New(expectedError),
		)
		return connector
	}

	// Create the client instance
	client := newSparkApiClient(
		"http://spark.example.com:4040",
		nil,
	)

	// Execute the function under test
	stages, err := client.GetApplicationStages(
		context.Background(),
		&mockApp,
	)

	// Verify results
	assert.Error(t, err)
	assert.Nil(t, stages, "Should return nil when error occurs")
	// Verify correct URL is used
	assert.Equal(
		t,
		"http://spark.example.com:4040/api/v1/applications/app-123456/stages?details=true",
		connector.url,
		"URL should have been passed correctly",
	)
	// Verify authenticator is nil
	assert.Nil(
		t,
		connector.authenticator,
		"Authenticator should have been nil",
	)
	assert.Equal(t, expectedError, err.Error())
}

func TestSparkApiClientImpl_GetApplicationRDDs(t *testing.T) {
	// Setup mock application
	mockApp := SparkApplication{
		Id:   "app-123456",
		Name: "Test Spark App 1",
	}

	// Setup mock RDDs response
	mockRDDs := `[
		{
			"id":   1,
			"name": "RDD 1"
		},
		{
			"id":   2,
			"name": "RDD 2"
		}
	]`

	// Create a mock for the newHttpGetConnector function
	originalNewHttpGetConnector := newHttpGetConnector
	defer func() { newHttpGetConnector = originalNewHttpGetConnector }()

	var connector *mockHttpGetConnector

	newHttpGetConnector = func(url string) httpGetConnector {
		// Save the mock connector for verification purposes
		connector = newMockHttpGetConnector(url, mockRDDs)
		return connector
	}

	// Create the client instance
	client := newSparkApiClient(
		"http://spark.example.com:4040",
		nil,
	)

	// Execute the function under test
	rdds, err := client.GetApplicationRDDs(
		context.Background(),
		&mockApp,
	)

	// Verify results
	assert.NoError(t, err)
	assert.Equal(t, 2, len(rdds))
	assert.Equal(t, 1, rdds[0].Id)
	assert.Equal(t, "RDD 1", rdds[0].Name)
	assert.Equal(t, 2, rdds[1].Id)
	assert.Equal(t, "RDD 2", rdds[1].Name)
	// Verify correct URL is used
	assert.Equal(
		t,
		"http://spark.example.com:4040/api/v1/applications/app-123456/storage/rdd",
		connector.url,
		"URL should have been passed correctly",
	)
	// Verify authenticator is nil
	assert.Nil(
		t,
		connector.authenticator,
		"Authenticator should have been nil",
	)
}

func TestSparkApiClientImpl_GetApplicationRDDs_EmptyList(t *testing.T) {
	// Setup mock application
	mockApp := SparkApplication{
		Id:   "app-123456",
		Name: "Test Spark App 1",
	}

	// Setup mock RDDs response
	mockRDDs := "[]"

	// Create a mock for the newHttpGetConnector function
	originalNewHttpGetConnector := newHttpGetConnector
	defer func() { newHttpGetConnector = originalNewHttpGetConnector }()

	var connector *mockHttpGetConnector

	newHttpGetConnector = func(url string) httpGetConnector {
		// Save the mock connector for verification purposes
		connector = newMockHttpGetConnector(url, mockRDDs)
		return connector
	}

	// Create the client instance
	client := newSparkApiClient(
		"http://spark.example.com:4040",
		nil,
	)

	// Execute the function under test
	rdds, err := client.GetApplicationRDDs(
		context.Background(),
		&mockApp,
	)

	// Verify results
	assert.NoError(t, err)
	assert.NotNil(t, rdds, "Should return a non-nil slice")
	assert.Empty(t, rdds, "Should return an empty slice when no RDDs are found")
	// Verify correct URL is used
	assert.Equal(
		t,
		"http://spark.example.com:4040/api/v1/applications/app-123456/storage/rdd",
		connector.url,
		"URL should have been passed correctly",
	)
	// Verify authenticator is nil
	assert.Nil(
		t,
		connector.authenticator,
		"Authenticator should have been nil",
	)
}

func TestSparkApiClientImpl_GetApplicationRDDs_Error(t *testing.T) {
	// Setup mock application
	mockApp := SparkApplication{
		Id:   "app-123456",
		Name: "Test Spark App 1",
	}

	// Setup expected error message
	expectedError := "mock error from Request()"

	// Create a mock for the newHttpGetConnector function
	originalNewHttpGetConnector := newHttpGetConnector
	defer func() { newHttpGetConnector = originalNewHttpGetConnector }()

	var connector *mockHttpGetConnector

	newHttpGetConnector = func(url string) httpGetConnector {
		// Save the mock connector for verification purposes
		connector = newMockHttpGetConnectorWithRequestError(
			url,
			"",
			errors.New(expectedError),
		)
		return connector
	}

	// Create the client instance
	client := newSparkApiClient(
		"http://spark.example.com:4040",
		nil,
	)

	// Execute the function under test
	rdds, err := client.GetApplicationRDDs(
		context.Background(),
		&mockApp,
	)

	// Verify results
	assert.Error(t, err)
	assert.Nil(t, rdds, "Should return nil when error occurs")
	// Verify correct URL is used
	assert.Equal(
		t,
		"http://spark.example.com:4040/api/v1/applications/app-123456/storage/rdd",
		connector.url,
		"URL should have been passed correctly",
	)
	// Verify authenticator is nil
	assert.Nil(
		t,
		connector.authenticator,
		"Authenticator should have been nil",
	)
	assert.Equal(t, expectedError, err.Error())
}

func TestMakeRequest(t *testing.T) {
	// Setup response struct
	var response struct {
		X string
		Y int
		Z struct {
			Foo string
			Bar bool
		}
		U []string
	}

	// Setup mock response JSON
	responseJson := `{
		"x": "foo",
		"y": 42,
		"z": {
			"foo": "bar",
			"bar": false
		},
		"u": ["alpha", "beta"]
	}`

	// Create a mock for the newHttpGetConnector function
	originalNewHttpGetConnector := newHttpGetConnector
	defer func() { newHttpGetConnector = originalNewHttpGetConnector }()

	var connector *mockHttpGetConnector

	newHttpGetConnector = func(url string) httpGetConnector {
		// Save the mock connector for verification purposes
		connector = newMockHttpGetConnector(url, responseJson)
		return connector
	}

	// Execute the function under test
	err := makeRequest(
		"https://www.fake-domain.com/api/v1/foo/bar",
		nil,
		&response,
	)

	// Verify results
	assert.NoError(t, err)
	assert.Equal(
		t,
		"https://www.fake-domain.com/api/v1/foo/bar",
		connector.url,
	)
	assert.Nil(t, connector.authenticator)
	assert.NotEmpty(t, connector.headers)
	assert.Contains(t, connector.headers, "Content-Type")
	assert.Equal(t, "application/json", connector.headers["Content-Type"])
	assert.Contains(t, connector.headers, "Accept")
	assert.Equal(t, "application/json", connector.headers["Accept"])
	assert.Contains(t, connector.headers, "User-Agent")
	assert.Equal(t, connectors.GetUserAgent(), connector.headers["User-Agent"])
	assert.Equal(t, "foo", response.X)
	assert.Equal(t, 42, response.Y)
	assert.Equal(t, "bar", response.Z.Foo)
	assert.False(t, response.Z.Bar)
	assert.Equal(t, []string{"alpha", "beta"}, response.U)
}

func TestMakeRequest_Authenticator(t *testing.T) {
	// Setup response struct
	var response string

	// Setup mock response JSON
	responseJson := `"foo"`

	// Create a mock authenticator
	mockAuthenticator := &MockHttpAuthenticator{}

	// Create a mock for the newHttpGetConnector function
	originalNewHttpGetConnector := newHttpGetConnector
	defer func() { newHttpGetConnector = originalNewHttpGetConnector }()

	var connector *mockHttpGetConnector

	newHttpGetConnector = func(url string) httpGetConnector {
		// Save the mock connector for verification purposes
		connector = newMockHttpGetConnector(url, responseJson)
		return connector
	}

	// Execute the function under test
	err := makeRequest(
		"https://www.fake-domain.com/api/v1/foo/bar",
		mockAuthenticator,
		&response,
	)

	// Verify results
	assert.NoError(t, err)
	assert.Equal(
		t,
		"https://www.fake-domain.com/api/v1/foo/bar",
		connector.url,
	)
	assert.NotNil(t, connector.authenticator)
	assert.Equal(t, mockAuthenticator, connector.authenticator)
	assert.Equal(t, "foo", response)
}

func TestMakeRequest_RequestError(t *testing.T) {
	// Setup response struct
	var response string

	// Setup mock response JSON
	responseJson := `"foo"`

	// Setup expected error message
	expectedError := "mock error from Request()"

	// Create a mock for the newHttpGetConnector function
	originalNewHttpGetConnector := newHttpGetConnector
	defer func() { newHttpGetConnector = originalNewHttpGetConnector }()

	var connector *mockHttpGetConnector

	newHttpGetConnector = func(url string) httpGetConnector {
		// Save the mock connector for verification purposes
		connector = newMockHttpGetConnectorWithRequestError(
			url,
			responseJson,
			errors.New(expectedError),
		)
		return connector
	}

	// Execute the function under test
	err := makeRequest(
		"https://www.fake-domain.com/api/v1/foo/bar",
		nil,
		&response,
	)

	// Verify results
	assert.Error(t, err)
	assert.Equal(t, expectedError, err.Error())
	assert.Equal(
		t,
		"https://www.fake-domain.com/api/v1/foo/bar",
		connector.url,
	)
	assert.Nil(t, connector.authenticator)
}

func TestMakeRequest_DecodeError(t *testing.T) {
	// Setup response struct
	var response string

	// Setup mock response JSON
	responseJson := `}`

	// Create a mock for the newHttpGetConnector function
	originalNewHttpGetConnector := newHttpGetConnector
	defer func() { newHttpGetConnector = originalNewHttpGetConnector }()

	var connector *mockHttpGetConnector

	newHttpGetConnector = func(url string) httpGetConnector {
		// Save the mock connector for verification purposes
		connector = newMockHttpGetConnector(url, responseJson)
		return connector
	}

	// Execute the function under test
	err := makeRequest(
		"https://www.fake-domain.com/api/v1/foo/bar",
		nil,
		&response,
	)

	// Verify results
	assert.Error(t, err)
	assert.Equal(
		t,
		"https://www.fake-domain.com/api/v1/foo/bar",
		connector.url,
	)
	assert.Nil(t, connector.authenticator)
}
