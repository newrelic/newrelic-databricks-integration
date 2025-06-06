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
	mockApps := []SparkApplication{
		{
			Id:   "app-123456",
			Name: "Test Spark App 1",
		},
		{
			Id:   "app-789012",
			Name: "Test Spark App 2",
		},
	}

	// Create a mock for the makeRequest function
	originalMakeRequest := makeRequest
	defer func() { makeRequest = originalMakeRequest }()

	makeRequest = func(
		url string,
		authenticator connectors.HttpAuthenticator,
		response interface{},
	) error {
		// Verify correct URL is used
		assert.Equal(
			t,
			"http://spark.example.com:4040/api/v1/applications",
			url,
			"URL should have been passed correctly",
		)
		// Verify authenticator is nil
		assert.Nil(
            t,
            authenticator,
            "Authenticator should have been nil",
        )

		// Set the mock response
		apps := response.(*[]SparkApplication)
		*apps = mockApps
		return nil
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
}

func TestSparkApiClientImpl_GetApplications_CorrectRequestParams(t *testing.T) {
	// Setup mock applications response
	mockApps := []SparkApplication{
		{
			Id:   "app-123456",
			Name: "Test Spark App 1",
		},
	}

	// Setup a mock authenticator to track if it was passed correctly
	mockAuthenticator := &MockHttpAuthenticator{}

	// Track if makeRequest was called
	makeRequestCalled := false

	// Create a mock for the makeRequest function
	originalMakeRequest := makeRequest
	defer func() { makeRequest = originalMakeRequest }()

	makeRequest = func(
		url string,
		authenticator connectors.HttpAuthenticator,
		response interface{},
	) error {
		// Verify correct URL is used
		assert.Equal(
			t,
			"http://spark-custom-host:8080/api/v1/applications",
			url,
			"URL should have been passed correctly",
		)
		// Verify correct authenticator is used
		assert.True(
			t,
			authenticator == mockAuthenticator,
			"Authenticator should have been passed correctly",
		)
		// Set makeRequestCalled to true
		makeRequestCalled = true

		// Set a mock response
		apps := response.(*[]SparkApplication)
		*apps = mockApps
		return nil
	}

	// Create the client instance
	client := newSparkApiClient(
		"http://spark-custom-host:8080",
		mockAuthenticator,
	)

	// Execute the function under test
	applications, err := client.GetApplications(context.Background())

	// Verify results
	assert.NoError(t, err)
	assert.True(t, makeRequestCalled, "makeRequest should have been called")
	assert.NotNil(t, applications, "Should return a non-nil slice")
	assert.Equal(t, 1, len(applications))
	assert.Equal(t, "app-123456", applications[0].Id)
	assert.Equal(t, "Test Spark App 1", applications[0].Name)
}

func TestSparkApiClientImpl_GetApplications_EmptyList(t *testing.T) {
	// Create a mock for the makeRequest function
	originalMakeRequest := makeRequest
	defer func() { makeRequest = originalMakeRequest }()

	makeRequest = func(
		url string,
		authenticator connectors.HttpAuthenticator,
		response interface{},
	) error {
		// Verify correct URL is used
		assert.Equal(
			t,
			"http://spark.example.com:4040/api/v1/applications",
			url,
			"URL should have been passed correctly",
		)
		// Verify authenticator is nil
		assert.Nil(
            t,
            authenticator,
            "Authenticator should have been nil",
        )

		// Set an empty response (no applications found)
		apps := response.(*[]SparkApplication)
		*apps = []SparkApplication{}
		return nil
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
}

func TestSparkApiClientImpl_GetApplications_Error(t *testing.T) {
	// Setup expected error message
	expectedError := "mock error from makeRequest"

	// Create a mock for the makeRequest function that returns an error
	originalMakeRequest := makeRequest
	defer func() { makeRequest = originalMakeRequest }()

	makeRequest = func(
		url string,
		authenticator connectors.HttpAuthenticator,
		response interface{},
	) error {
		// Verify correct URL is used
		assert.Equal(
			t,
			"http://spark.example.com:4040/api/v1/applications",
			url,
			"URL should have been passed correctly",
		)
		// Verify authenticator is nil
		assert.Nil(
            t,
            authenticator,
            "Authenticator should have been nil",
        )

		// Return the expected error
		return errors.New(expectedError)
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
	assert.Nil(t, applications, "Should return nil when error occurs")
	assert.Equal(t, expectedError, err.Error())
}

func TestSparkApiClientImpl_GetApplicationExecutors(t *testing.T) {
	// Setup mock application
	mockApp := SparkApplication{
		Id:   "app-123456",
		Name: "Test Spark App 1",
	}

	// Setup mock executors
	mockExecutors := []SparkExecutor{
		{
			Id: "exec-1",
		},
		{
			Id: "exec-2",
		},
	}

	// Create a mock for the makeRequest function
	originalMakeRequest := makeRequest
	defer func() { makeRequest = originalMakeRequest }()

	makeRequest = func(
		url string,
		authenticator connectors.HttpAuthenticator,
		response interface{},
	) error {
		// Verify correct URL is used
		assert.Equal(
			t,
			"http://spark.example.com:4040/api/v1/applications/app-123456/executors",
			url,
			"URL should have been passed correctly",
		)
		// Verify authenticator is nil
		assert.Nil(
            t,
            authenticator,
            "Authenticator should have been nil",
        )

		// Set the mock response
		executors := response.(*[]SparkExecutor)
		*executors = mockExecutors
		return nil
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
}

func TestSparkApiClientImpl_GetApplicationExecutors_CorrectRequestParams(
	t *testing.T,
) {
	// Setup mock application
	mockApp := SparkApplication{
		Id:   "app-123456",
		Name: "Test Spark App 1",
	}

	// Setup mock executors
	mockExecutors := []SparkExecutor{
		{
			Id: "exec-1",
		},
	}

	// Setup a mock authenticator to track if it was passed correctly
	mockAuthenticator := &MockHttpAuthenticator{}

	// Track if makeRequest was called
	makeRequestCalled := false

	// Create a mock for the makeRequest function
	originalMakeRequest := makeRequest
	defer func() { makeRequest = originalMakeRequest }()

	makeRequest = func(
		url string,
		authenticator connectors.HttpAuthenticator,
		response interface{},
	) error {
		// Verify correct URL is used
		assert.Equal(
			t,
			"http://spark-custom-host:8080/api/v1/applications/app-123456/executors",
			url,
			"URL should have been passed correctly",
		)
		// Verify correct authenticator is used
		assert.True(
			t,
			authenticator == mockAuthenticator,
			"Authenticator should have been passed correctly",
		)
		makeRequestCalled = true

		// Set a mock response
		executors := response.(*[]SparkExecutor)
		*executors = mockExecutors
		return nil
	}

	// Create the client instance
	client := newSparkApiClient(
		"http://spark-custom-host:8080",
		mockAuthenticator,
	)

	// Execute the function under test
	executors, err := client.GetApplicationExecutors(
		context.Background(),
		&mockApp,
	)

	// Verify results
	assert.NoError(t, err)
	assert.True(t, makeRequestCalled, "makeRequest should have been called")
	assert.NotNil(t, executors, "Should return a non-nil slice")
	assert.Equal(t, 1, len(executors))
	assert.Equal(t, "exec-1", executors[0].Id)
}

func TestSparkApiClientImpl_GetApplicationExecutors_EmptyList(t *testing.T) {
	// Setup mock application
	mockApp := SparkApplication{
		Id:   "app-123456",
		Name: "Test Spark App 1",
	}

	// Create a mock for the makeRequest function
	originalMakeRequest := makeRequest
	defer func() { makeRequest = originalMakeRequest }()

	makeRequest = func(
		url string,
		authenticator connectors.HttpAuthenticator,
		response interface{},
	) error {
		// Verify correct URL is used
		assert.Equal(
			t,
			"http://spark.example.com:4040/api/v1/applications/app-123456/executors",
			url,
			"URL should have been passed correctly",
		)
		// Verify authenticator is nil
		assert.Nil(
            t,
            authenticator,
            "Authenticator should have been nil",
        )

		// Set the mock response
		executors := response.(*[]SparkExecutor)
		*executors = []SparkExecutor{}
		return nil
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
}

func TestSparkApiClientImpl_GetApplicationExecutors_Error(t *testing.T) {
	// Setup mock application
	mockApp := SparkApplication{
		Id:   "app-123456",
		Name: "Test Spark App 1",
	}

	// Setup expected error message
	expectedError := "mock error from makeRequest"

	// Create a mock for the makeRequest function that returns an error
	originalMakeRequest := makeRequest
	defer func() { makeRequest = originalMakeRequest }()

	makeRequest = func(
		url string,
		authenticator connectors.HttpAuthenticator,
		response interface{},
	) error {
		// Verify correct URL is used
		assert.Equal(
			t,
			"http://spark.example.com:4040/api/v1/applications/app-123456/executors",
			url,
			"URL should have been passed correctly",
		)
		// Verify authenticator is nil
		assert.Nil(
            t,
            authenticator,
            "Authenticator should have been nil",
        )

		// Return the expected error
		return errors.New(expectedError)
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
	assert.Equal(t, expectedError, err.Error())
}

func TestSparkApiClientImpl_GetApplicationJobs(t *testing.T) {
	// Setup mock application
	mockApp := SparkApplication{
		Id:   "app-123456",
		Name: "Test Spark App 1",
	}

	// Setup mock jobs
	mockJobs := []SparkJob{
		{
			JobId:   1,
			Name:   "Job 1",
			Status: "RUNNING",
		},
		{
			JobId:   2,
			Name:   "Job 2",
			Status: "FAILED",
		},
	}

	// Create a mock for the makeRequest function
	originalMakeRequest := makeRequest
	defer func() { makeRequest = originalMakeRequest }()

	makeRequest = func(
		url string,
		authenticator connectors.HttpAuthenticator,
		response interface{},
	) error {
		// Verify correct URL is used
		assert.Equal(
			t,
			"http://spark.example.com:4040/api/v1/applications/app-123456/jobs",
			url,
			"URL should have been passed correctly",
		)
		// Verify authenticator is nil
		assert.Nil(
            t,
            authenticator,
            "Authenticator should have been nil",
        )

		// Set the mock response
		jobs := response.(*[]SparkJob)
		*jobs = mockJobs
		return nil
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
}

func TestSparkApiClientImpl_GetApplicationJobs_CorrectRequestParams(
	t *testing.T,
) {
	// Setup mock application
	mockApp := SparkApplication{
		Id:   "app-123456",
		Name: "Test Spark App 1",
	}

	// Setup mock jobs
	mockJobs := []SparkJob{
		{
			JobId: 1,
		},
	}

	// Create mock authenticator to track if it was passed correctly
	mockAuthenticator := &MockHttpAuthenticator{}

	// Track if makeRequest was called
	makeRequestCalled := false

	// Create a mock for the makeRequest function
	originalMakeRequest := makeRequest
	defer func() { makeRequest = originalMakeRequest }()

	makeRequest = func(
		url string,
		authenticator connectors.HttpAuthenticator,
		response interface{},
	) error {
		// Verify correct URL is used
		assert.Equal(
			t,
			"http://spark-custom-host:8080/api/v1/applications/app-123456/jobs",
			url,
			"URL should have been passed correctly",
		)
		// Verify correct authenticator is used
		assert.True(
			t,
			authenticator == mockAuthenticator,
			"Authenticator should have been passed correctly",
		)
		// Set makeRequestCalled to true
		makeRequestCalled = true

		// Set a mock response
		jobs := response.(*[]SparkJob)
		*jobs = mockJobs
		return nil
	}

	// Create the client instance
	client := newSparkApiClient(
		"http://spark-custom-host:8080",
		mockAuthenticator,
	)

	// Execute the function under test
	jobs, err := client.GetApplicationJobs(
		context.Background(),
		&mockApp,
	)

	// Verify results
	assert.NoError(t, err)
	assert.True(t, makeRequestCalled, "makeRequest should have been called")
	assert.NotNil(t, jobs, "Should return a non-nil slice")
	assert.Equal(t, 1, len(jobs))
	assert.Equal(t, 1, jobs[0].JobId)
}

func TestSparkApiClientImpl_GetApplicationJobs_EmptyList(t *testing.T) {
	// Setup mock application
	mockApp := SparkApplication{
		Id:   "app-123456",
		Name: "Test Spark App 1",
	}

	// Create a mock for the makeRequest function
	originalMakeRequest := makeRequest
	defer func() { makeRequest = originalMakeRequest }()

	makeRequest = func(
		url string,
		authenticator connectors.HttpAuthenticator,
		response interface{},
	) error {
		// Verify correct URL is used
		assert.Equal(
			t,
			"http://spark.example.com:4040/api/v1/applications/app-123456/jobs",
			url,
			"URL should have been passed correctly",
		)
		// Verify authenticator is nil
		assert.Nil(
            t,
            authenticator,
            "Authenticator should have been nil",
        )

		// Set the mock response
		jobs := response.(*[]SparkJob)
		*jobs = []SparkJob{}
		return nil
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
}

func TestSparkApiClientImpl_GetApplicationJobs_Error(t *testing.T) {
	// Setup mock application
	mockApp := SparkApplication{
		Id:   "app-123456",
		Name: "Test Spark App 1",
	}

	// Setup expected error message
	expectedError := "mock error from makeRequest"

	// Create a mock for the makeRequest function that returns an error
	originalMakeRequest := makeRequest
	defer func() { makeRequest = originalMakeRequest }()

	makeRequest = func(
		url string,
		authenticator connectors.HttpAuthenticator,
		response interface{},
	) error {
		// Verify correct URL is used
		assert.Equal(
			t,
			"http://spark.example.com:4040/api/v1/applications/app-123456/jobs",
			url,
			"URL should have been passed correctly",
		)
		// Verify authenticator is nil
		assert.Nil(
            t,
            authenticator,
            "Authenticator should have been nil",
        )

        // Return the expected error
		return errors.New(expectedError)
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
	assert.Equal(t, expectedError, err.Error())
}

func TestSparkApiClientImpl_GetApplicationStages(t *testing.T) {
	// Setup mock application
	mockApp := SparkApplication{
		Id:   "app-123456",
		Name: "Test Spark App 1",
	}

	// Setup mock stages
	mockStages := []SparkStage{
		{
			StageId: 1,
			Name:   "Stage 1",
			Status: "ACTIVE",
		},
		{
			StageId: 2,
			Name:   "Stage 2",
			Status: "FAILED",
		},
	}

	// Create a mock for the makeRequest function
	originalMakeRequest := makeRequest
	defer func() { makeRequest = originalMakeRequest }()

	makeRequest = func(
		url string,
		authenticator connectors.HttpAuthenticator,
		response interface{},
	) error {
		// Verify correct URL is used
		assert.Equal(
			t,
			"http://spark.example.com:4040/api/v1/applications/app-123456/stages?details=true",
			url,
			"URL should have been passed correctly",
		)
		// Verify authenticator is nil
		assert.Nil(
            t,
            authenticator,
            "Authenticator should have been nil",
        )

		// Set the mock response
		stages := response.(*[]SparkStage)
		*stages = mockStages
		return nil
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
}

func TestSparkApiClientImpl_GetApplicationStages_CorrectRequestParams(t *testing.T) {
	// Setup mock application
	mockApp := SparkApplication{
		Id:   "app-123456",
		Name: "Test Spark App 1",
	}

	// Setup mock stages
	mockStages := []SparkStage{
        {
            StageId: 1,
        },
    }

	// Create mock authenticator to track if it was passed correctly
	mockAuthenticator := &MockHttpAuthenticator{}

	// Track if makeRequest was called
	makeRequestCalled := false

	// Create a mock for the makeRequest function
	originalMakeRequest := makeRequest
	defer func() { makeRequest = originalMakeRequest }()

	makeRequest = func(
		url string,
		authenticator connectors.HttpAuthenticator,
		response interface{},
	) error {
		// Verify correct URL is used
		assert.Equal(
			t,
			"http://spark-custom-host:8080/api/v1/applications/app-123456/stages?details=true",
			url,
			"URL should have been passed correctly",
		)
		// Verify correct authenticator is used
		assert.True(
			t,
			authenticator == mockAuthenticator,
			"Authenticator should have been passed correctly",
		)
		makeRequestCalled = true

		// Set a mock response
		stages := response.(*[]SparkStage)
		*stages = mockStages
		return nil
	}

	// Create the client instance
	client := newSparkApiClient(
		"http://spark-custom-host:8080",
		mockAuthenticator,
	)

	// Execute the function under test
	stages, err := client.GetApplicationStages(
		context.Background(),
		&mockApp,
	)

	// Verify results
	assert.NoError(t, err)
	assert.True(t, makeRequestCalled, "makeRequest should have been called")
	assert.NotNil(t, stages, "Should return a non-nil slice")
	assert.Equal(t, 1, len(stages))
	assert.Equal(t, 1, stages[0].StageId)
}

func TestSparkApiClientImpl_GetApplicationStages_EmptyList(t *testing.T) {
	// Setup mock application
	mockApp := SparkApplication{
		Id:   "app-123456",
		Name: "Test Spark App 1",
	}

	// Create a mock for the makeRequest function
	originalMakeRequest := makeRequest
	defer func() { makeRequest = originalMakeRequest }()

	makeRequest = func(
		url string,
		authenticator connectors.HttpAuthenticator,
		response interface{},
	) error {
		// Verify correct URL is used
		assert.Equal(
			t,
			"http://spark.example.com:4040/api/v1/applications/app-123456/stages?details=true",
			url,
			"URL should have been passed correctly",
		)
		// Verify authenticator is nil
		assert.Nil(
            t,
            authenticator,
            "Authenticator should have been nil",
        )

		// Set the mock response
		stages := response.(*[]SparkStage)
		*stages = []SparkStage{}
		return nil
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
}

func TestSparkApiClientImpl_GetApplicationStages_Error(t *testing.T) {
	// Setup mock application
	mockApp := SparkApplication{
		Id:   "app-123456",
		Name: "Test Spark App 1",
	}

	// Setup expected error message
	expectedError := "mock error from makeRequest"

	// Create a mock for the makeRequest function that returns an error
	originalMakeRequest := makeRequest
	defer func() { makeRequest = originalMakeRequest }()

	makeRequest = func(
		url string,
		authenticator connectors.HttpAuthenticator,
		response interface{},
	) error {
		// Verify correct URL is used
		assert.Equal(
			t,
			"http://spark.example.com:4040/api/v1/applications/app-123456/stages?details=true",
			url,
			"URL should have been passed correctly",
		)
		// Verify authenticator is nil
		assert.Nil(
            t,
            authenticator,
            "Authenticator should have been nil",
        )

		// Return the expected error
		return errors.New(expectedError)
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
	assert.Equal(t, expectedError, err.Error())
}

func TestSparkApiClientImpl_GetApplicationRDDs(t *testing.T) {
	// Setup mock application
	mockApp := SparkApplication{
		Id:   "app-123456",
		Name: "Test Spark App 1",
	}

	// Setup mock RDDs
	mockRDDs := []SparkRDD{
		{
			Id:   1,
			Name: "RDD 1",
		},
		{
			Id:   2,
			Name: "RDD 2",
		},
	}

	// Create a mock for the makeRequest function
	originalMakeRequest := makeRequest
	defer func() { makeRequest = originalMakeRequest }()

	makeRequest = func(
		url string,
		authenticator connectors.HttpAuthenticator,
		response interface{},
	) error {
		// Verify correct URL is used
		assert.Equal(
			t,
			"http://spark.example.com:4040/api/v1/applications/app-123456/storage/rdd",
			url,
			"URL should have been passed correctly",
		)
		// Verify authenticator is nil
		assert.Nil(
            t,
            authenticator,
            "Authenticator should have been nil",
        )

		// Set the mock response
		rdds := response.(*[]SparkRDD)
		*rdds = mockRDDs
		return nil
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
}

func TestSparkApiClientImpl_GetApplicationRDDs_CorrectRequestParams(
	t *testing.T,
) {
	// Setup mock application
	mockApp := SparkApplication{
		Id:   "app-123456",
		Name: "Test Spark App 1",
	}

	// Setup mock RDDs
	mockRDDs := []SparkRDD{
		{
			Id: 1,
		},
	}

	// Create mock authenticator to track if it was passed correctly
	mockAuthenticator := &MockHttpAuthenticator{}

	// Track if makeRequest was called
	makeRequestCalled := false

	// Create a mock for the makeRequest function
	originalMakeRequest := makeRequest
	defer func() { makeRequest = originalMakeRequest }()

	makeRequest = func(
		url string,
		authenticator connectors.HttpAuthenticator,
		response interface{},
	) error {
		// Verify the URL is constructed correctly
		assert.Equal(
			t,
			"http://spark-custom-host:8080/api/v1/applications/app-123456/storage/rdd",
			url,
			"URL should have been passed correctly",
		)
		// Verify correct authenticator is used
		assert.True(
			t,
			authenticator == mockAuthenticator,
			"Authenticator should have been passed correctly",
		)

		// Set makeRequestCalled to true
		makeRequestCalled = true

		// Set a mock response
		rdds := response.(*[]SparkRDD)
		*rdds = mockRDDs
		return nil
	}

	// Create the client instance
	client := newSparkApiClient(
		"http://spark-custom-host:8080",
		mockAuthenticator,
	)

	// Execute the function under test
	rdds, err := client.GetApplicationRDDs(
		context.Background(),
		&mockApp,
	)

	// Verify results
	assert.NoError(t, err)
	assert.True(t, makeRequestCalled, "makeRequest should have been called")
	assert.NotNil(t, rdds, "Should return a non-nil slice")
	assert.Equal(t, 1, len(rdds))
	assert.Equal(t, 1, rdds[0].Id)
}

func TestSparkApiClientImpl_GetApplicationRDDs_EmptyList(t *testing.T) {
	// Setup mock application
	mockApp := SparkApplication{
		Id:   "app-123456",
		Name: "Test Spark App 1",
	}

	// Create a mock for the makeRequest function
	originalMakeRequest := makeRequest
	defer func() { makeRequest = originalMakeRequest }()

	makeRequest = func(
		url string,
		authenticator connectors.HttpAuthenticator,
		response interface{},
	) error {
		// Verify correct URL is used
		assert.Equal(
			t,
			"http://spark.example.com:4040/api/v1/applications/app-123456/storage/rdd",
			url,
			"URL should have been passed correctly",
		)
		// Verify authenticator is nil
		assert.Nil(
            t,
            authenticator,
            "Authenticator should have been nil",
        )

		// Set the mock response
		rdds := response.(*[]SparkRDD)
		*rdds = []SparkRDD{}
		return nil
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
}

func TestSparkApiClientImpl_GetApplicationRDDs_Error(t *testing.T) {
	// Setup mock application
	mockApp := SparkApplication{
		Id:   "app-123456",
		Name: "Test Spark App 1",
	}

	// Setup expected error message
	expectedError := "mock error from makeRequest"

	// Create a mock for the makeRequest function that returns an error
	originalMakeRequest := makeRequest
	defer func() { makeRequest = originalMakeRequest }()

	makeRequest = func(
		url string,
		authenticator connectors.HttpAuthenticator,
		response interface{},
	) error {
		// Verify correct URL is used
		assert.Equal(
			t,
			"http://spark.example.com:4040/api/v1/applications/app-123456/storage/rdd",
			url,
			"URL should have been passed correctly",
		)
		// Verify authenticator is nil
		assert.Nil(
            t,
            authenticator,
            "Authenticator should have been nil",
        )

		return errors.New(expectedError)
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
	assert.Equal(t, expectedError, err.Error())
}
