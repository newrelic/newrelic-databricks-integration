package databricks

import (
	"context"
	"errors"
	"net/url"
	"testing"
	"time"

	databricksSdkConfig "github.com/databricks/databricks-sdk-go/config"
	databricksSdkCompute "github.com/databricks/databricks-sdk-go/service/compute"
	databricksSdkSql "github.com/databricks/databricks-sdk-go/service/sql"
	"github.com/stretchr/testify/assert"
)

// This function is needed since the caches are global variables that are
// initialized on the first call to InitCaches(). To ensure we start from the
// same state for each test, we need to reset the caches before each test.
func resetCaches() {
	workspaceInfoCache = nil
	clusterInfoCache = nil
	warehouseInfoCache = nil
}

func TestNewMemoryCache(t *testing.T) {
	// Execute the function under test
	cache := newMemoryCache[string](5 * time.Minute)

	// Verify result
	assert.NotNil(t, cache)
	assert.IsType(t, &memoryCache[string]{}, cache)
	assert.Equal(t, 5 * time.Minute, cache.expiry)
	assert.NotNil(t, cache.values)
	assert.Empty(t, cache.values)
}

func TestGet_CacheMiss(t *testing.T) {
	// Create a new memory cache instance
	cache := newMemoryCache[string](5 * time.Minute)

	// Execute the function under test
	value := cache.get("key")

	// Verify result
	assert.Nil(t, value)
}

func TestGet_CacheHit(t *testing.T) {
	// Create a new memory cache instance
	cache := newMemoryCache[string](5 * time.Minute)

	// Setup mock and expected values
	mockValue := "value"
	expectedValue := "value"

	// Setup a mock cache entry
	cache.values["key"] = &cacheEntry[string]{
		value:      &mockValue,
		expiration: time.Now().Add(5 * time.Minute),
	}

	// Execute the function under test
	value := cache.get("key")

	// Verify result
	assert.NotNil(t, value)
	assert.Equal(t, expectedValue, *value)
}

func TestGet_CacheHit_Expired(t *testing.T) {
	// Mock Now
	now := time.Now()
	Now = func () time.Time { return now }

	// Create a new memory cache instance
	cache := newMemoryCache[string](5 * time.Minute)

	// Setup mock value
	mockValue := "value"

	// Setup a mock cache entry
	cache.values["key"] = &cacheEntry[string]{
		value:      &mockValue,
		expiration: now.Add(-5 * time.Minute),
	}

	// Execute the function under test
	value := cache.get("key")

	// Verify result
	assert.Nil(t, value)
	assert.NotContains(t, cache.values, "key")
}

func TestSet_CacheMiss(t *testing.T) {
	// Mock Now
	now := time.Now()
	Now = func () time.Time { return now }

	// Create a new memory cache instance
	cache := newMemoryCache[string](5 * time.Minute)

	// Setup mock value
	value := "value"
	expectedValue := "value"

	// Setup expected expiration
	expectedExpiration := now.Add(5 * time.Minute)

	// Assert preconditions
	assert.NotContains(t, cache.values, "key")

	// Execute the function under test
	cache.set("key", &value)

	// Verify result
	assert.Contains(t, cache.values, "key")
	assert.Equal(t, expectedValue, *cache.values["key"].value)
	assert.Equal(t, expectedExpiration, cache.values["key"].expiration)
}

func TestSet_CacheHit(t *testing.T) {
	// Mock Now
	now := time.Now()
	Now = func () time.Time { return now }

	// Create a new memory cache instance
	cache := newMemoryCache[string](5 * time.Minute)

	// Setup mock and expected values
	value := "value"
	newValue := "value2"
	expectedValue := "value2"

	// Setup mock and expected expiration times
	expiration := now.Add(2 * time.Minute)
	expectedExpiration := now.Add(5 * time.Minute)

	// Setup a mock cache entry
	cache.values["key"] = &cacheEntry[string]{
		value:      &value,
		expiration: expiration,
	}

	// Execute the function under test
	cache.set("key", &newValue)

	// Verify result
	assert.Contains(t, cache.values, "key")
	assert.Equal(t, expectedValue, *cache.values["key"].value)
	assert.Equal(t, expectedExpiration, cache.values["key"].expiration)
}

func Test_InitCaches(t *testing.T) {
	// Reset caches before the test
	resetCaches()

	// Assert preconditions
	assert.Nil(t, workspaceInfoCache)
	assert.Nil(t, clusterInfoCache)
	assert.Nil(t, warehouseInfoCache)

	// Execute the function under test
	InitCaches()

	// Verify result
	assert.NotNil(t, workspaceInfoCache)
	assert.IsType(t, &memoryCache[*WorkspaceInfo]{}, workspaceInfoCache)
	assert.Equal(t, 5 * time.Minute, workspaceInfoCache.expiry)
	assert.NotNil(t, workspaceInfoCache.values)
	assert.Empty(t, workspaceInfoCache.values)

	assert.NotNil(t, clusterInfoCache)
	assert.IsType(t, &memoryCache[*ClusterInfo]{}, clusterInfoCache)
	assert.Equal(t, 5 * time.Minute, clusterInfoCache.expiry)
	assert.NotNil(t, clusterInfoCache.values)
	assert.Empty(t, clusterInfoCache.values)

	assert.NotNil(t, warehouseInfoCache)
	assert.IsType(t, &memoryCache[*WarehouseInfo]{}, warehouseInfoCache)
	assert.Equal(t, 5 * time.Minute, warehouseInfoCache.expiry)
	assert.NotNil(t, warehouseInfoCache.values)
	assert.Empty(t, warehouseInfoCache.values)
}

func TestGetWorkspaceInfo_CacheHit(t *testing.T) {
	// Reset caches before the test
	resetCaches()

	// Setup mock workspace
	mockWorkspace := setupMockWorkspace()

	// Mock Now
	now := time.Now()
	Now = func () time.Time { return now }

	// Setup a mock workspace info
	workspaceInfo := &WorkspaceInfo{
		Id:           mockWorkspaceId,
		InstanceName: mockWorkspaceHost,
		Url:          "https://" + mockWorkspaceHost,
	}

	// Setup expected workspace info
	expectedWorkspaceInfo := &WorkspaceInfo{
		Id:           mockWorkspaceId,
		InstanceName: mockWorkspaceHost,
		Url:          "https://" + mockWorkspaceHost,
	}

	// Initialize the caches
	InitCaches()

	// Setup a mock cache entry
	workspaceInfoCache.values[WORKSPACE_INFO_CACHE_KEY] =
		&cacheEntry[*WorkspaceInfo]{
			value:      &workspaceInfo,
			expiration: now.Add(5 * time.Minute),
		}

	// Execute the function under test
	value, err := getWorkspaceInfo(
		context.Background(),
		mockWorkspace,
	)

	// Verify result
	assert.NoError(t, err)
	assert.NotNil(t, value)
	assert.Equal(t, expectedWorkspaceInfo, value)
}

func TestGetWorkspaceInfo_GetCurrentWorkspaceIdError(t *testing.T) {
	// Reset caches before the test
	resetCaches()

	// Setup mock workspace
	mockWorkspace := setupMockWorkspace()

	// Mock Now
	now := time.Now()
	Now = func () time.Time { return now }

	// Setup expected error message
	expectedError := "error getting workspace ID"

	// Set GetCurrentWorkspaceId to return an error
	mockWorkspace.GetCurrentWorkspaceIdFunc = func(
		ctx context.Context,
	) (int64, error) {
		return 0, errors.New(expectedError)
	}

	// Initialize the caches
	InitCaches()

	// Execute the function under test
	value, err := getWorkspaceInfo(
		context.Background(),
		mockWorkspace,
	)

	// Verify result
	assert.Error(t, err)
	assert.Nil(t, value)
	assert.Equal(t, expectedError, err.Error())
}

func TestGetWorkspaceInfo_ParseUrlError(t *testing.T) {
	// Reset caches before the test
	resetCaches()

	// Setup mock workspace
	mockWorkspace := setupMockWorkspace()

	// Mock Now
	now := time.Now()
	Now = func () time.Time { return now }

	// Get the expected error
	//lint:ignore SA1007 we are intentionally passing an invalid URL
	_, parseErr := url.Parse(":")

	// Set GetConfig to return a config with an invalid host
	mockWorkspace.GetConfigFunc = func() *databricksSdkConfig.Config {
		config := &databricksSdkConfig.Config{
			Host: ":",
		}

		return config
	}

	// Initialize the caches
	InitCaches()

	// Execute the function under test
	value, err := getWorkspaceInfo(
		context.Background(),
		mockWorkspace,
	)

	// Verify result
	assert.Error(t, err)
	assert.Nil(t, value)
	assert.Equal(
		t,
		"could not parse workspace URL :: " + parseErr.Error(),
		err.Error(),
	)
}

func TestGetWorkspaceInfo_ValidWorkspaceIdAndHost(t *testing.T) {
	// Reset caches before the test
	resetCaches()

	// Setup mock workspace
	mockWorkspace := setupMockWorkspace()

	// Mock Now
	now := time.Now()
	Now = func () time.Time { return now }

	// Set GetCurrentWorkspaceId to return a valid ID
	mockWorkspace.GetCurrentWorkspaceIdFunc = func(
		ctx context.Context,
	) (int64, error) {
		return mockWorkspaceId, nil
	}

	// Set GetConfig to return a config with a valid host
	mockWorkspace.GetConfigFunc = func() *databricksSdkConfig.Config {
		config := &databricksSdkConfig.Config{
			Host: "https://" + mockWorkspaceHost,
		}

		return config
	}

	// Initialize the caches
	InitCaches()

	// Execute the function under test
	workspaceInfo, err := getWorkspaceInfo(
		context.Background(),
		mockWorkspace,
	)

	// Verify result
	assert.NoError(t, err)
	assert.NotNil(t, workspaceInfo)
	assert.Equal(t, mockWorkspaceId, workspaceInfo.Id)
	assert.Equal(t, mockWorkspaceHost, workspaceInfo.InstanceName)
	assert.Equal(t, "https://" + mockWorkspaceHost, workspaceInfo.Url)
	assert.Contains(t, workspaceInfoCache.values, WORKSPACE_INFO_CACHE_KEY)
	assert.Equal(
		t,
		*workspaceInfoCache.values[WORKSPACE_INFO_CACHE_KEY].value,
		workspaceInfo,
	)
}

func TestGetClusterInfoById_CacheHit(t *testing.T) {
	// Reset caches before the test
	resetCaches()

	// Setup mock workspace
	mockWorkspace := setupMockWorkspace()

	// Mock Now
	now := time.Now()
	Now = func () time.Time { return now }

	// Setup mock cluster ID
	clusterId := "fake-cluster-id"

	// Setup a mock cluster info
	clusterInfo := &ClusterInfo{
		Name:           "fake-cluster-name",
		Source:         "fake-cluster-source",
		Creator:        "fake-cluster-creator",
		InstancePoolId: "fake-cluster-instance-pool-id",
		SingleUserName: "fake-cluster-single-user-name",
	}

	// Setup expected cluster info
	expectedClusterInfo := &ClusterInfo{
		Name:           "fake-cluster-name",
		Source:         "fake-cluster-source",
		Creator:        "fake-cluster-creator",
		InstancePoolId: "fake-cluster-instance-pool-id",
		SingleUserName: "fake-cluster-single-user-name",
	}

	// Initialize the caches
	InitCaches()

	// Setup a mock cache entry
	clusterInfoCache.values[clusterId] =
		&cacheEntry[*ClusterInfo]{
			value:      &clusterInfo,
			expiration: now.Add(5 * time.Minute),
		}

	// Execute the function under test
	value, err := getClusterInfoById(
		context.Background(),
		mockWorkspace,
		clusterId,
	)

	// Verify result
	assert.NoError(t, err)
	assert.NotNil(t, value)
	assert.Equal(t, expectedClusterInfo, value)
}

func TestGetClusterInfoById_GetClusterByIdError(t *testing.T) {
	// Reset caches before the test
	resetCaches()

	// Setup mock workspace
	mockWorkspace := setupMockWorkspace()

	// Mock Now
	now := time.Now()
	Now = func () time.Time { return now }

	// Setup expected error message
	expectedError := "error getting cluster by id"

	// Set GetClusterById to return an error
	mockWorkspace.GetClusterByIdFunc = func(
		ctx context.Context,
		clusterId string,
	) (*databricksSdkCompute.ClusterDetails, error) {
		return nil, errors.New(expectedError)
	}

	// Initialize the caches
	InitCaches()

	// Execute the function under test
	value, err := getClusterInfoById(
		context.Background(),
		mockWorkspace,
		"fake-cluster-id",
	)

	// Verify result
	assert.Error(t, err)
	assert.Nil(t, value)
	assert.Equal(t, expectedError, err.Error())
}

func TestGetClusterInfoById_ValidId(t *testing.T) {
	// Reset caches before the test
	resetCaches()

	// Setup mock workspace
	mockWorkspace := setupMockWorkspace()

	// Mock Now
	now := time.Now()
	Now = func () time.Time { return now }

	// Setup mock cluster ID
	clusterId := "fake-cluster-id"

	// Set GetClusterById to return valid cluster details
	mockWorkspace.GetClusterByIdFunc = func(
		ctx context.Context,
		clusterId string,
	) (*databricksSdkCompute.ClusterDetails, error) {
		return &databricksSdkCompute.ClusterDetails{
			ClusterName:     "fake-cluster-name",
			ClusterSource:
				databricksSdkCompute.ClusterSource("fake-cluster-source"),
			CreatorUserName: "fake-cluster-creator",
			InstancePoolId:  "fake-cluster-instance-pool-id",
			SingleUserName:  "fake-cluster-single-user-name",
		}, nil
	}

	// Initialize the caches
	InitCaches()

	// Execute the function under test
	clusterInfo, err := getClusterInfoById(
		context.Background(),
		mockWorkspace,
		clusterId,
	)

	// Verify result
	assert.NoError(t, err)
	assert.NotNil(t, clusterInfo)
	assert.Equal(t, "fake-cluster-name", clusterInfo.Name)
	assert.Equal(t, "fake-cluster-source", clusterInfo.Source)
	assert.Equal(t, "fake-cluster-creator", clusterInfo.Creator)
	assert.Equal(t, "fake-cluster-instance-pool-id", clusterInfo.InstancePoolId)
	assert.Equal(t, "fake-cluster-single-user-name", clusterInfo.SingleUserName)
	assert.Contains(t, clusterInfoCache.values, clusterId)
	assert.Equal(
		t,
		*clusterInfoCache.values[clusterId].value,
		clusterInfo,
	)
}

func TestGetWarehouseInfoById_CacheHit(t *testing.T) {
	// Reset caches before the test
	resetCaches()

	// Setup mock workspace
	mockWorkspace := setupMockWorkspace()

	// Mock Now
	now := time.Now()
	Now = func () time.Time { return now }

	// Setup mock warehouse ID
	warehouseId := "fake-warehouse-id"

	// Setup a mock warehouse info
	warehouseInfo := &WarehouseInfo{
		Name:    "fake-warehouse-name",
		Creator: "fake-warehouse-creator",
	}

	// Setup expected cluster info
	expectedWarehouseInfo := &WarehouseInfo{
		Name:    "fake-warehouse-name",
		Creator: "fake-warehouse-creator",
	}

	// Initialize the caches
	InitCaches()

	// Setup a mock cache entry
	warehouseInfoCache.values[warehouseId] =
		&cacheEntry[*WarehouseInfo]{
			value:      &warehouseInfo,
			expiration: now.Add(5 * time.Minute),
		}

	// Execute the function under test
	value, err := getWarehouseInfoById(
		context.Background(),
		mockWorkspace,
		warehouseId,
	)

	// Verify result
	assert.NoError(t, err)
	assert.NotNil(t, value)
	assert.Equal(t, expectedWarehouseInfo, value)
}

func TestGetWarehouseInfoById_GetWarehouseByIdError(t *testing.T) {
	// Reset caches before the test
	resetCaches()

	// Setup mock workspace
	mockWorkspace := setupMockWorkspace()

	// Mock Now
	now := time.Now()
	Now = func () time.Time { return now }

	// Setup expected error message
	expectedError := "error getting warehouse by id"

	// Set GetWarehouseById to return an error
	mockWorkspace.GetWarehouseByIdFunc = func(
		ctx context.Context,
		warehouseId string,
	) (*databricksSdkSql.GetWarehouseResponse, error) {
		return nil, errors.New(expectedError)
	}

	// Initialize the caches
	InitCaches()

	// Execute the function under test
	value, err := getWarehouseInfoById(
		context.Background(),
		mockWorkspace,
		"fake-warehouse-id",
	)

	// Verify result
	assert.Error(t, err)
	assert.Nil(t, value)
	assert.Equal(t, expectedError, err.Error())
}

func TestGetWarehouseInfoById_ValidId(t *testing.T) {
	// Reset caches before the test
	resetCaches()

	// Setup mock workspace
	mockWorkspace := setupMockWorkspace()

	// Mock Now
	now := time.Now()
	Now = func () time.Time { return now }

	// Setup mock warehouse ID
	warehouseId := "fake-warehouse-id"

	// Set GetWarehouseById to return valid warehouse details
	mockWorkspace.GetWarehouseByIdFunc = func(
		ctx context.Context,
		warehouseId string,
	) (*databricksSdkSql.GetWarehouseResponse, error) {
		return &databricksSdkSql.GetWarehouseResponse{
			Name:        "fake-warehouse-name",
			CreatorName: "fake-warehouse-creator",
		}, nil
	}

	// Initialize the caches
	InitCaches()

	// Execute the function under test
	warehouseInfo, err := getWarehouseInfoById(
		context.Background(),
		mockWorkspace,
		warehouseId,
	)

	// Verify result
	assert.NoError(t, err)
	assert.NotNil(t, warehouseInfo)
	assert.Equal(t, "fake-warehouse-name", warehouseInfo.Name)
	assert.Equal(t, "fake-warehouse-creator", warehouseInfo.Creator)
	assert.Contains(t, warehouseInfoCache.values, warehouseId)
	assert.Equal(
		t,
		*warehouseInfoCache.values[warehouseId].value,
		warehouseInfo,
	)
}
