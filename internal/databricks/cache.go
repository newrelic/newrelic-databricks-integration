package databricks

import (
	"context"
	"fmt"
	"net/url"
	"sync"
	"time"

	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/log"
)

type cacheEntry[T interface{}] struct {
	expiration 	time.Time
	value		*T
}

type memoryCache[T interface{}] struct {
	mu				sync.Mutex
	expiry			time.Duration
	values			map[string]*cacheEntry[T]
}

const WORKSPACE_INFO_CACHE_KEY = "workspace"

type WorkspaceInfo struct {
	Id				int64
	Url				string
	InstanceName	string
}

type ClusterInfo struct {
	Name			string
	Source			string
	Creator			string
	InstancePoolId	string
	SingleUserName	string
}

type WarehouseInfo struct {
	Name			string
	Creator		    string
}

var (
	workspaceInfoCache			*memoryCache[*WorkspaceInfo]
	clusterInfoCache			*memoryCache[*ClusterInfo]
	warehouseInfoCache			*memoryCache[*WarehouseInfo]
    // These functions are exposed like this for dependency injection purposes
	// to enable mocking of the function in tests.
    GetWorkspaceInfo = getWorkspaceInfo
	GetClusterInfoById = getClusterInfoById
	GetWarehouseInfoById = getWarehouseInfoById
)

func newMemoryCache[T interface{}](expiry time.Duration) *memoryCache[T] {
	return &memoryCache[T] {
		expiry: expiry,
		values: make(map[string]*cacheEntry[T]),
	}
}

func (m *memoryCache[T]) get(id string) *T {
	m.mu.Lock()
	defer m.mu.Unlock()

	entry, ok := m.values[id]
	if !ok {
		// cache miss
		return nil
	}

	// cache hit
	if !Now().After(entry.expiration) {
		// entry not expired
		return entry.value
	}

	// entry expired so evict the expired entry and return nil
	delete(m.values, id)

	return nil
}

func (m *memoryCache[T]) set(id string, value *T) {
	m.mu.Lock()
	defer m.mu.Unlock()

	entry, ok := m.values[id]
	if !ok {
		// cache miss, create entry
		entry = &cacheEntry[T] { value: value }
	} else {
		// cache hit, refresh value
		entry.value = value
	}

	// set/update expiration
	entry.expiration = Now().Add(m.expiry)

	m.values[id] = entry
}

// @todo: allow cache expiry values to be configured

// This function is not thread-safe and should not be called concurrently.
// For now it is called from databricks.go in the function InitPipelines()
// and spark/databricks.go in the function NewDatabricksSparkEventDecorator()
// and these functions do not run concurrently. If this changes, this
// implementation may need to be updated to handle concurrent access.
func InitCaches() {
	if workspaceInfoCache == nil {
		workspaceInfoCache = newMemoryCache[*WorkspaceInfo](5 * time.Minute)
	}
	if clusterInfoCache == nil {
		clusterInfoCache = newMemoryCache[*ClusterInfo](5 * time.Minute)
	}
	if warehouseInfoCache == nil {
		warehouseInfoCache = newMemoryCache[*WarehouseInfo](5 * time.Minute)
	}
}

func getWorkspaceInfo(
	ctx context.Context,
	w DatabricksWorkspace,
) (*WorkspaceInfo, error) {
	info := workspaceInfoCache.get(WORKSPACE_INFO_CACHE_KEY)
	if info != nil {
		return *info, nil
	}

	// Because we no longer hold the mutex on the cache, it is possible that
	// multiple goroutines could end up building the workspace info
	// simultaneously and the last one to build it and set it into the cache
	// "wins". Since the info each calculates will be the same, this doesn't
	// matter much and hopefully the likelihood of an interleaving like that
	// is low, so for now, we'll treat this possibility as acceptable.

	log.Debugf("building workspace info...")

	workspaceId, err := w.GetCurrentWorkspaceId(ctx)
	if err != nil {
		return nil, err
	}

	host := w.GetConfig().Host

	url, err := url.Parse(host)
	if err != nil {
		return nil, fmt.Errorf(
			"could not parse workspace URL %s: %v",
			host,
			err,
		)
	}

	urlStr := url.String()
	hostname := url.Hostname()

	log.Debugf(
		"workspace ID: %d ; workspace URL: %s ; workspace instance name: %s",
		workspaceId,
		urlStr,
		hostname,
	)

	workspaceInfo := &WorkspaceInfo{
		Id: workspaceId,
		Url: urlStr,
		InstanceName: hostname,
	}

	workspaceInfoCache.set(WORKSPACE_INFO_CACHE_KEY, &workspaceInfo)

	return workspaceInfo, nil
}

func getClusterInfoById(
	ctx context.Context,
	w DatabricksWorkspace,
	clusterId string,
) (*ClusterInfo, error) {
	info := clusterInfoCache.get(clusterId)
	if info != nil {
		return *info, nil
	}

	// Because we no longer hold the mutex on the cache, it is possible that
	// multiple goroutines could end up building cluster info for this cluster
	// simultaneously and the last one to build it and set it into the cache
	// "wins". Since the info each calculates will be the same, this doesn't
	// matter much and hopefully the likelihood of an interleaving like that
	// is low, so for now, we'll treat this possibility as acceptable.

	log.Debugf(
		"building cluster info for cluster %s and workspacehost %s...",
		clusterId,
		w.GetConfig().Host,
	)

	c, err := w.GetClusterById(ctx, clusterId)
	if err != nil {
		return nil, err
	}

	log.Debugf(
		"cluster ID: %s ; cluster name: %s",
		c.ClusterId,
		c.ClusterName,
	)

	clusterInfo := &ClusterInfo{
		Name: c.ClusterName,
		Source: string(c.ClusterSource),
		Creator: c.CreatorUserName,
		SingleUserName: c.SingleUserName,
		InstancePoolId: c.InstancePoolId,
	}

	clusterInfoCache.set(clusterId, &clusterInfo)

	return clusterInfo, nil
}

func getWarehouseInfoById(
	ctx context.Context,
	w DatabricksWorkspace,
	warehouseId string,
) (*WarehouseInfo, error) {
	info := warehouseInfoCache.get(warehouseId)
	if info != nil {
		return *info, nil
	}

	// Because we no longer hold the mutex on the cache, it is possible that
	// multiple goroutines could end up building warehouse info for this
	// warehouse simultaneously and the last one to build it and set it into the
	// cache "wins". Since the info each calculates will be the same, this
	// doesn't matter much and hopefully the likelihood of an interleaving like
	// that is low, so for now, we'll treat this possibility as acceptable.

	log.Debugf(
		"building warehouse info for warehouse %s and workspace host %s...",
		warehouseId,
		w.GetConfig().Host,
	)

	warehouse, err := w.GetWarehouseById(ctx, warehouseId)
	if err != nil {
		return nil, err
	}

	log.Debugf(
		"warehouse ID: %s ; warehouse name: %s",
		warehouse.Id,
		warehouse.Name,
	)

	warehouseInfo := &WarehouseInfo{
		Name: warehouse.Name,
		Creator: warehouse.CreatorName,
	}

	warehouseInfoCache.set(warehouseId, &warehouseInfo)

	return warehouseInfo, nil
}
