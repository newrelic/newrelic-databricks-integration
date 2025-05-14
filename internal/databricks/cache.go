package databricks

import (
	"context"
	"fmt"
	"net/url"
	"sync"
	"time"

	databricksSdk "github.com/databricks/databricks-sdk-go"
	databricksSdkCompute "github.com/databricks/databricks-sdk-go/service/compute"
	databricksSql "github.com/databricks/databricks-sdk-go/service/sql"
	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/log"
)

type cacheLoaderFunc[T interface{}] func(ctx context.Context) (*T, error)

type memoryCache[T interface{}] struct {
	mu				sync.Mutex
	expiry			time.Duration
	value			*T
	expiration		time.Time
	loader			cacheLoaderFunc[T]
}

type workspaceInfo struct {
	id				int64
	url				string
	instanceName	string
}

type clusterInfo struct {
	name			string
	source			string
	creator			string
	instancePoolId	string
	singleUserName	string
}

type warehouseInfo struct {
	name			string
	creator			string
}

var (
	workspaceInfoCache			*memoryCache[*workspaceInfo]
	clusterInfoCache			*memoryCache[map[string]*clusterInfo]
	warehouseInfoCache			*memoryCache[map[string]*warehouseInfo]
)

func newMemoryCache[T interface{}](
	expiry time.Duration,
	loader cacheLoaderFunc[T],
) *memoryCache[T] {
	return &memoryCache[T] {
		expiry: expiry,
		loader: loader,
	}
}

func (m *memoryCache[T]) get(ctx context.Context) (*T, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.value == nil || time.Now().After(m.expiration) {
		val, err := m.loader(ctx)
		if err != nil {
			return nil, err
		}

		m.value = val
		m.expiration = time.Now().Add(m.expiry * time.Second)
	}

	return m.value, nil
}

/*
func (m *memoryCache[T]) invalidate() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.expiration = time.Time{}
	m.value = nil
}
*/

// @todo: allow cache expiry values to be configured

// This function is not thread-safe and should not be called concurrently.
// For now it is only called from databricks.go in the function InitPipelines()
// so it is safe to assume that it will not be called concurrently.
func initInfoByIdCaches(
	w *databricksSdk.WorkspaceClient,
) {
	if workspaceInfoCache == nil {
		workspaceInfoCache = newMemoryCache(
			5 * time.Minute,
			func(ctx context.Context) (**workspaceInfo, error) {
				workspaceInfo, err := buildWorkspaceInfo(ctx, w)
				if err != nil {
					return nil, err
				}

				return &workspaceInfo, nil
			},
		)
	}

	if clusterInfoCache == nil {
		clusterInfoCache = newMemoryCache(
			5 * time.Minute,
			func(ctx context.Context) (*map[string]*clusterInfo, error) {
				m, err := buildClusterInfoByIdMap(ctx, w)
				if err != nil {
					return nil, err
				}

				return &m, nil
			},
		)
	}

	if warehouseInfoCache == nil {
		warehouseInfoCache = newMemoryCache(
			5 * time.Minute,
			func(ctx context.Context) (*map[string]*warehouseInfo, error) {
				m, err := buildWarehouseInfoByIdMap(ctx, w)
				if err != nil {
					return nil, err
				}

				return &m, nil
			},
		)
	}
}

func buildWorkspaceInfo(
	ctx context.Context,
	w *databricksSdk.WorkspaceClient,
) (*workspaceInfo, error) {
	log.Debugf("building workspace info...")

	workspaceId, err := w.CurrentWorkspaceID(ctx)
	if err != nil {
		return nil, err
	}

	url, err := url.Parse(w.Config.Host)
	if err != nil {
		return nil, fmt.Errorf(
			"could not parse workspace URL %s: %v",
			w.Config.Host,
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

	return &workspaceInfo{
		workspaceId,
		urlStr,
		hostname,
	}, nil
}

func getWorkspaceInfo(ctx context.Context) (*workspaceInfo, error) {
	wi, err := workspaceInfoCache.get(ctx)
	if err != nil {
		return nil, err
	}

	return *wi, nil
}

func buildClusterInfoByIdMap(
	ctx context.Context,
	w *databricksSdk.WorkspaceClient,
) (map[string]*clusterInfo, error) {
	log.Debugf("building cluster info by ID map...")

	m := map[string]*clusterInfo{}

	log.Debugf("listing clusters for workspace host %s", w.Config.Host)

	all := w.Clusters.List(
		ctx,
		databricksSdkCompute.ListClustersRequest{ PageSize: 100 },
	)

	for ; all.HasNext(ctx);  {
		c, err := all.Next(ctx)
		if err != nil {
			return nil, err
		}

		log.Debugf(
			"cluster ID: %s ; cluster name: %s",
			c.ClusterId,
			c.ClusterName,
		)

		clusterInfo := &clusterInfo{}
		clusterInfo.name = c.ClusterName
		clusterInfo.source = string(c.ClusterSource)
		clusterInfo.creator = c.CreatorUserName
		clusterInfo.singleUserName = c.SingleUserName
		clusterInfo.instancePoolId = c.InstancePoolId

		m[c.ClusterId] = clusterInfo
	}

	return m, nil
}

func getClusterInfoById(
	ctx context.Context,
	clusterId string,
) (*clusterInfo, error) {
	clusterInfoMap, err := clusterInfoCache.get(ctx)
	if err != nil {
		return nil, err
	}

	clusterInfo, ok := (*clusterInfoMap)[clusterId]
	if ok {
		return clusterInfo, nil
	}

	return nil, nil
}

func buildWarehouseInfoByIdMap(
	ctx context.Context,
	w *databricksSdk.WorkspaceClient,
) (map[string]*warehouseInfo, error) {
	log.Debugf("building warehouse info by ID map...")

	m := map[string]*warehouseInfo{}

	log.Debugf("listing warehouses for workspace host %s", w.Config.Host)

	all := w.Warehouses.List(
		ctx,
		databricksSql.ListWarehousesRequest{},
	)

	for ; all.HasNext(ctx); {
		warehouse, err := all.Next(ctx)
		if err != nil {
			return nil, err
		}

		log.Debugf(
			"warehouse ID: %s ; warehouse name: %s",
			warehouse.Id,
			warehouse.Name,
		)

		warehouseInfo := &warehouseInfo{}
		warehouseInfo.name = warehouse.Name
		warehouseInfo.creator = warehouse.CreatorName

		m[warehouse.Id] = warehouseInfo
	}

	return m, nil
}

func getWarehouseInfoById(
	ctx context.Context,
	warehouseId string,
) (*warehouseInfo, error) {
	warehouseInfoMap, err := warehouseInfoCache.get(ctx)
	if err != nil {
		return nil, err
	}

	warehouseInfo, ok := (*warehouseInfoMap)[warehouseId]
	if ok {
		return warehouseInfo, nil
	}

	return nil, nil
}
