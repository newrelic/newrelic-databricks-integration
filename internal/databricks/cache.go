package databricks

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	databricksSdk "github.com/databricks/databricks-sdk-go"
	databricksSdkCompute "github.com/databricks/databricks-sdk-go/service/compute"
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
	clusterInfoCache			*memoryCache[*clusterInfo]
	warehouseInfoCache			*memoryCache[*warehouseInfo]
	sparkUrlCache				*memoryCache[string]
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
	if !time.Now().After(entry.expiration) {
		// entry not expired
		return entry.value
	}

	// entry expired
	delete(m.values, id)

	return nil
}

func (m *memoryCache[T]) set(id string, value *T) {
	m.mu.Lock()
	defer m.mu.Unlock()

	entry, ok := m.values[id]
	if !ok {
		// cache miss
		entry = &cacheEntry[T] { value: value }
	} else {
		entry.value = value
	}

	entry.expiration = time.Now().Add(m.expiry)

	m.values[id] = entry
}

// @todo: allow cache expiry values to be configured

func initCaches() {
	workspaceInfoCache = newMemoryCache[*workspaceInfo](5 * time.Minute)
	clusterInfoCache = newMemoryCache[*clusterInfo](5 * time.Minute)
	warehouseInfoCache = newMemoryCache[*warehouseInfo](5 * time.Minute)
	// It's unlikely Spark URLs change very often and they are expensive to
	// calculate so this can have a high expiration
	sparkUrlCache = newMemoryCache[string](30 * time.Minute)
}

func getWorkspaceInfo(
	ctx context.Context,
	w *databricksSdk.WorkspaceClient,
) (*workspaceInfo, error) {
	info := workspaceInfoCache.get(WORKSPACE_INFO_CACHE_KEY)
	if info != nil {
		return *info, nil
	}

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

	workspaceInfo := &workspaceInfo{
		workspaceId,
		urlStr,
		hostname,
	}

	workspaceInfoCache.set(WORKSPACE_INFO_CACHE_KEY, &workspaceInfo)

	return workspaceInfo, nil
}

func getClusterInfoById(
	ctx context.Context,
	w *databricksSdk.WorkspaceClient,
	clusterId string,
) (*clusterInfo, error) {
	info := clusterInfoCache.get(clusterId)
	if info != nil {
		return *info, nil
	}

	c, err := w.Clusters.GetByClusterId(ctx, clusterId)
	if err != nil {
		return nil, err
	}

	log.Debugf(
		"cluster ID: %s ; cluster name: %s",
		c.ClusterId,
		c.ClusterName,
	)

	clusterInfo := &clusterInfo{
		name: c.ClusterName,
		source: string(c.ClusterSource),
		creator: c.CreatorUserName,
		singleUserName: c.SingleUserName,
		instancePoolId: c.InstancePoolId,
	}

	clusterInfoCache.set(clusterId, &clusterInfo)

	return clusterInfo, nil
}

func getWarehouseInfoById(
	ctx context.Context,
	w *databricksSdk.WorkspaceClient,
	warehouseId string,
) (*warehouseInfo, error) {
	info := warehouseInfoCache.get(warehouseId)
	if info != nil {
		return *info, nil
	}

	warehouse, err := w.Warehouses.GetById(ctx, warehouseId)
	if err != nil {
		return nil, err
	}

	log.Debugf(
		"warehouse ID: %s ; warehouse name: %s",
		warehouse.Id,
		warehouse.Name,
	)

	warehouseInfo := &warehouseInfo{
		name: warehouse.Name,
		creator: warehouse.CreatorName,
	}

	warehouseInfoCache.set(warehouseId, &warehouseInfo)

	return warehouseInfo, nil
}

func getSparkContextUiPathForCluster(
	ctx context.Context,
	w *databricksSdk.WorkspaceClient,
	c *databricksSdkCompute.ClusterDetails,
) (string, error) {
	// @see https://databrickslabs.github.io/overwatch/assets/_index/realtime_helpers.html

	sparkContextUiPath := sparkUrlCache.get(c.ClusterId)
	if sparkContextUiPath != nil {
		return *sparkContextUiPath, nil
	}

	/// resolve the spark context UI URL for the cluster
	log.Debugf(
		"resolving Spark context UI URL for cluster %s (%s) with source %s",
		c.ClusterName,
		c.ClusterId,
		c.ClusterSource,
	)

	clusterId := c.ClusterId

	log.Debugf(
		"creating execution context for cluster %s (%s)",
		c.ClusterName,
		clusterId,
	)

	waitContextStatus, err := w.CommandExecution.Create(
		ctx,
		databricksSdkCompute.CreateContext{
			ClusterId: clusterId,
			Language: databricksSdkCompute.LanguagePython,
		},
	)
	if err != nil {
		return "", err
	}

	execContext, err := waitContextStatus.Get()
	if err != nil {
		return "", err
	}

	cmd := databricksSdkCompute.Command{
		ClusterId: clusterId,
		Command: `
		print(f'{spark.conf.get("spark.databricks.clusterUsageTags.clusterOwnerOrgId")}')
		print(f'{spark.conf.get("spark.ui.port")}')
		`,
		ContextId: execContext.Id,
		Language: databricksSdkCompute.LanguagePython,
	}

	log.Debugf(
		"executing context UI discovery script for cluster %s (%s)",
		c.ClusterName,
		clusterId,
	)

	waitCommandStatus, err := w.CommandExecution.Execute(
		ctx,
		cmd,
	)
	if err != nil {
		return "", err
	}

	resp, err := waitCommandStatus.Get()
	if err != nil {
		return "", err
	}

	data, ok := resp.Results.Data.(string);
	if !ok {
		return "", fmt.Errorf("command result is not a string value")
	}

	vals := strings.Split(data, "\n")
	if len(vals) != 2 {
		return "", fmt.Errorf("invalid command result")
	}

	if vals[0] == "" || vals[1] == "" {
		return "", fmt.Errorf("empty command results")
	}

	// @TODO: I think this URL pattern only works for multi-tenant accounts.
	// We may need a flag for single tenant accounts and use the o/0 form
	// shown on the overwatch site.

	url := fmt.Sprintf(
		"/driver-proxy-api/o/%s/%s/%s",
		vals[0],
		clusterId,
		vals[1],
	)

	log.Debugf(
		"final spark context ui path for cluster %s (%s): %s",
		c.ClusterName,
		clusterId,
		url,
	)

	sparkUrlCache.set(c.ClusterId, &url)

	return url, nil
}
