package databricks

import (
	"context"
	"strconv"
	"time"

	databricksSdkSql "github.com/databricks/databricks-sdk-go/service/sql"
)

const (
	BILLING_USAGE_QUERY_ID = "billing_usage"
	LIST_PRICES_QUERY_ID = "list_prices"
	JOBS_COST_LIST_COST_PER_JOB_QUERY_ID = "jobs_cost_list_cost_per_job"
	JOBS_COST_LIST_COST_PER_JOB_RUN_QUERY_ID = "jobs_cost_list_cost_per_job_run"
	JOBS_COST_FREQUENT_FAILURES_QUERY_ID = "jobs_cost_frequent_failures"
	JOBS_COST_MOST_RETRIES_QUERY_ID = "jobs_cost_most_retries"
)

func workspaceIdParameterResolver(
	ctx context.Context,
	w DatabricksWorkspace,
) ([]databricksSdkSql.StatementParameterListItem, error) {
	workspaceId, err := w.GetCurrentWorkspaceId(ctx)
	if err != nil {
		return nil, err
	}

	return []databricksSdkSql.StatementParameterListItem{
		{
			Name: "workspace_id",
			Value: strconv.FormatInt(workspaceId, 10),
		},
	}, nil
}

var (
	gBillingUsageQuery = query{
		id: BILLING_USAGE_QUERY_ID,
		title: "system.billing.usage data for prior day",
		query: `
with most_recent_jobs as (
	SELECT
		*,
		ROW_NUMBER() OVER(PARTITION BY workspace_id, job_id ORDER BY change_time DESC) as rn
	FROM
		system.lakeflow.jobs
	WHERE
		workspace_id = :workspace_id
	QUALIFY rn=1
)
SELECT
	t1.account_id AS account_id, record_id, sku_name, cloud,
	usage_start_time, usage_end_time, usage_date, custom_tags,
	usage_unit, usage_quantity,	record_type, ingestion_date,
	billing_origin_product, usage_type,
	usage_metadata.cluster_id,
	usage_metadata.warehouse_id, usage_metadata.instance_pool_id,
	usage_metadata.node_type, usage_metadata.job_id,
	usage_metadata.job_run_id, usage_metadata.job_name,
	usage_metadata.notebook_id, usage_metadata.notebook_path,
	usage_metadata.dlt_pipeline_id, usage_metadata.dlt_update_id,
	usage_metadata.dlt_maintenance_id, usage_metadata.run_name,
	usage_metadata.endpoint_name, usage_metadata.endpoint_id,
	usage_metadata.central_clean_room_id, identity_metadata.run_as,
	product_features.jobs_tier, product_features.sql_tier,
	product_features.dlt_tier, product_features.is_serverless,
	product_features.is_photon, product_features.serving_type,
	t2.name AS lf_job_name, t2.tags AS lf_job_tags
FROM
	system.billing.usage t1
	LEFT JOIN most_recent_jobs t2 ON t2.workspace_id = t1.workspace_id AND t2.job_id = t1.usage_metadata.job_id
WHERE t1.usage_date = CURRENT_DATE() - INTERVAL 1 DAY AND t1.workspace_id = :workspace_id
LIMIT 15000`,
		eventType: DATABRICKS_USAGE_EVENT_TYPE,
		attributes: []attributeNameAndType{
			{ "account_id", STRING_ATTRIBUTE_TYPE },
			{ "record_id", STRING_ATTRIBUTE_TYPE },
			{ "sku_name", STRING_ATTRIBUTE_TYPE },
			{ "cloud", STRING_ATTRIBUTE_TYPE },
			{ "usage_start_time", RFC3339_DATE_ATTRIBUTE_TYPE },
			{ "usage_end_time", RFC3339_DATE_ATTRIBUTE_TYPE },
			{ "usage_date", STRING_ATTRIBUTE_TYPE },
			{ "custom_tags", TAGS_ATTRIBUTE_TYPE },
			{ "usage_unit", STRING_ATTRIBUTE_TYPE },
			{ "usage_quantity", FLOAT64_ATTRIBUTE_TYPE },
			{ "record_type", STRING_ATTRIBUTE_TYPE },
			{ "ingestion_date", STRING_ATTRIBUTE_TYPE },
			{ "billing_origin_product", STRING_ATTRIBUTE_TYPE },
			{ "usage_type", STRING_ATTRIBUTE_TYPE },
			{ "cluster_id", CLUSTER_ID_ATTRIBUTE_TYPE },
			{ "warehouse_id", WAREHOUSE_ID_ATTRIBUTE_TYPE },
			{ "instance_pool_id", STRING_ATTRIBUTE_TYPE },
			{ "node_type", STRING_ATTRIBUTE_TYPE },
			{ "job_id", STRING_ATTRIBUTE_TYPE },
			{ "job_run_id", STRING_ATTRIBUTE_TYPE },
			{ "serverless_job_name", STRING_ATTRIBUTE_TYPE },
			{ "notebook_id", STRING_ATTRIBUTE_TYPE },
			{ "notebook_path", STRING_ATTRIBUTE_TYPE },
			{ "dlt_pipeline_id", STRING_ATTRIBUTE_TYPE },
			{ "dlt_update_id", STRING_ATTRIBUTE_TYPE },
			{ "dlt_maintenance_id", STRING_ATTRIBUTE_TYPE },
			{ "run_name", STRING_ATTRIBUTE_TYPE },
			{ "endpoint_name", STRING_ATTRIBUTE_TYPE },
			{ "endpoint_id", STRING_ATTRIBUTE_TYPE },
			{ "central_clean_room_id", STRING_ATTRIBUTE_TYPE },
			{ "run_as", ID_ATTRIBUTE_TYPE },
			{ "jobs_tier", STRING_ATTRIBUTE_TYPE },
			{ "sql_tier", STRING_ATTRIBUTE_TYPE },
			{ "dlt_tier", STRING_ATTRIBUTE_TYPE },
			{ "is_serverless", STRING_ATTRIBUTE_TYPE },
			{ "is_photon", STRING_ATTRIBUTE_TYPE },
			{ "serving_type", STRING_ATTRIBUTE_TYPE },
			{ "job_name", STRING_ATTRIBUTE_TYPE },
			{ "job_tags", TAGS_ATTRIBUTE_TYPE },
		},
		offset: 23 * time.Hour,
		includeWorkspaceInfo: true,
		parameterResolver: workspaceIdParameterResolver,
	}
	gListPricesQuery = query{
		id: LIST_PRICES_QUERY_ID,
		title: "system.billing.list_prices data",
		query: `
SELECT
	account_id, price_start_time, price_end_time, sku_name,
	cloud, currency_code, usage_unit, pricing.default
FROM
	system.billing.list_prices
LIMIT 15000`,
		eventType: DATABRICKS_LIST_PRICES_LOOKUP_TABLE_NAME,
		attributes: []attributeNameAndType{
			{ "account_id", STRING_ATTRIBUTE_TYPE },
			{ "price_start_time", RFC3339_DATE_ATTRIBUTE_TYPE },
			{ "price_end_time", RFC3339_DATE_ATTRIBUTE_TYPE },
			{ "sku_name", STRING_ATTRIBUTE_TYPE },
			{ "cloud", STRING_ATTRIBUTE_TYPE },
			{ "currency_code", STRING_ATTRIBUTE_TYPE },
			{ "usage_unit", STRING_ATTRIBUTE_TYPE },
			{ "list_price", FLOAT64_ATTRIBUTE_TYPE },
		},
	}
	gOptionalUsageQueries = []query{
		{
			id: JOBS_COST_LIST_COST_PER_JOB_RUN_QUERY_ID,
			title: "List cost per job run for prior day",
			query: `
with list_cost_per_job_run as (
	SELECT
		t1.workspace_id,
		t1.usage_metadata.job_id,
		t1.usage_metadata.job_run_id as run_id,
		SUM(t1.usage_quantity * list_prices.pricing.default) as list_cost,
		first(identity_metadata.run_as, true) as run_as,
		first(t1.custom_tags, true) as custom_tags,
		MAX(t1.usage_end_time) as last_seen_date
	FROM system.billing.usage t1
	INNER JOIN system.billing.list_prices list_prices on
		t1.cloud = list_prices.cloud and
		t1.sku_name = list_prices.sku_name and
		t1.usage_start_time >= list_prices.price_start_time and
		(t1.usage_end_time <= list_prices.price_end_time or list_prices.price_end_time is null)
	WHERE
		t1.sku_name LIKE '%JOBS%'
		AND t1.usage_metadata.job_id IS NOT NULL
		AND t1.usage_metadata.job_run_id IS NOT NULL
		AND t1.usage_date = CURRENT_DATE() - INTERVAL 1 DAY
		AND t1.workspace_id = :workspace_id
	GROUP BY ALL
),
most_recent_jobs as (
	SELECT
		*,
		ROW_NUMBER() OVER(PARTITION BY workspace_id, job_id ORDER BY change_time DESC) as rn
	FROM
		system.lakeflow.jobs
	WHERE
		workspace_id = :workspace_id
	QUALIFY rn=1
)
SELECT
	t2.name,
	t1.job_id,
	t1.run_id,
	t1.run_as,
	t1.list_cost,
	t1.last_seen_date
FROM list_cost_per_job_run t1
LEFT JOIN most_recent_jobs t2 USING (workspace_id, job_id)
GROUP BY ALL
ORDER BY list_cost DESC`,
			eventType: DATABRICKS_JOB_COST_EVENT_TYPE,
			attributes: []attributeNameAndType{
				{ "job_name", STRING_ATTRIBUTE_TYPE },
				{ "job_id", STRING_ATTRIBUTE_TYPE },
				{ "run_id", STRING_ATTRIBUTE_TYPE },
				{ "run_as", ID_ATTRIBUTE_TYPE },
				{ "list_cost", FLOAT64_ATTRIBUTE_TYPE },
				{ "last_seen_date", RFC3339_DATE_ATTRIBUTE_TYPE },
			},
			offset: 23 * time.Hour,
			includeWorkspaceInfo: true,
			parameterResolver: workspaceIdParameterResolver,
		},
		{
			id: JOBS_COST_LIST_COST_PER_JOB_QUERY_ID,
			title: "List cost per job for prior day",
			query: `
with list_cost_per_job as (
	SELECT
		t1.workspace_id,
		t1.usage_metadata.job_id,
		COUNT(DISTINCT t1.usage_metadata.job_run_id) as runs,
		SUM(t1.usage_quantity * list_prices.pricing.default) as list_cost,
		first(identity_metadata.run_as, true) as run_as,
		first(t1.custom_tags, true) as custom_tags,
		MAX(t1.usage_end_time) as last_seen_date
	FROM system.billing.usage t1
	INNER JOIN system.billing.list_prices list_prices on
		t1.cloud = list_prices.cloud and
		t1.sku_name = list_prices.sku_name and
		t1.usage_start_time >= list_prices.price_start_time and
		(t1.usage_end_time <= list_prices.price_end_time or list_prices.price_end_time is null)
	WHERE
		t1.sku_name LIKE '%JOBS%'
		AND t1.usage_metadata.job_id IS NOT NULL
		AND t1.usage_date = CURRENT_DATE() - INTERVAL 1 DAY
		AND t1.workspace_id = :workspace_id
	GROUP BY ALL
),
most_recent_jobs as (
	SELECT
		*,
		ROW_NUMBER() OVER(PARTITION BY workspace_id, job_id ORDER BY change_time DESC) as rn
	FROM
		system.lakeflow.jobs
	WHERE
		workspace_id = :workspace_id
	QUALIFY rn=1
)
SELECT
	t2.name,
	t1.job_id,
	t1.runs,
	t1.run_as,
	t1.list_cost,
	t1.last_seen_date
FROM list_cost_per_job t1
	LEFT JOIN most_recent_jobs t2 USING (workspace_id, job_id)
GROUP BY ALL
ORDER BY list_cost DESC`,
			eventType: DATABRICKS_JOB_COST_EVENT_TYPE,
			attributes: []attributeNameAndType{
				{ "job_name", STRING_ATTRIBUTE_TYPE },
				{ "job_id", STRING_ATTRIBUTE_TYPE },
				{ "runs", INT64_ATTRIBUTE_TYPE },
				{ "run_as", ID_ATTRIBUTE_TYPE },
				{ "list_cost", FLOAT64_ATTRIBUTE_TYPE },
				{ "last_seen_date", RFC3339_DATE_ATTRIBUTE_TYPE },
			},
			offset: 23 * time.Hour,
			includeWorkspaceInfo: true,
			parameterResolver: workspaceIdParameterResolver,
		},
		{
			id: JOBS_COST_FREQUENT_FAILURES_QUERY_ID,
			title: "Jobs with frequent and costly failures for prior day",
			query: `
with job_run_timeline_with_cost as (
	SELECT
		t1.*,
		t1.identity_metadata.run_as as run_as,
		t2.job_id,
		t2.run_id,
		t2.result_state,
		t1.usage_quantity * list_prices.pricing.default as list_cost
	FROM system.billing.usage t1
		INNER JOIN system.lakeflow.job_run_timeline t2
			ON
				t1.workspace_id=t2.workspace_id
				AND t1.usage_metadata.job_id = t2.job_id
				AND t1.usage_metadata.job_run_id = t2.run_id
				AND t1.usage_start_time >= date_trunc("Hour", t2.period_start_time)
				AND t1.usage_start_time < date_trunc("Hour", t2.period_end_time) + INTERVAL 1 HOUR
		INNER JOIN system.billing.list_prices list_prices on
			t1.cloud = list_prices.cloud and
			t1.sku_name = list_prices.sku_name and
			t1.usage_start_time >= list_prices.price_start_time and
			(t1.usage_end_time <= list_prices.price_end_time or list_prices.price_end_time is null)
	WHERE
		t1.sku_name LIKE '%JOBS%'
		AND t1.usage_date = CURRENT_DATE() - INTERVAL 1 DAYS
		AND t1.workspace_id = :workspace_id
		AND t2.workspace_id = :workspace_id
),
cumulative_run_status_cost as (
	SELECT
		workspace_id,
		job_id,
		run_id,
		run_as,
		result_state,
		usage_end_time,
		SUM(list_cost) OVER (ORDER BY workspace_id, job_id, run_id, usage_end_time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cost
	FROM job_run_timeline_with_cost
	ORDER BY workspace_id, job_id, run_id, usage_end_time
),
cost_per_status as (
	SELECT
			workspace_id,
			job_id,
			run_id,
			run_as,
			result_state,
			usage_end_time,
			cumulative_cost - COALESCE(LAG(cumulative_cost) OVER (ORDER BY workspace_id, job_id, run_id, usage_end_time), 0) AS result_state_cost
	FROM cumulative_run_status_cost
	WHERE result_state IS NOT NULL
	ORDER BY workspace_id, job_id, run_id, usage_end_time),
cost_per_status_agg as (
	SELECT
		workspace_id,
		job_id,
		FIRST(run_as, TRUE) as run_as,
		SUM(result_state_cost) as list_cost
	FROM cost_per_status
	WHERE
		result_state IN ('ERROR', 'FAILED', 'TIMED_OUT')
	GROUP BY ALL
),
terminal_statues as (
	SELECT
		workspace_id,
		job_id,
		CASE WHEN result_state IN ('ERROR', 'FAILED', 'TIMED_OUT') THEN 1 ELSE 0 END as is_failure,
		period_end_time as last_seen_date
	FROM system.lakeflow.job_run_timeline
	WHERE
		result_state IS NOT NULL AND
		period_end_time >= CURRENT_DATE() - INTERVAL 1 DAYS AND
		period_end_time < CURRENT_DATE() AND
		workspace_id = :workspace_id
),
most_recent_jobs as (
	SELECT
		*,
		ROW_NUMBER() OVER(PARTITION BY workspace_id, job_id ORDER BY change_time DESC) as rn
	FROM
		system.lakeflow.jobs
	WHERE
		workspace_id = :workspace_id
	QUALIFY rn=1
)
SELECT
	first(t2.name) as name,
	t1.job_id,
	COUNT(*) as runs,
	t3.run_as,
	SUM(is_failure) as failures,
	first(t3.list_cost) as failure_list_cost,
	MAX(t1.last_seen_date) as last_seen_date
FROM terminal_statues t1
	LEFT JOIN most_recent_jobs t2 USING (workspace_id, job_id)
	LEFT JOIN cost_per_status_agg t3 USING (workspace_id, job_id)
GROUP BY ALL
ORDER BY failures DESC`,
			eventType: DATABRICKS_JOB_COST_EVENT_TYPE,
			attributes: []attributeNameAndType{
				{ "job_name", STRING_ATTRIBUTE_TYPE },
				{ "job_id", STRING_ATTRIBUTE_TYPE },
				{ "runs", INT64_ATTRIBUTE_TYPE },
				{ "run_as", ID_ATTRIBUTE_TYPE },
				{ "failures", INT64_ATTRIBUTE_TYPE },
				{ "list_cost", FLOAT64_ATTRIBUTE_TYPE },
				{ "last_seen_date", RFC3339_DATE_ATTRIBUTE_TYPE },
			},
			offset: 23 * time.Hour,
			includeWorkspaceInfo: true,
			parameterResolver: workspaceIdParameterResolver,
		},
		{
			id: JOBS_COST_MOST_RETRIES_QUERY_ID,
			title: "Jobs with the highest number of retries for prior day",
			query: `
with job_run_timeline_with_cost as (
	SELECT
		t1.*,
		t2.job_id,
		t2.run_id,
		t1.identity_metadata.run_as as run_as,
		t2.result_state,
		t1.usage_quantity * list_prices.pricing.default as list_cost
	FROM system.billing.usage t1
		INNER JOIN system.lakeflow.job_run_timeline t2
			ON
				t1.workspace_id=t2.workspace_id
				AND t1.usage_metadata.job_id = t2.job_id
				AND t1.usage_metadata.job_run_id = t2.run_id
				AND t1.usage_start_time >= date_trunc("Hour", t2.period_start_time)
				AND t1.usage_start_time < date_trunc("Hour", t2.period_end_time) + INTERVAL 1 HOUR
		INNER JOIN system.billing.list_prices list_prices on
			t1.cloud = list_prices.cloud and
			t1.sku_name = list_prices.sku_name and
			t1.usage_start_time >= list_prices.price_start_time and
			(t1.usage_end_time <= list_prices.price_end_time or list_prices.price_end_time is null)
	WHERE
		t1.sku_name LIKE '%JOBS%'
		AND t1.usage_date = CURRENT_DATE() - INTERVAL 1 DAYS
		AND t1.workspace_id = :workspace_id
		AND t2.workspace_id = :workspace_id
),
cumulative_run_status_cost as (
	SELECT
		workspace_id,
		job_id,
		run_id,
		run_as,
		result_state,
		usage_end_time,
		SUM(list_cost) OVER (ORDER BY workspace_id, job_id, run_id, usage_end_time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cost
	FROM job_run_timeline_with_cost
	ORDER BY workspace_id, job_id, run_id, usage_end_time
),
cost_per_status as (
	SELECT
		workspace_id,
		job_id,
		run_id,
		run_as,
		result_state,
		usage_end_time,
		cumulative_cost - COALESCE(LAG(cumulative_cost) OVER (ORDER BY workspace_id, job_id, run_id, usage_end_time), 0) AS result_state_cost
	FROM cumulative_run_status_cost
	WHERE result_state IS NOT NULL
	ORDER BY workspace_id, job_id, run_id, usage_end_time
),
cost_per_unsuccesful_status_agg as (
	SELECT
		workspace_id,
		job_id,
		run_id,
		first(run_as, TRUE) as run_as,
		SUM(result_state_cost) as list_cost
	FROM cost_per_status
	WHERE
		result_state != "SUCCEEDED"
	GROUP BY ALL
),
repaired_runs as (
	SELECT
		workspace_id, job_id, run_id, COUNT(*) as cnt
	FROM system.lakeflow.job_run_timeline
	WHERE result_state IS NOT NULL AND workspace_id = :workspace_id
	GROUP BY ALL
	HAVING cnt > 1
),
successful_repairs as (
	SELECT t1.workspace_id, t1.job_id, t1.run_id, MAX(t1.period_end_time) as period_end_time
	FROM system.lakeflow.job_run_timeline t1
		JOIN repaired_runs t2
		ON t1.workspace_id=t2.workspace_id AND t1.job_id=t2.job_id AND t1.run_id=t2.run_id
	WHERE t1.result_state="SUCCEEDED" AND t1.workspace_id = :workspace_id
	GROUP BY ALL
),
combined_repairs as (
	SELECT
		t1.*,
		t2.period_end_time,
		t1.cnt as repairs
	FROM repaired_runs t1
		LEFT JOIN successful_repairs t2 USING (workspace_id, job_id, run_id)
),
most_recent_jobs as (
	SELECT
		*,
		ROW_NUMBER() OVER(PARTITION BY workspace_id, job_id ORDER BY change_time DESC) as rn
	FROM
		system.lakeflow.jobs
	WHERE
		workspace_id = :workspace_id
	QUALIFY rn=1
)
SELECT
	last(t3.name) as name,
	t1.job_id,
	t1.run_id,
	first(t4.run_as, TRUE) as run_as,
	first(t1.repairs) - 1 as repairs,
	first(t4.list_cost) as repair_list_cost,
	CASE WHEN t1.period_end_time IS NOT NULL THEN CAST(t1.period_end_time - MIN(t2.period_end_time) as LONG) ELSE NULL END AS repair_time_seconds
FROM combined_repairs t1
	JOIN system.lakeflow.job_run_timeline t2 USING (workspace_id, job_id, run_id)
	LEFT JOIN most_recent_jobs t3 USING (workspace_id, job_id)
	LEFT JOIN cost_per_unsuccesful_status_agg t4 USING (workspace_id, job_id, run_id)
WHERE
	t2.result_state IS NOT NULL
	AND t2.workspace_id = :workspace_id
GROUP BY t1.workspace_id, t1.job_id, t1.run_id, t1.period_end_time
ORDER BY repairs DESC`,
			eventType: DATABRICKS_JOB_COST_EVENT_TYPE,
			attributes: []attributeNameAndType{
				{ "job_name", STRING_ATTRIBUTE_TYPE },
				{ "job_id", STRING_ATTRIBUTE_TYPE },
				{ "run_id", STRING_ATTRIBUTE_TYPE },
				{ "run_as", ID_ATTRIBUTE_TYPE },
				{ "repairs", INT64_ATTRIBUTE_TYPE },
				{ "list_cost", FLOAT64_ATTRIBUTE_TYPE },
				{ "repair_time_seconds", INT64_ATTRIBUTE_TYPE },
			},
			offset: 23 * time.Hour,
			includeWorkspaceInfo: true,
			parameterResolver: workspaceIdParameterResolver,
		}}
)
