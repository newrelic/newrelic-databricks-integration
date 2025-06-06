package databricks

import (
	"context"
	"encoding/json"
	"time"

	databricksSdkSql "github.com/databricks/databricks-sdk-go/service/sql"
	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/log"
	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/model"
	"github.com/spf13/cast"
)

type eventAttributeType	int64

const (
	STRING_ATTRIBUTE_TYPE eventAttributeType = iota
	INT64_ATTRIBUTE_TYPE
	FLOAT64_ATTRIBUTE_TYPE
	RFC3339_DATE_ATTRIBUTE_TYPE
	TAGS_ATTRIBUTE_TYPE
	ID_ATTRIBUTE_TYPE
	CLUSTER_ID_ATTRIBUTE_TYPE
	WAREHOUSE_ID_ATTRIBUTE_TYPE
	IGNORE_ATTRIBUTE_TYPE
)

type parameterResolverFunc func(
	ctx context.Context,
	w DatabricksWorkspace,
) ([]databricksSdkSql.StatementParameterListItem, error)

type attributeNameAndType struct {
	attrName		string
	attrType		eventAttributeType
}

type query struct {
	id						string
    title       			string
	query					string
	eventType				string
	attributes				[]attributeNameAndType
	offset					time.Duration
	includeInvalidRows		bool
	includeWorkspaceInfo	bool
	parameterResolver		parameterResolverFunc
}

type processRowFunc		func(query *query, attrs map[string]interface{}) error

type DatabricksQueryReceiver struct {
	id							string
	w							DatabricksWorkspace
	warehouseId 				string
	defaultCatalog				string
	defaultSchema				string
	includeIdentityMetadata 	bool
	queries						[]*query
}

func NewDatabricksQueryReceiver(
	id string,
	w DatabricksWorkspace,
	warehouseId string,
	defaultCatalog string,
	defaultSchema string,
	includeIdentityMetadata bool,
	queries []*query,
) *DatabricksQueryReceiver {
	return &DatabricksQueryReceiver{
		id,
		w,
		warehouseId,
		defaultCatalog,
		defaultSchema,
		includeIdentityMetadata,
		queries,
	}
}

func (d *DatabricksQueryReceiver) GetId() string {
	return d.id
}

func (d *DatabricksQueryReceiver) PollEvents(
	ctx context.Context,
	writer chan <- model.Event,
) error {
	processRowFunc := func(query *query, attrs map[string]interface{}) error {
		writer <- model.NewEvent(
			query.eventType,
			attrs,
			time.Now().Add(-query.offset),
		)

		return nil
	}

	for _, query := range d.queries {
		err := runQuery(
			ctx,
			d.w,
			d.warehouseId,
			d.defaultCatalog,
			d.defaultSchema,
			d.includeIdentityMetadata,
			query,
			processRowFunc,
		)
		if err != nil {
			log.Warnf(
				"query %s failed with error: %v",
				query.id,
				err,
			)
			continue
		}
	}

	return nil
}

func runQuery(
	ctx context.Context,
	w DatabricksWorkspace,
	warehouseId string,
	defaultCatalog string,
	defaultSchema string,
	includeIdentityMetadata bool,
	query *query,
	processRowFunc processRowFunc,
) error {
	log.Debugf("running query %s (%s)", query.id, query.title)

	var (
		params 	[]databricksSdkSql.StatementParameterListItem
		err		error
	)

	if query.parameterResolver != nil {
		params, err = query.parameterResolver(ctx, w)
		if err != nil {
			return err
		}
	}

	var workspaceInfo *WorkspaceInfo

	if query.includeWorkspaceInfo {
		workspaceInfo, err = GetWorkspaceInfo(ctx)
		if err != nil {
			return err
		}
	}

	rows, err := executeStatementOnWarehouse(
		ctx,
		w,
		warehouseId,
		defaultCatalog,
		defaultSchema,
		query.query,
		params,
	)
	if err != nil {
		return err
	}

	attributes := query.attributes

	LOOP:

	for i, row := range rows {
		if len(row) != len(attributes) {
			log.Warnf(
				"number of result columns %d in row %d does not match number of expected columns %d",
				len(row),
				i,
				len(attributes),
			)
			continue
		}

		attrs := map[string]interface{}{}
		attrs["query_id"] = query.id
		attrs["query_title"] = query.title

		if workspaceInfo != nil {
			attrs["workspace_id"] = workspaceInfo.Id
			attrs["workspace_url"] = workspaceInfo.Url
			attrs["workspace_instance_name"] = workspaceInfo.InstanceName
		}

		for j, col := range row {
			attrName := attributes[j].attrName
			ok := true

			switch attributes[j].attrType {
			case IGNORE_ATTRIBUTE_TYPE:
				continue

			case STRING_ATTRIBUTE_TYPE:
				attrs[attrName] = col

			case INT64_ATTRIBUTE_TYPE:
				ok = columnToInt64(
					attrs,
					attrName,
					col,
					i,
					j,
				)

			case FLOAT64_ATTRIBUTE_TYPE:
				ok = columnToFloat64(
					attrs,
					attrName,
					col,
					i,
					j,
				)

			case RFC3339_DATE_ATTRIBUTE_TYPE:
				ok = columnToTimeMillis(
					attrs,
					attrName,
					col,
					i,
					j,
				)

			case TAGS_ATTRIBUTE_TYPE:
				ok = columnToTags(
					attrs,
					attrName,
					col,
					i,
					j,
				)

			case ID_ATTRIBUTE_TYPE:
				if !includeIdentityMetadata {
					continue
				}

				attrs[attrName] = col

			case CLUSTER_ID_ATTRIBUTE_TYPE:
				ok = columnToClusterInfo(
					ctx,
					attrs,
					attrName,
					col,
					i,
					j,
					includeIdentityMetadata,
				)

			case WAREHOUSE_ID_ATTRIBUTE_TYPE:
				ok = columnToWarehouseInfo(
					ctx,
					attrs,
					attrName,
					col,
					i,
					j,
					includeIdentityMetadata,
				)

			default:
				ok = false
				log.Warnf(
					"invalid type %d for query attribute %s while processing column %d of row %d",
					attributes[j].attrType,
					attrName,
					j,
					i,
				)
			}

			if !ok && !query.includeInvalidRows {
				continue LOOP
			}
		}

		err = processRowFunc(query, attrs)
		if err != nil {
			return err
		}
	}

	return nil
}

func columnToInt64(
	attrs map[string]interface{},
	attrName string,
	col string,
	rowIndex int,
	colIndex int,
) bool {
	if col == "" {
		return true
	}

	val, err := cast.ToInt64E(col)
	if err != nil {
		log.Warnf(
			"could not cast result column %d in row %d for query attribute %s from %v to int64",
			colIndex,
			rowIndex,
			attrName,
			col,
		)

		return false
	}

	attrs[attrName] = val

	return true
}


func columnToFloat64(
	attrs map[string]interface{},
	attrName string,
	col string,
	rowIndex int,
	colIndex int,
) bool {
	if col == "" {
		return true
	}

	val, err := cast.ToFloat64E(col)
	if err != nil {
		log.Warnf(
			"could not cast result column %d in row %d for query attribute %s from %v to float64",
			colIndex,
			rowIndex,
			attrName,
			col,
		)

		return false
	}

	attrs[attrName] = val

	return true
}

func columnToTimeMillis(
	attrs map[string]interface{},
	attrName string,
	col string,
	rowIndex int,
	colIndex int,
) bool {
	if col == "" {
		return true
	}

	t, err := time.Parse(time.RFC3339Nano, col)
	if err != nil {
		log.Warnf(
			"invalid date \"%s\" for query attribute %s while processing result column %d in row %d",
			col,
			attrName,
			colIndex,
			rowIndex,
		)

		return false
	}

	attrs[attrName] = t.UnixMilli()

	return true
}

func columnToTags(
	attrs map[string]interface{},
	attrName string,
	col string,
	rowIndex int,
	colIndex int,
) bool {
	if col == "" {
		return true
	}

	tags, err := unmarshalStruct(col)
	if err != nil {
		log.Warnf(
			"invalid json \"%s\" for query attribute %s while processing result column %d in row %d",
			col,
			attrName,
			colIndex,
			rowIndex,
		)

		return false
	}

	for k, v := range tags {
		attrs[k] = v
	}

	return true
}

func columnToClusterInfo(
	ctx context.Context,
	attrs map[string]interface{},
	attrName string,
	col string,
	rowIndex int,
	colIndex int,
	includeIdentityMetadata bool,
) bool {
	if col == "" {
		return true
	}

	attrs[attrName] = col

	clusterInfo, err := getClusterInfoById(
		ctx,
		col,
	)
	if err != nil {
		log.Warnf(
			"could not resolve cluster ID %s to cluster info for query attribute %s while processing column %d in row %d: %v",
			col,
			attrName,
			colIndex,
			rowIndex,
			err,
		)
	} else if clusterInfo == nil {
		log.Warnf(
			"could not resolve cluster ID %s to cluster info for query attribute %s while processing column %d in row %d: cluster ID not found",
			col,
			attrName,
			colIndex,
			rowIndex,
		)
	} else {
		attrs["cluster_name"] = clusterInfo.name

		if includeIdentityMetadata {
			attrs["cluster_single_user_name"] = clusterInfo.singleUserName
			attrs["cluster_creator"] = clusterInfo.creator
		}

		attrs["cluster_source"] = clusterInfo.source
		attrs["cluster_instance_pool_id"] = clusterInfo.instancePoolId
	}

	// unresolved cluster id is not considered a failure
	// query result will just be missing the cluster info

	return true
}

func columnToWarehouseInfo(
	ctx context.Context,
	attrs map[string]interface{},
	attrName string,
	col string,
	rowIndex int,
	colIndex int,
	includeIdentityMetadata bool,
) bool {
	if col == "" {
		return true
	}

	attrs[attrName] = col

	warehouseInfo, err := getWarehouseInfoById(
		ctx,
		col,
	)
	if err != nil {
		log.Warnf(
			"could not resolve warehouse ID %s to warehouse info for query attribute %s while processing column %d in row %d: %v",
			col,
			attrName,
			colIndex,
			rowIndex,
			err,
		)
	} else if warehouseInfo == nil {
		log.Warnf(
			"could not resolve warehouse ID %s to warehouse info for query attribute %s while processing column %d in row %d: warehouse ID not found",
			col,
			attrName,
			colIndex,
			rowIndex,
		)
	} else {
		attrs["warehouse_name"] = warehouseInfo.name

		if includeIdentityMetadata {
			attrs["warehouse_creator"] = warehouseInfo.creator
		}
	}

	// unresolved warehouse id is not considered a failure
	// query result will just be missing the warehouse info

	return true
}

func unmarshalStruct(col string) (map[string]interface{}, error) {
	var m map[string]interface{}

	attrs := map[string]interface{}{}

	err := json.Unmarshal([]byte(col), &m)
	if err != nil {
		return nil, err
	}

	for k, v := range m {
		switch val := v.(type) {
		case string, float64, bool, nil:
			attrs[k] = val
		default:
			log.Warnf(
				"skipping unsupported scalar or complex value type found for key %s when converting struct JSON",
				k,
			)
		}
	}

	return attrs, nil
}
