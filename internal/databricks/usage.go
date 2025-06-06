package databricks

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"

	"time"

	"github.com/newrelic/newrelic-client-go/v2/pkg/region"
	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration"
	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/connectors"
	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/log"
)

const (
	DATABRICKS_USAGE_EVENT_TYPE = "DatabricksUsage"
	DATABRICKS_LIST_PRICES_LOOKUP_TABLE_NAME = "DatabricksListPrices"
	DATABRICKS_JOB_COST_EVENT_TYPE = "DatabricksJobCost"
)

type newRelicNrqlLookupsApiRequest struct {
	Table struct {
		Headers []string 				`json:"headers"`
		Rows [][]interface{}			`json:"rows"`
	} 									`json:"table"`
}

func NewDatabricksListPricesCollector(
	li *integration.LabsIntegration,
	w DatabricksWorkspace,
	warehouseId string,
) *integration.SimpleComponent {
	return integration.NewSimpleComponent(
		"databricks-list-prices-collector",
		li,
		func(ctx context.Context, i *integration.LabsIntegration) error {
			return li.UseLastUpdate(ctx, func(lastUpdate time.Time) error {
				log.Debugf("collecting billing list prices data")
				err := importBillingListPricesTable(
					ctx,
					w,
					warehouseId,
					i.GetRegion(),
					i.GetAccountId(),
					i.GetApiKey(),
					i.DryRun,
					lastUpdate,
				)
				if err != nil {
					return err
				}

				return nil
			}, "databricks-list-prices-collector")
		},
	)
}

func buildListPricesNrqlLookupsApiRequest(
	rows []map[string]interface{},
) *newRelicNrqlLookupsApiRequest {
	body := &newRelicNrqlLookupsApiRequest{}

	body.Table.Headers = []string{
		"account_id",
		"price_start_time",
		"price_end_time",
		"sku_name",
		"cloud",
		"currency_code",
		"usage_unit",
		"list_price",
	}

	tableRows := [][]interface{}{}

	for _, row := range rows {
		startTime, ok := row["price_start_time"]
		if !ok {
			log.Warnf(
				"missing price start time for sku %s",
				row["sku_name"],
			)
			continue
		}

		endTime, ok := row["price_end_time"]
		if !ok {
			// missing end time is valid and we'll default to 0 in this case
			endTime = int64(0)
		}

		listPrice, ok := row["list_price"]
		if !ok {
			log.Warnf(
				"missing list price for sku %s",
				row["sku_name"],
			)
			continue
		}

		tableRows = append(tableRows, []interface{}{
			row["account_id"],
			startTime,
			endTime,
			row["sku_name"],
			row["cloud"],
			row["currency_code"],
			row["usage_unit"],
			listPrice,
		})
	}

	body.Table.Rows = tableRows

	return body
}

func listPricesPostBodyBuilder(rows []map[string]interface{}) connectors.HttpBodyBuilder {
	return func() (any, error) {
		jsonValue, err := json.Marshal(
			buildListPricesNrqlLookupsApiRequest(rows),
		)
		if err != nil {
			return nil, err
		}

		return bytes.NewReader(jsonValue), nil
	}
}

func importBillingListPricesTable(
	ctx context.Context,
	w DatabricksWorkspace,
	warehouseId string,
	newRelicRegion region.Name,
	newRelicAccountId int,
	newRelicApiKey string,
	dryRun bool,
	lastUpdate time.Time,
) error {
	rows := []map[string]interface{}{}

	processRowFunc := func(query *query, attrs map[string]interface{}) error {
		rows = append(rows, attrs)
		return nil
	}

	err := runQuery(
		ctx,
		w,
		warehouseId,
		"system",
		"billing",
		false,
		&gListPricesQuery,
		processRowFunc,
	)
	if err != nil {
		return err
	}

	client := connectors.NewHttpPostConnector(
		getNrqlLookupsApiUrl(
			newRelicRegion,
			newRelicAccountId,
			&gListPricesQuery,
		),
		listPricesPostBodyBuilder(rows),
	)

	// On original creation, method must be POST but on updates, must be PUT
	// so if the last update time is not the zero time, we have already created
	// the lookup table so use PUT. Otherwise use POST.
	if !lastUpdate.IsZero() {
		client.SetMethod("PUT")
	}

	if dryRun || log.IsDebugEnabled() {
		log.Debugf("list prices lookup table payload JSON follows")
		log.PrettyPrintJson(buildListPricesNrqlLookupsApiRequest(rows))

		if dryRun {
			return nil
		}
	}

	headers := map[string]string{
		"Accept": "application/json",
		"Content-Type": "application/json",
		"Api-Key": newRelicApiKey,
	}

	client.SetHeaders(headers)

	readCloser, err := client.Request()
	if err != nil {
		return err
	}

	defer readCloser.Close()

	if log.IsDebugEnabled() {
		log.Debugf("table api response payload follows")

		bytes, err := io.ReadAll(readCloser)
		if err != nil {
			log.Warnf("error reading table api response: %v", err)
		} else {
			log.Debugf(string(bytes))
		}
	}

	return nil
}

func getNrqlLookupsApiUrl(
	newRelicRegion region.Name,
	newRelicAccountId int,
	listPricesQuery *query,
) string {
	if newRelicRegion == region.EU {
		return fmt.Sprintf(
			"https://nrql-lookup.service.eu.newrelic.com/v1/accounts/%d/%s",
			newRelicAccountId,
			listPricesQuery.eventType,
		)
	}

	return fmt.Sprintf(
		"https://nrql-lookup.service.newrelic.com/v1/accounts/%d/%s",
		newRelicAccountId,
		listPricesQuery.eventType,
	)
}
