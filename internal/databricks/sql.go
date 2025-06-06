package databricks

import (
	"context"
	"fmt"
	"sync"
	"time"

	databricksSdkSql "github.com/databricks/databricks-sdk-go/service/sql"
	"github.com/newrelic/newrelic-labs-sdk/v2/pkg/integration/log"
	"github.com/spf13/viper"
)

const (
	DEFAULT_SQL_STATEMENT_TIMEOUT = 30
)

func executeStatementOnWarehouse(
	ctx context.Context,
	w DatabricksWorkspace,
	warehouseId string,
	defaultCatalog string,
	defaultSchema string,
	stmt string,
	params []databricksSdkSql.StatementParameterListItem,
) ([][]string, error) {
	log.Debugf(
		"executing statement %s for catalog %s and schema %s",
		stmt,
		defaultCatalog,
		defaultSchema,
	)

	execResp, err := w.ExecuteStatement(
		ctx,
        warehouseId,
        defaultCatalog,
        defaultSchema,
        stmt,
        params,
	)
	if err != nil {
		return nil, err
	}

	if execResp.Status != nil &&
		execResp.Status.State == databricksSdkSql.StatementStateFailed {
		return nil, fmt.Errorf(execResp.Status.Error.Message)
	}

	statementId := execResp.StatementId

	var (
		wg sync.WaitGroup
		rows [][]string
	)

	wg.Add(1)

	go func() {
		defer wg.Done()

		rows, err = pollStatement(ctx, w, statementId)
	}()

	wg.Wait()

	if err != nil {
		return nil, err
	}

	log.Debugf("received %d rows", len(rows))

	return rows, nil
}

func pollStatement(
	ctx context.Context,
	w DatabricksWorkspace,
	statementId string,
) ([][]string, error) {
	sqlStatementTimeout := viper.GetInt("databricks.sqlStatementTimeout")
	if sqlStatementTimeout == 0 {
		sqlStatementTimeout = DEFAULT_SQL_STATEMENT_TIMEOUT
	}

	log.Debugf("using sql statement timeout %d", sqlStatementTimeout)

	rows := [][]string{}
	chunkIndex := 0
	ticker := time.NewTicker(1 * time.Second)
	timer := time.NewTimer(time.Duration(sqlStatementTimeout) * time.Second)

	defer func() {
		ticker.Stop()
		timer.Stop()
	}()

	for {
		select {
		case <- ticker.C:
			if chunkIndex == 0 {
				// still processing the first chunk

				result, nextChunkIndex, done, err := getStatementStatus(
					ctx,
					w,
					statementId,
				)

				// statement failed
				if err != nil {
					return nil, err
				}

				// no err, not done: statement pending or running, keep waiting
				if !done {
					continue
				}

				// no err, done, next chunk == 0: statement done, no additional
				// chunks to retrieve
				if nextChunkIndex == 0 {
					return result, nil
				}

				// no err, done, next chunk != 0: statement done, additional
				// chunks to retrieve

				// append first chunk of results
				rows = append(rows, result...)

				// move to the next chunk
				chunkIndex = nextChunkIndex
			}

			// get the next chunk of results
			log.Debugf("getting chunk %d", chunkIndex)
			resultData, err := w.GetStatementResultChunkNByStatementIdAndChunkIndex(
				ctx,
				statementId,
				chunkIndex,
			)
			if err != nil {
				return nil, err
			}

			// append this chunk of results
			rows = append(rows, resultData.DataArray...)

			if resultData.NextChunkIndex == 0 {
				// no additional chunks to retrieve, all done
				return rows, nil
			}

			// additional chunks to retrieve, keep waiting

		case <- timer.C:
			w.CancelExecution(
				ctx,
				statementId,
			)

			return nil,
				fmt.Errorf("sql result data not received in timely manner")
		case <- ctx.Done():
			w.CancelExecution(
				ctx,
				statementId,
			)

			return nil,
				fmt.Errorf("sql result data not received in timely manner")
		}
	}
}

func getStatementStatus(
	ctx context.Context,
	w DatabricksWorkspace,
	statementId string,

) ([][]string, int, bool, error) {
	log.Debugf("polling statement status for statement %s", statementId)

	getResp, err := w.GetStatementByStatementId(
		ctx,
		statementId,
	)
	if err != nil {
		// api call error
		return nil, 0, false, err
	}

	if getResp.Status == nil {
		// guard against nil status (shouldn't happen)
		return nil, 0, false, fmt.Errorf("status unexpectedly nil on response")
	}

	status := getResp.Status.State

	if status == databricksSdkSql.StatementStatePending ||
		status == databricksSdkSql.StatementStateRunning {
		// statement is not done yet
		return nil, 0, false, nil
	}

	if status == databricksSdkSql.StatementStateFailed {
		// sql statement execution failed
		return nil, 0, false, fmt.Errorf(getResp.Status.Error.Message)
	}

	if status == databricksSdkSql.StatementStateCanceled ||
		status == databricksSdkSql.StatementStateClosed {
		// sql statement cancelled by Databricks, a user, or another process, or
		// statement succeeded, was closed, and result can no longer be
		// retrieved (shouldn't happen in our case)
		return nil,
			0,
			false,
			fmt.Errorf("statement unexpectedly canceled or closed")
	}

	// statement succeeded

	if getResp.Result == nil {
		// guard against nil result (shouldn't happen)
		return nil,
			0,
			false,
			fmt.Errorf("result unexpectedly nil on success")
	}

	// return rows, next chunk, done, and no error
	return getResp.Result.DataArray,
		getResp.Result.NextChunkIndex,
		true,
		nil
}
