package hub

import "C"

import (
	"context"
	"fmt"
	"log"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/turbot/steampipe-plugin-sdk/v6/grpc"
	"github.com/turbot/steampipe-plugin-sdk/v6/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v6/telemetry"
	"github.com/turbot/steampipe-postgres-fdw/v2/settings"
	"github.com/turbot/steampipe-postgres-fdw/v2/types"
	"github.com/turbot/steampipe/v2/pkg/constants"
	"github.com/turbot/steampipe/v2/pkg/query/queryresult"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

type hubBase struct {
	runningIterators     map[Iterator]struct{}
	runningIteratorsLock sync.RWMutex

	// cacheSettings
	cacheSettings *settings.HubCacheSettings

	// telemetry properties
	// callback function to shutdown telemetry
	telemetryShutdownFunc func()
	hydrateCallsCounter   metric.Int64Counter
	queryTiming           *queryTimingMetadata

	// parallel worker coordination
	parallelWorkerCoordination *ParallelWorkerCoordinator
}

// ParallelWorkerCoordinator manages parallel worker lifecycle and timeout
type ParallelWorkerCoordinator struct {
	activeWorkers      map[int]struct{} // Track active worker PIDs
	workerTimeout      int              // Timeout in seconds
	executionStart     int64            // Execution start time (Unix timestamp)
	coordinatorMutex   sync.RWMutex     // Protects coordinator state
	coordinatorRunning bool             // Track if global coordinator is running
}

// NewParallelWorkerCoordinator creates a new coordinator with default timeout
func NewParallelWorkerCoordinator() *ParallelWorkerCoordinator {
	timeout := 30 // Default 30 seconds
	if envTimeout := os.Getenv("STEAMPIPE_FDW_WORKER_TIMEOUT"); envTimeout != "" {
		if customTimeout, err := strconv.Atoi(envTimeout); err == nil && customTimeout > 0 {
			timeout = customTimeout
		}
	}

	return &ParallelWorkerCoordinator{
		activeWorkers:  make(map[int]struct{}),
		workerTimeout:  timeout,
		executionStart: time.Now().Unix(),
	}
}

// RegisterWorker registers a new parallel worker
func (pwc *ParallelWorkerCoordinator) RegisterWorker(pid int) {
	pwc.coordinatorMutex.Lock()
	defer pwc.coordinatorMutex.Unlock()

	pwc.activeWorkers[pid] = struct{}{}
	log.Printf("[DEBUG] Worker PID %d registered - total active: %d", pid, len(pwc.activeWorkers))
}

// UnregisterWorker removes a parallel worker from tracking
func (pwc *ParallelWorkerCoordinator) UnregisterWorker(pid int) {
	pwc.coordinatorMutex.Lock()
	defer pwc.coordinatorMutex.Unlock()

	delete(pwc.activeWorkers, pid)
	log.Printf("[DEBUG] Worker PID %d unregistered - total active: %d", pid, len(pwc.activeWorkers))

	// If this was the last worker, reset the coordinator state to allow cleanup
	if len(pwc.activeWorkers) == 0 {
		log.Printf("[DEBUG] Worker PID %d: All workers unregistered - resetting coordinator state", os.Getpid())
		pwc.coordinatorRunning = false
		pwc.executionStart = time.Now().Unix() // Reset execution start time
	}
}

// ShouldWorkerTimeout checks if a worker should timeout based on elapsed time
func (pwc *ParallelWorkerCoordinator) ShouldWorkerTimeout() bool {
	pwc.coordinatorMutex.RLock()
	defer pwc.coordinatorMutex.RUnlock()

	elapsed := time.Now().Unix() - pwc.executionStart
	return elapsed > int64(pwc.workerTimeout)
}

// MarkWorkerTimedOut marks a worker as timed out for graceful termination
func (pwc *ParallelWorkerCoordinator) MarkWorkerTimedOut(pid int) {
	pwc.coordinatorMutex.Lock()
	defer pwc.coordinatorMutex.Unlock()

	// Remove from active workers since it's timing out
	delete(pwc.activeWorkers, pid)
	log.Printf("[DEBUG] Worker PID %d marked as timed out - removed from active workers, total active: %d", pid, len(pwc.activeWorkers))
}

// GetActiveWorkerCount returns the number of active workers
func (pwc *ParallelWorkerCoordinator) GetActiveWorkerCount() int {
	pwc.coordinatorMutex.RLock()
	defer pwc.coordinatorMutex.RUnlock()
	return len(pwc.activeWorkers)
}

// StartGlobalCoordinator starts a global coordinator that monitors all workers
// This should only be called once, typically by the first worker to register
func (pwc *ParallelWorkerCoordinator) StartGlobalCoordinator() {
	pwc.coordinatorMutex.Lock()
	defer pwc.coordinatorMutex.Unlock()

	// Check if coordinator is already running
	if pwc.coordinatorRunning {
		return
	}
	pwc.coordinatorRunning = true

	log.Printf("[INFO] Worker PID %d: Starting global parallel worker coordinator", os.Getpid())

	// Start background coordinator goroutine with resource-efficient monitoring
	go func() {
		defer func() {
			// Ensure coordinator is marked as stopped when goroutine exits
			pwc.coordinatorMutex.Lock()
			pwc.coordinatorRunning = false
			pwc.coordinatorMutex.Unlock()
			log.Printf("[INFO] Worker PID %d: Global Coordinator: Coordinator goroutine exiting", os.Getpid())
		}()

		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		maxIterations := 60 // Maximum 5 minutes of monitoring (60 * 5 seconds)
		iterations := 0

		for {
			select {
			case <-ticker.C:
				iterations++

				pwc.coordinatorMutex.RLock()
				activeCount := len(pwc.activeWorkers)
				shouldTimeout := pwc.ShouldWorkerTimeout()
				pwc.coordinatorMutex.RUnlock()

				// Exit if no workers are active for several iterations
				if activeCount == 0 {
					log.Printf("[INFO] Worker PID %d: Global Coordinator: No active workers, exiting coordinator", os.Getpid())
					return
				}

				// Exit after maximum monitoring time to prevent resource leaks
				if iterations >= maxIterations {
					log.Printf("[INFO] Worker PID %d: Global Coordinator: Maximum monitoring time reached, exiting coordinator", os.Getpid())
					return
				}

				if shouldTimeout && activeCount > 0 {
					log.Printf("[INFO] Worker PID %d: Global Coordinator: Timeout detected with %d active workers", os.Getpid(), activeCount)

					// Get list of active worker PIDs
					pwc.coordinatorMutex.RLock()
					var workerPids []int
					for pid := range pwc.activeWorkers {
						workerPids = append(workerPids, pid)
					}
					pwc.coordinatorMutex.RUnlock()

					// Mark all workers as timed out
					for _, pid := range workerPids {
						log.Printf("[INFO] Worker PID %d: Global Coordinator: Marking worker PID %d as timed out", os.Getpid(), pid)
						pwc.MarkWorkerTimedOut(pid)
					}

					// Exit coordinator after handling timeout
					return
				}
			}
		}
	}()
}

// ResetExecution resets the execution start time for new parallel execution
func (pwc *ParallelWorkerCoordinator) ResetExecution() {
	pwc.coordinatorMutex.Lock()
	defer pwc.coordinatorMutex.Unlock()

	pwc.executionStart = time.Now().Unix()
	pwc.activeWorkers = make(map[int]struct{})
	log.Printf("[DEBUG] Worker PID %d: Parallel execution reset - timeout: %d seconds", os.Getpid(), pwc.workerTimeout)
}

// CheckForIdleWorkers monitors for idle workers and handles timeouts
func (pwc *ParallelWorkerCoordinator) CheckForIdleWorkers() []int {
	pwc.coordinatorMutex.RLock()
	defer pwc.coordinatorMutex.RUnlock()

	if !pwc.ShouldWorkerTimeout() {
		return nil
	}

	// If we've exceeded the timeout, return list of active workers that should be terminated
	var idleWorkers []int
	for pid := range pwc.activeWorkers {
		idleWorkers = append(idleWorkers, pid)
	}

	if len(idleWorkers) > 0 {
		log.Printf("[WARN] Worker PID %d: Detected %d idle workers after %d second timeout", os.Getpid(), len(idleWorkers), pwc.workerTimeout)
	}

	return idleWorkers
}

// MonitorWorkerTimeout starts a background goroutine to monitor worker timeouts
func (h *hubBase) MonitorWorkerTimeout(ctx context.Context) {
	if h.parallelWorkerCoordination == nil {
		return
	}

	go func() {
		ticker := time.NewTicker(5 * time.Second) // Check every 5 seconds
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				log.Printf("[DEBUG] Worker PID %d: Worker timeout monitor stopping", os.Getpid())
				return
			case <-ticker.C:
				idleWorkers := h.parallelWorkerCoordination.CheckForIdleWorkers()
				if len(idleWorkers) > 0 {
					log.Printf("[WARN] Worker PID %d: Found %d idle workers that may need termination: %v", os.Getpid(), len(idleWorkers), idleWorkers)
					// In a real implementation, we might send signals to these workers
					// or implement other coordination mechanisms

					// For now, we'll log the issue and let PostgreSQL handle cleanup
					// The key insight is that we've detected the problem
				}
			}
		}
	}()

	log.Printf("[DEBUG] Worker PID %d: Started worker timeout monitor with %d second timeout", os.Getpid(), h.parallelWorkerCoordination.workerTimeout)
}

// RegisterParallelWorker implements the Hub interface
func (h *hubBase) RegisterParallelWorker(pid int) {
	log.Printf("[DEBUG] Worker PID %d: RegisterParallelWorker() called", pid)
	if h.parallelWorkerCoordination != nil {
		h.parallelWorkerCoordination.RegisterWorker(pid)
	} else {
		log.Printf("[DEBUG] Worker PID %d: No parallel worker coordination available", pid)
	}
}

// UnregisterParallelWorker implements the Hub interface
func (h *hubBase) UnregisterParallelWorker(pid int) {
	log.Printf("[DEBUG] Worker PID %d: UnregisterParallelWorker() called", pid)
	if h.parallelWorkerCoordination != nil {
		h.parallelWorkerCoordination.UnregisterWorker(pid)
	} else {
		log.Printf("[DEBUG] Worker PID %d: No parallel worker coordination available", pid)
	}
}

// GetParallelWorkerCoordinator implements the Hub interface
func (h *hubBase) GetParallelWorkerCoordinator() *ParallelWorkerCoordinator {
	return h.parallelWorkerCoordination
}

func newHubBase(enableScanMetadata bool) *hubBase {
	h := &hubBase{
		runningIterators:           make(map[Iterator]struct{}),
		parallelWorkerCoordination: NewParallelWorkerCoordinator(),
	}
	if enableScanMetadata {
		h.queryTiming = newQueryTimingMetadata()
	}
	return h
}

// GetRelSize is a method called from the planner to estimate the resulting relation size for a scan.
//
//	It will help the planner in deciding between different types of plans,
//	according to their costs.
//	Args:
//	    columns (list): The list of columns that must be returned.
//	    quals (list): A list of Qual instances describing the filters
//	        applied to this scan.
//	Returns:
//	    A struct of the form (expected_number_of_rows, avg_row_width (in bytes))
func (h *hubBase) GetRelSize(columns []string, quals []*proto.Qual, opts types.Options) (types.RelSize, error) {
	result := types.RelSize{
		// Default to 1M rows, because these tables are typically expensive
		// relative to standard postgres.
		Rows: 1000000,
		// Width is in bytes, assuming an average of 100 per column.
		Width: 100 * len(columns),
	}
	return result, nil
}

// GetPathKeys Is a method called from the planner to add additional Path to the planner.
//
//	By default, the planner generates an (unparameterized) path, which
//	can be reasoned about like a SequentialScan, optionally filtered.
//	This method allows the implementor to declare other Paths,
//	corresponding to faster access methods for specific attributes.
//	Such a parameterized path can be reasoned about like an IndexScan.
//	For example, with the following query::
//	    select * from foreign_table inner join local_table using(id);
//	where foreign_table is a foreign table containing 100000 rows, and
//	local_table is a regular table containing 100 rows.
//	The previous query would probably be transformed to a plan similar to
//	this one::
//	    ┌────────────────────────────────────────────────────────────────────────────────────┐
//	    │                                     QUERY PLAN                                     │
//	    ├────────────────────────────────────────────────────────────────────────────────────┤
//	    │ Hash Join  (cost=57.67..4021812.67 rows=615000 width=68)                           │
//	    │   Hash Cond: (foreign_table.id = local_table.id)                                   │
//	    │   ->  Foreign Scan on foreign_table (cost=20.00..4000000.00 rows=100000 width=40)  │
//	    │   ->  Hash  (cost=22.30..22.30 rows=1230 width=36)                                 │
//	    │         ->  Seq Scan on local_table (cost=0.00..22.30 rows=1230 width=36)          │
//	    └────────────────────────────────────────────────────────────────────────────────────┘
//	But with a parameterized path declared on the id key, with the knowledge that this key
//	is unique on the foreign side, the following plan might get chosen::
//	    ┌───────────────────────────────────────────────────────────────────────┐
//	    │                              QUERY PLAN                               │
//	    ├───────────────────────────────────────────────────────────────────────┤
//	    │ Nested Loop  (cost=20.00..49234.60 rows=615000 width=68)              │
//	    │   ->  Seq Scan on local_table (cost=0.00..22.30 rows=1230 width=36)   │
//	    │   ->  Foreign Scan on remote_table (cost=20.00..40.00 rows=1 width=40)│
//	    │         Filter: (id = local_table.id)                                 │
//	    └───────────────────────────────────────────────────────────────────────┘
//	Returns:
//	    A list of tuples of the form: (key_columns, expected_rows),
//	    where key_columns is a tuple containing the columns on which
//	    the path can be used, and expected_rows is the number of rows
//	    this path might return for a simple lookup.
//	    For example, the return value corresponding to the previous scenario would be::
//	        [(('id',), 1)]
func (h *hubBase) getPathKeys(connectionSchema *proto.Schema, opts types.Options) ([]types.PathKey, error) {
	connectionName := opts["connection"]
	table := opts["table"]

	log.Printf("[TRACE] Worker PID %d: hub.GetPathKeys for connection '%s`, table `%s`", os.Getpid(), connectionName, table)
	tableSchema, ok := connectionSchema.Schema[table]
	if !ok {
		return nil, fmt.Errorf("no schema loaded for connection '%s', table '%s'", connectionName, table)
	}
	var allColumns = make([]string, len(tableSchema.Columns))
	for i, c := range tableSchema.Columns {
		allColumns[i] = c.Name
	}

	var pathKeys []types.PathKey

	// build path keys based on the table key columns
	// NOTE: the schema data has changed in SDK version 1.3 - we must handle plugins using legacy sdk explicitly
	// check for legacy sdk versions
	if tableSchema.ListCallKeyColumns != nil {
		log.Printf("[TRACE] Worker PID %d: schema response include ListCallKeyColumns, it is using legacy protobuff interface ", os.Getpid())
		pathKeys = types.LegacyKeyColumnsToPathKeys(tableSchema.ListCallKeyColumns, tableSchema.ListCallOptionalKeyColumns, allColumns)
	} else if tableSchema.ListCallKeyColumnList != nil {
		log.Printf("[TRACE] Worker PID %d: schema response include ListCallKeyColumnList, it is using the updated protobuff interface ", os.Getpid())
		// generate path keys if there are required list key columns
		// this increases the chances that Postgres will generate a plan which provides the quals when querying the table
		pathKeys = types.KeyColumnsToPathKeys(tableSchema.ListCallKeyColumnList, allColumns)
	}
	// NOTE: in the future we may (optionally) add in path keys for Get call key columns.
	// We do not do this by default as it is likely to actually reduce join performance in the general case,
	// particularly when caching is taken into account

	//var getCallPathKeys []types.PathKey
	//if getKeyColumns := schema.GetCallKeyColumns; getKeyColumns != nil {
	//	getCallPathKeys = types.KeyColumnsToPathKeys(getKeyColumns)
	//}
	//pathKeys := types.MergePathKeys(getCallPathKeys, listCallPathKeys)

	log.Printf("[TRACE] Worker PID %d: GetPathKeys for connection '%s`, table `%s` returning", os.Getpid(), connectionName, table)
	return pathKeys, nil
}

// Explain ::  hook called on explain.
//
//	Returns:
//	    An iterable of strings to display in the EXPLAIN output.
func (h *hubBase) Explain(columns []string, quals []*proto.Qual, sortKeys []string, verbose bool, opts types.Options) ([]string, error) {
	return make([]string, 0), nil
}

// StartScan starts a scan
func (h *hubBase) StartScan(i Iterator) error {
	pid := os.Getpid()
	log.Printf("[DEBUG] Worker PID %d: StartScan - beginning for iterator %p", pid, i)

	// Note: Worker registration now happens in the C parallel callback (fdwInitializeWorkerForeignScan)
	// This ensures ALL workers are registered, not just those that get assigned work
	// This fixes the core issue where idle workers were never registered

	// if iterator is not a pluginIterator, do nothing
	// (i.e. is it an InMemoryIterator
	// This code should never be called for them anyway as they are initialized to be in a `started` state,
	// and the scan is started using the function executeCommandScan)
	iterator, ok := i.(pluginIterator)
	if !ok {
		// unexpected
		log.Printf("[WARN] Worker PID %d: StartScan called for non-pluginIterator %T", pid, i)
		return nil
	}

	// ask the iterator for the executor interface
	// (the iterator just returns itself - we need to do it this way because of the way nested structs work )
	log.Printf("[DEBUG] Worker PID %d: StartScan - about to call iterator.Start() for %p", pid, i)
	iterator.Start(i.(pluginExecutor))
	log.Printf("[DEBUG] Worker PID %d: StartScan - iterator.Start() completed for %p", pid, i)

	// add iterator to running list
	log.Printf("[DEBUG] Worker PID %d: StartScan - running iterator count before add: %d", pid, len(h.runningIterators))
	h.addIterator(iterator)
	log.Printf("[DEBUG] Worker PID %d: StartScan - running iterator count after add: %d", pid, len(h.runningIterators))
	log.Printf("[DEBUG] Worker PID %d: StartScan - completed for iterator %p", pid, i)

	return nil
}

// EndScan is called when Postgres terminates the scan (because it has received enough rows of data)
func (h *hubBase) EndScan(iter Iterator, limit int64) {
	pid := os.Getpid()
	log.Printf("[DEBUG] Worker PID %d: EndScan - starting for iterator %p, status: %s", pid, iter, iter.Status())
	log.Printf("[DEBUG] Worker PID %d: EndScan - running iterator count before cleanup: %d", pid, len(h.runningIterators))

	// Unregister worker from parallel coordination if enabled
	if h.parallelWorkerCoordination != nil {
		h.parallelWorkerCoordination.UnregisterWorker(pid)
		log.Printf("[DEBUG] Worker PID %d: EndScan - unregistered parallel worker", pid)
	}

	// is the iterator still running? If so it means postgres is stopping a scan before all rows have been read
	if iter.Status() == QueryStatusStarted {
		log.Printf("[INFO] Worker PID %d: ending scan before iterator complete - limit: %v, iterator: %p", pid, limit, iter)
		// we normally add the scan metadata when the scan completes - if the scan has not completed, we need to
		// add the metadata here
		h.AddScanMetadata(iter)
		log.Printf("[DEBUG] Worker PID %d: EndScan - closing iterator %p", pid, iter)
		iter.Close()
	} else {
		log.Printf("[DEBUG] Worker PID %d: EndScan - iterator status is %s (not STARTED) for %p", pid, iter.Status(), iter)
	}

	log.Printf("[DEBUG] Worker PID %d: EndScan - removing iterator %p from running list", pid, iter)
	h.RemoveIterator(iter)
	log.Printf("[DEBUG] Worker PID %d: EndScan - running iterator count after cleanup: %d", pid, len(h.runningIterators))
	log.Printf("[INFO] Worker PID %d: EndScan - completed for iterator %p", pid, iter)
}

// AddScanMetadata adds the scan metadata from the given iterator to the hubs array
// we append to this every time a scan completes (either due to end of data, or Postgres terminating)
// the full array is returned whenever a pop_scan_metadata command is received and the array is cleared
func (h *hubBase) AddScanMetadata(i Iterator) {
	// for local hub we do not store scan metadata
	if h.queryTiming == nil {
		return
	}
	// if iterator is not a pluginIterator, do nothing
	iter, ok := i.(pluginIterator)
	if !ok {
		return
	}

	queryTimestamp := iter.GetQueryTimestamp()

	log.Printf("[INFO] Worker PID %d: AddScanMetadata for iterator %p query timestamp %d (%s)", os.Getpid(), iter, queryTimestamp, iter.GetConnectionName())

	h.queryTiming.scanMetadataLock.Lock()
	defer h.queryTiming.scanMetadataLock.Unlock()

	ctx := iter.GetTraceContext().Ctx

	connectionName := iter.GetConnectionName()
	pluginName := iter.GetPluginName()

	// get scan metadata from iterator
	scanMetadata := iter.GetScanMetadata()
	querySummary := h.queryTiming.queryRowSummary[queryTimestamp]
	if querySummary == nil {
		querySummary = queryresult.NewQueryRowSummary()
	}

	// ensure we only keep scan metadata for this query, and limit the amount of metadata we keep
	h.queryTiming.removeStaleScanMetadata(queryTimestamp)

	for _, m := range scanMetadata {
		// update summary
		querySummary.Update(m)

		// add the scan metadata to the list
		// (if we have exceeded the max number of scan metadata items, this will keep the slowest items
		h.queryTiming.addScanMetadata(queryTimestamp, m)

		// hydrate metric labels
		labels := []attribute.KeyValue{
			attribute.String("table", m.Table),
			attribute.String("connection", connectionName),
			attribute.String("plugin", pluginName),
		}
		h.hydrateCallsCounter.Add(ctx, m.HydrateCalls, metric.WithAttributes(labels...))
	}
	// write the scan metadata and summary back to the hub
	h.queryTiming.queryRowSummary[queryTimestamp] = querySummary
}

// Close shuts down all plugin clients
func (h *hubBase) Close() {
	log.Printf("[TRACE] Worker PID %d: hub: close", os.Getpid())

	if h.telemetryShutdownFunc != nil {
		log.Printf("[TRACE] Worker PID %d: shutdown telemetry", os.Getpid())
		h.telemetryShutdownFunc()
	}
}

// Abort shuts down currently running queries
func (h *hubBase) Abort() {
	log.Printf("[WARN] Worker PID %d: Abort - acquiring read lock to abort %d running iterators", os.Getpid(), len(h.runningIterators))

	// for all running iterators
	h.runningIteratorsLock.RLock()
	defer h.runningIteratorsLock.RUnlock()

	iteratorCount := len(h.runningIterators)
	log.Printf("[WARN] Worker PID %d: Abort - found %d running iterators to abort", os.Getpid(), iteratorCount)

	for iter := range h.runningIterators {
		log.Printf("[DEBUG] Worker PID %d: Abort - processing iterator %p, status: %s", os.Getpid(), iter, iter.Status())
		// read the scan metadata from the iterator and add to our stack
		h.AddScanMetadata(iter)
		// close the iterator
		log.Printf("[DEBUG] Worker PID %d: Abort - closing iterator %p", os.Getpid(), iter)
		iter.Close()
	}
	// clear running iterators
	h.runningIterators = make(map[Iterator]struct{})
	log.Printf("[WARN] Worker PID %d: Abort - completed, cleared %d iterators", os.Getpid(), iteratorCount)
}

// settings

func (h *hubBase) ApplySetting(key string, value string) error {
	log.Printf("[TRACE] Worker PID %d: ApplySetting [%s => %s]", os.Getpid(), key, value)
	return h.cacheSettings.Apply(key, value)
}

func (h *hubBase) GetSettingsSchema() map[string]*proto.TableSchema {
	return map[string]*proto.TableSchema{
		constants.ForeignTableSettings: {
			Columns: []*proto.ColumnDefinition{
				{Name: constants.ForeignTableSettingsKeyColumn, Type: proto.ColumnType_STRING},
				{Name: constants.ForeignTableSettingsValueColumn, Type: proto.ColumnType_STRING},
			},
		},
		constants.ForeignTableScanMetadata: {
			Columns: []*proto.ColumnDefinition{
				{Name: "connection", Type: proto.ColumnType_STRING},
				{Name: "table", Type: proto.ColumnType_STRING},
				{Name: "cache_hit", Type: proto.ColumnType_BOOL},
				{Name: "rows_fetched", Type: proto.ColumnType_INT},
				{Name: "hydrate_calls", Type: proto.ColumnType_INT},
				{Name: "start_time", Type: proto.ColumnType_TIMESTAMP},
				{Name: "duration_ms", Type: proto.ColumnType_INT},
				{Name: "columns", Type: proto.ColumnType_JSON},
				{Name: "limit", Type: proto.ColumnType_INT},
				{Name: "quals", Type: proto.ColumnType_JSON},
			},
		},
		constants.ForeignTableScanMetadataSummary: {
			Columns: []*proto.ColumnDefinition{
				{Name: "cached_rows_fetched", Type: proto.ColumnType_INT},
				{Name: "uncached_rows_fetched", Type: proto.ColumnType_INT},
				{Name: "hydrate_calls", Type: proto.ColumnType_INT},
				{Name: "duration_ms", Type: proto.ColumnType_INT},
				{Name: "scan_count", Type: proto.ColumnType_INT},
				{Name: "connection_count", Type: proto.ColumnType_INT},
			},
		},
	}
}

func (h *hubBase) ValidateCacheCommand(command string) error {
	validCommands := []string{constants.LegacyCommandCacheClear, constants.LegacyCommandCacheOn, constants.LegacyCommandCacheOff}

	if !slices.Contains(validCommands, command) {
		return fmt.Errorf("invalid command '%s' - supported commands are %s", command, strings.Join(validCommands, ","))
	}
	return nil
}

func (h *hubBase) GetConnectionConfigByName(string) *proto.ConnectionConfig {
	// do nothing- only implemented in standalone
	return nil
}

func (h *hubBase) ProcessImportForeignSchemaOptions(types.Options, string) error {
	// do nothing- only implemented in standalone
	return nil
}

func (h *hubBase) executeCommandScan(connectionName, table string, queryTimestamp int64) (Iterator, error) {
	switch table {
	case constants.ForeignTableScanMetadataSummary:
		// we expect to only have metadata for  one query at a time - this is enforced by the metadata writing code
		if summaryCount := len(h.queryTiming.queryRowSummary); summaryCount > 1 {
			return nil, fmt.Errorf(" executeCommandScan for table '%s' - %d summaries in metadata store - there should only be 1. ", table, summaryCount)
		}

		res := &QueryResult{}
		for _, summary := range h.queryTiming.queryRowSummary {
			res.Rows = append(res.Rows, summary.AsResultRow())
		}
		// now we have read the summary, we can clear the cached data
		h.queryTiming.clearSummary()

		return newInMemoryIterator(connectionName, res, queryTimestamp), nil
	case constants.ForeignTableScanMetadata, constants.LegacyCommandTableScanMetadata:
		if metadataCount := len(h.queryTiming.scanMetadata); metadataCount > 1 {
			return nil, fmt.Errorf(" executeCommandScan for table '%s' - %d summaries in metadata store - there should only be 1. ", table, metadataCount)
		}

		res := &QueryResult{}
		for _, scansForQuery := range h.queryTiming.scanMetadata {
			for _, m := range scansForQuery {
				res.Rows = append(res.Rows, m.AsResultRow())
			}
		}
		// now we have read the scan metadata, we can clear the cached data
		h.queryTiming.clearScanMetadata()

		return newInMemoryIterator(connectionName, res, queryTimestamp), nil
	default:
		return nil, fmt.Errorf("cannot select from command table '%s'", table)
	}
}

func (h *hubBase) traceContextForScan(table string, columns []string, limit int64, qualMap map[string]*proto.Quals, connectionName string, opts types.Options) *telemetry.TraceCtx {
	var baseCtx context.Context = context.Background()

	// Check if we have trace context from session variables
	if traceContextStr, exists := opts["trace_context"]; exists && traceContextStr != "" {
		log.Printf("[DEBUG] Worker PID %d: traceContextForScan received trace context: %s", os.Getpid(), traceContextStr)
		if parentCtx := h.parseTraceContext(traceContextStr); parentCtx != nil {
			baseCtx = parentCtx
			log.Printf("[TRACE] Worker PID %d: Using parent trace context for scan of table: %s", os.Getpid(), table)

			// Verify the parent context has the expected trace ID
			parentSpanCtx := trace.SpanContextFromContext(parentCtx)
			if parentSpanCtx.IsValid() {
				log.Printf("[DEBUG] Worker PID %d: Parent context TraceID: %s, SpanID: %s", os.Getpid(),
					parentSpanCtx.TraceID().String(), parentSpanCtx.SpanID().String())
			}
		} else {
			log.Printf("[WARN] Worker PID %d: Failed to parse trace context for table: %s", os.Getpid(), table)
		}
	} else {
		log.Printf("[DEBUG] Worker PID %d: No trace context found in options for table: %s", os.Getpid(), table)
	}

	// Create span with potentially propagated context
	ctx, span := telemetry.StartSpan(baseCtx, FdwName, "RemoteHub.Scan (%s)", table)
	span.SetAttributes(
		attribute.StringSlice("columns", columns),
		attribute.String("table", table),
		attribute.String("quals", grpc.QualMapToString(qualMap, false)),
		attribute.String("connection", connectionName),
	)
	if limit != -1 {
		span.SetAttributes(attribute.Int64("limit", limit))
	}

	spanCtx := span.SpanContext()
	if spanCtx.IsValid() {
		log.Printf("[DEBUG] Worker PID %d: Created span for table %s - TraceID: %s, SpanID: %s", os.Getpid(),
			table, spanCtx.TraceID().String(), spanCtx.SpanID().String())
	}

	return &telemetry.TraceCtx{Ctx: ctx, Span: span}
}

// parseTraceContext parses trace context string from session variables or SQLcommenter
// Supports both formats:
// - Session variables: "traceparent=00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01;tracestate=rojo=00f067aa0ba902b7"
// - SQLcommenter: "traceparent='00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01',tracestate='rojo=00f067aa0ba902b7'"
func (h *hubBase) parseTraceContext(traceContextString string) context.Context {
	log.Printf("[DEBUG] Worker PID %d: parseTraceContext called with: %s", os.Getpid(), traceContextString)

	if traceContextString == "" {
		log.Printf("[DEBUG] Worker PID %d: Empty trace context string", os.Getpid())
		return nil
	}

	carrier := propagation.MapCarrier{}

	// Detect format and parse accordingly
	var parts []string
	if strings.Contains(traceContextString, ",") {
		// SQLcommenter format: "traceparent='...',tracestate='...'"
		parts = strings.Split(traceContextString, ",")
		log.Printf("[DEBUG] Worker PID %d: Detected SQLcommenter format, split into %d parts: %v", os.Getpid(), len(parts), parts)
	} else {
		// Session variable format: "traceparent=..;tracestate=.."
		parts = strings.Split(traceContextString, ";")
		log.Printf("[DEBUG] Worker PID %d: Detected session variable format, split into %d parts: %v", os.Getpid(), len(parts), parts)
	}

	for _, part := range parts {
		if kv := strings.SplitN(part, "=", 2); len(kv) == 2 {
			key := strings.TrimSpace(kv[0])
			value := strings.TrimSpace(kv[1])

			// Remove quotes from SQLcommenter format
			if (strings.HasPrefix(value, "'") && strings.HasSuffix(value, "'")) ||
				(strings.HasPrefix(value, "\"") && strings.HasSuffix(value, "\"")) {
				value = value[1 : len(value)-1]
				log.Printf("[DEBUG] Worker PID %d: Removed quotes from value: %s", os.Getpid(), value)
			}

			carrier[key] = value
			log.Printf("[DEBUG] Worker PID %d: Added to carrier: %s = %s", os.Getpid(), key, value)
		} else {
			log.Printf("[DEBUG] Worker PID %d: Skipping invalid part: %s", os.Getpid(), part)
		}
	}

	log.Printf("[DEBUG] Worker PID %d: Final carrier contents: %v", os.Getpid(), carrier)

	if len(carrier) == 0 {
		log.Printf("[WARN] Worker PID %d: No valid trace context found in: %s", os.Getpid(), traceContextString)
		return nil
	}

	// Use OpenTelemetry propagator to extract context
	propagator := propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	)
	extractedCtx := propagator.Extract(context.Background(), carrier)

	// Verify we actually got a valid span context
	spanCtx := trace.SpanContextFromContext(extractedCtx)
	if spanCtx.IsValid() {
		log.Printf("[TRACE] Worker PID %d: Successfully extracted trace context - TraceID: %s, SpanID: %s", os.Getpid(),
			spanCtx.TraceID().String(), spanCtx.SpanID().String())
		return extractedCtx
	}

	log.Printf("[WARN] Worker PID %d: Extracted trace context is not valid - carrier was: %v", os.Getpid(), carrier)
	return nil
}

// determine whether to include the limit, based on the quals
// we ONLY pushdown the limit if all quals have corresponding key columns,
// and if the qual operator is supported by the key column
func (h *hubBase) shouldPushdownLimit(table string, qualMap map[string]*proto.Quals, unhandledRestrictions int, connectionSchema *proto.Schema) bool {
	// if we have any unhandled restrictions, we CANNOT push limit down
	if unhandledRestrictions > 0 {
		return false
	}

	// build a map of all key columns
	tableSchema, ok := connectionSchema.Schema[table]
	if !ok {
		// any errors, just default to NOT pushing down the limit
		return false
	}
	var keyColumnMap = make(map[string]*proto.KeyColumn)
	for _, k := range tableSchema.ListCallKeyColumnList {
		keyColumnMap[k.Name] = k
	}
	for _, k := range tableSchema.GetCallKeyColumnList {
		keyColumnMap[k.Name] = k
	}

	// for every qual, determine if it has a key column and if the operator is supported
	// if NOT, we cannot push down the limit

	for col, quals := range qualMap {
		// check whether this qual is declared as a key column for this table
		if k, ok := keyColumnMap[col]; ok {
			log.Printf("[TRACE] Worker PID %d: shouldPushdownLimit found key column for column %s: %v", os.Getpid(), col, k)

			// check whether every qual for this column has a supported operator
			for _, q := range quals.Quals {
				operator := q.GetStringValue()
				if !slices.Contains(k.Operators, operator) {
					log.Printf("[INFO] Worker PID %d: operator '%s' not supported for column '%s'. NOT pushing down limit", os.Getpid(), operator, col)
					return false
				}
				log.Printf("[TRACE] Worker PID %d: shouldPushdownLimit operator '%s' is supported for column '%s'.", os.Getpid(), operator, col)
			}
		} else {
			// no key column defined for this qual - DO NOT push down the limit
			log.Printf("[INFO] Worker PID %d: shouldPushdownLimit no key column found for column '%s'. NOT pushing down limit", os.Getpid(), col)
			return false
		}
	}

	// all quals are supported - push down limit
	log.Printf("[INFO] Worker PID %d: shouldPushdownLimit all quals are supported - pushing down limit", os.Getpid())
	return true
}

func (h *hubBase) initialiseTelemetry() error {
	log.Printf("[TRACE] Worker PID %d: init telemetry", os.Getpid())
	shutdownTelemetry, err := telemetry.Init(FdwName)
	if err != nil {
		return fmt.Errorf("failed to initialise telemetry: %s", err.Error())
	}

	h.telemetryShutdownFunc = shutdownTelemetry

	hydrateCalls, err := otel.GetMeterProvider().Meter(FdwName).Int64Counter(
		fmt.Sprintf("%s-hydrate_calls_total", FdwName),
		metric.WithDescription("The total number of hydrate calls"),
	)
	if err != nil {
		log.Printf("[WARN] Worker PID %d: init telemetry failed to create hydrateCallsCounter", os.Getpid())
		return err
	}
	h.hydrateCallsCounter = hydrateCalls
	return nil
}

func (h *hubBase) addIterator(iterator Iterator) {
	h.runningIteratorsLock.Lock()
	defer h.runningIteratorsLock.Unlock()

	h.runningIterators[iterator] = struct{}{}
}

// RemoveIterator removes an iterator from list of running iterators
func (h *hubBase) RemoveIterator(iterator Iterator) {
	h.runningIteratorsLock.Lock()
	defer h.runningIteratorsLock.Unlock()

	delete(h.runningIterators, iterator)
}

func (h *hubBase) GetLegacySettingsSchema() map[string]*proto.TableSchema {
	return map[string]*proto.TableSchema{
		constants.LegacyCommandTableCache: {
			Columns: []*proto.ColumnDefinition{
				{Name: constants.LegacyCommandTableCacheOperationColumn, Type: proto.ColumnType_STRING},
			},
		},
		constants.LegacyCommandTableScanMetadata: {
			Columns: []*proto.ColumnDefinition{
				{Name: "id", Type: proto.ColumnType_INT},
				{Name: "table", Type: proto.ColumnType_STRING},
				{Name: "cache_hit", Type: proto.ColumnType_BOOL},
				{Name: "rows_fetched", Type: proto.ColumnType_INT},
				{Name: "hydrate_calls", Type: proto.ColumnType_INT},
				{Name: "start_time", Type: proto.ColumnType_TIMESTAMP},
				{Name: "duration", Type: proto.ColumnType_DOUBLE},
				{Name: "columns", Type: proto.ColumnType_JSON},
				{Name: "limit", Type: proto.ColumnType_INT},
				{Name: "quals", Type: proto.ColumnType_STRING},
			},
		},
	}
}

func (h *hubBase) HandleLegacyCacheCommand(command string) error {
	if err := h.ValidateCacheCommand(command); err != nil {
		return err
	}

	log.Printf("[TRACE] Worker PID %d: HandleLegacyCacheCommand %s", os.Getpid(), command)

	switch command {
	case constants.LegacyCommandCacheClear:
		// set the cache clear time for the remote query cache
		h.cacheSettings.Apply(string(settings.SettingKeyCacheClearTimeOverride), "")

	case constants.LegacyCommandCacheOn:
		h.cacheSettings.Apply(string(settings.SettingKeyCacheEnabled), "true")
	case constants.LegacyCommandCacheOff:
		h.cacheSettings.Apply(string(settings.SettingKeyCacheClearTimeOverride), "false")
	}
	return nil
}

// GetSortableFields
func (h *hubBase) GetSortableFields(tableName, connectionName string) map[string]proto.SortOrder {
	return nil
}
