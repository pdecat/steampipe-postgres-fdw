package main

/*
#cgo linux LDFLAGS: -Wl,-unresolved-symbols=ignore-all
#cgo darwin LDFLAGS: -Wl,-undefined,dynamic_lookup
#include "fdw_helpers.h"

#include "utils/rel.h"
#include "nodes/pg_list.h"
#include "utils/timestamp.h"

*/
import "C"

import (
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"syscall"
	"time"
	"unsafe"

	"github.com/hashicorp/go-hclog"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/logging"
	"github.com/turbot/steampipe-plugin-sdk/v5/sperr"
	"github.com/turbot/steampipe-postgres-fdw/v2/hub"
	"github.com/turbot/steampipe-postgres-fdw/v2/types"
	"github.com/turbot/steampipe-postgres-fdw/v2/version"
	"github.com/turbot/steampipe/v2/pkg/cmdconfig"
	"github.com/turbot/steampipe/v2/pkg/constants"
)

var logger hclog.Logger

// force loading of this module
//
//export goInit
func goInit() {}

// Register a parallel worker for timeout monitoring
// This is called from C code when a parallel worker is initialized
//
//export goFdwRegisterParallelWorker
func goFdwRegisterParallelWorker(pid C.int) {
	workerPid := int(pid)
	log.Printf("[DEBUG] Worker PID %d: goFdwRegisterParallelWorker() called - registering for timeout monitoring", workerPid)

	// Get the current hub instance and register the worker
	currentHub := hub.GetHub()
	if currentHub != nil {
		// Use the interface method to register the worker
		currentHub.RegisterParallelWorker(workerPid)
		log.Printf("[DEBUG] Worker PID %d: Successfully registered with parallel worker coordinator", workerPid)
	} else {
		log.Printf("[WARN] Worker PID %d: No hub instance available for worker registration", workerPid)
	}
}

func init() {
	if logger != nil {
		return
	}

	// HACK: env vars do not all get copied into the Go env vars so explicitly copy them
	SetEnvVars()
	// set steampipe app specific constants
	cmdconfig.SetAppSpecificConstants()

	level := logging.LogLevel()
	log.Printf("[INFO] Worker PID %d: Log level %s", os.Getpid(), level)
	if level != "TRACE" {
		// suppress logs
		log.SetOutput(io.Discard)
	}
	logger = logging.NewLogger(&hclog.LoggerOptions{
		Name:       "hub",
		TimeFn:     func() time.Time { return time.Now().UTC() },
		TimeFormat: "2006-01-02 15:04:05.000 UTC",
	})
	log.SetOutput(logger.StandardWriter(&hclog.StandardLoggerOptions{InferLevels: true}))
	log.SetPrefix("")
	log.SetFlags(0)
	// create hub
	if err := hub.CreateHub(); err != nil {
		panic(err)
	}
	log.Printf("[INFO] Worker PID %d: .\n******************************************************\n\n\t\tsteampipe postgres fdw init\n\n******************************************************\n", os.Getpid())
	log.Printf("[INFO] Worker PID %d: Version:   v%s", os.Getpid(), version.FdwVersion.String())
	log.Printf("[INFO] Worker PID %d: Log level: %s", os.Getpid(), level)

	// Register this worker immediately when the FDW extension loads
	// This catches ALL workers, including idle parallel workers that never call FDW functions
	currentHub := hub.GetHub()
	if currentHub != nil {
		currentHub.RegisterParallelWorker(os.Getpid())
		log.Printf("[DEBUG] Worker PID %d: Registered for parallel worker coordination in init()", os.Getpid())
		
		// Start global timeout monitor for this worker
		// This is critical for idle workers that never call any FDW functions
		go func() {
			workerPid := os.Getpid()
			coordinator := currentHub.GetParallelWorkerCoordinator()
			if coordinator == nil {
				log.Printf("[DEBUG] Worker PID %d: No coordinator available for timeout monitoring", workerPid)
				return
			}
			
			// Check timeout every 5 seconds
			ticker := time.NewTicker(5 * time.Second)
			defer ticker.Stop()
			
			for {
				select {
				case <-ticker.C:
					if coordinator.ShouldWorkerTimeout() {
						log.Printf("[INFO] Worker PID %d: Global timeout detected - idle worker should terminate", workerPid)
						// For idle workers that never call FDW functions, we need to force termination
						// This is the only way to handle workers that are completely idle
						coordinator.MarkWorkerTimedOut(workerPid)
						// Send SIGTERM to self to trigger graceful shutdown
						// This is safer than os.Exit(0) as it allows PostgreSQL to handle cleanup
						if err := syscall.Kill(workerPid, syscall.SIGTERM); err != nil {
							log.Printf("[WARN] Worker PID %d: Failed to send SIGTERM to self: %v", workerPid, err)
						}
						return
					}
				}
			}
		}()
	}

	if _, found := os.LookupEnv("STEAMPIPE_FDW_PPROF"); found {
		log.Printf("[INFO] Worker PID %d: PROFILING!!!!", os.Getpid())
		go func() {
			listener, err := net.Listen("tcp", "localhost:0")
			if err != nil {
				log.Printf("[ERROR] Worker PID %d: %v", os.Getpid(), err)
				return
			}
			log.Printf("[INFO] Worker PID %d: Check http://localhost:%d/debug/pprof/", os.Getpid(), listener.Addr().(*net.TCPAddr).Port)
			log.Printf("[ERROR] Worker PID %d: %v", os.Getpid(), http.Serve(listener, nil))
		}()
	}
}

// Given a list of FdwDeparsedSortGroup and a FdwPlanState,
// construct a list FdwDeparsedSortGroup that can be pushed down
//
//export goFdwCanSort
func goFdwCanSort(deparsed *C.List, planstate *C.FdwPlanState) *C.List {
	// This will be the list of FdwDeparsedSortGroup items that can be pushed down
	var pushDownList *C.List = nil

	// Iterate over the deparsed list
	if deparsed == nil {
		return pushDownList
	}

	// Convert the sortable fields into a lookup
	sortableFields := getSortableFields(planstate.foreigntableid)
	if len(sortableFields) == 0 {
		return pushDownList
	}

	for it := C.list_head(deparsed); it != nil; it = C.lnext(deparsed, it) {
		deparsedSortGroup := C.cellGetFdwDeparsedSortGroup(it)
		columnName := C.GoString(C.nameStr(deparsedSortGroup.attname))

		supportedOrder := sortableFields[columnName]
		requiredOrder := proto.SortOrder_Asc
		if deparsedSortGroup.reversed {
			requiredOrder = proto.SortOrder_Desc
		}
		log.Println("[INFO] goFdwCanSort column", columnName, "supportedOrder", supportedOrder, "requiredOrder", requiredOrder)

		if supportedOrder == requiredOrder || supportedOrder == proto.SortOrder_All {
			log.Printf("[INFO] Worker PID %d: goFdwCanSort column %s can be pushed down", os.Getpid(), columnName)
			// add deparsedSortGroup to pushDownList
			pushDownList = C.lappend(pushDownList, unsafe.Pointer(deparsedSortGroup))
		} else {
			log.Printf("[INFO] Worker PID %d: goFdwCanSort column %s CANNOT be pushed down - not pushing down any further columns", os.Getpid(), columnName)
			break
		}
	}

	return pushDownList
}

func getSortableFields(foreigntableid C.Oid) map[string]proto.SortOrder {
	opts := GetFTableOptions(types.Oid(foreigntableid))
	connection := GetSchemaNameFromForeignTableId(types.Oid(foreigntableid))
	if connection == constants.InternalSchema || connection == constants.LegacyCommandSchema {
		return nil
	}

	tableName := opts["table"]
	pluginHub := hub.GetHub()
	return pluginHub.GetSortableFields(tableName, connection)
}

//export goFdwGetRelSize
func goFdwGetRelSize(state *C.FdwPlanState, root *C.PlannerInfo, rows *C.double, width *C.int, baserel *C.RelOptInfo) {
	logging.ClearProfileData()

	log.Printf("[TRACE] Worker PID %d: goFdwGetRelSize", os.Getpid())

	pluginHub := hub.GetHub()

	// get connection name
	connName := GetSchemaNameFromForeignTableId(types.Oid(state.foreigntableid))

	log.Printf("[TRACE] Worker PID %d: connection name: %s", os.Getpid(), connName)

	// here we are loading the server options(again) so that they are not lost after the session is restarted
	serverOpts := GetForeignServerOptionsFromFTableId(types.Oid(state.foreigntableid))
	err := pluginHub.ProcessImportForeignSchemaOptions(serverOpts, connName)
	if err != nil {
		FdwError(sperr.WrapWithMessage(err, "failed to process options"))
	}

	// reload connection config
	// TODO remove need for fdw to load connection config
	_, err = pluginHub.LoadConnectionConfig()
	if err != nil {
		log.Printf("[ERROR] Worker PID %d: LoadConnectionConfig failed %v", os.Getpid(), err)
		FdwError(err)
		return
	}

	tableOpts := GetFTableOptions(types.Oid(state.foreigntableid))

	// Extract trace context if available
	var traceContext string
	if state.trace_context_string != nil {
		traceContext = C.GoString(state.trace_context_string)
		log.Printf("[TRACE] Worker PID %d: Extracted trace context from session: %s", os.Getpid(), traceContext)

		if len(traceContext) > 0 {
			log.Printf("[DEBUG] Worker PID %d: Trace context length: %d characters", os.Getpid(), len(traceContext))
			if strings.Contains(traceContext, "traceparent=") {
				log.Printf("[DEBUG] Worker PID %d: Trace context contains traceparent field", os.Getpid())
			} else {
				log.Printf("[WARN] Worker PID %d: Trace context missing traceparent field - may be malformed", os.Getpid())
			}
		}
	} else {
		log.Printf("[DEBUG] Worker PID %d: No trace context found in session variables", os.Getpid())
	}

	// Add trace context to options for hub layer
	if traceContext != "" {
		tableOpts["trace_context"] = traceContext
		log.Printf("[DEBUG] Worker PID %d: Added trace context to table options", os.Getpid())
	}

	// build columns
	var columns []string
	if state.target_list != nil {
		columns = CStringListToGoArray(state.target_list)
	}

	result, err := pluginHub.GetRelSize(columns, nil, tableOpts)
	if err != nil {
		log.Printf("[ERROR] Worker PID %d: pluginHub.GetRelSize", os.Getpid())
		FdwError(err)
		return
	}

	*rows = C.double(result.Rows)
	*width = C.int(result.Width)

	return
}

//export goFdwGetPathKeys
func goFdwGetPathKeys(state *C.FdwPlanState) *C.List {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[WARN] Worker PID %d: goFdwGetPathKeys failed with panic: %v", os.Getpid(), r)

			FdwError(fmt.Errorf("%v", r))
		}
	}()

	log.Printf("[TRACE] Worker PID %d: goFdwGetPathKeys", os.Getpid())
	pluginHub := hub.GetHub()

	var result *C.List
	opts := GetFTableOptions(types.Oid(state.foreigntableid))
	// get the connection name - this is the namespace (i.e. the local schema)
	opts["connection"] = GetSchemaNameFromForeignTableId(types.Oid(state.foreigntableid))

	if opts["connection"] == constants.InternalSchema || opts["connection"] == constants.LegacyCommandSchema {
		return result
	}

	// ask the hub for path keys - it will use the table schema to create path keys for all key columns
	pathKeys, err := pluginHub.GetPathKeys(opts)
	if err != nil {
		FdwError(err)
	}

	for _, pathKey := range pathKeys {
		var item *C.List
		var attnums *C.List
		for _, key := range pathKey.ColumnNames {
			// Lookup the attribute number by its key.
			for k := 0; k < int(state.numattrs); k++ {
				ci := C.getConversionInfo(state.cinfos, C.int(k))
				if ci == nil {
					continue
				}
				if key == C.GoString(ci.attrname) {
					attnums = C.list_append_unique_int(attnums, ci.attnum)
					break
				}
			}
		}

		item = C.lappend(item, unsafe.Pointer(attnums))
		item = C.lappend(item, unsafe.Pointer(C.makeConst(C.INT4OID, -1, C.InvalidOid, 4, C.ulong(pathKey.Rows), false, true)))
		result = C.lappend(result, unsafe.Pointer(item))
	}

	return result
}

//export goFdwExplainForeignScan
func goFdwExplainForeignScan(node *C.ForeignScanState, es *C.ExplainState) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[WARN] Worker PID %d: goFdwExplainForeignScan failed with panic: %v", os.Getpid(), r)
			FdwError(fmt.Errorf("%v", r))
		}
	}()

	log.Printf("[TRACE] Worker PID %d: goFdwExplainForeignScan", os.Getpid())
	s := GetExecState(node.fdw_state)
	if s == nil {
		return
	}
	// Produce extra output for EXPLAIN
	if e, ok := s.Iter.(Explainable); ok {
		e.Explain(Explainer{ES: es})
	}
	ClearExecState(node.fdw_state)
	node.fdw_state = nil
}

//export goFdwBeginForeignScan
func goFdwBeginForeignScan(node *C.ForeignScanState, eflags C.int) {
	// Outer recover: catches panics during early initialization (before inner defer is registered)
	// This provides defense-in-depth - if the inner defer's recovery fails, this catches it
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[WARN] Worker PID %d: goFdwBeginForeignScan failed with panic during early init: %v", os.Getpid(), r)
			FdwError(fmt.Errorf("%v", r))
		}
	}()
	// read the explain flag
	explain := eflags&C.EXEC_FLAG_EXPLAIN_ONLY == C.EXEC_FLAG_EXPLAIN_ONLY

	log.Printf("[DEBUG] Worker PID %d: goFdwBeginForeignScan() called", os.Getpid())

	// Register this worker for parallel coordination and timeout monitoring
	// This ensures all workers (including idle ones) are tracked
	currentHub := hub.GetHub()
	if currentHub != nil {
		currentHub.RegisterParallelWorker(os.Getpid())
		log.Printf("[DEBUG] Worker PID %d: Registered for parallel worker coordination in BeginForeignScan", os.Getpid())

		// Start a background timeout monitor for this worker
		// This is critical for idle workers that never call IterateForeignScan
		go func() {
			workerPid := os.Getpid()
			coordinator := currentHub.GetParallelWorkerCoordinator()
			if coordinator == nil {
				log.Printf("[DEBUG] Worker PID %d: No coordinator available for timeout monitoring", workerPid)
				return
			}

			// Check timeout every 5 seconds
			ticker := time.NewTicker(5 * time.Second)
			defer ticker.Stop()

			for {
				select {
				case <-ticker.C:
					if coordinator.ShouldWorkerTimeout() {
						log.Printf("[INFO] Worker PID %d: Background timeout detected - marking worker for graceful termination", workerPid)
						// Mark this worker as timed out so it can exit gracefully
						// when IterateForeignScan is called (or return immediately if never called)
						coordinator.MarkWorkerTimedOut(workerPid)
						return // Exit the goroutine, don't force process exit
					}
				}
			}
		}()
	}
	logging.LogTime("[fdw] BeginForeignScan start")
	rel := BuildRelation(node.ss.ss_currentRelation)
	opts := GetFTableOptions(rel.ID)
	// get the connection name - this is the namespace (i.e. the local schema)
	opts["connection"] = rel.Namespace

	log.Printf("[INFO] Worker PID %d: goFdwBeginForeignScan, connection '%s', table '%s', explain: %v", os.Getpid(), opts["connection"], opts["table"], explain)

	// Inner recover: catches panics during main scan processing
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[WARN] Worker PID %d: goFdwBeginForeignScan failed with panic: %v", os.Getpid(), r)
			FdwError(fmt.Errorf("%v", r))
		}
	}()

	// retrieve exec state
	plan := (*C.ForeignScan)(unsafe.Pointer(node.ss.ps.plan))
	var execState *C.FdwExecState = C.initializeExecState(unsafe.Pointer(plan.fdw_private))

	// Extract trace context from session variables for scan operation
	var traceContext string
	if traceContextPtr := C.getTraceContext(); traceContextPtr != nil {
		traceContext = C.GoString(traceContextPtr)
		log.Printf("[TRACE] Worker PID %d: Extracted trace context from session for scan: %s", os.Getpid(), traceContext)
	} else {
		log.Printf("[DEBUG] Worker PID %d: No trace context found in session variables for scan", os.Getpid())
	}

	// Add trace context to options for hub layer
	if traceContext != "" {
		opts["trace_context"] = traceContext
		log.Printf("[DEBUG] Worker PID %d: Added trace context to scan options", os.Getpid())
	}

	log.Printf("[INFO] Worker PID %d: goFdwBeginForeignScan, canPushdownAllSortFields %v", os.Getpid(), execState.canPushdownAllSortFields)
	var columns []string
	if execState.target_list != nil {
		columns = CStringListToGoArray(execState.target_list)
	}

	// get conversion info
	var tupdesc C.TupleDesc = node.ss.ss_currentRelation.rd_att
	C.initConversioninfo(execState.cinfos, C.TupleDescGetAttInMetadata(tupdesc))

	// create a wrapper struct for cinfos
	cinfos := newConversionInfos(execState)
	quals, unhandledRestrictions := restrictionsToQuals(node, cinfos)

	// start the plugin hub

	pluginHub := hub.GetHub()
	s := &ExecState{
		Rel:   rel,
		Opts:  opts,
		State: execState,
	}
	// if we are NOT explaining, create an iterator to scan for us
	if !explain {
		var sortOrder = getSortColumns(execState)
		log.Printf("[INFO] Worker PID %d: goFdwBeginForeignScan, table '%s', sortOrder: %v", os.Getpid(), opts["table"], sortOrder)
		// get the limit
		limit := int64(execState.limit)
		// if we cannot push down ALL sort fields, do not push down limit
		if !execState.canPushdownAllSortFields {
			log.Printf("[INFO] Worker PID %d: goFdwBeginForeignScan, table '%s', cannot push down all sort fields, setting limit to -1", os.Getpid(), opts["table"])
			limit = -1
		}

		ts := int64(C.GetSQLCurrentTimestamp(0))
		iter, err := pluginHub.GetIterator(columns, quals, unhandledRestrictions, limit, sortOrder, ts, opts)
		if err != nil {
			log.Printf("[WARN] Worker PID %d: pluginHub.GetIterator FAILED: %s", os.Getpid(), err)
			FdwError(err)
			return
		}
		s.Iter = iter
	}

	log.Printf("[TRACE] Worker PID %d: goFdwBeginForeignScan: save exec state %v\n", os.Getpid(), s)
	node.fdw_state = SaveExecState(s)

	logging.LogTime("[fdw] BeginForeignScan end")
}

func getSortColumns(state *C.FdwExecState) []*proto.SortColumn {
	sortGroups := state.pathkeys
	var res []*proto.SortColumn
	for it := C.list_head(sortGroups); it != nil; it = C.lnext(sortGroups, it) {
		deparsedSortGroup := C.cellGetFdwDeparsedSortGroup(it)
		columnName := C.GoString(C.nameStr(deparsedSortGroup.attname))
		requiredOrder := proto.SortOrder_Asc
		if deparsedSortGroup.reversed {
			requiredOrder = proto.SortOrder_Desc
		}

		res = append(res, &proto.SortColumn{
			Column: columnName,
			Order:  requiredOrder,
		})
	}
	return res
}

//export goFdwIterateForeignScan
func goFdwIterateForeignScan(node *C.ForeignScanState) *C.TupleTableSlot {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[WARN] Worker PID %d: goFdwIterateForeignScan failed with panic: %v", os.Getpid(), r)
			FdwError(fmt.Errorf("%v", r))
		}
	}()
	log.Printf("[DEBUG] Worker PID %d: goFdwIterateForeignScan() called", os.Getpid())

	// Check if this worker should timeout (for idle worker detection)
	currentHub := hub.GetHub()
	if currentHub != nil {
		if coordinator := currentHub.GetParallelWorkerCoordinator(); coordinator != nil {
			if coordinator.ShouldWorkerTimeout() {
				log.Printf("[INFO] Worker PID %d: Worker timeout detected in IterateForeignScan - terminating idle worker", os.Getpid())
				// Return empty result to signal completion and allow worker to exit
				return nil
			}
		}
	}

	logging.LogTime("[fdw] IterateForeignScan start")

	s := GetExecState(node.fdw_state)

	slot := node.ss.ss_ScanTupleSlot
	C.ExecClearTuple(slot)
	pluginHub := hub.GetHub()

	log.Printf("[TRACE] Worker PID %d: goFdwIterateForeignScan, table '%s' (%p)", os.Getpid(), s.Opts["table"], s.Iter)
	// if the iterator has not started, start
	if s.Iter.Status() == hub.QueryStatusReady {
		log.Printf("[INFO] Worker PID %d: goFdwIterateForeignScan calling pluginHub.StartScan, table '%s' Current timestamp: %d (%p)", os.Getpid(), s.Opts["table"], s.Iter.GetQueryTimestamp(), s.Iter)
		if err := pluginHub.StartScan(s.Iter); err != nil {
			FdwError(err)
			return slot
		}
	}
	// call the iterator
	// row is a map of column name to value (as an interface)
	row, err := s.Iter.Next()
	if err != nil {
		log.Printf("[INFO] Worker PID %d: goFdwIterateForeignScan Next returned error: %s (%p)", os.Getpid(), err.Error(), s.Iter)
		FdwError(err)
		return slot
	}

	if len(row) == 0 {
		log.Printf("[INFO] Worker PID %d: goFdwIterateForeignScan returned empty row - this scan complete (%p)", os.Getpid(), s.Iter)
		// add scan metadata to hub
		pluginHub.AddScanMetadata(s.Iter)
		logging.LogTime("[fdw] IterateForeignScan end")
		// show profiling - ignore intervals less than 1ms
		//logging.DisplayProfileData(10*time.Millisecond, logger)
		return slot
	}

	isNull := make([]C.bool, len(s.Rel.Attr.Attrs))
	data := make([]C.Datum, len(s.Rel.Attr.Attrs))

	for i, attr := range s.Rel.Attr.Attrs {
		column := attr.Name

		var val = row[column]
		if val == nil {
			isNull[i] = C.bool(true)
			continue
		}
		// get the conversion info for this column
		ci := C.getConversionInfo(s.State.cinfos, C.int(i))
		// convert value into a datum
		if datum, err := ValToDatum(val, ci, s.State.buffer); err != nil {
			log.Printf("[WARN] Worker PID %d: goFdwIterateForeignScan ValToDatum error %v (%p)", os.Getpid(), err, s.Iter)
			FdwError(err)
			return slot
		} else {
			// everyone loves manually calculating array offsets
			data[i] = datum
		}
	}

	C.fdw_saveTuple(&data[0], &isNull[0], &node.ss)
	logging.LogTime("[fdw] IterateForeignScan end")

	return slot
}

//export goFdwReScanForeignScan
func goFdwReScanForeignScan(node *C.ForeignScanState) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[WARN] Worker PID %d: goFdwReScanForeignScan failed with panic: %v", os.Getpid(), r)
			FdwError(fmt.Errorf("%v", r))
		}
	}()
	rel := BuildRelation(node.ss.ss_currentRelation)
	opts := GetFTableOptions(rel.ID)

	log.Printf("[INFO] Worker PID %d: goFdwReScanForeignScan, connection '%s', table '%s'", os.Getpid(), opts["connection"], opts["table"])
	// restart the scan
	goFdwBeginForeignScan(node, 0)
}

//export goFdwEndForeignScan
func goFdwEndForeignScan(node *C.ForeignScanState) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[WARN] Worker PID %d: goFdwEndForeignScan failed with panic: %v", os.Getpid(), r)
			FdwError(fmt.Errorf("%v", r))
		}
	}()

	log.Printf("[DEBUG] Worker PID %d: goFdwEndForeignScan() called", os.Getpid())

	// Unregister this worker from parallel coordination
	pluginHub := hub.GetHub()
	if pluginHub != nil {
		pluginHub.UnregisterParallelWorker(os.Getpid())
		log.Printf("[DEBUG] Worker PID %d: Unregistered from parallel worker coordination in EndForeignScan", os.Getpid())
	}

	s := GetExecState(node.fdw_state)
	if s != nil {
		log.Printf("[INFO] Worker PID %d: goFdwEndForeignScan, iterator: %p", os.Getpid(), s.Iter)
		pluginHub.EndScan(s.Iter, int64(s.State.limit))
	}
	ClearExecState(node.fdw_state)
	node.fdw_state = nil

}

//export goFdwAbortCallback
func goFdwAbortCallback() {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[WARN] Worker PID %d: goFdwAbortCallback failed with panic: %v", os.Getpid(), r)
			// DO NOT call FdwError or we will recurse
		}
	}()
	log.Printf("[INFO] Worker PID %d: goFdwAbortCallback", os.Getpid())
	pluginHub := hub.GetHub()
	pluginHub.Abort()

}

//export goFdwImportForeignSchema
func goFdwImportForeignSchema(stmt *C.ImportForeignSchemaStmt, serverOid C.Oid) *C.List {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[WARN] Worker PID %d: goFdwImportForeignSchema failed with panic: %v", os.Getpid(), r)
			FdwError(fmt.Errorf("%v", r))
		}
	}()

	log.Printf("[INFO] Worker PID %d: goFdwImportForeignSchema remote '%s' local '%s'\n", os.Getpid(), C.GoString(stmt.remote_schema), C.GoString(stmt.local_schema))
	// get the plugin hub,
	pluginHub := hub.GetHub()

	remoteSchema := C.GoString(stmt.remote_schema)
	localSchema := C.GoString(stmt.local_schema)

	// special handling for the command schema
	if remoteSchema == constants.InternalSchema {
		log.Printf("[INFO] Worker PID %d: importing setting tables into %s", os.Getpid(), remoteSchema)
		settingsSchema := pluginHub.GetSettingsSchema()
		sql := SchemaToSql(settingsSchema, stmt, serverOid)
		return sql
	}
	if remoteSchema == constants.LegacyCommandSchema {
		log.Printf("[INFO] Worker PID %d: importing setting tables into %s", os.Getpid(), remoteSchema)
		settingsSchema := pluginHub.GetLegacySettingsSchema()
		sql := SchemaToSql(settingsSchema, stmt, serverOid)
		return sql
	}

	fServer := C.GetForeignServer(serverOid)
	serverOptions := GetForeignServerOptions(fServer)

	log.Println("[TRACE] goFdwImportForeignSchema serverOptions:", serverOptions)

	err := pluginHub.ProcessImportForeignSchemaOptions(serverOptions, localSchema)
	if err != nil {
		FdwError(sperr.WrapWithMessage(err, "failed to process options"))
	}

	schema, err := pluginHub.GetSchema(remoteSchema, localSchema)
	if err != nil {
		log.Printf("[WARN] Worker PID %d: goFdwImportForeignSchema failed: %s", os.Getpid(), err)
		FdwError(err)
		return nil
	}
	res := SchemaToSql(schema.Schema, stmt, serverOid)

	return res
}

//export goFdwExecForeignInsert
func goFdwExecForeignInsert(estate *C.EState, rinfo *C.ResultRelInfo, slot *C.TupleTableSlot, planSlot *C.TupleTableSlot) *C.TupleTableSlot {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[WARN] Worker PID %d: goFdwExecForeignInsert failed with panic: %v", os.Getpid(), r)
			FdwError(fmt.Errorf("%v", r))
		}
	}()

	// get the connection from the relation namespace
	relid := rinfo.ri_RelationDesc.rd_id
	rel := C.RelationIdGetRelation(relid)
	defer C.RelationClose(rel)
	connection := getNamespace(rel)
	// if this is a command insert, handle it
	if connection == constants.InternalSchema || connection == constants.LegacyCommandSchema {
		return handleCommandInsert(rinfo, slot, rel)
	}

	return nil
}

func handleCommandInsert(rinfo *C.ResultRelInfo, slot *C.TupleTableSlot, rel C.Relation) *C.TupleTableSlot {
	relid := rinfo.ri_RelationDesc.rd_id
	opts := GetFTableOptions(types.Oid(relid))
	pluginHub := hub.GetHub()

	switch opts["table"] {
	case constants.LegacyCommandTableCache:
		// we know there is just a single column - operation
		var isNull C.bool
		datum := C.slot_getattr(slot, 1, &isNull)
		operation := C.GoString(C.fdw_datumGetString(datum))
		if err := pluginHub.HandleLegacyCacheCommand(operation); err != nil {
			FdwError(err)
			return nil
		}

	case constants.ForeignTableSettings:
		tupleDesc := buildTupleDesc(rel.rd_att)
		attributes := tupleDesc.Attrs
		var key *string
		var value *string

		// iterate through the attributes
		for i, a := range attributes {
			var isNull C.bool
			datum := C.slot_getattr(slot, C.int(i+1), &isNull)
			if isNull {
				continue
			}
			// get a string from the memory slot
			datumStr := C.GoString(C.fdw_datumGetString(datum))

			log.Println("[TRACE] name", a.Name)
			log.Println("[TRACE] datum", datum)
			log.Println("[TRACE] datumstr", datumStr)

			// map it to one of key/value
			switch a.Name {
			case constants.ForeignTableSettingsKeyColumn:
				key = &datumStr
			case constants.ForeignTableSettingsValueColumn:
				value = &datumStr
			}
		}

		// if both key and value are not set, ERROR
		if key == nil || value == nil {
			FdwError(fmt.Errorf("invalid setting: both 'key' and 'value' columns need to be set"))
			return nil
		}

		// apply the setting
		if err := pluginHub.ApplySetting(*key, *value); err != nil {
			FdwError(err)
		}
		return nil

	}

	return nil

	/*
		here is how to fetch each attribute value:
		tupleDesc := buildTupleDesc(rel.rd_att)
		attributes := tupleDesc.Attrs
		for i, a := range attributes {
			var isNull C.bool
			datum := C.slot_getattr(slot, C.int(i+1), &isNull)
		}*/
}

//export goFdwShutdown
func goFdwShutdown() {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[WARN] Worker PID %d: goFdwShutdown failed with panic: %v", os.Getpid(), r)
			// DO NOT call FdwError or we will recurse
		}
	}()
	log.Printf("[INFO] Worker PID %d: .\n******************************************************\n\n\t\tsteampipe postgres fdw shutdown\n\n******************************************************\n", os.Getpid())
	pluginHub := hub.GetHub()
	pluginHub.Close()
}

//export goFdwValidate
func goFdwValidate(coid C.Oid, opts *C.List) {
	// Validate the generic options given to a FOREIGN DATA WRAPPER, SERVER,
	// USER MAPPING or FOREIGN TABLE that uses fdw.
	// Raise an ERROR if the option or its value are considered invalid
	// or a required option is missing.
}

// required by buildmode=c-archive
func main() {}
