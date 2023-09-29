package hub

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/logging"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/telemetry"
	"github.com/turbot/steampipe-postgres-fdw/settings"
	"github.com/turbot/steampipe-postgres-fdw/types"
	"github.com/turbot/steampipe/pkg/constants"
	"github.com/turbot/steampipe/pkg/filepaths"
	"github.com/turbot/steampipe/pkg/steampipeconfig"
	"github.com/turbot/steampipe/pkg/steampipeconfig/modconfig"
	"github.com/turbot/steampipe/pkg/utils"
)

const (
	rowBufferSize = 100
)

// RemoteHub is a structure representing plugin hub
type RemoteHub struct {
	hubBase
	connections *connectionFactory
}

// global hub instance
var hubSingleton *RemoteHub

// mutex protecting hub creation
var hubMux sync.Mutex

//// lifecycle ////

// GetHub returns a hub singleton
// if there is an existing hub singleton instance return it, otherwise create it
// if a hub exists, but a different pluginDir is specified, reinitialise the hub with the new dir
func GetHub() (Hub, error) {
	logging.LogTime("GetHub start")

	// lock access to singleton
	hubMux.Lock()
	defer hubMux.Unlock()

	var err error
	if hubSingleton == nil {
		hubSingleton, err = newRemoteHub()
		if err != nil {
			return nil, err
		}
	}
	logging.LogTime("GetHub end")
	return hubSingleton, err
}

func newRemoteHub() (*RemoteHub, error) {
	hub := &RemoteHub{}
	hub.connections = newConnectionFactory(hub)

	hub.cacheSettings = settings.NewCacheSettings()

	// TODO CHECK TELEMETRY ENABLED?
	if err := hub.initialiseTelemetry(); err != nil {
		return nil, err
	}

	// NOTE: Steampipe determine it's install directory from the input arguments (with a default)
	// as we are using shared Steampipe code we must set the install directory.
	// we can derive it from the working directory (which is underneath the install directectory)
	steampipeDir, err := getInstallDirectory()
	if err != nil {
		return nil, err
	}
	filepaths.SteampipeDir = steampipeDir

	log.Printf("[INFO] newRemoteHub RemoteHub.LoadConnectionConfig ")
	if _, err := hub.LoadConnectionConfig(); err != nil {
		return nil, err
	}

	return hub, nil
}

// get the install folder - derive from our working folder
func getInstallDirectory() (string, error) {
	// we need to do this as we are sharing steampipe code to read the config
	// and steampipe may set the install folder from a cmd line arg, so it cannot be hard coded
	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	return path.Join(wd, "../../.."), nil
}

//// public fdw functions ////

// GetSchema returns the schema for a name. Load the plugin for the connection if needed
func (h *RemoteHub) GetSchema(remoteSchema string, localSchema string) (*proto.Schema, error) {
	log.Printf("[TRACE] RemoteHub GetSchema %s %s", remoteSchema, localSchema)
	pluginFQN := remoteSchema
	connectionName := localSchema
	log.Printf("[TRACE] getSchema remoteSchema: %s, name %s\n", remoteSchema, connectionName)

	return h.connections.getSchema(pluginFQN, connectionName)
}

// GetIterator creates and returns an iterator
func (h *RemoteHub) GetIterator(columns []string, quals *proto.Quals, unhandledRestrictions int, limit int64, opts types.Options) (Iterator, error) {
	logging.LogTime("GetIterator start")
	qualMap, err := buildQualMap(quals)
	connectionName := opts["connection"]
	table := opts["table"]
	log.Printf("[TRACE] RemoteHub GetIterator() table '%s'", table)

	if connectionName == constants.InternalSchema || connectionName == constants.LegacyCommandSchema {
		return h.executeCommandScan(connectionName, table)
	}

	// create a span for this scan
	scanTraceCtx := h.traceContextForScan(table, columns, limit, qualMap, connectionName)
	iterator, err := h.startScanForConnection(connectionName, table, qualMap, unhandledRestrictions, columns, limit, scanTraceCtx)

	if err != nil {
		log.Printf("[TRACE] RemoteHub GetIterator() failed :( %s", err)
		return nil, err
	}
	log.Printf("[TRACE] RemoteHub GetIterator() created iterator (%p)", iterator)

	return iterator, nil
}

// LoadConnectionConfig loads the connection config and returns whether it has changed
func (h *RemoteHub) LoadConnectionConfig() (bool, error) {
	log.Printf("[INFO] RemoteHub.LoadConnectionConfig ")
	// load connection conFig
	connectionConfig, errorsAndWarnings := steampipeconfig.LoadConnectionConfig()
	if errorsAndWarnings.GetError() != nil {
		log.Printf("[WARN] LoadConnectionConfig failed %v ", errorsAndWarnings)
		return false, errorsAndWarnings.GetError()
	}

	configChanged := steampipeconfig.GlobalConfig == connectionConfig
	steampipeconfig.GlobalConfig = connectionConfig

	return configChanged, nil
}

// GetPathKeys Is a method called from the planner to add additional Path to the planner.
//
// fetch schema and call base implementation
func (h *RemoteHub) GetPathKeys(opts types.Options) ([]types.PathKey, error) {
	connectionName := opts["connection"]
	table := opts["table"]

	log.Printf("[TRACE] hub.GetPathKeys for connection '%s`, table `%s`", connectionName, table)

	// get the schema for this connection
	connectionPlugin, err := h.getConnectionPlugin(connectionName)
	if err != nil {
		return nil, err
	}

	connectionSchema, err := connectionPlugin.GetSchema(connectionName)
	if err != nil {
		return nil, err
	}

	return h.getPathKeys(connectionSchema, opts)
}

//// internal implementation ////

// startScanForConnection starts a scan for a single connection, using a scanIterator or a legacyScanIterator
func (h *RemoteHub) startScanForConnection(connectionName string, table string, qualMap map[string]*proto.Quals, unhandledRestrictions int, columns []string, limit int64, scanTraceCtx *telemetry.TraceCtx) (_ Iterator, err error) {
	defer func() {
		if err != nil {
			// close the span in case of errir
			scanTraceCtx.Span.End()
		}
	}()

	log.Printf("[TRACE] RemoteHub startScanForConnection '%s'", connectionName)
	// get connection plugin for this connection
	connectionPlugin, err := h.getConnectionPlugin(connectionName)
	if err != nil {
		log.Printf("[TRACE] getConnectionPlugin failed: %s", err.Error())
		return nil, err
	}

	// ok so this is a multi connection plugin, build list of connections,
	// if this connection is NOT an aggregator, only execute for the named connection

	// get connection config
	connectionConfig, ok := steampipeconfig.GlobalConfig.Connections[connectionName]
	if !ok {
		return nil, fmt.Errorf("no connection config loaded for connection '%s'", connectionName)
	}

	var connectionNames = []string{connectionName}
	if connectionConfig.Type == modconfig.ConnectionTypeAggregator {
		connectionNames = connectionConfig.GetResolveConnectionNames()
		// if there are no connections, do not proceed
		if len(connectionNames) == 0 {
			return nil, errors.New(connectionConfig.GetEmptyAggregatorError())
		}
	}

	// for each connection, determine whether to pushdown the limit
	connectionLimitMap, err := h.buildConnectionLimitMap(table, qualMap, unhandledRestrictions, connectionNames, limit, connectionPlugin)
	if err != nil {
		return nil, err
	}

	if len(qualMap) > 0 {
		log.Printf("[INFO] connection '%s', table '%s', quals %s", connectionName, table, grpc.QualMapToString(qualMap, true))
	} else {
		log.Println("[INFO] --------")
		log.Println("[INFO] no quals")
		log.Println("[INFO] --------")
	}

	log.Printf("[TRACE] startScanForConnection creating a new scan iterator")
	iterator := newScanIterator(h, connectionPlugin, connectionName, table, connectionLimitMap, qualMap, columns, limit, scanTraceCtx)
	return iterator, nil
}

func (h *RemoteHub) buildConnectionLimitMap(table string, qualMap map[string]*proto.Quals, unhandledRestrictions int, connectionNames []string, limit int64, connectionPlugin *steampipeconfig.ConnectionPlugin) (map[string]int64, error) {
	log.Printf("[TRACE] buildConnectionLimitMap, table: '%s', %d %s, limit: %d", table, len(connectionNames), utils.Pluralize("connection", len(connectionNames)), limit)

	connectionSchema, err := connectionPlugin.GetSchema(connectionNames[0])
	if err != nil {
		return nil, err
	}
	schemaMode := connectionSchema.Mode

	// pushing the limit down or not is dependent on the schema.
	// for a static schema, the limit will be the same for all connections (i.e. we either pushdown for all or none)
	// check once whether we should push down
	if limit != -1 && schemaMode == plugin.SchemaModeStatic {
		log.Printf("[TRACE] static schema - using same limit for all connections")
		if !h.shouldPushdownLimit(table, qualMap, unhandledRestrictions, connectionSchema) {
			limit = -1
		}
	}

	// set the limit for the one and only connection
	var connectionLimitMap = make(map[string]int64)
	for _, c := range connectionNames {
		connectionLimit := limit
		// if schema mode is dynamic, check whether we should push down for each connection
		if schemaMode == plugin.SchemaModeDynamic && !h.shouldPushdownLimit(table, qualMap, unhandledRestrictions, connectionSchema) {
			log.Printf("[INFO] not pushing limit down for connection %s", c)
			connectionLimit = -1
		}
		connectionLimitMap[c] = connectionLimit
	}

	return connectionLimitMap, nil
}

// StartScan starts a scan (for scanIterators only = legacy iterators will have already started)
func (h *RemoteHub) StartScan(i Iterator) error {
	// iterator must be a scan iterator
	// if iterator is not a scan iterator, do nothing
	iterator, ok := i.(*scanIterator)
	if !ok {
		return nil
	}

	table := iterator.table
	connectionPlugin := iterator.connectionPlugin

	req := &proto.ExecuteRequest{
		Table:        table,
		QueryContext: iterator.queryContext,
		CallId:       iterator.callId,
		// pass connection name - used for aggregators
		Connection:            iterator.GetConnectionName(),
		TraceContext:          grpc.CreateCarrierFromContext(iterator.traceCtx.Ctx),
		ExecuteConnectionData: make(map[string]*proto.ExecuteConnectionData),
	}

	// build executeConnectionData map
	for connectionName, limit := range iterator.connectionLimitMap {
		data := &proto.ExecuteConnectionData{}
		if limit != -1 {
			data.Limit = &proto.NullableInt{Value: limit}
		}
		data.CacheTtl = int64(h.cacheTTL(connectionName).Seconds())
		data.CacheEnabled = h.cacheEnabled(connectionName)

		req.ExecuteConnectionData[connectionName] = data
	}

	log.Printf("[INFO] StartScan for table: %s, cache enabled: %v, iterator %p, %d quals (%s)", table, req.CacheEnabled, iterator, len(iterator.queryContext.Quals), iterator.callId)
	stream, ctx, cancel, err := connectionPlugin.PluginClient.Execute(req)
	// format GRPC errors and ignore not implemented errors for backwards compatibility
	err = grpc.HandleGrpcError(err, connectionPlugin.PluginName, "Execute")
	if err != nil {
		log.Printf("[WARN] startScan: plugin Execute function callId: %s returned error: %v\n", iterator.callId, err)
		iterator.setError(err)
		return err
	}
	iterator.Start(stream, ctx, cancel)

	// add iterator to running list
	h.addIterator(iterator)

	return nil
}

// getConnectionPlugin returns the connectionPlugin for the provided connection
// it also makes sure that the plugin is up and running.
// if the plugin is not running, it attempts to restart the plugin - errors if unable
func (h *RemoteHub) getConnectionPlugin(connectionName string) (*steampipeconfig.ConnectionPlugin, error) {
	log.Printf("[TRACE] hub.getConnectionPlugin for connection '%s`", connectionName)

	// get the plugin FQN
	connectionConfig, ok := steampipeconfig.GlobalConfig.Connections[connectionName]
	if !ok {
		log.Printf("[WARN] no connection config loaded for connection '%s'", connectionName)
		return nil, fmt.Errorf("no connection config loaded for connection '%s'", connectionName)
	}
	pluginFQN := connectionConfig.Plugin

	// ask connection map to get or create this connection
	c, err := h.connections.getOrCreate(pluginFQN, connectionName)
	if err != nil {
		log.Printf("[TRACE] getConnectionPlugin getConnectionPlugin failed: %s", err.Error())
		return nil, err
	}

	return c, nil
}

func (h *RemoteHub) cacheEnabled(connectionName string) bool {
	if h.cacheSettings.Enabled != nil {
		return *h.cacheSettings.Enabled
	}
	// ask the steampipe config for resolved plugin options - this will use default values where needed
	connectionOptions := steampipeconfig.GlobalConfig.GetConnectionOptions(connectionName)

	// the config loading code should ALWAYS populate the connection options, using defaults if needed
	if connectionOptions.Cache == nil {
		panic(fmt.Sprintf("No cache options found for connection %s", connectionName))
	}
	return *connectionOptions.Cache
}

func (h *RemoteHub) cacheTTL(connectionName string) time.Duration {
	// if the cache ttl has been overridden, then enforce the value
	if h.cacheSettings.Ttl != nil {
		return *h.cacheSettings.Ttl
	}

	// ask the steampipe config for resolved plugin options - this will use default values where needed
	connectionOptions := steampipeconfig.GlobalConfig.GetConnectionOptions(connectionName)

	// the config loading code should ALWAYS populate the connection options, using defaults if needed
	if connectionOptions.CacheTTL == nil {
		panic(fmt.Sprintf("No cache options found for connection %s", connectionName))
	}

	ttl := time.Duration(*connectionOptions.CacheTTL) * time.Second

	// would this give data earlier than the cacheClearTime
	now := time.Now()
	if now.Add(-ttl).Before(h.cacheSettings.ClearTime) {
		ttl = now.Sub(h.cacheSettings.ClearTime)
	}
	return ttl
}

func (h *RemoteHub) ApplySetting(key string, value string) error {
	log.Printf("[TRACE] ApplySetting [%s => %s]", key, value)
	return h.cacheSettings.Apply(key, value)
}

func (h *RemoteHub) GetSettingsSchema() map[string]*proto.TableSchema {
	return map[string]*proto.TableSchema{
		constants.ForeignTableSettings: {
			Columns: []*proto.ColumnDefinition{
				{Name: constants.ForeignTableSettingsKeyColumn, Type: proto.ColumnType_STRING},
				{Name: constants.ForeignTableSettingsValueColumn, Type: proto.ColumnType_STRING},
			},
		},
		constants.ForeignTableScanMetadata: {
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

func (h *RemoteHub) GetLegacySettingsSchema() map[string]*proto.TableSchema {
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

func (h *RemoteHub) HandleLegacyCacheCommand(command string) error {
	if err := h.ValidateCacheCommand(command); err != nil {
		return err
	}

	log.Printf("[TRACE] HandleLegacyCacheCommand %s", command)

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

func (h *RemoteHub) ValidateCacheCommand(command string) error {
	validCommands := []string{constants.LegacyCommandCacheClear, constants.LegacyCommandCacheOn, constants.LegacyCommandCacheOff}

	if !helpers.StringSliceContains(validCommands, command) {
		return fmt.Errorf("invalid command '%s' - supported commands are %s", command, strings.Join(validCommands, ","))
	}
	return nil
}
