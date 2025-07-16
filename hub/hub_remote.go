package hub

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path"
	"time"

	typehelpers "github.com/turbot/go-kit/types"
	"github.com/turbot/pipe-fittings/v2/app_specific"
	"github.com/turbot/pipe-fittings/v2/modconfig"
	"github.com/turbot/pipe-fittings/v2/utils"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/logging"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/telemetry"
	"github.com/turbot/steampipe-postgres-fdw/v2/settings"
	"github.com/turbot/steampipe-postgres-fdw/v2/types"
	"github.com/turbot/steampipe/v2/pkg/constants"
	"github.com/turbot/steampipe/v2/pkg/steampipeconfig"
)

const (
	rowBufferSize = 100
)

// RemoteHub is a structure representing plugin hub
type RemoteHub struct {
	*hubBase
	connections *connectionFactory
}

//// lifecycle ////

func newRemoteHub() (*RemoteHub, error) {
	hub := &RemoteHub{
		hubBase: newHubBase(true),
	}
	hub.connections = newConnectionFactory(hub)

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
	app_specific.InstallDir = steampipeDir

	log.Printf("[INFO] Worker PID %d: newRemoteHub RemoteHub.LoadConnectionConfig", os.Getpid())
	if _, err := hub.LoadConnectionConfig(); err != nil {
		return nil, err
	}

	hub.cacheSettings = settings.NewCacheSettings(hub.clearConnectionCache, hub.getServerCacheEnabled())

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
	log.Printf("[TRACE] Worker PID %d: RemoteHub GetSchema %s %s", os.Getpid(), remoteSchema, localSchema)
	pluginFQN := remoteSchema
	connectionName := localSchema
	log.Printf("[TRACE] Worker PID %d: getSchema remoteSchema: %s, name %s", os.Getpid(), remoteSchema, connectionName)

	return h.connections.getSchema(pluginFQN, connectionName)
}

// GetIterator creates and returns an iterator
func (h *RemoteHub) GetIterator(columns []string, quals *proto.Quals, unhandledRestrictions int, limit int64, sortOrder []*proto.SortColumn, queryTimestamp int64, opts types.Options) (Iterator, error) {
	logging.LogTime("GetIterator start")
	qualMap, err := buildQualMap(quals)
	connectionName := opts["connection"]
	table := opts["table"]
	log.Printf("[TRACE] Worker PID %d: RemoteHub GetIterator() table '%s'", os.Getpid(), table)

	if connectionName == constants.InternalSchema || connectionName == constants.LegacyCommandSchema {
		return h.executeCommandScan(connectionName, table, queryTimestamp)
	}

	// create a span for this scan
	scanTraceCtx := h.traceContextForScan(table, columns, limit, qualMap, connectionName, opts)
	iterator, err := h.startScanForConnection(connectionName, table, qualMap, unhandledRestrictions, columns, limit, sortOrder, queryTimestamp, scanTraceCtx)

	if err != nil {
		log.Printf("[TRACE] Worker PID %d: RemoteHub GetIterator() failed :( %s", os.Getpid(), err)
		return nil, err
	}
	log.Printf("[TRACE] Worker PID %d: RemoteHub GetIterator() created iterator (%p)", os.Getpid(), iterator)

	return iterator, nil
}

// LoadConnectionConfig loads the connection config and returns whether it has changed
func (h *RemoteHub) LoadConnectionConfig() (bool, error) {
	log.Printf("[INFO] Worker PID %d: RemoteHub.LoadConnectionConfig", os.Getpid())
	// load connection conFig
	connectionConfig, errorsAndWarnings := steampipeconfig.LoadConnectionConfig(context.Background())
	if errorsAndWarnings.GetError() != nil {
		log.Printf("[WARN] Worker PID %d: LoadConnectionConfig failed %v", os.Getpid(), errorsAndWarnings)
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

	log.Printf("[TRACE] Worker PID %d: hub.GetPathKeys for connection '%s`, table `%s`", os.Getpid(), connectionName, table)

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
func (h *RemoteHub) startScanForConnection(connectionName string, table string, qualMap map[string]*proto.Quals, unhandledRestrictions int, columns []string, limit int64, sortOrder []*proto.SortColumn, queryTimestamp int64, scanTraceCtx *telemetry.TraceCtx) (_ Iterator, err error) {
	defer func() {
		if err != nil {
			// close the span in case of errir
			scanTraceCtx.Span.End()
		}
	}()

	log.Printf("[INFO] Worker PID %d: RemoteHub startScanForConnection '%s' limit %d", os.Getpid(), connectionName, limit)
	// get connection plugin for this connection
	connectionPlugin, err := h.getConnectionPlugin(connectionName)
	if err != nil {
		log.Printf("[TRACE] Worker PID %d: getConnectionPlugin failed: %s", os.Getpid(), err.Error())
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
		log.Printf("[INFO] Worker PID %d: connection '%s', table '%s', quals %s", os.Getpid(), connectionName, table, grpc.QualMapToString(qualMap, true))
	} else {
		log.Printf("[INFO] Worker PID %d: --------", os.Getpid())
		log.Printf("[INFO] Worker PID %d: no quals", os.Getpid())
		log.Printf("[INFO] Worker PID %d: --------", os.Getpid())
	}

	log.Printf("[TRACE] Worker PID %d: startScanForConnection creating a new scan iterator", os.Getpid())
	iterator := newScanIterator(h, connectionPlugin, connectionName, table, connectionLimitMap, qualMap, columns, limit, sortOrder, queryTimestamp, scanTraceCtx)
	return iterator, nil
}

func (h *RemoteHub) buildConnectionLimitMap(table string, qualMap map[string]*proto.Quals, unhandledRestrictions int, connectionNames []string, limit int64, connectionPlugin *steampipeconfig.ConnectionPlugin) (map[string]int64, error) {
	log.Printf("[INFO] Worker PID %d: buildConnectionLimitMap, table: '%s', %d %s, limit: %d", os.Getpid(), table, len(connectionNames), utils.Pluralize("connection", len(connectionNames)), limit)

	connectionSchema, err := connectionPlugin.GetSchema(connectionNames[0])
	if err != nil {
		return nil, err
	}
	schemaMode := connectionSchema.Mode

	// pushing the limit down or not is dependent on the schema.
	// for a static schema, the limit will be the same for all connections (i.e. we either pushdown for all or none)
	// check once whether we should push down
	if limit != -1 && schemaMode == plugin.SchemaModeStatic {
		log.Printf("[INFO] Worker PID %d: static schema - using same limit for all connections", os.Getpid())
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
			log.Printf("[INFO] Worker PID %d: not pushing limit down for connection %s", os.Getpid(), c)
			connectionLimit = -1
		}
		connectionLimitMap[c] = connectionLimit
	}

	return connectionLimitMap, nil
}

// getConnectionPlugin returns the connectionPlugin for the provided connection
// it also makes sure that the plugin is up and running.
// if the plugin is not running, it attempts to restart the plugin - errors if unable
func (h *RemoteHub) getConnectionPlugin(connectionName string) (*steampipeconfig.ConnectionPlugin, error) {
	log.Printf("[TRACE] Worker PID %d: hub.getConnectionPlugin for connection '%s`", os.Getpid(), connectionName)

	// get the plugin FQN
	connectionConfig, ok := steampipeconfig.GlobalConfig.Connections[connectionName]
	if !ok {
		log.Printf("[WARN] Worker PID %d: no connection config loaded for connection '%s'", os.Getpid(), connectionName)
		return nil, fmt.Errorf("no connection config loaded for connection '%s'", connectionName)
	}
	pluginFQN := connectionConfig.Plugin

	// ask connection map to get or create this connection
	c, err := h.connections.getOrCreate(pluginFQN, connectionName)
	if err != nil {
		log.Printf("[TRACE] Worker PID %d: getConnectionPlugin getConnectionPlugin failed: %s", os.Getpid(), err.Error())
		return nil, err
	}

	return c, nil
}

func (h *RemoteHub) clearConnectionCache(connection string) error {
	log.Printf("[INFO] Worker PID %d: clear connection cache for connection '%s'", os.Getpid(), connection)
	connectionPlugin, err := h.getConnectionPlugin(connection)
	if err != nil {
		log.Printf("[WARN] Worker PID %d: clearConnectionCache failed for connection %s: %s", os.Getpid(), connection, err)
		return err
	}

	_, err = connectionPlugin.PluginClient.SetConnectionCacheOptions(&proto.SetConnectionCacheOptionsRequest{ClearCacheForConnection: connection})
	if err != nil {
		log.Printf("[WARN] Worker PID %d: clearConnectionCache failed for connection %s: SetConnectionCacheOptions returned %s", os.Getpid(), connection, err)
	}
	log.Printf("[INFO] Worker PID %d: clear connection cache succeeded", os.Getpid())
	return err
}

func (h *RemoteHub) cacheEnabled(connectionName string) bool {
	// if the caching is disabled for the server, just return false
	if !h.cacheSettings.ServerCacheEnabled {
		log.Printf("[INFO] Worker PID %d: cacheEnabled returning false since server cache is disabled", os.Getpid())
		return false
	}

	if h.cacheSettings.ClientCacheEnabled != nil {
		log.Printf("[INFO] Worker PID %d: cacheEnabled returning %v since client cache is enabled", os.Getpid(), *h.cacheSettings.ClientCacheEnabled)
		return *h.cacheSettings.ClientCacheEnabled
	}
	log.Printf("[INFO] Worker PID %d: default cacheEnabled returning true", os.Getpid())

	return true
}

func (h *RemoteHub) cacheTTL(connectionName string) time.Duration {
	// initialise to default
	ttl := 300 * time.Second
	// if the cache ttl has been overridden, then enforce the value
	if h.cacheSettings.Ttl != nil {
		ttl = *h.cacheSettings.Ttl
	}
	// would this give data earlier than the cacheClearTime
	now := time.Now()
	if now.Add(-ttl).Before(h.cacheSettings.ClearTime) {
		ttl = now.Sub(h.cacheSettings.ClearTime)
	}

	return ttl
}

// resolve the server cache enabled property
func (h *RemoteHub) getServerCacheEnabled() bool {
	var res = true
	if val, ok := os.LookupEnv(constants.EnvCacheEnabled); ok {
		if boolVal, err := typehelpers.ToBool(val); err == nil {
			res = boolVal
		}
	}

	if steampipeconfig.GlobalConfig.DatabaseOptions != nil && steampipeconfig.GlobalConfig.DatabaseOptions.Cache != nil {
		res = *steampipeconfig.GlobalConfig.DatabaseOptions.Cache
	}

	log.Printf("[INFO] Worker PID %d: Hub.getServerCacheEnabled returning %v", os.Getpid(), res)

	return res
}

// GetSortableFields returns a slice of fields which are defined as sortable bythe plugin schema,
// as well as the sort order(s) supported
func (h *RemoteHub) GetSortableFields(tableName, connectionName string) map[string]proto.SortOrder {
	connectionPlugin, err := h.getConnectionPlugin(connectionName)
	if err != nil {
		log.Printf("[WARN] Worker PID %d: GetSortableFields getConnectionPlugin failed for connection %s: %s", os.Getpid(), connectionName, err.Error())
		return nil
	}

	schema, err := connectionPlugin.GetSchema(connectionName)
	if err != nil {
		log.Printf("[WARN] Worker PID %d: GetSortableFields GetSchema failed for connection %s: %s", os.Getpid(), connectionName, err.Error())
		return nil
	}

	tableSchema, ok := schema.Schema[tableName]
	if !ok {
		log.Printf("[WARN] Worker PID %d: GetSortableFields table schema not found for connection %s, table %s", os.Getpid(), connectionName, tableName)
		return nil
	}

	// build map of sortable fields
	var sortableFields = make(map[string]proto.SortOrder)
	for _, column := range tableSchema.Columns {
		sortableFields[column.Name] = column.SortOrder
	}

	if len(sortableFields) > 0 {
		log.Printf("[INFO] Worker PID %d: GetSortableFields for connection '%s`, table `%s`: %v", os.Getpid(), connectionName, tableName, sortableFields)
	}
	return sortableFields
}
