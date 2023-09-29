package hub

import (
	"github.com/turbot/steampipe-plugin-aws/aws"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/logging"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/telemetry"
	"github.com/turbot/steampipe-postgres-fdw/settings"
	"github.com/turbot/steampipe-postgres-fdw/types"
	"github.com/turbot/steampipe/pkg/constants"
	"github.com/turbot/steampipe/pkg/steampipeconfig/modconfig"
	"log"
)

type LocalHub struct {
	hubBase
	plugin *grpc.PluginServer
}

func newLocalHub() (*LocalHub, error) {
	// TODO dynamically control the plugin func at build time
	hub := &LocalHub{
		plugin: plugin.NewPluginServer(&plugin.ServeOpts{
			PluginFunc: aws.Plugin,
		}),
	}

	hub.cacheSettings = settings.NewCacheSettings()

	// TODO CHECK TELEMETRY ENABLED?
	if err := hub.initialiseTelemetry(); err != nil {
		return nil, err
	}

	return hub, nil
}

func (l *LocalHub) LoadConnectionConfig() (bool, error) {
	// do nothing
	return false, nil
}

func (l *LocalHub) GetSchema(pluginImageRef, connectionName string) (*proto.Schema, error) {
	res, err := l.plugin.GetSchema(&proto.GetSchemaRequest{Connection: connectionName})
	if err != nil {
		return nil, err
	}
	return res.GetSchema(), nil
}

func (l *LocalHub) GetIterator(columns []string, quals *proto.Quals, unhandledRestrictions int, limit int64, opts types.Options) (Iterator, error) {
	logging.LogTime("GetIterator start")
	qualMap, err := buildQualMap(quals)
	connectionName := opts["connection"]
	table := opts["table"]
	log.Printf("[TRACE] RemoteHub GetIterator() table '%s'", table)

	if connectionName == constants.InternalSchema || connectionName == constants.LegacyCommandSchema {
		return l.executeCommandScan(connectionName, table)
	}

	// create a span for this scan
	scanTraceCtx := l.traceContextForScan(table, columns, limit, qualMap, connectionName)
	iterator, err := l.startScanForConnection(connectionName, table, qualMap, unhandledRestrictions, columns, limit, scanTraceCtx)

	if err != nil {
		log.Printf("[TRACE] RemoteHub GetIterator() failed :( %s", err)
		return nil, err
	}
	log.Printf("[TRACE] RemoteHub GetIterator() created iterator (%p)", iterator)

	return iterator, nil
}

func (l *LocalHub) GetPathKeys(opts types.Options) ([]types.PathKey, error) {
	if err != nil {
		return nil, err
	}

	connectionSchema, err := connectionPlugin.GetSchema(connectionName)
	if err != nil {
		return nil, err
	}

	return h.getPathKeys(connectionSchema, opts)

}

func (l *LocalHub) Explain(columns []string, quals []*proto.Qual, sortKeys []string, verbose bool, opts types.Options) ([]string, error) {
	//TODO implement me
	panic("implement me")
}

func (l *LocalHub) ApplySetting(key string, value string) error {
	//TODO implement me
	panic("implement me")
}

func (l *LocalHub) GetSettingsSchema() map[string]*proto.TableSchema {
	//TODO implement me
	panic("implement me")
}

func (l *LocalHub) GetLegacySettingsSchema() map[string]*proto.TableSchema {
	//TODO implement me
	panic("implement me")
}

func (l *LocalHub) StartScan(i Iterator) error {
	//TODO implement me
	panic("implement me")
}

func (l *LocalHub) RemoveIterator(iterator Iterator) {
	//TODO implement me
	panic("implement me")
}

func (l *LocalHub) EndScan(iter Iterator, limit int64) {
	//TODO implement me
	panic("implement me")
}

func (l *LocalHub) AddScanMetadata(iter Iterator) {
	//TODO implement me
	panic("implement me")
}

func (l *LocalHub) Abort() {
	//TODO implement me
	panic("implement me")
}

func (l *LocalHub) Close() {
	//TODO implement me
	panic("implement me")
}

func (l *LocalHub) HandleLegacyCacheCommand(command string) error {
	//TODO implement me
	panic("implement me")
}

func (l *LocalHub) ValidateCacheCommand(command string) error {
	//TODO implement me
	panic("implement me")
}

// startScanForConnection starts a scan for a single connection, using a scanIterator or a legacyScanIterator
func (l *LocalHub) startScanForConnection(connectionName string, table string, qualMap map[string]*proto.Quals, unhandledRestrictions int, columns []string, limit int64, scanTraceCtx *telemetry.TraceCtx) (_ Iterator, err error) {
	defer func() {
		if err != nil {
			// close the span in case of errir
			scanTraceCtx.Span.End()
		}
	}()

	// ok so this is a multi connection plugin, build list of connections,
	// if this connection is NOT an aggregator, only execute for the named connection

	//// get connection config
	//connectionConfig, ok := l.getConnectionconfig(ConnectionName)
	//if !ok {
	//	return nil, fmt.Errorf("no connection config loaded for connection '%s'", ConnectionName)
	//}

	// determine whether to pushdown the limit
	connectionLimitMap, err := l.buildConnectionLimitMap(connectionName, table, qualMap, unhandledRestrictions, limit)
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
	iterator := newLocalScanIterator(l, connectionName, table, connectionLimitMap, qualMap, columns, limit, scanTraceCtx)
	return iterator, nil
}

func (l *LocalHub) getConnectionconfig(name string) (*modconfig.Connection, bool) {
	// TODO KAI how will we get connection config for connections
	// pass into FDW as options???
	return nil, false
}

func (l *LocalHub) buildConnectionLimitMap(connection, table string, qualMap map[string]*proto.Quals, unhandledRestrictions int, limit int64) (map[string]int64, error) {
	connectionSchema, err := l.GetSchema("", connection)
	if err != nil {
		return nil, err
	}
	schemaMode := connectionSchema.Mode

	// pushing the limit down or not is dependent on the schema.
	// for a static schema, the limit will be the same for all connections (i.e. we either pushdown for all or none)
	// check once whether we should push down
	if limit != -1 && schemaMode == plugin.SchemaModeStatic {
		log.Printf("[TRACE] static schema - using same limit for all connections")
		if !l.shouldPushdownLimit(table, qualMap, unhandledRestrictions, connectionSchema) {
			limit = -1
		}
	}

	// set the limit for the one and only connection
	var connectionLimitMap = make(map[string]int64)
	connectionLimit := limit
	// if schema mode is dynamic, check whether we should push down for each connection
	if schemaMode == plugin.SchemaModeDynamic && !l.shouldPushdownLimit(table, qualMap, unhandledRestrictions, connectionSchema) {
		log.Printf("[INFO] not pushing limit down for connection %s", connection)
		connectionLimit = -1
	}
	connectionLimitMap[connection] = connectionLimit

	//return ConnectionLimitMap, nil
	return make(map[string]int64), nil
}
