package hub

import (
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/telemetry"
)

type localScanIterator struct {
	baseScanIterator
	pluginName string
}

func newLocalScanIterator(hub Hub, connectionName, table, pluginName string, connectionLimitMap map[string]int64, qualMap map[string]*proto.Quals, columns []string, limit int64, traceCtx *telemetry.TraceCtx) *localScanIterator {
	return &localScanIterator{
		baseScanIterator: newBaseScanIterator(hub, connectionName, table, connectionLimitMap, qualMap, columns, limit, traceCtx),
		pluginName:       pluginName,
	}
}

// GetPluginName implements Iterator
func (i *localScanIterator) GetPluginName() string {
	return i.pluginName
}
