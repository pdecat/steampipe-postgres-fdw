package hub

import (
	"context"
	sdkgrpc "github.com/turbot/steampipe-plugin-sdk/v5/grpc"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/row_stream"
	"github.com/turbot/steampipe-plugin-sdk/v5/telemetry"
)

// Iterator is an interface for table scanner implementations.
type Iterator interface {
	// GetConnectionName returns the connection name that this iterator uses.
	GetConnectionName() string
	GetPluginName() string
	// Next returns a row. Nil slice means there is no more rows to scan.
	Next() (map[string]interface{}, error)
	// Close stops an iteration and frees any resources.
	Close()
	Status() queryStatus
	Error() error
	CanIterate() bool
	GetTraceContext() *telemetry.TraceCtx
}

type pluginExecutor interface {
	execute(req *proto.ExecuteRequest) (str row_stream.Receiver, ctx context.Context, cancel context.CancelFunc, err error)
}

type pluginIterator interface {
	Iterator
	GetPluginExecutor() pluginExecutor
	GetQueryContext() *proto.QueryContext
	GetCallId() string
	GetConnectionLimitMap() map[string]int64
	SetError(err error)
	GetTable() string
	GetScanMetadata() []ScanMetadata
	Start(pluginExecutor) error
}

type grpcExecutor struct {
	pluginClient *sdkgrpc.PluginClient
}

//func (g grpcExecutor) Execute(req *proto.ExecuteRequest) (str row_stream.Receiver, ctx context.Context, cancel context.CancelFunc, err error) {
//
//	log.Printf("[INFO] StartScan for table: %s, cache enabled: %v, iterator %p, %d quals (%s)", i.table, req.CacheEnabled, i, len(i.queryContext.Quals), i.callId)
//	stream, ctx, cancel, err := g.pluginClient.Execute(req)
//	// format GRPC errors
//	err = sdkgrpc.HandleGrpcError(err, i.connectionPlugin.PluginName, "Execute")
//	if err != nil {
//		return nil, nil, nil, err
//	}
//	return stream, ctx, cancel, nil
//
//
//}

//type localExecutor struct {
//	localPluginStream
//}
//
//func (l localExecutor) Execute(req *proto.ExecuteRequest) (str row_stream.Receiver, ctx context.Context, cancel context.CancelFunc, err error) {
//	//TODO implement me
//	panic("implement me")
//}
