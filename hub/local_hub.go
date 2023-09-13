package hub

import (
	"github.com/turbot/steampipe-plugin-aws/aws"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-postgres-fdw/types"
)

type LocalHub struct {
	plugin *grpc.PluginServer
}

func newLocalHub() (*LocalHub, error) {
	// TODO dynamically control the plugin func at build time
	hub := &LocalHub{
		plugin: plugin.NewPluginServer(&plugin.ServeOpts{
			PluginFunc: aws.Plugin,
		}),
	}
	return hub, nil
}

func (LocalHub) LoadConnectionConfig() (bool, error) {
	return false, nil
}

func (LocalHub) GetSchema(remoteSchema string, localSchema string) (*proto.Schema, error) {
	//TODO implement me
	panic("implement me")
}

func (LocalHub) GetIterator(columns []string, quals *proto.Quals, unhandledRestrictions int, limit int64, opts types.Options) (Iterator, error) {
	//TODO implement me
	panic("implement me")
}

func (LocalHub) GetRelSize(columns []string, quals []*proto.Qual, opts types.Options) (types.RelSize, error) {
	//TODO implement me
	panic("implement me")
}

func (LocalHub) GetPathKeys(opts types.Options) ([]types.PathKey, error) {
	//TODO implement me
	panic("implement me")
}

func (LocalHub) Explain(columns []string, quals []*proto.Qual, sortKeys []string, verbose bool, opts types.Options) ([]string, error) {
	//TODO implement me
	panic("implement me")
}

func (LocalHub) ApplySetting(key string, value string) error {
	//TODO implement me
	panic("implement me")
}

func (LocalHub) GetSettingsSchema() map[string]*proto.TableSchema {
	//TODO implement me
	panic("implement me")
}

func (LocalHub) GetLegacySettingsSchema() map[string]*proto.TableSchema {
	//TODO implement me
	panic("implement me")
}

func (LocalHub) StartScan(i Iterator) error {
	//TODO implement me
	panic("implement me")
}

func (LocalHub) RemoveIterator(iterator Iterator) {
	//TODO implement me
	panic("implement me")
}

func (LocalHub) EndScan(iter Iterator, limit int64) {
	//TODO implement me
	panic("implement me")
}

func (LocalHub) AddScanMetadata(iter Iterator) {
	//TODO implement me
	panic("implement me")
}

func (LocalHub) Abort() {
	//TODO implement me
	panic("implement me")
}

func (LocalHub) Close() {
	//TODO implement me
	panic("implement me")
}

func (LocalHub) HandleLegacyCacheCommand(command string) error {
	//TODO implement me
	panic("implement me")
}

func (LocalHub) ValidateCacheCommand(command string) error {
	//TODO implement me
	panic("implement me")
}
