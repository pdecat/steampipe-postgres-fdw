package hub

import (
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/logging"
	"github.com/turbot/steampipe-postgres-fdw/types"
	"sync"
	"time"
)

// TODO check which can be non-imported
type Hub interface {
	LoadConnectionConfig() (bool, error)
	GetSchema(remoteSchema string, localSchema string) (*proto.Schema, error)
	GetIterator(columns []string, quals *proto.Quals, unhandledRestrictions int, limit int64, opts types.Options) (Iterator, error)
	GetRelSize(columns []string, quals []*proto.Qual, opts types.Options) (types.RelSize, error)
	GetPathKeys(opts types.Options) ([]types.PathKey, error)
	Explain(columns []string, quals []*proto.Qual, sortKeys []string, verbose bool, opts types.Options) ([]string, error)
	ApplySetting(key string, value string) error
	GetSettingsSchema() map[string]*proto.TableSchema
	GetLegacySettingsSchema() map[string]*proto.TableSchema
	StartScan(i Iterator) error
	RemoveIterator(iterator Iterator)
	EndScan(iter Iterator, limit int64)
	AddScanMetadata(iter Iterator)
	Abort()
	Close()
	HandleLegacyCacheCommand(command string) error
	ValidateCacheCommand(command string) error
	cacheTTL(name string) time.Duration
	cacheEnabled(name string) bool
}

// global hub instance
var hubSingleton Hub

// mutex protecting hub creation
var hubMux sync.Mutex

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
		// TODO configure build to select between local and remote hub
		// TODO get connection config from import foreign schema options

		hubSingleton, err = newLocalHub(map[string]string{
			"aws": "",
		})
		if err != nil {
			return nil, err
		}
	}
	logging.LogTime("GetHub end")
	return hubSingleton, err
}
