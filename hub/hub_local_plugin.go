package hub

import (
	"github.com/turbot/steampipe-plugin-aws/aws"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
)

// TODO NOTE: TEMPLATE ONLY

func getPluginFunc() plugin.PluginFunc {
	return aws.Plugin
}
