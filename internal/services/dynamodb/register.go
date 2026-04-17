// SPDX-License-Identifier: Apache-2.0

package dynamodb

import "github.com/skyoo2003/devcloud/internal/plugin"

func init() {
	plugin.DefaultRegistry.Register("dynamodb", func(cfg plugin.PluginConfig) plugin.ServicePlugin {
		return &DynamoDBProvider{}
	})
}
