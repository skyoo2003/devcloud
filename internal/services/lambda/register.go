// SPDX-License-Identifier: Apache-2.0

package lambda

import "github.com/skyoo2003/devcloud/internal/plugin"

func init() {
	plugin.DefaultRegistry.Register("lambda", func(cfg plugin.PluginConfig) plugin.ServicePlugin {
		return &LambdaProvider{}
	})
}
