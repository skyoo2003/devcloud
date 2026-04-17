// SPDX-License-Identifier: Apache-2.0

package iam

import "github.com/skyoo2003/devcloud/internal/plugin"

func init() {
	plugin.DefaultRegistry.Register("iam", func(cfg plugin.PluginConfig) plugin.ServicePlugin {
		return &IAMProvider{}
	})
	plugin.DefaultRegistry.Register("sts", func(cfg plugin.PluginConfig) plugin.ServicePlugin {
		return &STSProvider{}
	})
}
