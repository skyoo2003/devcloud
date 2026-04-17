// SPDX-License-Identifier: Apache-2.0

package sqs

import "github.com/skyoo2003/devcloud/internal/plugin"

func init() {
	plugin.DefaultRegistry.Register("sqs", func(cfg plugin.PluginConfig) plugin.ServicePlugin {
		return &SQSProvider{}
	})
}
