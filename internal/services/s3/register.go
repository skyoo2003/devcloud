// SPDX-License-Identifier: Apache-2.0

package s3

import "github.com/skyoo2003/devcloud/internal/plugin"

func init() {
	plugin.DefaultRegistry.Register("s3", func(cfg plugin.PluginConfig) plugin.ServicePlugin {
		return &S3Provider{}
	})
}
