// SPDX-License-Identifier: Apache-2.0

package shared

import (
	"fmt"

	"github.com/skyoo2003/devcloud/internal/plugin"
)

const (
	DefaultRegion = "us-east-1"
	// DefaultAccountID mirrors plugin.DefaultAccountID so ARN builders and
	// providers cannot drift apart.
	DefaultAccountID = plugin.DefaultAccountID
)

func BuildARN(service, resourceType, resourceID string) string {
	return BuildARNWithAccount(service, resourceType, resourceID, DefaultRegion, DefaultAccountID)
}

func BuildARNWithAccount(service, resourceType, resourceID, region, accountID string) string {
	if resourceType == "" {
		return fmt.Sprintf("arn:aws:%s:::%s", service, resourceID)
	}
	return fmt.Sprintf("arn:aws:%s:%s:%s:%s/%s", service, region, accountID, resourceType, resourceID)
}
