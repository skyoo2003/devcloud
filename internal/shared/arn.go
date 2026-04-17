// SPDX-License-Identifier: Apache-2.0

package shared

import "fmt"

const (
	DefaultRegion    = "us-east-1"
	DefaultAccountID = "000000000000"
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
