// SPDX-License-Identifier: Apache-2.0

package shared

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBuildARN_WithResourceType(t *testing.T) {
	arn := BuildARN("ecs", "cluster", "my-cluster")
	assert.Equal(t, "arn:aws:ecs:us-east-1:000000000000:cluster/my-cluster", arn)
}

func TestBuildARN_WithoutResourceType(t *testing.T) {
	arn := BuildARN("s3", "", "my-bucket")
	assert.Equal(t, "arn:aws:s3:::my-bucket", arn)
}

func TestBuildARN_CustomRegionAndAccount(t *testing.T) {
	arn := BuildARNWithAccount("lambda", "function", "my-fn", "ap-northeast-2", "123456789012")
	assert.Equal(t, "arn:aws:lambda:ap-northeast-2:123456789012:function/my-fn", arn)
}

func TestBuildARN_EmptyResource(t *testing.T) {
	arn := BuildARN("iam", "", "")
	assert.Equal(t, "arn:aws:iam:::", arn)
}
