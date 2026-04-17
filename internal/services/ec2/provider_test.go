// SPDX-License-Identifier: Apache-2.0

package ec2

import (
	"context"
	"fmt"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestEC2Provider(t *testing.T) *Provider {
	t.Helper()
	dir := t.TempDir()
	p := &Provider{}
	err := p.Init(plugin.PluginConfig{DataDir: dir, Options: map[string]any{
		"db_path": filepath.Join(dir, "ec2.db"),
	}})
	require.NoError(t, err)
	t.Cleanup(func() { p.Shutdown(context.Background()) })
	return p
}

func ec2Request(t *testing.T, p *Provider, form string) *plugin.Response {
	t.Helper()
	req := httptest.NewRequest("POST", "/", strings.NewReader(form))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := p.HandleRequest(context.Background(), "", req)
	require.NoError(t, err)
	return resp
}

func TestRunAndDescribeInstances(t *testing.T) {
	p := newTestEC2Provider(t)

	resp := ec2Request(t, p, "Action=RunInstances&ImageId=ami-12345678&MinCount=2&InstanceType=t3.micro")
	assert.Equal(t, 200, resp.StatusCode)
	body := string(resp.Body)
	assert.Contains(t, body, "RunInstancesResponse")
	assert.Contains(t, body, "ami-12345678")
	assert.Contains(t, body, "t3.micro")

	resp2 := ec2Request(t, p, "Action=DescribeInstances")
	assert.Equal(t, 200, resp2.StatusCode)
	body2 := string(resp2.Body)
	assert.Contains(t, body2, "DescribeInstancesResponse")
	assert.Contains(t, body2, "ami-12345678")
}

func TestTerminateInstances(t *testing.T) {
	p := newTestEC2Provider(t)

	runResp := ec2Request(t, p, "Action=RunInstances&ImageId=ami-abc&MinCount=1")
	require.Equal(t, 200, runResp.StatusCode)
	body := string(runResp.Body)

	// Extract instance ID from response
	start := strings.Index(body, "<instanceId>") + len("<instanceId>")
	end := strings.Index(body, "</instanceId>")
	require.True(t, start > len("<instanceId>")-1 && end > start)
	instanceID := body[start:end]

	termResp := ec2Request(t, p, fmt.Sprintf("Action=TerminateInstances&InstanceId.1=%s", instanceID))
	assert.Equal(t, 200, termResp.StatusCode)
	assert.Contains(t, string(termResp.Body), "terminated")
}

func TestCreateVpcAndSubnet(t *testing.T) {
	p := newTestEC2Provider(t)

	vpcResp := ec2Request(t, p, "Action=CreateVpc&CidrBlock=10.0.0.0/16")
	assert.Equal(t, 200, vpcResp.StatusCode)
	vpcBody := string(vpcResp.Body)
	assert.Contains(t, vpcBody, "CreateVpcResponse")
	assert.Contains(t, vpcBody, "10.0.0.0/16")

	start := strings.Index(vpcBody, "<vpcId>") + len("<vpcId>")
	end := strings.Index(vpcBody, "</vpcId>")
	vpcID := vpcBody[start:end]

	subResp := ec2Request(t, p, fmt.Sprintf("Action=CreateSubnet&VpcId=%s&CidrBlock=10.0.1.0/24", vpcID))
	assert.Equal(t, 200, subResp.StatusCode)
	assert.Contains(t, string(subResp.Body), "CreateSubnetResponse")
	assert.Contains(t, string(subResp.Body), "10.0.1.0/24")

	descResp := ec2Request(t, p, "Action=DescribeVpcs")
	assert.Contains(t, string(descResp.Body), vpcID)
}

func TestSecurityGroupCRUD(t *testing.T) {
	p := newTestEC2Provider(t)

	createResp := ec2Request(t, p, "Action=CreateSecurityGroup&GroupName=my-sg&Description=test+sg")
	assert.Equal(t, 200, createResp.StatusCode)
	body := string(createResp.Body)
	assert.Contains(t, body, "CreateSecurityGroupResponse")
	assert.Contains(t, body, "sg-")

	descResp := ec2Request(t, p, "Action=DescribeSecurityGroups")
	assert.Equal(t, 200, descResp.StatusCode)
	assert.Contains(t, string(descResp.Body), "my-sg")
}

func TestVolumeLifecycle(t *testing.T) {
	p := newTestEC2Provider(t)

	createResp := ec2Request(t, p, "Action=CreateVolume&Size=10&AvailabilityZone=us-east-1a&VolumeType=gp3")
	assert.Equal(t, 200, createResp.StatusCode)
	body := string(createResp.Body)
	assert.Contains(t, body, "CreateVolumeResponse")
	assert.Contains(t, body, "vol-")

	start := strings.Index(body, "<volumeId>") + len("<volumeId>")
	end := strings.Index(body, "</volumeId>")
	require.True(t, start > len("<volumeId>")-1 && end > start)
	volumeID := body[start:end]

	descResp := ec2Request(t, p, fmt.Sprintf("Action=DescribeVolumes&VolumeId.1=%s", volumeID))
	assert.Equal(t, 200, descResp.StatusCode)
	assert.Contains(t, string(descResp.Body), volumeID)

	// ModifyVolume
	modResp := ec2Request(t, p, fmt.Sprintf("Action=ModifyVolume&VolumeId=%s&Size=20", volumeID))
	assert.Equal(t, 200, modResp.StatusCode)
	assert.Contains(t, string(modResp.Body), "ModifyVolumeResponse")

	// AttachVolume - create an instance first
	runResp := ec2Request(t, p, "Action=RunInstances&ImageId=ami-test&MinCount=1")
	require.Equal(t, 200, runResp.StatusCode)
	runBody := string(runResp.Body)
	iStart := strings.Index(runBody, "<instanceId>") + len("<instanceId>")
	iEnd := strings.Index(runBody, "</instanceId>")
	instanceID := runBody[iStart:iEnd]

	attachResp := ec2Request(t, p, fmt.Sprintf("Action=AttachVolume&VolumeId=%s&InstanceId=%s&Device=/dev/sdf", volumeID, instanceID))
	assert.Equal(t, 200, attachResp.StatusCode)
	assert.Contains(t, string(attachResp.Body), "AttachVolumeResponse")

	// DetachVolume
	detachResp := ec2Request(t, p, fmt.Sprintf("Action=DetachVolume&VolumeId=%s", volumeID))
	assert.Equal(t, 200, detachResp.StatusCode)
	assert.Contains(t, string(detachResp.Body), "DetachVolumeResponse")

	// DeleteVolume
	delResp := ec2Request(t, p, fmt.Sprintf("Action=DeleteVolume&VolumeId=%s", volumeID))
	assert.Equal(t, 200, delResp.StatusCode)
	assert.Contains(t, string(delResp.Body), "DeleteVolumeResponse")
}

func TestSnapshotLifecycle(t *testing.T) {
	p := newTestEC2Provider(t)

	volResp := ec2Request(t, p, "Action=CreateVolume&Size=5&AvailabilityZone=us-east-1a")
	require.Equal(t, 200, volResp.StatusCode)
	volBody := string(volResp.Body)
	vStart := strings.Index(volBody, "<volumeId>") + len("<volumeId>")
	vEnd := strings.Index(volBody, "</volumeId>")
	volumeID := volBody[vStart:vEnd]

	snapResp := ec2Request(t, p, fmt.Sprintf("Action=CreateSnapshot&VolumeId=%s&Description=test+snap", volumeID))
	assert.Equal(t, 200, snapResp.StatusCode)
	snapBody := string(snapResp.Body)
	assert.Contains(t, snapBody, "CreateSnapshotResponse")
	assert.Contains(t, snapBody, "snap-")

	sStart := strings.Index(snapBody, "<snapshotId>") + len("<snapshotId>")
	sEnd := strings.Index(snapBody, "</snapshotId>")
	snapshotID := snapBody[sStart:sEnd]

	descResp := ec2Request(t, p, fmt.Sprintf("Action=DescribeSnapshots&SnapshotId.1=%s", snapshotID))
	assert.Equal(t, 200, descResp.StatusCode)
	assert.Contains(t, string(descResp.Body), snapshotID)

	// CopySnapshot
	copyResp := ec2Request(t, p, fmt.Sprintf("Action=CopySnapshot&SourceSnapshotId=%s&SourceRegion=us-east-1&Description=copy", snapshotID))
	assert.Equal(t, 200, copyResp.StatusCode)
	assert.Contains(t, string(copyResp.Body), "snap-")

	// DeleteSnapshot
	delResp := ec2Request(t, p, fmt.Sprintf("Action=DeleteSnapshot&SnapshotId=%s", snapshotID))
	assert.Equal(t, 200, delResp.StatusCode)
	assert.Contains(t, string(delResp.Body), "DeleteSnapshotResponse")
}

func TestKeyPairLifecycle(t *testing.T) {
	p := newTestEC2Provider(t)

	createResp := ec2Request(t, p, "Action=CreateKeyPair&KeyName=test-key")
	assert.Equal(t, 200, createResp.StatusCode)
	body := string(createResp.Body)
	assert.Contains(t, body, "CreateKeyPairResponse")
	assert.Contains(t, body, "test-key")
	assert.Contains(t, body, "BEGIN RSA PRIVATE KEY")

	descResp := ec2Request(t, p, "Action=DescribeKeyPairs&KeyName.1=test-key")
	assert.Equal(t, 200, descResp.StatusCode)
	assert.Contains(t, string(descResp.Body), "test-key")

	// ImportKeyPair
	importResp := ec2Request(t, p, "Action=ImportKeyPair&KeyName=imported-key&PublicKeyMaterial=ssh-rsa+AAAA")
	assert.Equal(t, 200, importResp.StatusCode)
	assert.Contains(t, string(importResp.Body), "ImportKeyPairResponse")
	assert.Contains(t, string(importResp.Body), "imported-key")

	// DeleteKeyPair
	delResp := ec2Request(t, p, "Action=DeleteKeyPair&KeyName=test-key")
	assert.Equal(t, 200, delResp.StatusCode)
	assert.Contains(t, string(delResp.Body), "DeleteKeyPairResponse")
}

func TestImageLifecycle(t *testing.T) {
	p := newTestEC2Provider(t)

	// RegisterImage
	regResp := ec2Request(t, p, "Action=RegisterImage&Name=test-ami&Architecture=x86_64&RootDeviceName=/dev/xvda")
	assert.Equal(t, 200, regResp.StatusCode)
	regBody := string(regResp.Body)
	assert.Contains(t, regBody, "RegisterImageResponse")
	assert.Contains(t, regBody, "ami-")

	start := strings.Index(regBody, "<imageId>") + len("<imageId>")
	end := strings.Index(regBody, "</imageId>")
	require.True(t, start > len("<imageId>")-1 && end > start)
	imageID := regBody[start:end]
	assert.True(t, strings.HasPrefix(imageID, "ami-"))

	// DescribeImages
	descResp := ec2Request(t, p, fmt.Sprintf("Action=DescribeImages&ImageId.1=%s", imageID))
	assert.Equal(t, 200, descResp.StatusCode)
	assert.Contains(t, string(descResp.Body), imageID)

	// CreateImage (from instance - instance doesn't need to exist in our emulator)
	ciResp := ec2Request(t, p, "Action=CreateImage&InstanceId=i-test&Name=created-ami&Description=test")
	assert.Equal(t, 200, ciResp.StatusCode)
	assert.Contains(t, string(ciResp.Body), "ami-")

	// CopyImage
	copyResp := ec2Request(t, p, fmt.Sprintf("Action=CopyImage&SourceImageId=%s&SourceRegion=us-east-1&Name=copy-ami", imageID))
	assert.Equal(t, 200, copyResp.StatusCode)
	assert.Contains(t, string(copyResp.Body), "ami-")

	// DeregisterImage
	deregResp := ec2Request(t, p, fmt.Sprintf("Action=DeregisterImage&ImageId=%s", imageID))
	assert.Equal(t, 200, deregResp.StatusCode)
	assert.Contains(t, string(deregResp.Body), "DeregisterImageResponse")
}

func TestRouteTableLifecycle(t *testing.T) {
	p := newTestEC2Provider(t)

	vpcResp := ec2Request(t, p, "Action=CreateVpc&CidrBlock=10.0.0.0/16")
	require.Equal(t, 200, vpcResp.StatusCode)
	vpcBody := string(vpcResp.Body)
	vStart := strings.Index(vpcBody, "<vpcId>") + len("<vpcId>")
	vEnd := strings.Index(vpcBody, "</vpcId>")
	vpcID := vpcBody[vStart:vEnd]

	// CreateRouteTable
	rtResp := ec2Request(t, p, fmt.Sprintf("Action=CreateRouteTable&VpcId=%s", vpcID))
	assert.Equal(t, 200, rtResp.StatusCode)
	rtBody := string(rtResp.Body)
	assert.Contains(t, rtBody, "rtb-")

	rStart := strings.Index(rtBody, "<routeTableId>") + len("<routeTableId>")
	rEnd := strings.Index(rtBody, "</routeTableId>")
	require.True(t, rStart > len("<routeTableId>")-1 && rEnd > rStart)
	rtID := rtBody[rStart:rEnd]
	assert.True(t, strings.HasPrefix(rtID, "rtb-"))

	// DescribeRouteTables
	descResp := ec2Request(t, p, fmt.Sprintf("Action=DescribeRouteTables&RouteTableId.1=%s", rtID))
	assert.Equal(t, 200, descResp.StatusCode)
	assert.Contains(t, string(descResp.Body), rtID)

	// CreateRoute
	crResp := ec2Request(t, p, fmt.Sprintf("Action=CreateRoute&RouteTableId=%s&DestinationCidrBlock=0.0.0.0/0&GatewayId=igw-test", rtID))
	assert.Equal(t, 200, crResp.StatusCode)

	// DeleteRoute
	drResp := ec2Request(t, p, fmt.Sprintf("Action=DeleteRoute&RouteTableId=%s&DestinationCidrBlock=0.0.0.0/0", rtID))
	assert.Equal(t, 200, drResp.StatusCode)

	// AssociateRouteTable
	subResp := ec2Request(t, p, fmt.Sprintf("Action=CreateSubnet&VpcId=%s&CidrBlock=10.0.1.0/24", vpcID))
	require.Equal(t, 200, subResp.StatusCode)
	subBody := string(subResp.Body)
	sStart := strings.Index(subBody, "<subnetId>") + len("<subnetId>")
	sEnd := strings.Index(subBody, "</subnetId>")
	subnetID := subBody[sStart:sEnd]

	assocResp := ec2Request(t, p, fmt.Sprintf("Action=AssociateRouteTable&RouteTableId=%s&SubnetId=%s", rtID, subnetID))
	assert.Equal(t, 200, assocResp.StatusCode)
	assert.Contains(t, string(assocResp.Body), "rtbassoc-")

	// DeleteRouteTable
	delResp := ec2Request(t, p, fmt.Sprintf("Action=DeleteRouteTable&RouteTableId=%s", rtID))
	assert.Equal(t, 200, delResp.StatusCode)
	assert.Contains(t, string(delResp.Body), "DeleteRouteTableResponse")
}

func TestInternetGatewayLifecycle(t *testing.T) {
	p := newTestEC2Provider(t)

	vpcResp := ec2Request(t, p, "Action=CreateVpc&CidrBlock=10.1.0.0/16")
	require.Equal(t, 200, vpcResp.StatusCode)
	vpcBody := string(vpcResp.Body)
	vStart := strings.Index(vpcBody, "<vpcId>") + len("<vpcId>")
	vEnd := strings.Index(vpcBody, "</vpcId>")
	vpcID := vpcBody[vStart:vEnd]

	// CreateInternetGateway
	igwResp := ec2Request(t, p, "Action=CreateInternetGateway")
	assert.Equal(t, 200, igwResp.StatusCode)
	igwBody := string(igwResp.Body)
	assert.Contains(t, igwBody, "igw-")

	iStart := strings.Index(igwBody, "<internetGatewayId>") + len("<internetGatewayId>")
	iEnd := strings.Index(igwBody, "</internetGatewayId>")
	require.True(t, iStart > len("<internetGatewayId>")-1 && iEnd > iStart)
	igwID := igwBody[iStart:iEnd]
	assert.True(t, strings.HasPrefix(igwID, "igw-"))

	// DescribeInternetGateways
	descResp := ec2Request(t, p, fmt.Sprintf("Action=DescribeInternetGateways&InternetGatewayId.1=%s", igwID))
	assert.Equal(t, 200, descResp.StatusCode)
	assert.Contains(t, string(descResp.Body), igwID)

	// AttachInternetGateway
	attachResp := ec2Request(t, p, fmt.Sprintf("Action=AttachInternetGateway&InternetGatewayId=%s&VpcId=%s", igwID, vpcID))
	assert.Equal(t, 200, attachResp.StatusCode)

	// DetachInternetGateway
	detachResp := ec2Request(t, p, fmt.Sprintf("Action=DetachInternetGateway&InternetGatewayId=%s&VpcId=%s", igwID, vpcID))
	assert.Equal(t, 200, detachResp.StatusCode)
}

func TestNetworkACLLifecycle(t *testing.T) {
	p := newTestEC2Provider(t)

	vpcResp := ec2Request(t, p, "Action=CreateVpc&CidrBlock=10.2.0.0/16")
	require.Equal(t, 200, vpcResp.StatusCode)
	vpcBody := string(vpcResp.Body)
	vStart := strings.Index(vpcBody, "<vpcId>") + len("<vpcId>")
	vEnd := strings.Index(vpcBody, "</vpcId>")
	vpcID := vpcBody[vStart:vEnd]

	// CreateNetworkAcl
	aclResp := ec2Request(t, p, fmt.Sprintf("Action=CreateNetworkAcl&VpcId=%s", vpcID))
	assert.Equal(t, 200, aclResp.StatusCode)
	aclBody := string(aclResp.Body)
	assert.Contains(t, aclBody, "acl-")

	aStart := strings.Index(aclBody, "<networkAclId>") + len("<networkAclId>")
	aEnd := strings.Index(aclBody, "</networkAclId>")
	require.True(t, aStart > len("<networkAclId>")-1 && aEnd > aStart)
	aclID := aclBody[aStart:aEnd]
	assert.True(t, strings.HasPrefix(aclID, "acl-"))

	// DescribeNetworkAcls
	descResp := ec2Request(t, p, fmt.Sprintf("Action=DescribeNetworkAcls&NetworkAclId.1=%s", aclID))
	assert.Equal(t, 200, descResp.StatusCode)
	assert.Contains(t, string(descResp.Body), aclID)

	// CreateNetworkAclEntry
	entryResp := ec2Request(t, p, fmt.Sprintf(
		"Action=CreateNetworkAclEntry&NetworkAclId=%s&RuleNumber=100&Protocol=-1&RuleAction=allow&CidrBlock=0.0.0.0/0&Egress=false",
		aclID,
	))
	assert.Equal(t, 200, entryResp.StatusCode)

	// ReplaceNetworkAclEntry
	replaceResp := ec2Request(t, p, fmt.Sprintf(
		"Action=ReplaceNetworkAclEntry&NetworkAclId=%s&RuleNumber=100&Protocol=-1&RuleAction=deny&CidrBlock=0.0.0.0/0&Egress=false",
		aclID,
	))
	assert.Equal(t, 200, replaceResp.StatusCode)

	// DeleteNetworkAcl
	delResp := ec2Request(t, p, fmt.Sprintf("Action=DeleteNetworkAcl&NetworkAclId=%s", aclID))
	assert.Equal(t, 200, delResp.StatusCode)
	assert.Contains(t, string(delResp.Body), "DeleteNetworkAclResponse")
}

func TestVpcAttributeLifecycle(t *testing.T) {
	p := newTestEC2Provider(t)

	vpcResp := ec2Request(t, p, "Action=CreateVpc&CidrBlock=10.3.0.0/16")
	require.Equal(t, 200, vpcResp.StatusCode)
	vpcBody := string(vpcResp.Body)
	vStart := strings.Index(vpcBody, "<vpcId>") + len("<vpcId>")
	vEnd := strings.Index(vpcBody, "</vpcId>")
	vpcID := vpcBody[vStart:vEnd]

	// ModifyVpcAttribute - enableDnsHostnames
	modResp := ec2Request(t, p, fmt.Sprintf("Action=ModifyVpcAttribute&VpcId=%s&EnableDnsHostnames.Value=true", vpcID))
	assert.Equal(t, 200, modResp.StatusCode)

	// DescribeVpcAttribute - enableDnsHostnames
	descResp := ec2Request(t, p, fmt.Sprintf("Action=DescribeVpcAttribute&VpcId=%s&Attribute=enableDnsHostnames", vpcID))
	assert.Equal(t, 200, descResp.StatusCode)
	descBody := string(descResp.Body)
	assert.Contains(t, descBody, "DescribeVpcAttributeResponse")
	assert.Contains(t, descBody, "<value>true</value>")

	// ModifyVpcAttribute - enableDnsSupport
	modResp2 := ec2Request(t, p, fmt.Sprintf("Action=ModifyVpcAttribute&VpcId=%s&EnableDnsSupport.Value=false", vpcID))
	assert.Equal(t, 200, modResp2.StatusCode)

	// DescribeVpcAttribute - enableDnsSupport
	descResp2 := ec2Request(t, p, fmt.Sprintf("Action=DescribeVpcAttribute&VpcId=%s&Attribute=enableDnsSupport", vpcID))
	assert.Equal(t, 200, descResp2.StatusCode)
	assert.Contains(t, string(descResp2.Body), "<value>false</value>")
}
