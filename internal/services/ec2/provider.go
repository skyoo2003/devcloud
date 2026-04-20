// SPDX-License-Identifier: Apache-2.0

package ec2

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/skyoo2003/devcloud/internal/plugin"
)

const (
	defaultAccountID = plugin.DefaultAccountID
	// maxRunInstancesBatchCount matches the AWS EC2 default on-demand instance limit per launch request.
	maxRunInstancesBatchCount = 1000
)

// Provider implements the EC2 service (Query/XML protocol).
type Provider struct {
	store *EC2Store
}

func (p *Provider) ServiceID() string             { return "ec2" }
func (p *Provider) ServiceName() string           { return "Amazon EC2" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolQuery }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return fmt.Errorf("init ec2: %w", err)
	}
	var err error
	p.store, err = NewEC2Store(cfg.DataDir)
	return err
}

func (p *Provider) Shutdown(_ context.Context) error {
	if p.store != nil {
		return p.store.Close()
	}
	return nil
}

func (p *Provider) HandleRequest(ctx context.Context, op string, req *http.Request) (*plugin.Response, error) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return ec2XMLError("InvalidRequest", "failed to read request body", http.StatusBadRequest), nil
	}
	form, err := url.ParseQuery(string(body))
	if err != nil {
		return ec2XMLError("InvalidRequest", "failed to parse form body", http.StatusBadRequest), nil
	}
	action := op
	if action == "" {
		action = form.Get("Action")
	}

	switch action {
	case "RunInstances":
		return p.handleRunInstances(form)
	case "DescribeInstances":
		return p.handleDescribeInstances(form)
	case "TerminateInstances":
		return p.handleTerminateInstances(form)
	case "CreateVpc":
		return p.handleCreateVpc(form)
	case "DescribeVpcs":
		return p.handleDescribeVpcs(form)
	case "DeleteVpc":
		return p.handleDeleteVpc(form)
	case "CreateSubnet":
		return p.handleCreateSubnet(form)
	case "DescribeSubnets":
		return p.handleDescribeSubnets(form)
	case "CreateSecurityGroup":
		return p.handleCreateSecurityGroup(form)
	case "DescribeSecurityGroups":
		return p.handleDescribeSecurityGroups(form)
	case "CreateTags":
		return p.handleCreateTags(form)
	case "AllocateAddress":
		return p.handleAllocateAddress(form)
	case "CreateVolume":
		return p.handleCreateVolume(form)
	case "DescribeVolumes":
		return p.handleDescribeVolumes(form)
	case "DeleteVolume":
		return p.handleDeleteVolume(form)
	case "AttachVolume":
		return p.handleAttachVolume(form)
	case "DetachVolume":
		return p.handleDetachVolume(form)
	case "ModifyVolume":
		return p.handleModifyVolume(form)
	case "CreateSnapshot":
		return p.handleCreateSnapshot(form)
	case "DescribeSnapshots":
		return p.handleDescribeSnapshots(form)
	case "DeleteSnapshot":
		return p.handleDeleteSnapshot(form)
	case "CopySnapshot":
		return p.handleCopySnapshot(form)
	case "CreateKeyPair":
		return p.handleCreateKeyPair(form)
	case "DescribeKeyPairs":
		return p.handleDescribeKeyPairs(form)
	case "DeleteKeyPair":
		return p.handleDeleteKeyPair(form)
	case "ImportKeyPair":
		return p.handleImportKeyPair(form)
	// Image/AMI
	case "CreateImage":
		return p.handleCreateImage(form)
	case "DescribeImages":
		return p.handleDescribeImages(form)
	case "DeregisterImage":
		return p.handleDeregisterImage(form)
	case "RegisterImage":
		return p.handleRegisterImage(form)
	case "CopyImage":
		return p.handleCopyImage(form)
	// RouteTable
	case "CreateRouteTable":
		return p.handleCreateRouteTable(form)
	case "DescribeRouteTables":
		return p.handleDescribeRouteTables(form)
	case "DeleteRouteTable":
		return p.handleDeleteRouteTable(form)
	case "CreateRoute":
		return p.handleCreateRoute(form)
	case "DeleteRoute":
		return p.handleDeleteRoute(form)
	case "AssociateRouteTable":
		return p.handleAssociateRouteTable(form)
	// InternetGateway
	case "CreateInternetGateway":
		return p.handleCreateInternetGateway(form)
	case "DescribeInternetGateways":
		return p.handleDescribeInternetGateways(form)
	case "AttachInternetGateway":
		return p.handleAttachInternetGateway(form)
	case "DetachInternetGateway":
		return p.handleDetachInternetGateway(form)
	// NetworkACL
	case "CreateNetworkAcl":
		return p.handleCreateNetworkACL(form)
	case "DescribeNetworkAcls":
		return p.handleDescribeNetworkACLs(form)
	case "DeleteNetworkAcl":
		return p.handleDeleteNetworkACL(form)
	case "CreateNetworkAclEntry":
		return p.handleCreateNetworkACLEntry(form)
	case "ReplaceNetworkAclEntry":
		return p.handleReplaceNetworkACLEntry(form)
	// VPC attributes
	case "ModifyVpcAttribute":
		return p.handleModifyVpcAttribute(form)
	case "DescribeVpcAttribute":
		return p.handleDescribeVpcAttribute(form)
	default:
		return ec2XMLError("InvalidAction", fmt.Sprintf("unknown action: %s", action), http.StatusBadRequest), nil
	}
}

func (p *Provider) ListResources(ctx context.Context) ([]plugin.Resource, error) {
	instances, err := p.store.DescribeInstances(defaultAccountID, nil)
	if err != nil {
		return nil, err
	}
	out := make([]plugin.Resource, 0, len(instances))
	for _, inst := range instances {
		out = append(out, plugin.Resource{Type: "instance", ID: inst.InstanceID, Name: inst.InstanceID})
	}
	return out, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// --- operation handlers ---

func (p *Provider) handleRunInstances(form url.Values) (*plugin.Response, error) {
	imageID := form.Get("ImageId")
	if imageID == "" {
		return ec2XMLError("MissingParameter", "ImageId is required", http.StatusBadRequest), nil
	}
	instanceType := form.Get("InstanceType")
	count := 1
	if s := form.Get("MinCount"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n <= 0 {
			return ec2XMLError("InvalidParameterValue", "MinCount must be a positive integer", http.StatusBadRequest), nil
		}
		if n > maxRunInstancesBatchCount {
			return ec2XMLError("InvalidParameterValue", fmt.Sprintf("MinCount exceeds maximum allowed value (%d)", maxRunInstancesBatchCount), http.StatusBadRequest), nil
		}
		count = n
	}
	if s := form.Get("MaxCount"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n <= 0 {
			return ec2XMLError("InvalidParameterValue", "MaxCount must be a positive integer", http.StatusBadRequest), nil
		}
		if n > maxRunInstancesBatchCount {
			return ec2XMLError("InvalidParameterValue", fmt.Sprintf("MaxCount exceeds maximum allowed value (%d)", maxRunInstancesBatchCount), http.StatusBadRequest), nil
		}
		if n < count {
			return ec2XMLError("InvalidParameterValue", "MaxCount must be greater than or equal to MinCount", http.StatusBadRequest), nil
		}
		count = n
	}

	instances, err := p.store.RunInstances(defaultAccountID, imageID, instanceType, count)
	if err != nil {
		return nil, err
	}

	type instanceItemXML struct {
		InstanceId    string `xml:"instanceId"`
		ImageId       string `xml:"imageId"`
		InstanceType  string `xml:"instanceType"`
		InstanceState struct {
			Name string `xml:"name"`
		} `xml:"instanceState"`
		PrivateIpAddress string `xml:"privateIpAddress"`
	}
	type reservationXML struct {
		ReservationId string            `xml:"reservationId"`
		InstancesSet  []instanceItemXML `xml:"instancesSet>item"`
	}
	type runInstancesResponse struct {
		XMLName xml.Name `xml:"RunInstancesResponse"`
		reservationXML
	}

	items := make([]instanceItemXML, 0, len(instances))
	for _, inst := range instances {
		item := instanceItemXML{
			InstanceId:       inst.InstanceID,
			ImageId:          inst.ImageID,
			InstanceType:     inst.InstanceType,
			PrivateIpAddress: inst.PrivateIP,
		}
		item.InstanceState.Name = inst.State
		items = append(items, item)
	}
	resID, _ := randHex(8)
	return ec2XMLResponse(http.StatusOK, runInstancesResponse{
		reservationXML: reservationXML{
			ReservationId: "r-" + resID,
			InstancesSet:  items,
		},
	})
}

func (p *Provider) handleDescribeInstances(form url.Values) (*plugin.Response, error) {
	var ids []string
	for i := 1; ; i++ {
		id := form.Get(fmt.Sprintf("InstanceId.%d", i))
		if id == "" {
			break
		}
		ids = append(ids, id)
	}

	instances, err := p.store.DescribeInstances(defaultAccountID, ids)
	if err != nil {
		return nil, err
	}

	type instanceStateXML struct {
		Name string `xml:"name"`
	}
	type tagItemXML struct {
		Key   string `xml:"key"`
		Value string `xml:"value"`
	}
	type instanceItemXML struct {
		InstanceId       string           `xml:"instanceId"`
		ImageId          string           `xml:"imageId"`
		InstanceType     string           `xml:"instanceType"`
		InstanceState    instanceStateXML `xml:"instanceState"`
		PrivateIpAddress string           `xml:"privateIpAddress"`
		TagSet           []tagItemXML     `xml:"tagSet>item,omitempty"`
	}
	type reservationItemXML struct {
		ReservationId string            `xml:"reservationId"`
		InstancesSet  []instanceItemXML `xml:"instancesSet>item"`
	}
	type describeInstancesResponse struct {
		XMLName        xml.Name             `xml:"DescribeInstancesResponse"`
		ReservationSet []reservationItemXML `xml:"reservationSet>item"`
	}

	items := make([]instanceItemXML, 0, len(instances))
	for _, inst := range instances {
		item := instanceItemXML{
			InstanceId:       inst.InstanceID,
			ImageId:          inst.ImageID,
			InstanceType:     inst.InstanceType,
			InstanceState:    instanceStateXML{Name: inst.State},
			PrivateIpAddress: inst.PrivateIP,
		}
		if tags, err := p.store.GetTags(inst.InstanceID); err == nil && len(tags) > 0 {
			for _, t := range tags {
				item.TagSet = append(item.TagSet, tagItemXML{Key: t.Key, Value: t.Value})
			}
		}
		items = append(items, item)
	}
	resID, _ := randHex(8)
	return ec2XMLResponse(http.StatusOK, describeInstancesResponse{
		ReservationSet: []reservationItemXML{{ReservationId: "r-" + resID, InstancesSet: items}},
	})
}

func (p *Provider) handleTerminateInstances(form url.Values) (*plugin.Response, error) {
	var ids []string
	for i := 1; ; i++ {
		id := form.Get(fmt.Sprintf("InstanceId.%d", i))
		if id == "" {
			break
		}
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		return ec2XMLError("MissingParameter", "at least one InstanceId is required", http.StatusBadRequest), nil
	}
	if err := p.store.TerminateInstances(defaultAccountID, ids); err != nil {
		if errors.Is(err, ErrInstanceNotFound) {
			return ec2XMLError("InvalidInstanceID.NotFound", err.Error(), http.StatusBadRequest), nil
		}
		return nil, err
	}

	type stateChangeXML struct {
		InstanceId   string `xml:"instanceId"`
		CurrentState struct {
			Name string `xml:"name"`
		} `xml:"currentState"`
		PreviousState struct {
			Name string `xml:"name"`
		} `xml:"previousState"`
	}
	type terminateInstancesResponse struct {
		XMLName      xml.Name         `xml:"TerminateInstancesResponse"`
		InstancesSet []stateChangeXML `xml:"instancesSet>item"`
	}

	items := make([]stateChangeXML, 0, len(ids))
	for _, id := range ids {
		sc := stateChangeXML{InstanceId: id}
		sc.CurrentState.Name = "terminated"
		sc.PreviousState.Name = "running"
		items = append(items, sc)
	}
	return ec2XMLResponse(http.StatusOK, terminateInstancesResponse{InstancesSet: items})
}

func (p *Provider) handleCreateVpc(form url.Values) (*plugin.Response, error) {
	cidr := form.Get("CidrBlock")
	if cidr == "" {
		return ec2XMLError("MissingParameter", "CidrBlock is required", http.StatusBadRequest), nil
	}
	vpc, err := p.store.CreateVpc(defaultAccountID, cidr)
	if err != nil {
		return nil, err
	}
	type vpcXML struct {
		VpcId     string `xml:"vpcId"`
		CidrBlock string `xml:"cidrBlock"`
		State     string `xml:"state"`
	}
	type createVpcResponse struct {
		XMLName xml.Name `xml:"CreateVpcResponse"`
		Vpc     vpcXML   `xml:"vpc"`
	}
	return ec2XMLResponse(http.StatusOK, createVpcResponse{
		Vpc: vpcXML{VpcId: vpc.VpcID, CidrBlock: vpc.CidrBlock, State: vpc.State},
	})
}

func (p *Provider) handleDescribeVpcs(_ url.Values) (*plugin.Response, error) {
	vpcs, err := p.store.DescribeVpcs(defaultAccountID)
	if err != nil {
		return nil, err
	}
	type vpcItemXML struct {
		VpcId     string `xml:"vpcId"`
		CidrBlock string `xml:"cidrBlock"`
		State     string `xml:"state"`
	}
	type describeVpcsResponse struct {
		XMLName xml.Name     `xml:"DescribeVpcsResponse"`
		VpcSet  []vpcItemXML `xml:"vpcSet>item"`
	}
	items := make([]vpcItemXML, 0, len(vpcs))
	for _, v := range vpcs {
		items = append(items, vpcItemXML{VpcId: v.VpcID, CidrBlock: v.CidrBlock, State: v.State})
	}
	return ec2XMLResponse(http.StatusOK, describeVpcsResponse{VpcSet: items})
}

func (p *Provider) handleDeleteVpc(form url.Values) (*plugin.Response, error) {
	vpcID := form.Get("VpcId")
	if vpcID == "" {
		return ec2XMLError("MissingParameter", "VpcId is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteVpc(defaultAccountID, vpcID); err != nil {
		return ec2XMLError("InvalidVpcID.NotFound", "vpc not found", http.StatusBadRequest), nil
	}
	type deleteVpcResponse struct {
		XMLName xml.Name `xml:"DeleteVpcResponse"`
		Return  bool     `xml:"return"`
	}
	return ec2XMLResponse(http.StatusOK, deleteVpcResponse{Return: true})
}

func (p *Provider) handleCreateSubnet(form url.Values) (*plugin.Response, error) {
	vpcID := form.Get("VpcId")
	cidr := form.Get("CidrBlock")
	if vpcID == "" || cidr == "" {
		return ec2XMLError("MissingParameter", "VpcId and CidrBlock are required", http.StatusBadRequest), nil
	}
	az := form.Get("AvailabilityZone")
	sub, err := p.store.CreateSubnet(defaultAccountID, vpcID, cidr, az)
	if err != nil {
		return nil, err
	}
	type subnetXML struct {
		SubnetId         string `xml:"subnetId"`
		VpcId            string `xml:"vpcId"`
		CidrBlock        string `xml:"cidrBlock"`
		AvailabilityZone string `xml:"availabilityZone"`
	}
	type createSubnetResponse struct {
		XMLName xml.Name  `xml:"CreateSubnetResponse"`
		Subnet  subnetXML `xml:"subnet"`
	}
	return ec2XMLResponse(http.StatusOK, createSubnetResponse{
		Subnet: subnetXML{SubnetId: sub.SubnetID, VpcId: sub.VpcID, CidrBlock: sub.CidrBlock, AvailabilityZone: sub.AZ},
	})
}

func (p *Provider) handleDescribeSubnets(_ url.Values) (*plugin.Response, error) {
	subnets, err := p.store.DescribeSubnets(defaultAccountID)
	if err != nil {
		return nil, err
	}
	type subnetItemXML struct {
		SubnetId         string `xml:"subnetId"`
		VpcId            string `xml:"vpcId"`
		CidrBlock        string `xml:"cidrBlock"`
		AvailabilityZone string `xml:"availabilityZone"`
	}
	type describeSubnetsResponse struct {
		XMLName   xml.Name        `xml:"DescribeSubnetsResponse"`
		SubnetSet []subnetItemXML `xml:"subnetSet>item"`
	}
	items := make([]subnetItemXML, 0, len(subnets))
	for _, sub := range subnets {
		items = append(items, subnetItemXML{SubnetId: sub.SubnetID, VpcId: sub.VpcID, CidrBlock: sub.CidrBlock, AvailabilityZone: sub.AZ})
	}
	return ec2XMLResponse(http.StatusOK, describeSubnetsResponse{SubnetSet: items})
}

func (p *Provider) handleCreateSecurityGroup(form url.Values) (*plugin.Response, error) {
	groupName := form.Get("GroupName")
	desc := form.Get("Description")
	vpcID := form.Get("VpcId")
	if groupName == "" {
		return ec2XMLError("MissingParameter", "GroupName is required", http.StatusBadRequest), nil
	}
	sg, err := p.store.CreateSecurityGroup(defaultAccountID, groupName, vpcID, desc)
	if err != nil {
		return ec2XMLError("InvalidGroup.Duplicate", fmt.Sprintf("security group %q already exists", groupName), http.StatusBadRequest), nil
	}
	type createSecurityGroupResponse struct {
		XMLName xml.Name `xml:"CreateSecurityGroupResponse"`
		GroupId string   `xml:"groupId"`
	}
	return ec2XMLResponse(http.StatusOK, createSecurityGroupResponse{GroupId: sg.GroupID})
}

func (p *Provider) handleDescribeSecurityGroups(_ url.Values) (*plugin.Response, error) {
	sgs, err := p.store.DescribeSecurityGroups(defaultAccountID)
	if err != nil {
		return nil, err
	}
	type sgItemXML struct {
		GroupId     string `xml:"groupId"`
		GroupName   string `xml:"groupName"`
		VpcId       string `xml:"vpcId"`
		Description string `xml:"groupDescription"`
	}
	type describeSecurityGroupsResponse struct {
		XMLName           xml.Name    `xml:"DescribeSecurityGroupsResponse"`
		SecurityGroupInfo []sgItemXML `xml:"securityGroupInfo>item"`
	}
	items := make([]sgItemXML, 0, len(sgs))
	for _, sg := range sgs {
		items = append(items, sgItemXML{GroupId: sg.GroupID, GroupName: sg.GroupName, VpcId: sg.VpcID, Description: sg.Description})
	}
	return ec2XMLResponse(http.StatusOK, describeSecurityGroupsResponse{SecurityGroupInfo: items})
}

func (p *Provider) handleCreateTags(form url.Values) (*plugin.Response, error) {
	var resourceIDs []string
	for i := 1; ; i++ {
		id := form.Get(fmt.Sprintf("ResourceId.%d", i))
		if id == "" {
			break
		}
		resourceIDs = append(resourceIDs, id)
	}
	if len(resourceIDs) == 0 {
		return ec2XMLError("MissingParameter", "at least one ResourceId is required", http.StatusBadRequest), nil
	}
	var tags []Tag
	for i := 1; ; i++ {
		key := form.Get(fmt.Sprintf("Tag.%d.Key", i))
		if key == "" {
			break
		}
		value := form.Get(fmt.Sprintf("Tag.%d.Value", i))
		tags = append(tags, Tag{Key: key, Value: value})
	}
	if err := p.store.CreateTags(defaultAccountID, resourceIDs, tags); err != nil {
		return nil, err
	}
	type createTagsResponse struct {
		XMLName xml.Name `xml:"CreateTagsResponse"`
		Return  bool     `xml:"return"`
	}
	return ec2XMLResponse(http.StatusOK, createTagsResponse{Return: true})
}

func (p *Provider) handleAllocateAddress(form url.Values) (*plugin.Response, error) {
	domain := form.Get("Domain")
	addr, err := p.store.AllocateAddress(defaultAccountID, domain)
	if err != nil {
		return nil, err
	}
	type allocateAddressResponse struct {
		XMLName      xml.Name `xml:"AllocateAddressResponse"`
		PublicIp     string   `xml:"publicIp"`
		AllocationId string   `xml:"allocationId"`
		Domain       string   `xml:"domain"`
	}
	return ec2XMLResponse(http.StatusOK, allocateAddressResponse{
		PublicIp:     addr.PublicIP,
		AllocationId: addr.AllocationID,
		Domain:       addr.Domain,
	})
}

// --- Volume handlers ---

func (p *Provider) handleCreateVolume(form url.Values) (*plugin.Response, error) {
	sizeStr := form.Get("Size")
	if sizeStr == "" {
		return ec2XMLError("MissingParameter", "Size is required", http.StatusBadRequest), nil
	}
	size, err := strconv.Atoi(sizeStr)
	if err != nil || size <= 0 {
		return ec2XMLError("InvalidParameterValue", "Size must be a positive integer", http.StatusBadRequest), nil
	}
	az := form.Get("AvailabilityZone")
	volumeType := form.Get("VolumeType")
	iops := 0
	if s := form.Get("Iops"); s != "" {
		iops, _ = strconv.Atoi(s)
	}
	encrypted := form.Get("Encrypted") == "true" || form.Get("Encrypted") == "1"
	kmsKeyID := form.Get("KmsKeyId")

	vol, err := p.store.CreateVolume(defaultAccountID, size, az, volumeType, iops, encrypted, kmsKeyID)
	if err != nil {
		return nil, err
	}

	type createVolumeResponse struct {
		XMLName          xml.Name `xml:"CreateVolumeResponse"`
		VolumeId         string   `xml:"volumeId"`
		Size             int      `xml:"size"`
		AvailabilityZone string   `xml:"availabilityZone"`
		Status           string   `xml:"status"`
		CreateTime       string   `xml:"createTime"`
		VolumeType       string   `xml:"volumeType"`
		Encrypted        bool     `xml:"encrypted"`
	}
	return ec2XMLResponse(http.StatusOK, createVolumeResponse{
		VolumeId:         vol.VolumeID,
		Size:             vol.Size,
		AvailabilityZone: vol.AvailabilityZone,
		Status:           vol.State,
		CreateTime:       vol.CreatedAt.Format("2006-01-02T15:04:05.000Z"),
		VolumeType:       vol.VolumeType,
		Encrypted:        vol.Encrypted,
	})
}

func (p *Provider) handleDescribeVolumes(form url.Values) (*plugin.Response, error) {
	var ids []string
	for i := 1; ; i++ {
		id := form.Get(fmt.Sprintf("VolumeId.%d", i))
		if id == "" {
			break
		}
		ids = append(ids, id)
	}
	vols, err := p.store.DescribeVolumes(defaultAccountID, ids)
	if err != nil {
		return nil, err
	}

	type volumeItemXML struct {
		VolumeId         string `xml:"volumeId"`
		Size             int    `xml:"size"`
		AvailabilityZone string `xml:"availabilityZone"`
		Status           string `xml:"status"`
		CreateTime       string `xml:"createTime"`
		VolumeType       string `xml:"volumeType"`
		Encrypted        bool   `xml:"encrypted"`
		InstanceId       string `xml:"attachmentSet>item>instanceId,omitempty"`
		Device           string `xml:"attachmentSet>item>device,omitempty"`
	}
	type describeVolumesResponse struct {
		XMLName   xml.Name        `xml:"DescribeVolumesResponse"`
		VolumeSet []volumeItemXML `xml:"volumeSet>item"`
	}
	items := make([]volumeItemXML, 0, len(vols))
	for _, v := range vols {
		items = append(items, volumeItemXML{
			VolumeId:         v.VolumeID,
			Size:             v.Size,
			AvailabilityZone: v.AvailabilityZone,
			Status:           v.State,
			CreateTime:       v.CreatedAt.Format("2006-01-02T15:04:05.000Z"),
			VolumeType:       v.VolumeType,
			Encrypted:        v.Encrypted,
			InstanceId:       v.InstanceID,
			Device:           v.Device,
		})
	}
	return ec2XMLResponse(http.StatusOK, describeVolumesResponse{VolumeSet: items})
}

func (p *Provider) handleDeleteVolume(form url.Values) (*plugin.Response, error) {
	volumeID := form.Get("VolumeId")
	if volumeID == "" {
		return ec2XMLError("MissingParameter", "VolumeId is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteVolume(defaultAccountID, volumeID); err != nil {
		if errors.Is(err, ErrVolumeNotFound) {
			return ec2XMLError("InvalidVolume.NotFound", "volume not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	type deleteVolumeResponse struct {
		XMLName xml.Name `xml:"DeleteVolumeResponse"`
		Return  bool     `xml:"return"`
	}
	return ec2XMLResponse(http.StatusOK, deleteVolumeResponse{Return: true})
}

func (p *Provider) handleAttachVolume(form url.Values) (*plugin.Response, error) {
	volumeID := form.Get("VolumeId")
	instanceID := form.Get("InstanceId")
	device := form.Get("Device")
	if volumeID == "" || instanceID == "" || device == "" {
		return ec2XMLError("MissingParameter", "VolumeId, InstanceId, and Device are required", http.StatusBadRequest), nil
	}
	vol, err := p.store.AttachVolume(defaultAccountID, volumeID, instanceID, device)
	if err != nil {
		if errors.Is(err, ErrVolumeNotFound) {
			return ec2XMLError("InvalidVolume.NotFound", "volume not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	type attachVolumeResponse struct {
		XMLName    xml.Name `xml:"AttachVolumeResponse"`
		VolumeId   string   `xml:"volumeId"`
		InstanceId string   `xml:"instanceId"`
		Device     string   `xml:"device"`
		Status     string   `xml:"status"`
		AttachTime string   `xml:"attachTime"`
	}
	return ec2XMLResponse(http.StatusOK, attachVolumeResponse{
		VolumeId:   vol.VolumeID,
		InstanceId: vol.InstanceID,
		Device:     vol.Device,
		Status:     vol.State,
		AttachTime: vol.CreatedAt.Format("2006-01-02T15:04:05.000Z"),
	})
}

func (p *Provider) handleDetachVolume(form url.Values) (*plugin.Response, error) {
	volumeID := form.Get("VolumeId")
	if volumeID == "" {
		return ec2XMLError("MissingParameter", "VolumeId is required", http.StatusBadRequest), nil
	}
	vol, err := p.store.DetachVolume(defaultAccountID, volumeID)
	if err != nil {
		if errors.Is(err, ErrVolumeNotFound) {
			return ec2XMLError("InvalidVolume.NotFound", "volume not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	type detachVolumeResponse struct {
		XMLName  xml.Name `xml:"DetachVolumeResponse"`
		VolumeId string   `xml:"volumeId"`
		Status   string   `xml:"status"`
	}
	return ec2XMLResponse(http.StatusOK, detachVolumeResponse{
		VolumeId: vol.VolumeID,
		Status:   vol.State,
	})
}

func (p *Provider) handleModifyVolume(form url.Values) (*plugin.Response, error) {
	volumeID := form.Get("VolumeId")
	if volumeID == "" {
		return ec2XMLError("MissingParameter", "VolumeId is required", http.StatusBadRequest), nil
	}
	size := 0
	if s := form.Get("Size"); s != "" {
		size, _ = strconv.Atoi(s)
	}
	volumeType := form.Get("VolumeType")
	iops := 0
	if s := form.Get("Iops"); s != "" {
		iops, _ = strconv.Atoi(s)
	}
	vol, err := p.store.ModifyVolume(defaultAccountID, volumeID, size, volumeType, iops)
	if err != nil {
		if errors.Is(err, ErrVolumeNotFound) {
			return ec2XMLError("InvalidVolume.NotFound", "volume not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	type modifyVolumeResponse struct {
		XMLName            xml.Name `xml:"ModifyVolumeResponse"`
		VolumeModification struct {
			VolumeId   string `xml:"volumeId"`
			ModState   string `xml:"modificationState"`
			TargetSize int    `xml:"targetSize"`
		} `xml:"volumeModification"`
	}
	resp := modifyVolumeResponse{}
	resp.VolumeModification.VolumeId = vol.VolumeID
	resp.VolumeModification.ModState = "completed"
	resp.VolumeModification.TargetSize = vol.Size
	return ec2XMLResponse(http.StatusOK, resp)
}

// --- Snapshot handlers ---

func (p *Provider) handleCreateSnapshot(form url.Values) (*plugin.Response, error) {
	volumeID := form.Get("VolumeId")
	if volumeID == "" {
		return ec2XMLError("MissingParameter", "VolumeId is required", http.StatusBadRequest), nil
	}
	description := form.Get("Description")
	snap, err := p.store.CreateSnapshot(defaultAccountID, volumeID, description)
	if err != nil {
		return nil, err
	}
	type createSnapshotResponse struct {
		XMLName     xml.Name `xml:"CreateSnapshotResponse"`
		SnapshotId  string   `xml:"snapshotId"`
		VolumeId    string   `xml:"volumeId"`
		VolumeSize  int      `xml:"volumeSize"`
		Status      string   `xml:"status"`
		StartTime   string   `xml:"startTime"`
		Description string   `xml:"description"`
	}
	return ec2XMLResponse(http.StatusOK, createSnapshotResponse{
		SnapshotId:  snap.SnapshotID,
		VolumeId:    snap.VolumeID,
		VolumeSize:  snap.VolumeSize,
		Status:      snap.State,
		StartTime:   snap.CreatedAt.Format("2006-01-02T15:04:05.000Z"),
		Description: snap.Description,
	})
}

func (p *Provider) handleDescribeSnapshots(form url.Values) (*plugin.Response, error) {
	var ids []string
	for i := 1; ; i++ {
		id := form.Get(fmt.Sprintf("SnapshotId.%d", i))
		if id == "" {
			break
		}
		ids = append(ids, id)
	}
	snaps, err := p.store.DescribeSnapshots(defaultAccountID, ids)
	if err != nil {
		return nil, err
	}
	type snapshotItemXML struct {
		SnapshotId  string `xml:"snapshotId"`
		VolumeId    string `xml:"volumeId"`
		VolumeSize  int    `xml:"volumeSize"`
		Status      string `xml:"status"`
		StartTime   string `xml:"startTime"`
		Description string `xml:"description"`
	}
	type describeSnapshotsResponse struct {
		XMLName     xml.Name          `xml:"DescribeSnapshotsResponse"`
		SnapshotSet []snapshotItemXML `xml:"snapshotSet>item"`
	}
	items := make([]snapshotItemXML, 0, len(snaps))
	for _, snap := range snaps {
		items = append(items, snapshotItemXML{
			SnapshotId:  snap.SnapshotID,
			VolumeId:    snap.VolumeID,
			VolumeSize:  snap.VolumeSize,
			Status:      snap.State,
			StartTime:   snap.CreatedAt.Format("2006-01-02T15:04:05.000Z"),
			Description: snap.Description,
		})
	}
	return ec2XMLResponse(http.StatusOK, describeSnapshotsResponse{SnapshotSet: items})
}

func (p *Provider) handleDeleteSnapshot(form url.Values) (*plugin.Response, error) {
	snapshotID := form.Get("SnapshotId")
	if snapshotID == "" {
		return ec2XMLError("MissingParameter", "SnapshotId is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteSnapshot(defaultAccountID, snapshotID); err != nil {
		if errors.Is(err, ErrSnapshotNotFound) {
			return ec2XMLError("InvalidSnapshot.NotFound", "snapshot not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	type deleteSnapshotResponse struct {
		XMLName xml.Name `xml:"DeleteSnapshotResponse"`
		Return  bool     `xml:"return"`
	}
	return ec2XMLResponse(http.StatusOK, deleteSnapshotResponse{Return: true})
}

func (p *Provider) handleCopySnapshot(form url.Values) (*plugin.Response, error) {
	sourceSnapshotID := form.Get("SourceSnapshotId")
	if sourceSnapshotID == "" {
		return ec2XMLError("MissingParameter", "SourceSnapshotId is required", http.StatusBadRequest), nil
	}
	description := form.Get("Description")
	snap, err := p.store.CopySnapshot(defaultAccountID, sourceSnapshotID, description)
	if err != nil {
		return nil, err
	}
	type copySnapshotResponse struct {
		XMLName    xml.Name `xml:"CopySnapshotResponse"`
		SnapshotId string   `xml:"snapshotId"`
	}
	return ec2XMLResponse(http.StatusOK, copySnapshotResponse{SnapshotId: snap.SnapshotID})
}

// --- KeyPair handlers ---

func (p *Provider) handleCreateKeyPair(form url.Values) (*plugin.Response, error) {
	keyName := form.Get("KeyName")
	if keyName == "" {
		return ec2XMLError("MissingParameter", "KeyName is required", http.StatusBadRequest), nil
	}
	kp, err := p.store.CreateKeyPair(defaultAccountID, keyName)
	if err != nil {
		if errors.Is(err, ErrKeyPairAlreadyExists) {
			return ec2XMLError("InvalidKeyPair.Duplicate", fmt.Sprintf("key pair %q already exists", keyName), http.StatusBadRequest), nil
		}
		return nil, err
	}
	type createKeyPairResponse struct {
		XMLName        xml.Name `xml:"CreateKeyPairResponse"`
		KeyName        string   `xml:"keyName"`
		KeyFingerprint string   `xml:"keyFingerprint"`
		KeyMaterial    string   `xml:"keyMaterial"`
		KeyPairId      string   `xml:"keyPairId"`
	}
	return ec2XMLResponse(http.StatusOK, createKeyPairResponse{
		KeyName:        kp.KeyName,
		KeyFingerprint: kp.KeyFingerprint,
		KeyMaterial:    kp.KeyMaterial,
		KeyPairId:      kp.KeyPairID,
	})
}

func (p *Provider) handleDescribeKeyPairs(form url.Values) (*plugin.Response, error) {
	var names []string
	for i := 1; ; i++ {
		name := form.Get(fmt.Sprintf("KeyName.%d", i))
		if name == "" {
			break
		}
		names = append(names, name)
	}
	kps, err := p.store.DescribeKeyPairs(defaultAccountID, names)
	if err != nil {
		return nil, err
	}
	type keyPairItemXML struct {
		KeyName        string `xml:"keyName"`
		KeyFingerprint string `xml:"keyFingerprint"`
		KeyPairId      string `xml:"keyPairId"`
	}
	type describeKeyPairsResponse struct {
		XMLName xml.Name         `xml:"DescribeKeyPairsResponse"`
		KeySet  []keyPairItemXML `xml:"keySet>item"`
	}
	items := make([]keyPairItemXML, 0, len(kps))
	for _, kp := range kps {
		items = append(items, keyPairItemXML{
			KeyName:        kp.KeyName,
			KeyFingerprint: kp.KeyFingerprint,
			KeyPairId:      kp.KeyPairID,
		})
	}
	return ec2XMLResponse(http.StatusOK, describeKeyPairsResponse{KeySet: items})
}

func (p *Provider) handleDeleteKeyPair(form url.Values) (*plugin.Response, error) {
	keyName := form.Get("KeyName")
	if keyName == "" {
		return ec2XMLError("MissingParameter", "KeyName is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteKeyPair(defaultAccountID, keyName); err != nil {
		if errors.Is(err, ErrKeyPairNotFound) {
			return ec2XMLError("InvalidKeyPair.NotFound", "key pair not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	type deleteKeyPairResponse struct {
		XMLName xml.Name `xml:"DeleteKeyPairResponse"`
		Return  bool     `xml:"return"`
	}
	return ec2XMLResponse(http.StatusOK, deleteKeyPairResponse{Return: true})
}

func (p *Provider) handleImportKeyPair(form url.Values) (*plugin.Response, error) {
	keyName := form.Get("KeyName")
	publicKeyMaterial := form.Get("PublicKeyMaterial")
	if keyName == "" || publicKeyMaterial == "" {
		return ec2XMLError("MissingParameter", "KeyName and PublicKeyMaterial are required", http.StatusBadRequest), nil
	}
	kp, err := p.store.ImportKeyPair(defaultAccountID, keyName, publicKeyMaterial)
	if err != nil {
		if errors.Is(err, ErrKeyPairAlreadyExists) {
			return ec2XMLError("InvalidKeyPair.Duplicate", fmt.Sprintf("key pair %q already exists", keyName), http.StatusBadRequest), nil
		}
		return nil, err
	}
	type importKeyPairResponse struct {
		XMLName        xml.Name `xml:"ImportKeyPairResponse"`
		KeyName        string   `xml:"keyName"`
		KeyFingerprint string   `xml:"keyFingerprint"`
		KeyPairId      string   `xml:"keyPairId"`
	}
	return ec2XMLResponse(http.StatusOK, importKeyPairResponse{
		KeyName:        kp.KeyName,
		KeyFingerprint: kp.KeyFingerprint,
		KeyPairId:      kp.KeyPairID,
	})
}

// --- Image/AMI handlers ---

func (p *Provider) handleCreateImage(form url.Values) (*plugin.Response, error) {
	instanceID := form.Get("InstanceId")
	name := form.Get("Name")
	if instanceID == "" || name == "" {
		return ec2XMLError("MissingParameter", "InstanceId and Name are required", http.StatusBadRequest), nil
	}
	description := form.Get("Description")
	img, err := p.store.CreateImage(defaultAccountID, instanceID, name, description)
	if err != nil {
		return nil, err
	}
	type createImageResponse struct {
		XMLName xml.Name `xml:"CreateImageResponse"`
		ImageId string   `xml:"imageId"`
	}
	return ec2XMLResponse(http.StatusOK, createImageResponse{ImageId: img.ImageID})
}

func (p *Provider) handleDescribeImages(form url.Values) (*plugin.Response, error) {
	var ids []string
	for i := 1; ; i++ {
		id := form.Get(fmt.Sprintf("ImageId.%d", i))
		if id == "" {
			break
		}
		ids = append(ids, id)
	}
	imgs, err := p.store.DescribeImages(defaultAccountID, ids)
	if err != nil {
		return nil, err
	}
	type imageItemXML struct {
		ImageId            string `xml:"imageId"`
		Name               string `xml:"name"`
		Description        string `xml:"description"`
		ImageType          string `xml:"imageType"`
		ImageState         string `xml:"imageState"`
		Architecture       string `xml:"architecture"`
		RootDeviceName     string `xml:"rootDeviceName"`
		RootDeviceType     string `xml:"rootDeviceType"`
		VirtualizationType string `xml:"virtualizationType"`
	}
	type describeImagesResponse struct {
		XMLName   xml.Name       `xml:"DescribeImagesResponse"`
		ImagesSet []imageItemXML `xml:"imagesSet>item"`
	}
	items := make([]imageItemXML, 0, len(imgs))
	for _, img := range imgs {
		items = append(items, imageItemXML{
			ImageId:            img.ImageID,
			Name:               img.Name,
			Description:        img.Description,
			ImageType:          img.ImageType,
			ImageState:         img.State,
			Architecture:       img.Architecture,
			RootDeviceName:     img.RootDeviceName,
			RootDeviceType:     img.RootDeviceType,
			VirtualizationType: img.VirtualizationType,
		})
	}
	return ec2XMLResponse(http.StatusOK, describeImagesResponse{ImagesSet: items})
}

func (p *Provider) handleDeregisterImage(form url.Values) (*plugin.Response, error) {
	imageID := form.Get("ImageId")
	if imageID == "" {
		return ec2XMLError("MissingParameter", "ImageId is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeregisterImage(defaultAccountID, imageID); err != nil {
		if errors.Is(err, ErrImageNotFound) {
			return ec2XMLError("InvalidAMIID.NotFound", "image not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	type deregisterImageResponse struct {
		XMLName xml.Name `xml:"DeregisterImageResponse"`
		Return  bool     `xml:"return"`
	}
	return ec2XMLResponse(http.StatusOK, deregisterImageResponse{Return: true})
}

func (p *Provider) handleRegisterImage(form url.Values) (*plugin.Response, error) {
	name := form.Get("Name")
	if name == "" {
		return ec2XMLError("MissingParameter", "Name is required", http.StatusBadRequest), nil
	}
	architecture := form.Get("Architecture")
	rootDeviceName := form.Get("RootDeviceName")
	img, err := p.store.RegisterImage(defaultAccountID, name, architecture, rootDeviceName)
	if err != nil {
		return nil, err
	}
	type registerImageResponse struct {
		XMLName xml.Name `xml:"RegisterImageResponse"`
		ImageId string   `xml:"imageId"`
	}
	return ec2XMLResponse(http.StatusOK, registerImageResponse{ImageId: img.ImageID})
}

func (p *Provider) handleCopyImage(form url.Values) (*plugin.Response, error) {
	sourceImageID := form.Get("SourceImageId")
	name := form.Get("Name")
	if sourceImageID == "" || name == "" {
		return ec2XMLError("MissingParameter", "SourceImageId and Name are required", http.StatusBadRequest), nil
	}
	description := form.Get("Description")
	img, err := p.store.CopyImage(defaultAccountID, sourceImageID, name, description)
	if err != nil {
		return nil, err
	}
	type copyImageResponse struct {
		XMLName xml.Name `xml:"CopyImageResponse"`
		ImageId string   `xml:"imageId"`
	}
	return ec2XMLResponse(http.StatusOK, copyImageResponse{ImageId: img.ImageID})
}

// --- RouteTable handlers ---

func (p *Provider) handleCreateRouteTable(form url.Values) (*plugin.Response, error) {
	vpcID := form.Get("VpcId")
	if vpcID == "" {
		return ec2XMLError("MissingParameter", "VpcId is required", http.StatusBadRequest), nil
	}
	rt, err := p.store.CreateRouteTable(defaultAccountID, vpcID)
	if err != nil {
		return nil, err
	}
	type routeTableXML struct {
		RouteTableId string `xml:"routeTableId"`
		VpcId        string `xml:"vpcId"`
	}
	type createRouteTableResponse struct {
		XMLName    xml.Name      `xml:"CreateRouteTableResponse"`
		RouteTable routeTableXML `xml:"routeTable"`
	}
	return ec2XMLResponse(http.StatusOK, createRouteTableResponse{
		RouteTable: routeTableXML{RouteTableId: rt.RouteTableID, VpcId: rt.VpcID},
	})
}

func (p *Provider) handleDescribeRouteTables(form url.Values) (*plugin.Response, error) {
	var ids []string
	for i := 1; ; i++ {
		id := form.Get(fmt.Sprintf("RouteTableId.%d", i))
		if id == "" {
			break
		}
		ids = append(ids, id)
	}
	rts, err := p.store.DescribeRouteTables(defaultAccountID, ids)
	if err != nil {
		return nil, err
	}
	type routeItemXML struct {
		DestinationCidrBlock string `xml:"destinationCidrBlock"`
		GatewayId            string `xml:"gatewayId,omitempty"`
		InstanceId           string `xml:"instanceId,omitempty"`
		State                string `xml:"state"`
	}
	type routeTableItemXML struct {
		RouteTableId string         `xml:"routeTableId"`
		VpcId        string         `xml:"vpcId"`
		RouteSet     []routeItemXML `xml:"routeSet>item,omitempty"`
	}
	type describeRouteTablesResponse struct {
		XMLName       xml.Name            `xml:"DescribeRouteTablesResponse"`
		RouteTableSet []routeTableItemXML `xml:"routeTableSet>item"`
	}
	items := make([]routeTableItemXML, 0, len(rts))
	for _, rt := range rts {
		item := routeTableItemXML{RouteTableId: rt.RouteTableID, VpcId: rt.VpcID}
		for _, r := range rt.Routes {
			item.RouteSet = append(item.RouteSet, routeItemXML{
				DestinationCidrBlock: r.DestinationCIDR,
				GatewayId:            r.GatewayID,
				InstanceId:           r.InstanceID,
				State:                r.State,
			})
		}
		items = append(items, item)
	}
	return ec2XMLResponse(http.StatusOK, describeRouteTablesResponse{RouteTableSet: items})
}

func (p *Provider) handleDeleteRouteTable(form url.Values) (*plugin.Response, error) {
	rtID := form.Get("RouteTableId")
	if rtID == "" {
		return ec2XMLError("MissingParameter", "RouteTableId is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteRouteTable(defaultAccountID, rtID); err != nil {
		if errors.Is(err, ErrRouteTableNotFound) {
			return ec2XMLError("InvalidRouteTableID.NotFound", "route table not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	type deleteRouteTableResponse struct {
		XMLName xml.Name `xml:"DeleteRouteTableResponse"`
		Return  bool     `xml:"return"`
	}
	return ec2XMLResponse(http.StatusOK, deleteRouteTableResponse{Return: true})
}

func (p *Provider) handleCreateRoute(form url.Values) (*plugin.Response, error) {
	rtID := form.Get("RouteTableId")
	destCIDR := form.Get("DestinationCidrBlock")
	if rtID == "" || destCIDR == "" {
		return ec2XMLError("MissingParameter", "RouteTableId and DestinationCidrBlock are required", http.StatusBadRequest), nil
	}
	gatewayID := form.Get("GatewayId")
	instanceID := form.Get("InstanceId")
	if err := p.store.CreateRoute(defaultAccountID, rtID, destCIDR, gatewayID, instanceID); err != nil {
		return nil, err
	}
	type createRouteResponse struct {
		XMLName xml.Name `xml:"CreateRouteResponse"`
		Return  bool     `xml:"return"`
	}
	return ec2XMLResponse(http.StatusOK, createRouteResponse{Return: true})
}

func (p *Provider) handleDeleteRoute(form url.Values) (*plugin.Response, error) {
	rtID := form.Get("RouteTableId")
	destCIDR := form.Get("DestinationCidrBlock")
	if rtID == "" || destCIDR == "" {
		return ec2XMLError("MissingParameter", "RouteTableId and DestinationCidrBlock are required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteRoute(defaultAccountID, rtID, destCIDR); err != nil {
		return nil, err
	}
	type deleteRouteResponse struct {
		XMLName xml.Name `xml:"DeleteRouteResponse"`
		Return  bool     `xml:"return"`
	}
	return ec2XMLResponse(http.StatusOK, deleteRouteResponse{Return: true})
}

func (p *Provider) handleAssociateRouteTable(form url.Values) (*plugin.Response, error) {
	rtID := form.Get("RouteTableId")
	subnetID := form.Get("SubnetId")
	if rtID == "" || subnetID == "" {
		return ec2XMLError("MissingParameter", "RouteTableId and SubnetId are required", http.StatusBadRequest), nil
	}
	assocID, err := p.store.AssociateRouteTable(defaultAccountID, rtID, subnetID)
	if err != nil {
		return nil, err
	}
	type associateRouteTableResponse struct {
		XMLName       xml.Name `xml:"AssociateRouteTableResponse"`
		AssociationId string   `xml:"associationId"`
	}
	return ec2XMLResponse(http.StatusOK, associateRouteTableResponse{AssociationId: assocID})
}

// --- InternetGateway handlers ---

func (p *Provider) handleCreateInternetGateway(_ url.Values) (*plugin.Response, error) {
	igw, err := p.store.CreateInternetGateway(defaultAccountID)
	if err != nil {
		return nil, err
	}
	type igwXML struct {
		InternetGatewayId string `xml:"internetGatewayId"`
		State             string `xml:"state"`
	}
	type createInternetGatewayResponse struct {
		XMLName         xml.Name `xml:"CreateInternetGatewayResponse"`
		InternetGateway igwXML   `xml:"internetGateway"`
	}
	return ec2XMLResponse(http.StatusOK, createInternetGatewayResponse{
		InternetGateway: igwXML{InternetGatewayId: igw.InternetGatewayID, State: igw.State},
	})
}

func (p *Provider) handleDescribeInternetGateways(form url.Values) (*plugin.Response, error) {
	var ids []string
	for i := 1; ; i++ {
		id := form.Get(fmt.Sprintf("InternetGatewayId.%d", i))
		if id == "" {
			break
		}
		ids = append(ids, id)
	}
	igws, err := p.store.DescribeInternetGateways(defaultAccountID, ids)
	if err != nil {
		return nil, err
	}
	type attachmentXML struct {
		VpcId string `xml:"vpcId"`
		State string `xml:"state"`
	}
	type igwItemXML struct {
		InternetGatewayId string          `xml:"internetGatewayId"`
		AttachmentSet     []attachmentXML `xml:"attachmentSet>item,omitempty"`
	}
	type describeInternetGatewaysResponse struct {
		XMLName            xml.Name     `xml:"DescribeInternetGatewaysResponse"`
		InternetGatewaySet []igwItemXML `xml:"internetGatewaySet>item"`
	}
	items := make([]igwItemXML, 0, len(igws))
	for _, igw := range igws {
		item := igwItemXML{InternetGatewayId: igw.InternetGatewayID}
		if igw.VpcID != "" {
			item.AttachmentSet = []attachmentXML{{VpcId: igw.VpcID, State: "available"}}
		}
		items = append(items, item)
	}
	return ec2XMLResponse(http.StatusOK, describeInternetGatewaysResponse{InternetGatewaySet: items})
}

func (p *Provider) handleAttachInternetGateway(form url.Values) (*plugin.Response, error) {
	igwID := form.Get("InternetGatewayId")
	vpcID := form.Get("VpcId")
	if igwID == "" || vpcID == "" {
		return ec2XMLError("MissingParameter", "InternetGatewayId and VpcId are required", http.StatusBadRequest), nil
	}
	if err := p.store.AttachInternetGateway(defaultAccountID, igwID, vpcID); err != nil {
		if errors.Is(err, ErrInternetGatewayNotFound) {
			return ec2XMLError("InvalidInternetGatewayID.NotFound", "internet gateway not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	type attachInternetGatewayResponse struct {
		XMLName xml.Name `xml:"AttachInternetGatewayResponse"`
		Return  bool     `xml:"return"`
	}
	return ec2XMLResponse(http.StatusOK, attachInternetGatewayResponse{Return: true})
}

func (p *Provider) handleDetachInternetGateway(form url.Values) (*plugin.Response, error) {
	igwID := form.Get("InternetGatewayId")
	vpcID := form.Get("VpcId")
	if igwID == "" || vpcID == "" {
		return ec2XMLError("MissingParameter", "InternetGatewayId and VpcId are required", http.StatusBadRequest), nil
	}
	if err := p.store.DetachInternetGateway(defaultAccountID, igwID, vpcID); err != nil {
		if errors.Is(err, ErrInternetGatewayNotFound) {
			return ec2XMLError("InvalidInternetGatewayID.NotFound", "internet gateway not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	type detachInternetGatewayResponse struct {
		XMLName xml.Name `xml:"DetachInternetGatewayResponse"`
		Return  bool     `xml:"return"`
	}
	return ec2XMLResponse(http.StatusOK, detachInternetGatewayResponse{Return: true})
}

// --- NetworkACL handlers ---

func (p *Provider) handleCreateNetworkACL(form url.Values) (*plugin.Response, error) {
	vpcID := form.Get("VpcId")
	if vpcID == "" {
		return ec2XMLError("MissingParameter", "VpcId is required", http.StatusBadRequest), nil
	}
	acl, err := p.store.CreateNetworkACL(defaultAccountID, vpcID)
	if err != nil {
		return nil, err
	}
	type networkAclXML struct {
		NetworkAclId string `xml:"networkAclId"`
		VpcId        string `xml:"vpcId"`
		IsDefault    bool   `xml:"isDefault"`
	}
	type createNetworkAclResponse struct {
		XMLName    xml.Name      `xml:"CreateNetworkAclResponse"`
		NetworkAcl networkAclXML `xml:"networkAcl"`
	}
	return ec2XMLResponse(http.StatusOK, createNetworkAclResponse{
		NetworkAcl: networkAclXML{NetworkAclId: acl.NetworkACLID, VpcId: acl.VpcID, IsDefault: acl.IsDefault},
	})
}

func (p *Provider) handleDescribeNetworkACLs(form url.Values) (*plugin.Response, error) {
	var ids []string
	for i := 1; ; i++ {
		id := form.Get(fmt.Sprintf("NetworkAclId.%d", i))
		if id == "" {
			break
		}
		ids = append(ids, id)
	}
	acls, err := p.store.DescribeNetworkACLs(defaultAccountID, ids)
	if err != nil {
		return nil, err
	}
	type portRangeXML struct {
		From int `xml:"from"`
		To   int `xml:"to"`
	}
	type entryItemXML struct {
		RuleNumber int          `xml:"ruleNumber"`
		Protocol   string       `xml:"protocol"`
		RuleAction string       `xml:"ruleAction"`
		Egress     bool         `xml:"egress"`
		CidrBlock  string       `xml:"cidrBlock"`
		PortRange  portRangeXML `xml:"portRange,omitempty"`
	}
	type networkAclItemXML struct {
		NetworkAclId string         `xml:"networkAclId"`
		VpcId        string         `xml:"vpcId"`
		IsDefault    bool           `xml:"isDefault"`
		EntrySet     []entryItemXML `xml:"entrySet>item,omitempty"`
	}
	type describeNetworkAclsResponse struct {
		XMLName       xml.Name            `xml:"DescribeNetworkAclsResponse"`
		NetworkAclSet []networkAclItemXML `xml:"networkAclSet>item"`
	}
	items := make([]networkAclItemXML, 0, len(acls))
	for _, acl := range acls {
		item := networkAclItemXML{NetworkAclId: acl.NetworkACLID, VpcId: acl.VpcID, IsDefault: acl.IsDefault}
		for _, e := range acl.Entries {
			item.EntrySet = append(item.EntrySet, entryItemXML{
				RuleNumber: e.RuleNumber,
				Protocol:   e.Protocol,
				RuleAction: e.RuleAction,
				Egress:     e.Egress,
				CidrBlock:  e.CidrBlock,
				PortRange:  portRangeXML{From: e.PortFrom, To: e.PortTo},
			})
		}
		items = append(items, item)
	}
	return ec2XMLResponse(http.StatusOK, describeNetworkAclsResponse{NetworkAclSet: items})
}

func (p *Provider) handleDeleteNetworkACL(form url.Values) (*plugin.Response, error) {
	aclID := form.Get("NetworkAclId")
	if aclID == "" {
		return ec2XMLError("MissingParameter", "NetworkAclId is required", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteNetworkACL(defaultAccountID, aclID); err != nil {
		if errors.Is(err, ErrNetworkACLNotFound) {
			return ec2XMLError("InvalidNetworkAclID.NotFound", "network acl not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	type deleteNetworkAclResponse struct {
		XMLName xml.Name `xml:"DeleteNetworkAclResponse"`
		Return  bool     `xml:"return"`
	}
	return ec2XMLResponse(http.StatusOK, deleteNetworkAclResponse{Return: true})
}

func (p *Provider) handleCreateNetworkACLEntry(form url.Values) (*plugin.Response, error) {
	aclID := form.Get("NetworkAclId")
	if aclID == "" {
		return ec2XMLError("MissingParameter", "NetworkAclId is required", http.StatusBadRequest), nil
	}
	ruleNumber, _ := strconv.Atoi(form.Get("RuleNumber"))
	protocol := form.Get("Protocol")
	ruleAction := form.Get("RuleAction")
	cidrBlock := form.Get("CidrBlock")
	egress := form.Get("Egress") == "true" || form.Get("Egress") == "1"
	portFrom, _ := strconv.Atoi(form.Get("PortRange.From"))
	portTo, _ := strconv.Atoi(form.Get("PortRange.To"))

	if err := p.store.CreateNetworkACLEntry(defaultAccountID, aclID, ruleNumber, protocol, ruleAction, cidrBlock, egress, portFrom, portTo); err != nil {
		return nil, err
	}
	type createNetworkAclEntryResponse struct {
		XMLName xml.Name `xml:"CreateNetworkAclEntryResponse"`
		Return  bool     `xml:"return"`
	}
	return ec2XMLResponse(http.StatusOK, createNetworkAclEntryResponse{Return: true})
}

func (p *Provider) handleReplaceNetworkACLEntry(form url.Values) (*plugin.Response, error) {
	aclID := form.Get("NetworkAclId")
	if aclID == "" {
		return ec2XMLError("MissingParameter", "NetworkAclId is required", http.StatusBadRequest), nil
	}
	ruleNumber, _ := strconv.Atoi(form.Get("RuleNumber"))
	protocol := form.Get("Protocol")
	ruleAction := form.Get("RuleAction")
	cidrBlock := form.Get("CidrBlock")
	egress := form.Get("Egress") == "true" || form.Get("Egress") == "1"
	portFrom, _ := strconv.Atoi(form.Get("PortRange.From"))
	portTo, _ := strconv.Atoi(form.Get("PortRange.To"))

	if err := p.store.ReplaceNetworkACLEntry(defaultAccountID, aclID, ruleNumber, protocol, ruleAction, cidrBlock, egress, portFrom, portTo); err != nil {
		return nil, err
	}
	type replaceNetworkAclEntryResponse struct {
		XMLName xml.Name `xml:"ReplaceNetworkAclEntryResponse"`
		Return  bool     `xml:"return"`
	}
	return ec2XMLResponse(http.StatusOK, replaceNetworkAclEntryResponse{Return: true})
}

// --- VPC Attribute handlers ---

func (p *Provider) handleModifyVpcAttribute(form url.Values) (*plugin.Response, error) {
	vpcID := form.Get("VpcId")
	if vpcID == "" {
		return ec2XMLError("MissingParameter", "VpcId is required", http.StatusBadRequest), nil
	}
	var enableDnsSupport, enableDnsHostnames *bool
	if v := form.Get("EnableDnsSupport.Value"); v != "" {
		val := v == "true" || v == "1"
		enableDnsSupport = &val
	}
	if v := form.Get("EnableDnsHostnames.Value"); v != "" {
		val := v == "true" || v == "1"
		enableDnsHostnames = &val
	}
	if err := p.store.ModifyVpcAttribute(defaultAccountID, vpcID, enableDnsSupport, enableDnsHostnames); err != nil {
		if errors.Is(err, ErrVpcNotFound) {
			return ec2XMLError("InvalidVpcID.NotFound", "vpc not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	type modifyVpcAttributeResponse struct {
		XMLName xml.Name `xml:"ModifyVpcAttributeResponse"`
		Return  bool     `xml:"return"`
	}
	return ec2XMLResponse(http.StatusOK, modifyVpcAttributeResponse{Return: true})
}

func (p *Provider) handleDescribeVpcAttribute(form url.Values) (*plugin.Response, error) {
	vpcID := form.Get("VpcId")
	attribute := form.Get("Attribute")
	if vpcID == "" || attribute == "" {
		return ec2XMLError("MissingParameter", "VpcId and Attribute are required", http.StatusBadRequest), nil
	}
	val, err := p.store.DescribeVpcAttribute(defaultAccountID, vpcID, attribute)
	if err != nil {
		if errors.Is(err, ErrVpcNotFound) {
			return ec2XMLError("InvalidVpcID.NotFound", "vpc not found", http.StatusBadRequest), nil
		}
		return nil, err
	}
	type boolValueXML struct {
		Value bool `xml:"value"`
	}
	type describeVpcAttributeResponse struct {
		XMLName            xml.Name      `xml:"DescribeVpcAttributeResponse"`
		VpcId              string        `xml:"vpcId"`
		EnableDnsSupport   *boolValueXML `xml:"enableDnsSupport,omitempty"`
		EnableDnsHostnames *boolValueXML `xml:"enableDnsHostnames,omitempty"`
	}
	resp := describeVpcAttributeResponse{VpcId: vpcID}
	switch attribute {
	case "enableDnsSupport":
		resp.EnableDnsSupport = &boolValueXML{Value: val}
	case "enableDnsHostnames":
		resp.EnableDnsHostnames = &boolValueXML{Value: val}
	}
	return ec2XMLResponse(http.StatusOK, resp)
}

// --- helpers ---

func ec2XMLError(code, message string, status int) *plugin.Response {
	type errXML struct {
		XMLName xml.Name `xml:"Response"`
		Errors  struct {
			Error struct {
				Code    string `xml:"Code"`
				Message string `xml:"Message"`
			} `xml:"Error"`
		} `xml:"Errors"`
	}
	e := errXML{}
	e.Errors.Error.Code = code
	e.Errors.Error.Message = message
	body, _ := xml.Marshal(e)
	return &plugin.Response{StatusCode: status, ContentType: "text/xml", Body: body}
}

func ec2XMLResponse(status int, v any) (*plugin.Response, error) {
	body, err := xml.Marshal(v)
	if err != nil {
		return nil, err
	}
	return &plugin.Response{StatusCode: status, ContentType: "text/xml", Body: body}, nil
}

// Ensure unused import is not flagged (strings used in DescribeInstances filter).
var _ = strings.Contains
