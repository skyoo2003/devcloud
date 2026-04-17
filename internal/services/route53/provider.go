// SPDX-License-Identifier: Apache-2.0

// internal/services/route53/provider.go
package route53

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	generated "github.com/skyoo2003/devcloud/internal/generated/route53"
	"github.com/skyoo2003/devcloud/internal/plugin"
)

const defaultAccountID = plugin.DefaultAccountID
const defaultRegion = "us-east-1"

// Provider implements Route53 using REST-XML routing.
type Provider struct {
	store *Route53Store
}

func (p *Provider) ServiceID() string             { return "route53" }
func (p *Provider) ServiceName() string           { return "DnsV20130401" }
func (p *Provider) Protocol() plugin.ProtocolType { return plugin.ProtocolRESTXML }

func (p *Provider) Init(cfg plugin.PluginConfig) error {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = "."
	}
	var err error
	p.store, err = NewRoute53Store(filepath.Join(dataDir, "route53"))
	return err
}

func (p *Provider) Shutdown(_ context.Context) error {
	if p.store != nil {
		return p.store.Close()
	}
	return nil
}

func (p *Provider) HandleRequest(_ context.Context, op string, req *http.Request) (*plugin.Response, error) {
	// op comes from generated.MatchOperation (already resolved by the router).
	// If the router passes an empty op, resolve it ourselves.
	if op == "" {
		var params generated.PathParams
		op, params = generated.MatchOperation(req.Method, req.URL.Path)
		_ = params
	}
	switch op {
	case "CreateHostedZone":
		return p.createHostedZone(req)
	case "GetHostedZone":
		return p.getHostedZone(req)
	case "ListHostedZones":
		return p.listHostedZones(req)
	case "DeleteHostedZone":
		return p.deleteHostedZone(req)
	case "ChangeResourceRecordSets":
		return p.changeResourceRecordSets(req)
	case "ListResourceRecordSets":
		return p.listResourceRecordSets(req)
	case "ChangeTagsForResource":
		return p.changeTagsForResource(req)
	case "ListTagsForResource":
		return p.listTagsForResource(req)
	// Health checks
	case "CreateHealthCheck":
		return p.createHealthCheck(req)
	case "GetHealthCheck":
		return p.getHealthCheck(req)
	case "ListHealthChecks":
		return p.listHealthChecks(req)
	case "UpdateHealthCheck":
		return p.updateHealthCheck(req)
	case "DeleteHealthCheck":
		return p.deleteHealthCheck(req)
	case "GetHealthCheckStatus":
		return p.getHealthCheckStatus(req)
	// Traffic policies
	case "CreateTrafficPolicy":
		return p.createTrafficPolicy(req)
	case "GetTrafficPolicy":
		return p.getTrafficPolicy(req)
	case "ListTrafficPolicies":
		return p.listTrafficPolicies(req)
	case "DeleteTrafficPolicy":
		return p.deleteTrafficPolicy(req)
	case "CreateTrafficPolicyInstance":
		return p.createTrafficPolicyInstance(req)
	// Query logging
	case "CreateQueryLoggingConfig":
		return p.createQueryLoggingConfig(req)
	case "GetQueryLoggingConfig":
		return p.getQueryLoggingConfig(req)
	case "ListQueryLoggingConfigs":
		return p.listQueryLoggingConfigs(req)
	case "DeleteQueryLoggingConfig":
		return p.deleteQueryLoggingConfig(req)
	// DNSSEC
	case "GetDNSSEC":
		return p.getDNSSEC(req)
	case "EnableHostedZoneDNSSEC":
		return p.enableDNSSEC(req)
	case "DisableHostedZoneDNSSEC":
		return p.disableDNSSEC(req)
	case "CreateKeySigningKey":
		return p.createKeySigningKey(req)
	case "DeleteKeySigningKey":
		return p.deleteKeySigningKey(req)
	case "ListKeySigningKeys":
		return p.listKeySigningKeys(req)
	case "ActivateKeySigningKey":
		return p.activateKeySigningKey(req)
	case "DeactivateKeySigningKey":
		return p.deactivateKeySigningKey(req)
	// Traffic Policy Instance
	case "GetTrafficPolicyInstance":
		return p.getTrafficPolicyInstance(req)
	case "UpdateTrafficPolicyInstance":
		return p.updateTrafficPolicyInstance(req)
	case "DeleteTrafficPolicyInstance":
		return p.deleteTrafficPolicyInstance(req)
	case "ListTrafficPolicyInstances":
		return p.listTrafficPolicyInstances(req)
	case "ListTrafficPolicyInstancesByHostedZone":
		return p.listTrafficPolicyInstancesByHostedZone(req)
	case "ListTrafficPolicyInstancesByPolicy":
		return p.listTrafficPolicyInstancesByPolicy(req)
	// Cidr Collection
	case "CreateCidrCollection":
		return p.createCidrCollection(req)
	case "ChangeCidrCollection":
		return p.changeCidrCollection(req)
	case "ListCidrCollections":
		return p.listCidrCollections(req)
	case "ListCidrBlocks":
		return p.listCidrBlocks(req)
	case "ListCidrLocations":
		return p.listCidrLocations(req)
	case "DeleteCidrCollection":
		return p.deleteCidrCollection(req)
	// Reusable Delegation Set
	case "CreateReusableDelegationSet":
		return p.createReusableDelegationSet(req)
	case "GetReusableDelegationSet":
		return p.getReusableDelegationSet(req)
	case "ListReusableDelegationSets":
		return p.listReusableDelegationSets(req)
	case "DeleteReusableDelegationSet":
		return p.deleteReusableDelegationSet(req)
	// VPC association
	case "AssociateVPCWithHostedZone":
		return p.associateVPC(req)
	case "DisassociateVPCFromHostedZone":
		return p.disassociateVPC(req)
	default:
		return r53Error("InvalidAction", fmt.Sprintf("operation not implemented: %s", op), http.StatusBadRequest), nil
	}
}

func (p *Provider) ListResources(_ context.Context) ([]plugin.Resource, error) {
	zones, err := p.store.ListZones(defaultAccountID)
	if err != nil {
		return nil, err
	}
	out := make([]plugin.Resource, 0, len(zones))
	for _, z := range zones {
		out = append(out, plugin.Resource{Type: "hosted-zone", ID: z.ID, Name: z.Name})
	}
	return out, nil
}

func (p *Provider) GetMetrics(_ context.Context) (*plugin.ServiceMetrics, error) {
	return &plugin.ServiceMetrics{}, nil
}

// --- helpers ---

func zoneIDFromPath(path string) string {
	// path: /2013-04-01/hostedzone/{Id}[/...]
	parts := strings.Split(strings.Trim(path, "/"), "/")
	for i, p := range parts {
		if p == "hostedzone" && i+1 < len(parts) {
			return parts[i+1]
		}
	}
	return ""
}

func generateZoneID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return "Z" + strings.ToUpper(hex.EncodeToString(b))
}

func generateID() string {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return strings.ToLower(hex.EncodeToString(b))
}

func changeInfoXML(id string) struct {
	Id          string `xml:"Id"`
	Status      string `xml:"Status"`
	SubmittedAt string `xml:"SubmittedAt"`
} {
	return struct {
		Id          string `xml:"Id"`
		Status      string `xml:"Status"`
		SubmittedAt string `xml:"SubmittedAt"`
	}{
		Id:          "/change/" + id,
		Status:      "INSYNC",
		SubmittedAt: time.Now().UTC().Format(time.RFC3339),
	}
}

func r53Error(code, msg string, status int) *plugin.Response {
	type errResp struct {
		XMLName xml.Name `xml:"ErrorResponse"`
		Error   struct {
			Type    string `xml:"Type"`
			Code    string `xml:"Code"`
			Message string `xml:"Message"`
		} `xml:"Error"`
	}
	body, _ := xml.Marshal(errResp{
		Error: struct {
			Type    string `xml:"Type"`
			Code    string `xml:"Code"`
			Message string `xml:"Message"`
		}{Type: "Sender", Code: code, Message: msg},
	})
	return &plugin.Response{
		StatusCode:  status,
		Body:        append([]byte(xml.Header), body...),
		ContentType: "application/xml",
	}
}

func xmlResp(status int, v any) (*plugin.Response, error) {
	body, err := xml.Marshal(v)
	if err != nil {
		return nil, err
	}
	return &plugin.Response{
		StatusCode:  status,
		Body:        append([]byte(xml.Header), body...),
		ContentType: "application/xml",
	}, nil
}

// --- operation handlers ---

func (p *Provider) createHostedZone(req *http.Request) (*plugin.Response, error) {
	type createInput struct {
		XMLName          xml.Name `xml:"CreateHostedZoneRequest"`
		Name             string   `xml:"Name"`
		CallerReference  string   `xml:"CallerReference"`
		HostedZoneConfig struct {
			Comment     string `xml:"Comment"`
			PrivateZone bool   `xml:"PrivateZone"`
		} `xml:"HostedZoneConfig"`
	}
	var in createInput
	if err := xml.NewDecoder(req.Body).Decode(&in); err != nil {
		return r53Error("MalformedInput", "failed to parse request body", http.StatusBadRequest), nil
	}
	if in.Name == "" {
		return r53Error("InvalidDomainName", "Name is required", http.StatusBadRequest), nil
	}
	name := in.Name
	if !strings.HasSuffix(name, ".") {
		name += "."
	}
	id := generateZoneID()
	zone := &HostedZone{
		ID:        id,
		Name:      name,
		AccountID: defaultAccountID,
		CallerRef: in.CallerReference,
		Comment:   in.HostedZoneConfig.Comment,
		CreatedAt: time.Now(),
	}
	if err := p.store.CreateZone(zone); err != nil {
		return nil, err
	}

	type xmlZone struct {
		XMLName                xml.Name `xml:"HostedZone"`
		Id                     string   `xml:"Id"`
		Name                   string   `xml:"Name"`
		CallerReference        string   `xml:"CallerReference"`
		ResourceRecordSetCount int      `xml:"ResourceRecordSetCount"`
		Config                 struct {
			Comment     string `xml:"Comment"`
			PrivateZone bool   `xml:"PrivateZone"`
		} `xml:"Config"`
	}
	type changeInfo struct {
		Id          string `xml:"Id"`
		Status      string `xml:"Status"`
		SubmittedAt string `xml:"SubmittedAt"`
	}
	type delegationSet struct {
		NameServers struct {
			NameServer []string `xml:"NameServer"`
		} `xml:"NameServers"`
	}
	type response struct {
		XMLName       xml.Name      `xml:"CreateHostedZoneResponse"`
		HostedZone    xmlZone       `xml:"HostedZone"`
		ChangeInfo    changeInfo    `xml:"ChangeInfo"`
		DelegationSet delegationSet `xml:"DelegationSet"`
	}

	resp := response{}
	resp.HostedZone.Id = "/hostedzone/" + id
	resp.HostedZone.Name = name
	resp.HostedZone.CallerReference = in.CallerReference
	resp.HostedZone.Config.Comment = in.HostedZoneConfig.Comment
	resp.ChangeInfo = changeInfo{
		Id:          "/change/C" + id,
		Status:      "INSYNC",
		SubmittedAt: time.Now().UTC().Format(time.RFC3339),
	}
	resp.DelegationSet.NameServers.NameServer = []string{
		"ns-1.devcloud.internal", "ns-2.devcloud.internal",
	}
	return xmlResp(http.StatusCreated, resp)
}

func (p *Provider) getHostedZone(req *http.Request) (*plugin.Response, error) {
	id := zoneIDFromPath(req.URL.Path)
	if id == "" {
		return r53Error("InvalidInput", "missing zone ID in path", http.StatusBadRequest), nil
	}
	zone, err := p.store.GetZone(id, defaultAccountID)
	if err != nil {
		if err == ErrZoneNotFound {
			return r53Error("NoSuchHostedZone", "hosted zone not found", http.StatusNotFound), nil
		}
		return nil, err
	}

	type xmlZone struct {
		XMLName                xml.Name `xml:"HostedZone"`
		Id                     string   `xml:"Id"`
		Name                   string   `xml:"Name"`
		CallerReference        string   `xml:"CallerReference"`
		ResourceRecordSetCount int      `xml:"ResourceRecordSetCount"`
		Config                 struct {
			Comment string `xml:"Comment"`
		} `xml:"Config"`
	}
	type response struct {
		XMLName    xml.Name `xml:"GetHostedZoneResponse"`
		HostedZone xmlZone  `xml:"HostedZone"`
	}
	resp := response{}
	resp.HostedZone.Id = "/hostedzone/" + zone.ID
	resp.HostedZone.Name = zone.Name
	resp.HostedZone.CallerReference = zone.CallerRef
	resp.HostedZone.ResourceRecordSetCount = zone.RecordCount
	resp.HostedZone.Config.Comment = zone.Comment
	return xmlResp(http.StatusOK, resp)
}

func (p *Provider) listHostedZones(_ *http.Request) (*plugin.Response, error) {
	zones, err := p.store.ListZones(defaultAccountID)
	if err != nil {
		return nil, err
	}

	type xmlZone struct {
		Id                     string `xml:"Id"`
		Name                   string `xml:"Name"`
		CallerReference        string `xml:"CallerReference"`
		ResourceRecordSetCount int    `xml:"ResourceRecordSetCount"`
		Config                 struct {
			Comment string `xml:"Comment"`
		} `xml:"Config"`
	}
	type response struct {
		XMLName     xml.Name  `xml:"ListHostedZonesResponse"`
		HostedZones []xmlZone `xml:"HostedZones>HostedZone"`
		IsTruncated bool      `xml:"IsTruncated"`
		MaxItems    int       `xml:"MaxItems"`
		Marker      string    `xml:"Marker"`
	}
	resp := response{IsTruncated: false, MaxItems: 100}
	for _, z := range zones {
		xz := xmlZone{
			Id:                     "/hostedzone/" + z.ID,
			Name:                   z.Name,
			CallerReference:        z.CallerRef,
			ResourceRecordSetCount: z.RecordCount,
		}
		xz.Config.Comment = z.Comment
		resp.HostedZones = append(resp.HostedZones, xz)
	}
	return xmlResp(http.StatusOK, resp)
}

func (p *Provider) deleteHostedZone(req *http.Request) (*plugin.Response, error) {
	id := zoneIDFromPath(req.URL.Path)
	if id == "" {
		return r53Error("InvalidInput", "missing zone ID in path", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteZone(id, defaultAccountID); err != nil {
		switch err {
		case ErrZoneNotFound:
			return r53Error("NoSuchHostedZone", "hosted zone not found", http.StatusNotFound), nil
		case ErrZoneNotEmpty:
			return r53Error("HostedZoneNotEmpty", "hosted zone contains record sets", http.StatusConflict), nil
		}
		return nil, err
	}
	type changeInfo struct {
		Id          string `xml:"Id"`
		Status      string `xml:"Status"`
		SubmittedAt string `xml:"SubmittedAt"`
	}
	type response struct {
		XMLName    xml.Name   `xml:"DeleteHostedZoneResponse"`
		ChangeInfo changeInfo `xml:"ChangeInfo"`
	}
	return xmlResp(http.StatusOK, response{
		ChangeInfo: changeInfo{
			Id:          "/change/C" + id,
			Status:      "INSYNC",
			SubmittedAt: time.Now().UTC().Format(time.RFC3339),
		},
	})
}

func (p *Provider) changeResourceRecordSets(req *http.Request) (*plugin.Response, error) {
	zoneID := zoneIDFromPath(req.URL.Path)
	if zoneID == "" {
		return r53Error("InvalidInput", "missing zone ID in path", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetZone(zoneID, defaultAccountID); err != nil {
		return r53Error("NoSuchHostedZone", "hosted zone not found", http.StatusNotFound), nil
	}

	type xmlRecord struct {
		Value string `xml:"Value"`
	}
	type xmlResourceRecordSet struct {
		Name            string      `xml:"Name"`
		Type            string      `xml:"Type"`
		TTL             int64       `xml:"TTL"`
		ResourceRecords []xmlRecord `xml:"ResourceRecords>ResourceRecord"`
		AliasTarget     *struct {
			DNSName              string `xml:"DNSName"`
			HostedZoneId         string `xml:"HostedZoneId"`
			EvaluateTargetHealth bool   `xml:"EvaluateTargetHealth"`
		} `xml:"AliasTarget"`
	}
	type xmlChange struct {
		Action            string               `xml:"Action"`
		ResourceRecordSet xmlResourceRecordSet `xml:"ResourceRecordSet"`
	}
	type input struct {
		XMLName     xml.Name `xml:"ChangeResourceRecordSetsRequest"`
		ChangeBatch struct {
			Comment string      `xml:"Comment"`
			Changes []xmlChange `xml:"Changes>Change"`
		} `xml:"ChangeBatch"`
	}
	var in input
	if err := xml.NewDecoder(req.Body).Decode(&in); err != nil {
		return r53Error("MalformedInput", "failed to parse request body", http.StatusBadRequest), nil
	}

	for _, ch := range in.ChangeBatch.Changes {
		rrs := ch.ResourceRecordSet
		// Normalize record name to have trailing dot
		recName := rrs.Name
		if recName != "" && !strings.HasSuffix(recName, ".") {
			recName += "."
		}
		action := strings.ToUpper(ch.Action)
		switch action {
		case "CREATE", "UPSERT":
			r := &RecordSet{
				ZoneID:    zoneID,
				Name:      recName,
				Type:      rrs.Type,
				TTL:       rrs.TTL,
				AccountID: defaultAccountID,
			}
			for _, rec := range rrs.ResourceRecords {
				r.Records = append(r.Records, rec.Value)
			}
			if rrs.AliasTarget != nil {
				r.AliasTarget = &AliasTargetData{
					DNSName:              rrs.AliasTarget.DNSName,
					HostedZoneId:         rrs.AliasTarget.HostedZoneId,
					EvaluateTargetHealth: rrs.AliasTarget.EvaluateTargetHealth,
				}
			}
			if err := p.store.UpsertRecord(r); err != nil {
				return nil, err
			}
		case "DELETE":
			if err := p.store.DeleteRecord(zoneID, recName, rrs.Type, defaultAccountID); err != nil {
				if err == ErrRecordNotFound {
					return r53Error("InvalidChangeBatch", "record set not found", http.StatusBadRequest), nil
				}
				return nil, err
			}
		default:
			return r53Error("InvalidChangeBatch", fmt.Sprintf("unknown action: %s", action), http.StatusBadRequest), nil
		}
	}

	type changeInfo struct {
		Id          string `xml:"Id"`
		Status      string `xml:"Status"`
		SubmittedAt string `xml:"SubmittedAt"`
	}
	type response struct {
		XMLName    xml.Name   `xml:"ChangeResourceRecordSetsResponse"`
		ChangeInfo changeInfo `xml:"ChangeInfo"`
	}
	return xmlResp(http.StatusOK, response{
		ChangeInfo: changeInfo{
			Id:          "/change/C" + zoneID,
			Status:      "INSYNC",
			SubmittedAt: time.Now().UTC().Format(time.RFC3339),
		},
	})
}

func (p *Provider) listResourceRecordSets(req *http.Request) (*plugin.Response, error) {
	zoneID := zoneIDFromPath(req.URL.Path)
	if zoneID == "" {
		return r53Error("InvalidInput", "missing zone ID in path", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetZone(zoneID, defaultAccountID); err != nil {
		return r53Error("NoSuchHostedZone", "hosted zone not found", http.StatusNotFound), nil
	}
	recs, err := p.store.ListRecords(zoneID, defaultAccountID)
	if err != nil {
		return nil, err
	}

	type xmlRecord struct {
		Value string `xml:"Value"`
	}
	type xmlRRS struct {
		Name            string      `xml:"Name"`
		Type            string      `xml:"Type"`
		TTL             int64       `xml:"TTL,omitempty"`
		ResourceRecords []xmlRecord `xml:"ResourceRecords>ResourceRecord,omitempty"`
		AliasTarget     *struct {
			DNSName              string `xml:"DNSName"`
			HostedZoneId         string `xml:"HostedZoneId"`
			EvaluateTargetHealth bool   `xml:"EvaluateTargetHealth"`
		} `xml:"AliasTarget,omitempty"`
	}
	type response struct {
		XMLName            xml.Name `xml:"ListResourceRecordSetsResponse"`
		ResourceRecordSets []xmlRRS `xml:"ResourceRecordSets>ResourceRecordSet"`
		IsTruncated        bool     `xml:"IsTruncated"`
		MaxItems           int      `xml:"MaxItems"`
		NextRecordName     string   `xml:"NextRecordName,omitempty"`
		NextRecordType     string   `xml:"NextRecordType,omitempty"`
	}
	resp := response{IsTruncated: false, MaxItems: 300}
	for _, r := range recs {
		xr := xmlRRS{Name: r.Name, Type: r.Type, TTL: r.TTL}
		for _, v := range r.Records {
			xr.ResourceRecords = append(xr.ResourceRecords, xmlRecord{Value: v})
		}
		if r.AliasTarget != nil {
			xr.AliasTarget = &struct {
				DNSName              string `xml:"DNSName"`
				HostedZoneId         string `xml:"HostedZoneId"`
				EvaluateTargetHealth bool   `xml:"EvaluateTargetHealth"`
			}{
				DNSName:              r.AliasTarget.DNSName,
				HostedZoneId:         r.AliasTarget.HostedZoneId,
				EvaluateTargetHealth: r.AliasTarget.EvaluateTargetHealth,
			}
		}
		resp.ResourceRecordSets = append(resp.ResourceRecordSets, xr)
	}
	return xmlResp(http.StatusOK, resp)
}

// resourceIDFromTagPath extracts ResourceType and ResourceId from paths like
// /2013-04-01/tags/{ResourceType}/{ResourceId}
func resourceIDFromTagPath(path string) (string, string) {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	for i, p := range parts {
		if p == "tags" && i+2 < len(parts) {
			return parts[i+1], parts[i+2]
		}
	}
	return "", ""
}

func (p *Provider) changeTagsForResource(req *http.Request) (*plugin.Response, error) {
	resType, resID := resourceIDFromTagPath(req.URL.Path)
	if resType == "" || resID == "" {
		return r53Error("InvalidInput", "missing ResourceType or ResourceId in path", http.StatusBadRequest), nil
	}

	type xmlTag struct {
		Key   string `xml:"Key"`
		Value string `xml:"Value"`
	}
	type input struct {
		XMLName       xml.Name `xml:"ChangeTagsForResourceRequest"`
		AddTags       []xmlTag `xml:"AddTags>Tag"`
		RemoveTagKeys []string `xml:"RemoveTagKeys>Key"`
	}
	var in input
	if err := xml.NewDecoder(req.Body).Decode(&in); err != nil {
		return r53Error("MalformedInput", "failed to parse request body", http.StatusBadRequest), nil
	}

	// Build ARN for the resource
	arn := fmt.Sprintf("arn:aws:route53:::%s/%s", resType, resID)

	if len(in.AddTags) > 0 {
		tags := make(map[string]string, len(in.AddTags))
		for _, t := range in.AddTags {
			tags[t.Key] = t.Value
		}
		if err := p.store.tags.AddTags(arn, tags); err != nil {
			return nil, err
		}
	}

	if len(in.RemoveTagKeys) > 0 {
		if err := p.store.tags.RemoveTags(arn, in.RemoveTagKeys); err != nil {
			return nil, err
		}
	}

	type response struct {
		XMLName xml.Name `xml:"ChangeTagsForResourceResponse"`
	}
	return xmlResp(http.StatusOK, response{})
}

func (p *Provider) listTagsForResource(req *http.Request) (*plugin.Response, error) {
	resType, resID := resourceIDFromTagPath(req.URL.Path)
	if resType == "" || resID == "" {
		return r53Error("InvalidInput", "missing ResourceType or ResourceId in path", http.StatusBadRequest), nil
	}

	arn := fmt.Sprintf("arn:aws:route53:::%s/%s", resType, resID)
	tags, err := p.store.tags.ListTags(arn)
	if err != nil {
		return nil, err
	}

	type xmlTag struct {
		Key   string `xml:"Key"`
		Value string `xml:"Value"`
	}
	type resourceTagSet struct {
		ResourceType string   `xml:"ResourceType"`
		ResourceId   string   `xml:"ResourceId"`
		Tags         []xmlTag `xml:"Tags>Tag"`
	}
	type response struct {
		XMLName        xml.Name       `xml:"ListTagsForResourceResponse"`
		ResourceTagSet resourceTagSet `xml:"ResourceTagSet"`
	}
	resp := response{}
	resp.ResourceTagSet.ResourceType = resType
	resp.ResourceTagSet.ResourceId = resID
	for k, v := range tags {
		resp.ResourceTagSet.Tags = append(resp.ResourceTagSet.Tags, xmlTag{Key: k, Value: v})
	}
	if resp.ResourceTagSet.Tags == nil {
		resp.ResourceTagSet.Tags = []xmlTag{}
	}
	return xmlResp(http.StatusOK, resp)
}

// --- Health Check handlers ---

func healthCheckIDFromPath(path string) string {
	// /2013-04-01/healthcheck/{HealthCheckId}[/...]
	parts := strings.Split(strings.Trim(path, "/"), "/")
	for i, p := range parts {
		if p == "healthcheck" && i+1 < len(parts) {
			return parts[i+1]
		}
	}
	return ""
}

func (p *Provider) createHealthCheck(req *http.Request) (*plugin.Response, error) {
	type hcConfig struct {
		IPAddress                string `xml:"IPAddress"`
		Port                     int    `xml:"Port"`
		Type                     string `xml:"Type"`
		ResourcePath             string `xml:"ResourcePath"`
		FullyQualifiedDomainName string `xml:"FullyQualifiedDomainName"`
		SearchString             string `xml:"SearchString"`
		RequestInterval          int    `xml:"RequestInterval"`
		FailureThreshold         int    `xml:"FailureThreshold"`
	}
	type input struct {
		XMLName           xml.Name `xml:"CreateHealthCheckRequest"`
		CallerReference   string   `xml:"CallerReference"`
		HealthCheckConfig hcConfig `xml:"HealthCheckConfig"`
	}
	var in input
	if err := xml.NewDecoder(req.Body).Decode(&in); err != nil {
		return r53Error("MalformedInput", "failed to parse request body", http.StatusBadRequest), nil
	}
	if in.HealthCheckConfig.Type == "" {
		return r53Error("InvalidInput", "HealthCheckConfig.Type is required", http.StatusBadRequest), nil
	}
	ri := in.HealthCheckConfig.RequestInterval
	if ri == 0 {
		ri = 30
	}
	ft := in.HealthCheckConfig.FailureThreshold
	if ft == 0 {
		ft = 3
	}
	hc := &HealthCheck{
		ID:                       generateID(),
		CallerReference:          in.CallerReference,
		Type:                     in.HealthCheckConfig.Type,
		IPAddress:                in.HealthCheckConfig.IPAddress,
		Port:                     in.HealthCheckConfig.Port,
		ResourcePath:             in.HealthCheckConfig.ResourcePath,
		FullyQualifiedDomainName: in.HealthCheckConfig.FullyQualifiedDomainName,
		SearchString:             in.HealthCheckConfig.SearchString,
		RequestInterval:          ri,
		FailureThreshold:         ft,
		Status:                   "Healthy",
		AccountID:                defaultAccountID,
		CreatedAt:                time.Now(),
	}
	if err := p.store.CreateHealthCheck(hc); err != nil {
		return nil, err
	}
	type response struct {
		XMLName     xml.Name       `xml:"CreateHealthCheckResponse"`
		HealthCheck xmlHealthCheck `xml:"HealthCheck"`
	}
	return xmlResp(http.StatusCreated, response{HealthCheck: p.healthCheckToXML(hc)})
}

type xmlHealthCheckConfig struct {
	IPAddress                string `xml:"IPAddress,omitempty"`
	Port                     int    `xml:"Port,omitempty"`
	Type                     string `xml:"Type"`
	ResourcePath             string `xml:"ResourcePath,omitempty"`
	FullyQualifiedDomainName string `xml:"FullyQualifiedDomainName,omitempty"`
	SearchString             string `xml:"SearchString,omitempty"`
	RequestInterval          int    `xml:"RequestInterval"`
	FailureThreshold         int    `xml:"FailureThreshold"`
}

type xmlHealthCheck struct {
	XMLName            xml.Name             `xml:"HealthCheck"`
	Id                 string               `xml:"Id"`
	CallerReference    string               `xml:"CallerReference"`
	HealthCheckConfig  xmlHealthCheckConfig `xml:"HealthCheckConfig"`
	HealthCheckVersion int                  `xml:"HealthCheckVersion"`
}

func (p *Provider) healthCheckToXML(hc *HealthCheck) xmlHealthCheck {
	return xmlHealthCheck{
		Id:              hc.ID,
		CallerReference: hc.CallerReference,
		HealthCheckConfig: xmlHealthCheckConfig{
			IPAddress:                hc.IPAddress,
			Port:                     hc.Port,
			Type:                     hc.Type,
			ResourcePath:             hc.ResourcePath,
			FullyQualifiedDomainName: hc.FullyQualifiedDomainName,
			SearchString:             hc.SearchString,
			RequestInterval:          hc.RequestInterval,
			FailureThreshold:         hc.FailureThreshold,
		},
		HealthCheckVersion: 1,
	}
}

func (p *Provider) getHealthCheck(req *http.Request) (*plugin.Response, error) {
	id := healthCheckIDFromPath(req.URL.Path)
	if id == "" {
		return r53Error("InvalidInput", "missing health check ID", http.StatusBadRequest), nil
	}
	hc, err := p.store.GetHealthCheck(id, defaultAccountID)
	if err != nil {
		if err == ErrHealthCheckNotFound {
			return r53Error("NoSuchHealthCheck", "health check not found", http.StatusNotFound), nil
		}
		return nil, err
	}
	type response struct {
		XMLName     xml.Name       `xml:"GetHealthCheckResponse"`
		HealthCheck xmlHealthCheck `xml:"HealthCheck"`
	}
	return xmlResp(http.StatusOK, response{HealthCheck: p.healthCheckToXML(hc)})
}

func (p *Provider) listHealthChecks(_ *http.Request) (*plugin.Response, error) {
	hcs, err := p.store.ListHealthChecks(defaultAccountID)
	if err != nil {
		return nil, err
	}
	type response struct {
		XMLName      xml.Name         `xml:"ListHealthChecksResponse"`
		HealthChecks []xmlHealthCheck `xml:"HealthChecks>HealthCheck"`
		IsTruncated  bool             `xml:"IsTruncated"`
		MaxItems     int              `xml:"MaxItems"`
		Marker       string           `xml:"Marker"`
	}
	resp := response{IsTruncated: false, MaxItems: 100}
	for _, hc := range hcs {
		hcCopy := hc
		resp.HealthChecks = append(resp.HealthChecks, p.healthCheckToXML(&hcCopy))
	}
	return xmlResp(http.StatusOK, resp)
}

func (p *Provider) updateHealthCheck(req *http.Request) (*plugin.Response, error) {
	id := healthCheckIDFromPath(req.URL.Path)
	if id == "" {
		return r53Error("InvalidInput", "missing health check ID", http.StatusBadRequest), nil
	}
	existing, err := p.store.GetHealthCheck(id, defaultAccountID)
	if err != nil {
		if err == ErrHealthCheckNotFound {
			return r53Error("NoSuchHealthCheck", "health check not found", http.StatusNotFound), nil
		}
		return nil, err
	}

	type input struct {
		XMLName                  xml.Name `xml:"UpdateHealthCheckRequest"`
		IPAddress                string   `xml:"IPAddress"`
		Port                     int      `xml:"Port"`
		ResourcePath             string   `xml:"ResourcePath"`
		FullyQualifiedDomainName string   `xml:"FullyQualifiedDomainName"`
		SearchString             string   `xml:"SearchString"`
		FailureThreshold         int      `xml:"FailureThreshold"`
	}
	var in input
	if err := xml.NewDecoder(req.Body).Decode(&in); err != nil {
		return r53Error("MalformedInput", "failed to parse request body", http.StatusBadRequest), nil
	}
	if in.IPAddress != "" {
		existing.IPAddress = in.IPAddress
	}
	if in.Port != 0 {
		existing.Port = in.Port
	}
	if in.ResourcePath != "" {
		existing.ResourcePath = in.ResourcePath
	}
	if in.FullyQualifiedDomainName != "" {
		existing.FullyQualifiedDomainName = in.FullyQualifiedDomainName
	}
	if in.SearchString != "" {
		existing.SearchString = in.SearchString
	}
	if in.FailureThreshold != 0 {
		existing.FailureThreshold = in.FailureThreshold
	}
	if err := p.store.UpdateHealthCheck(existing); err != nil {
		return nil, err
	}
	type response struct {
		XMLName     xml.Name       `xml:"UpdateHealthCheckResponse"`
		HealthCheck xmlHealthCheck `xml:"HealthCheck"`
	}
	return xmlResp(http.StatusOK, response{HealthCheck: p.healthCheckToXML(existing)})
}

func (p *Provider) deleteHealthCheck(req *http.Request) (*plugin.Response, error) {
	id := healthCheckIDFromPath(req.URL.Path)
	if id == "" {
		return r53Error("InvalidInput", "missing health check ID", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteHealthCheck(id, defaultAccountID); err != nil {
		if err == ErrHealthCheckNotFound {
			return r53Error("NoSuchHealthCheck", "health check not found", http.StatusNotFound), nil
		}
		return nil, err
	}
	type response struct {
		XMLName xml.Name `xml:"DeleteHealthCheckResponse"`
	}
	return xmlResp(http.StatusOK, response{})
}

func (p *Provider) getHealthCheckStatus(req *http.Request) (*plugin.Response, error) {
	id := healthCheckIDFromPath(req.URL.Path)
	if id == "" {
		return r53Error("InvalidInput", "missing health check ID", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetHealthCheck(id, defaultAccountID); err != nil {
		if err == ErrHealthCheckNotFound {
			return r53Error("NoSuchHealthCheck", "health check not found", http.StatusNotFound), nil
		}
		return nil, err
	}
	type statusObs struct {
		IPAddress    string `xml:"IPAddress"`
		StatusReport struct {
			Status      string `xml:"Status"`
			CheckedTime string `xml:"CheckedTime"`
		} `xml:"StatusReport"`
	}
	type response struct {
		XMLName                 xml.Name    `xml:"GetHealthCheckStatusResponse"`
		HealthCheckObservations []statusObs `xml:"HealthCheckObservations>HealthCheckObservation"`
	}
	obs := statusObs{IPAddress: "0.0.0.0"}
	obs.StatusReport.Status = "Success"
	obs.StatusReport.CheckedTime = time.Now().UTC().Format(time.RFC3339)
	return xmlResp(http.StatusOK, response{HealthCheckObservations: []statusObs{obs}})
}

// --- Traffic Policy handlers ---

func trafficPolicyIDVersionFromPath(path string) (string, int) {
	// /2013-04-01/trafficpolicy/{Id}/{Version}
	parts := strings.Split(strings.Trim(path, "/"), "/")
	for i, p := range parts {
		if p == "trafficpolicy" && i+1 < len(parts) {
			id := parts[i+1]
			version := 0
			if i+2 < len(parts) {
				version, _ = strconv.Atoi(parts[i+2])
			}
			return id, version
		}
	}
	return "", 0
}

func (p *Provider) createTrafficPolicy(req *http.Request) (*plugin.Response, error) {
	type input struct {
		XMLName  xml.Name `xml:"CreateTrafficPolicyRequest"`
		Name     string   `xml:"Name"`
		Document string   `xml:"Document"`
		Comment  string   `xml:"Comment"`
	}
	var in input
	if err := xml.NewDecoder(req.Body).Decode(&in); err != nil {
		return r53Error("MalformedInput", "failed to parse request body", http.StatusBadRequest), nil
	}
	if in.Name == "" || in.Document == "" {
		return r53Error("InvalidInput", "Name and Document are required", http.StatusBadRequest), nil
	}
	tp := &TrafficPolicy{
		ID:        generateID(),
		Version:   1,
		Name:      in.Name,
		Document:  in.Document,
		Comment:   in.Comment,
		AccountID: defaultAccountID,
		CreatedAt: time.Now(),
	}
	if err := p.store.CreateTrafficPolicy(tp); err != nil {
		return nil, err
	}
	type xmlTP struct {
		XMLName  xml.Name `xml:"TrafficPolicy"`
		Id       string   `xml:"Id"`
		Version  int      `xml:"Version"`
		Name     string   `xml:"Name"`
		Document string   `xml:"Document"`
		Comment  string   `xml:"Comment,omitempty"`
	}
	type response struct {
		XMLName       xml.Name `xml:"CreateTrafficPolicyResponse"`
		TrafficPolicy xmlTP    `xml:"TrafficPolicy"`
		Location      string   `xml:"Location"`
	}
	return xmlResp(http.StatusCreated, response{
		TrafficPolicy: xmlTP{
			Id:       tp.ID,
			Version:  tp.Version,
			Name:     tp.Name,
			Document: tp.Document,
			Comment:  tp.Comment,
		},
		Location: fmt.Sprintf("/2013-04-01/trafficpolicy/%s/1", tp.ID),
	})
}

func (p *Provider) getTrafficPolicy(req *http.Request) (*plugin.Response, error) {
	id, version := trafficPolicyIDVersionFromPath(req.URL.Path)
	if id == "" {
		return r53Error("InvalidInput", "missing traffic policy ID", http.StatusBadRequest), nil
	}
	tp, err := p.store.GetTrafficPolicy(id, version, defaultAccountID)
	if err != nil {
		if err == ErrTrafficPolicyNotFound {
			return r53Error("NoSuchTrafficPolicy", "traffic policy not found", http.StatusNotFound), nil
		}
		return nil, err
	}
	type xmlTP struct {
		XMLName  xml.Name `xml:"TrafficPolicy"`
		Id       string   `xml:"Id"`
		Version  int      `xml:"Version"`
		Name     string   `xml:"Name"`
		Document string   `xml:"Document"`
		Comment  string   `xml:"Comment,omitempty"`
	}
	type response struct {
		XMLName       xml.Name `xml:"GetTrafficPolicyResponse"`
		TrafficPolicy xmlTP    `xml:"TrafficPolicy"`
	}
	return xmlResp(http.StatusOK, response{
		TrafficPolicy: xmlTP{
			Id:       tp.ID,
			Version:  tp.Version,
			Name:     tp.Name,
			Document: tp.Document,
			Comment:  tp.Comment,
		},
	})
}

func (p *Provider) listTrafficPolicies(_ *http.Request) (*plugin.Response, error) {
	tps, err := p.store.ListTrafficPolicies(defaultAccountID)
	if err != nil {
		return nil, err
	}
	type xmlTPSum struct {
		Id                 string `xml:"Id"`
		LatestVersion      int    `xml:"LatestVersion"`
		Name               string `xml:"Name"`
		TrafficPolicyCount int    `xml:"TrafficPolicyCount"`
	}
	type response struct {
		XMLName                xml.Name   `xml:"ListTrafficPoliciesResponse"`
		TrafficPolicySummaries []xmlTPSum `xml:"TrafficPolicySummaries>TrafficPolicySummary"`
		IsTruncated            bool       `xml:"IsTruncated"`
		MaxItems               int        `xml:"MaxItems"`
		TrafficPolicyIdMarker  string     `xml:"TrafficPolicyIdMarker"`
	}
	resp := response{IsTruncated: false, MaxItems: 100}
	for _, tp := range tps {
		resp.TrafficPolicySummaries = append(resp.TrafficPolicySummaries, xmlTPSum{
			Id:                 tp.ID,
			LatestVersion:      tp.Version,
			Name:               tp.Name,
			TrafficPolicyCount: 1,
		})
	}
	return xmlResp(http.StatusOK, resp)
}

func (p *Provider) deleteTrafficPolicy(req *http.Request) (*plugin.Response, error) {
	id, version := trafficPolicyIDVersionFromPath(req.URL.Path)
	if id == "" {
		return r53Error("InvalidInput", "missing traffic policy ID", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteTrafficPolicy(id, version, defaultAccountID); err != nil {
		if err == ErrTrafficPolicyNotFound {
			return r53Error("NoSuchTrafficPolicy", "traffic policy not found", http.StatusNotFound), nil
		}
		return nil, err
	}
	type response struct {
		XMLName xml.Name `xml:"DeleteTrafficPolicyResponse"`
	}
	return xmlResp(http.StatusOK, response{})
}

func (p *Provider) createTrafficPolicyInstance(req *http.Request) (*plugin.Response, error) {
	type input struct {
		XMLName              xml.Name `xml:"CreateTrafficPolicyInstanceRequest"`
		HostedZoneId         string   `xml:"HostedZoneId"`
		Name                 string   `xml:"Name"`
		TrafficPolicyId      string   `xml:"TrafficPolicyId"`
		TrafficPolicyVersion int      `xml:"TrafficPolicyVersion"`
		TTL                  int64    `xml:"TTL"`
	}
	var in input
	if err := xml.NewDecoder(req.Body).Decode(&in); err != nil {
		return r53Error("MalformedInput", "failed to parse request body", http.StatusBadRequest), nil
	}
	ttl := int(in.TTL)
	if ttl == 0 {
		ttl = 300
	}
	tpi := &TrafficPolicyInstance{
		ID:              generateID(),
		HostedZoneID:    in.HostedZoneId,
		Name:            in.Name,
		TrafficPolicyID: in.TrafficPolicyId,
		Version:         in.TrafficPolicyVersion,
		TTL:             ttl,
		State:           "Applied",
		AccountID:       defaultAccountID,
	}
	if err := p.store.CreateTrafficPolicyInstance(tpi); err != nil {
		return nil, err
	}
	type xmlTPI struct {
		XMLName              xml.Name `xml:"TrafficPolicyInstance"`
		Id                   string   `xml:"Id"`
		HostedZoneId         string   `xml:"HostedZoneId"`
		Name                 string   `xml:"Name"`
		TrafficPolicyId      string   `xml:"TrafficPolicyId"`
		TrafficPolicyVersion int      `xml:"TrafficPolicyVersion"`
		TTL                  int      `xml:"TTL"`
		State                string   `xml:"State"`
	}
	type response struct {
		XMLName               xml.Name `xml:"CreateTrafficPolicyInstanceResponse"`
		TrafficPolicyInstance xmlTPI   `xml:"TrafficPolicyInstance"`
		Location              string   `xml:"Location"`
	}
	return xmlResp(http.StatusCreated, response{
		TrafficPolicyInstance: xmlTPI{
			Id:                   tpi.ID,
			HostedZoneId:         tpi.HostedZoneID,
			Name:                 tpi.Name,
			TrafficPolicyId:      tpi.TrafficPolicyID,
			TrafficPolicyVersion: tpi.Version,
			TTL:                  tpi.TTL,
			State:                tpi.State,
		},
		Location: fmt.Sprintf("/2013-04-01/trafficpolicyinstance/%s", tpi.ID),
	})
}

func (p *Provider) deleteTrafficPolicyInstance(req *http.Request) (*plugin.Response, error) {
	id := trafficPolicyInstanceIDFromPath(req.URL.Path)
	if id == "" {
		return r53Error("InvalidInput", "missing traffic policy instance ID", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteTrafficPolicyInstance(id, defaultAccountID); err != nil {
		if err == ErrTrafficPolicyInstanceNotFound {
			return r53Error("NoSuchTrafficPolicyInstance", "traffic policy instance not found", http.StatusNotFound), nil
		}
		return nil, err
	}
	type response struct {
		XMLName xml.Name `xml:"DeleteTrafficPolicyInstanceResponse"`
	}
	return xmlResp(http.StatusOK, response{})
}

// --- Query Logging Config handlers ---

func queryLoggingIDFromPath(path string) string {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	for i, p := range parts {
		if p == "queryloggingconfig" && i+1 < len(parts) {
			return parts[i+1]
		}
	}
	return ""
}

func (p *Provider) createQueryLoggingConfig(req *http.Request) (*plugin.Response, error) {
	type input struct {
		XMLName                   xml.Name `xml:"CreateQueryLoggingConfigRequest"`
		HostedZoneId              string   `xml:"HostedZoneId"`
		CloudWatchLogsLogGroupArn string   `xml:"CloudWatchLogsLogGroupArn"`
	}
	var in input
	if err := xml.NewDecoder(req.Body).Decode(&in); err != nil {
		return r53Error("MalformedInput", "failed to parse request body", http.StatusBadRequest), nil
	}
	if in.HostedZoneId == "" || in.CloudWatchLogsLogGroupArn == "" {
		return r53Error("InvalidInput", "HostedZoneId and CloudWatchLogsLogGroupArn are required", http.StatusBadRequest), nil
	}
	qlc := &QueryLoggingConfig{
		ID:                    generateID(),
		HostedZoneID:          in.HostedZoneId,
		CloudWatchLogGroupARN: in.CloudWatchLogsLogGroupArn,
		AccountID:             defaultAccountID,
	}
	if err := p.store.CreateQueryLoggingConfig(qlc); err != nil {
		return nil, err
	}
	type xmlQLC struct {
		XMLName                   xml.Name `xml:"QueryLoggingConfig"`
		Id                        string   `xml:"Id"`
		HostedZoneId              string   `xml:"HostedZoneId"`
		CloudWatchLogsLogGroupArn string   `xml:"CloudWatchLogsLogGroupArn"`
	}
	type response struct {
		XMLName            xml.Name `xml:"CreateQueryLoggingConfigResponse"`
		QueryLoggingConfig xmlQLC   `xml:"QueryLoggingConfig"`
		Location           string   `xml:"Location"`
	}
	return xmlResp(http.StatusCreated, response{
		QueryLoggingConfig: xmlQLC{
			Id:                        qlc.ID,
			HostedZoneId:              qlc.HostedZoneID,
			CloudWatchLogsLogGroupArn: qlc.CloudWatchLogGroupARN,
		},
		Location: fmt.Sprintf("/2013-04-01/queryloggingconfig/%s", qlc.ID),
	})
}

func (p *Provider) getQueryLoggingConfig(req *http.Request) (*plugin.Response, error) {
	id := queryLoggingIDFromPath(req.URL.Path)
	if id == "" {
		return r53Error("InvalidInput", "missing query logging config ID", http.StatusBadRequest), nil
	}
	qlc, err := p.store.GetQueryLoggingConfig(id, defaultAccountID)
	if err != nil {
		if err == ErrQueryLoggingNotFound {
			return r53Error("NoSuchQueryLoggingConfig", "query logging config not found", http.StatusNotFound), nil
		}
		return nil, err
	}
	type xmlQLC struct {
		XMLName                   xml.Name `xml:"QueryLoggingConfig"`
		Id                        string   `xml:"Id"`
		HostedZoneId              string   `xml:"HostedZoneId"`
		CloudWatchLogsLogGroupArn string   `xml:"CloudWatchLogsLogGroupArn"`
	}
	type response struct {
		XMLName            xml.Name `xml:"GetQueryLoggingConfigResponse"`
		QueryLoggingConfig xmlQLC   `xml:"QueryLoggingConfig"`
	}
	return xmlResp(http.StatusOK, response{
		QueryLoggingConfig: xmlQLC{
			Id:                        qlc.ID,
			HostedZoneId:              qlc.HostedZoneID,
			CloudWatchLogsLogGroupArn: qlc.CloudWatchLogGroupARN,
		},
	})
}

func (p *Provider) listQueryLoggingConfigs(_ *http.Request) (*plugin.Response, error) {
	qlcs, err := p.store.ListQueryLoggingConfigs(defaultAccountID)
	if err != nil {
		return nil, err
	}
	type xmlQLC struct {
		Id                        string `xml:"Id"`
		HostedZoneId              string `xml:"HostedZoneId"`
		CloudWatchLogsLogGroupArn string `xml:"CloudWatchLogsLogGroupArn"`
	}
	type response struct {
		XMLName             xml.Name `xml:"ListQueryLoggingConfigsResponse"`
		QueryLoggingConfigs []xmlQLC `xml:"QueryLoggingConfigs>QueryLoggingConfig"`
		IsTruncated         bool     `xml:"IsTruncated"`
	}
	resp := response{IsTruncated: false}
	for _, qlc := range qlcs {
		resp.QueryLoggingConfigs = append(resp.QueryLoggingConfigs, xmlQLC{
			Id:                        qlc.ID,
			HostedZoneId:              qlc.HostedZoneID,
			CloudWatchLogsLogGroupArn: qlc.CloudWatchLogGroupARN,
		})
	}
	return xmlResp(http.StatusOK, resp)
}

func (p *Provider) deleteQueryLoggingConfig(req *http.Request) (*plugin.Response, error) {
	id := queryLoggingIDFromPath(req.URL.Path)
	if id == "" {
		return r53Error("InvalidInput", "missing query logging config ID", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteQueryLoggingConfig(id, defaultAccountID); err != nil {
		if err == ErrQueryLoggingNotFound {
			return r53Error("NoSuchQueryLoggingConfig", "query logging config not found", http.StatusNotFound), nil
		}
		return nil, err
	}
	type response struct {
		XMLName xml.Name `xml:"DeleteQueryLoggingConfigResponse"`
	}
	return xmlResp(http.StatusOK, response{})
}

// --- DNSSEC handlers ---

func (p *Provider) getDNSSEC(req *http.Request) (*plugin.Response, error) {
	zoneID := zoneIDFromPath(req.URL.Path)
	if zoneID == "" {
		return r53Error("InvalidInput", "missing hosted zone ID", http.StatusBadRequest), nil
	}
	dnssec, err := p.store.GetDNSSEC(zoneID, defaultAccountID)
	if err != nil {
		return nil, err
	}
	type statusType struct {
		ServeSignature string `xml:"ServeSignature"`
	}
	type response struct {
		XMLName xml.Name   `xml:"GetDNSSECResponse"`
		Status  statusType `xml:"Status"`
	}
	status := "NOT_SIGNING"
	if dnssec.Status == "Enabled" {
		status = "SIGNING"
	}
	return xmlResp(http.StatusOK, response{
		Status: statusType{ServeSignature: status},
	})
}

func (p *Provider) enableDNSSEC(req *http.Request) (*plugin.Response, error) {
	zoneID := zoneIDFromPath(req.URL.Path)
	if zoneID == "" {
		return r53Error("InvalidInput", "missing hosted zone ID", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetZone(zoneID, defaultAccountID); err != nil {
		if err == ErrZoneNotFound {
			return r53Error("NoSuchHostedZone", "hosted zone not found", http.StatusNotFound), nil
		}
		return nil, err
	}
	if err := p.store.EnableDNSSEC(zoneID, defaultAccountID); err != nil {
		return nil, err
	}
	type changeInfo struct {
		Id          string `xml:"Id"`
		Status      string `xml:"Status"`
		SubmittedAt string `xml:"SubmittedAt"`
	}
	type response struct {
		XMLName    xml.Name   `xml:"EnableHostedZoneDNSSECResponse"`
		ChangeInfo changeInfo `xml:"ChangeInfo"`
	}
	return xmlResp(http.StatusOK, response{
		ChangeInfo: changeInfo{
			Id:          "/change/E" + zoneID,
			Status:      "INSYNC",
			SubmittedAt: time.Now().UTC().Format(time.RFC3339),
		},
	})
}

func (p *Provider) disableDNSSEC(req *http.Request) (*plugin.Response, error) {
	zoneID := zoneIDFromPath(req.URL.Path)
	if zoneID == "" {
		return r53Error("InvalidInput", "missing hosted zone ID", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetZone(zoneID, defaultAccountID); err != nil {
		if err == ErrZoneNotFound {
			return r53Error("NoSuchHostedZone", "hosted zone not found", http.StatusNotFound), nil
		}
		return nil, err
	}
	if err := p.store.DisableDNSSEC(zoneID, defaultAccountID); err != nil {
		return nil, err
	}
	type changeInfo struct {
		Id          string `xml:"Id"`
		Status      string `xml:"Status"`
		SubmittedAt string `xml:"SubmittedAt"`
	}
	type response struct {
		XMLName    xml.Name   `xml:"DisableHostedZoneDNSSECResponse"`
		ChangeInfo changeInfo `xml:"ChangeInfo"`
	}
	return xmlResp(http.StatusOK, response{
		ChangeInfo: changeInfo{
			Id:          "/change/D" + zoneID,
			Status:      "INSYNC",
			SubmittedAt: time.Now().UTC().Format(time.RFC3339),
		},
	})
}

// --- Key Signing Key handlers ---

func keySigningKeyParamsFromPath(path string) (string, string) {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	for i, p := range parts {
		if p == "keysigningkey" && i+2 < len(parts) {
			return parts[i+1], parts[i+2]
		}
	}
	return "", ""
}

func (p *Provider) createKeySigningKey(req *http.Request) (*plugin.Response, error) {
	type input struct {
		XMLName          xml.Name `xml:"CreateKeySigningKeyRequest"`
		Name             string   `xml:"Name"`
		HostedZoneId     string   `xml:"HostedZoneId"`
		Use              string   `xml:"Use"`
		Algorithm        string   `xml:"Algorithm"`
		KeySpec          string   `xml:"KeySpec"`
		PublishStartTime string   `xml:"PublishStartTime"`
	}
	var in input
	if err := xml.NewDecoder(req.Body).Decode(&in); err != nil {
		return r53Error("MalformedInput", "failed to parse request body", http.StatusBadRequest), nil
	}
	if in.Name == "" || in.HostedZoneId == "" {
		return r53Error("InvalidInput", "Name and HostedZoneId are required", http.StatusBadRequest), nil
	}
	// Normalize hosted zone ID (strip "/hostedzone/" prefix if present)
	zoneID := strings.TrimPrefix(in.HostedZoneId, "/hostedzone/")
	if in.Algorithm == "" {
		in.Algorithm = "ECDSA_P256_SHA256"
	}
	if in.KeySpec == "" {
		in.KeySpec = "ECDSA_P256"
	}
	keyID := generateID()
	kss := &KeySigningKey{
		HostedZoneID: zoneID,
		Name:         in.Name,
		KeyID:        keyID,
		State:        "Pending",
		CreatedAt:    time.Now(),
	}
	if err := p.store.CreateKeySigningKey(kss); err != nil {
		return nil, err
	}
	type xmlKeySigningKey struct {
		XMLName      xml.Name `xml:"KeySigningKey"`
		Name         string   `xml:"Name"`
		HostedZoneId string   `xml:"HostedZoneId"`
		KeyId        string   `xml:"KeyId"`
		State        string   `xml:"State"`
		Algorithm    string   `xml:"Algorithm"`
		KeySpec      string   `xml:"KeySpec"`
		PublishTime  string   `xml:"PublishTime"`
	}
	type response struct {
		XMLName       xml.Name         `xml:"CreateKeySigningKeyResponse"`
		KeySigningKey xmlKeySigningKey `xml:"KeySigningKey"`
		Location      string           `xml:"Location"`
	}
	return xmlResp(http.StatusCreated, response{
		KeySigningKey: xmlKeySigningKey{
			Name:         in.Name,
			HostedZoneId: in.HostedZoneId,
			KeyId:        keyID,
			State:        "Pending",
			Algorithm:    in.Algorithm,
			KeySpec:      in.KeySpec,
			PublishTime:  in.PublishStartTime,
		},
		Location: fmt.Sprintf("/2013-04-01/keysigningkey/%s/%s", in.HostedZoneId, in.Name),
	})
}

func (p *Provider) deleteKeySigningKey(req *http.Request) (*plugin.Response, error) {
	zoneID, name := keySigningKeyParamsFromPath(req.URL.Path)
	if zoneID == "" || name == "" {
		return r53Error("InvalidInput", "missing hosted zone ID or key signing key name", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteKeySigningKey(zoneID, name, defaultAccountID); err != nil {
		if err == ErrKeySigningKeyNotFound {
			return r53Error("NoSuchKeySigningKey", "key signing key not found", http.StatusNotFound), nil
		}
		return nil, err
	}
	type response struct {
		XMLName xml.Name `xml:"DeleteKeySigningKeyResponse"`
	}
	return xmlResp(http.StatusOK, response{})
}

func (p *Provider) listKeySigningKeys(_ *http.Request) (*plugin.Response, error) {
	kssList, err := p.store.ListKeySigningKeys(defaultAccountID)
	if err != nil {
		return nil, err
	}
	type xmlKeySigningKey struct {
		Name         string `xml:"Name"`
		HostedZoneId string `xml:"HostedZoneId"`
		KeyId        string `xml:"KeyId"`
		State        string `xml:"State"`
		Algorithm    string `xml:"Algorithm"`
		KeySpec      string `xml:"KeySpec"`
		PublishTime  string `xml:"PublishTime"`
	}
	type response struct {
		XMLName               xml.Name           `xml:"ListKeySigningKeysResponse"`
		KeySigningKeys        []xmlKeySigningKey `xml:"KeySigningKeys>KeySigningKey"`
		IsTruncated           bool               `xml:"IsTruncated"`
		MaxItems              int                `xml:"MaxItems"`
		NextKeySigningKeyName string             `xml:"NextKeySigningKeyName"`
		NextHostedZoneId      string             `xml:"NextHostedZoneId"`
	}
	resp := response{IsTruncated: false, MaxItems: 100}
	for _, kss := range kssList {
		resp.KeySigningKeys = append(resp.KeySigningKeys, xmlKeySigningKey{
			Name:         kss.Name,
			HostedZoneId: kss.HostedZoneID,
			KeyId:        kss.KeyID,
			State:        kss.State,
		})
	}
	return xmlResp(http.StatusOK, resp)
}

func (p *Provider) activateKeySigningKey(req *http.Request) (*plugin.Response, error) {
	zoneID, name := keySigningKeyParamsFromPath(req.URL.Path)
	if zoneID == "" || name == "" {
		return r53Error("InvalidInput", "missing hosted zone ID or key signing key name", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetZone(zoneID, defaultAccountID); err != nil {
		if err == ErrZoneNotFound {
			return r53Error("NoSuchHostedZone", "hosted zone not found", http.StatusNotFound), nil
		}
		return nil, err
	}
	type xmlKeySigningKey struct {
		Name         string `xml:"Name"`
		HostedZoneId string `xml:"HostedZoneId"`
		KeyId        string `xml:"KeyId"`
		State        string `xml:"State"`
		Algorithm    string `xml:"Algorithm"`
		KeySpec      string `xml:"KeySpec"`
		PublishTime  string `xml:"PublishTime"`
	}
	type response struct {
		XMLName       xml.Name         `xml:"ActivateKeySigningKeyResponse"`
		KeySigningKey xmlKeySigningKey `xml:"KeySigningKey"`
	}
	return xmlResp(http.StatusOK, response{
		KeySigningKey: xmlKeySigningKey{
			Name:         name,
			HostedZoneId: zoneID,
			KeyId:        generateID(),
			State:        "Signing",
			Algorithm:    "ECDSA_P256_SHA256",
			KeySpec:      "ECDSA_P256",
		},
	})
}

func (p *Provider) deactivateKeySigningKey(req *http.Request) (*plugin.Response, error) {
	zoneID, name := keySigningKeyParamsFromPath(req.URL.Path)
	if zoneID == "" || name == "" {
		return r53Error("InvalidInput", "missing hosted zone ID or key signing key name", http.StatusBadRequest), nil
	}
	if _, err := p.store.GetZone(zoneID, defaultAccountID); err != nil {
		if err == ErrZoneNotFound {
			return r53Error("NoSuchHostedZone", "hosted zone not found", http.StatusNotFound), nil
		}
		return nil, err
	}
	type response struct {
		XMLName xml.Name `xml:"DeactivateKeySigningKeyResponse"`
	}
	return xmlResp(http.StatusOK, response{})
}

// --- Traffic Policy Instance handlers ---

func (p *Provider) updateTrafficPolicyInstance(req *http.Request) (*plugin.Response, error) {
	id := trafficPolicyInstanceIDFromPath(req.URL.Path)
	if id == "" {
		return r53Error("InvalidInput", "missing traffic policy instance ID", http.StatusBadRequest), nil
	}
	type input struct {
		XMLName              xml.Name `xml:"UpdateTrafficPolicyInstanceRequest"`
		Name                 string   `xml:"Name"`
		TrafficPolicyId      string   `xml:"TrafficPolicyId"`
		TrafficPolicyVersion int      `xml:"TrafficPolicyVersion"`
		TTL                  int64    `xml:"TTL"`
	}
	var in input
	if err := xml.NewDecoder(req.Body).Decode(&in); err != nil {
		return r53Error("MalformedInput", "failed to parse request body", http.StatusBadRequest), nil
	}
	tpi := &TrafficPolicyInstance{
		ID:              id,
		Name:            in.Name,
		TrafficPolicyID: in.TrafficPolicyId,
		Version:         in.TrafficPolicyVersion,
		TTL:             int(in.TTL),
		State:           "Applied",
		AccountID:       defaultAccountID,
	}
	if err := p.store.UpdateTrafficPolicyInstance(tpi); err != nil {
		if err == ErrTrafficPolicyInstanceNotFound {
			return r53Error("NoSuchTrafficPolicyInstance", "traffic policy instance not found", http.StatusNotFound), nil
		}
		return nil, err
	}
	type xmlTPI struct {
		XMLName              xml.Name `xml:"TrafficPolicyInstance"`
		Id                   string   `xml:"Id"`
		HostedZoneId         string   `xml:"HostedZoneId"`
		Name                 string   `xml:"Name"`
		TrafficPolicyId      string   `xml:"TrafficPolicyId"`
		TrafficPolicyVersion int      `xml:"TrafficPolicyVersion"`
		TTL                  int      `xml:"TTL"`
		State                string   `xml:"State"`
	}
	type response struct {
		XMLName               xml.Name `xml:"UpdateTrafficPolicyInstanceResponse"`
		TrafficPolicyInstance xmlTPI   `xml:"TrafficPolicyInstance"`
	}
	existing, err := p.store.GetTrafficPolicyInstance(id, defaultAccountID)
	if err != nil {
		return nil, err
	}
	return xmlResp(http.StatusOK, response{
		TrafficPolicyInstance: xmlTPI{
			Id:                   existing.ID,
			HostedZoneId:         existing.HostedZoneID,
			Name:                 existing.Name,
			TrafficPolicyId:      existing.TrafficPolicyID,
			TrafficPolicyVersion: existing.Version,
			TTL:                  existing.TTL,
			State:                existing.State,
		},
	})
}

func (p *Provider) listTrafficPolicyInstances(_ *http.Request) (*plugin.Response, error) {
	tpis, err := p.store.ListTrafficPolicyInstances(defaultAccountID)
	if err != nil {
		return nil, err
	}
	type xmlTPI struct {
		XMLName              xml.Name `xml:"TrafficPolicyInstance"`
		Id                   string   `xml:"Id"`
		HostedZoneId         string   `xml:"HostedZoneId"`
		Name                 string   `xml:"Name"`
		TrafficPolicyId      string   `xml:"TrafficPolicyId"`
		TrafficPolicyVersion int      `xml:"TrafficPolicyVersion"`
		TTL                  int      `xml:"TTL"`
		State                string   `xml:"State"`
	}
	type response struct {
		XMLName                xml.Name `xml:"ListTrafficPolicyInstancesResponse"`
		TrafficPolicyInstances []xmlTPI `xml:"TrafficPolicyInstances>TrafficPolicyInstance"`
		IsTruncated            bool     `xml:"IsTruncated"`
		MaxItems               int      `xml:"MaxItems"`
		NextId                 string   `xml:"NextId"`
	}
	resp := response{IsTruncated: false, MaxItems: 100}
	for _, tpi := range tpis {
		resp.TrafficPolicyInstances = append(resp.TrafficPolicyInstances, xmlTPI{
			Id:                   tpi.ID,
			HostedZoneId:         tpi.HostedZoneID,
			Name:                 tpi.Name,
			TrafficPolicyId:      tpi.TrafficPolicyID,
			TrafficPolicyVersion: tpi.Version,
			TTL:                  tpi.TTL,
			State:                tpi.State,
		})
	}
	return xmlResp(http.StatusOK, resp)
}

func (p *Provider) listTrafficPolicyInstancesByHostedZone(_ *http.Request) (*plugin.Response, error) {
	tpis, err := p.store.ListTrafficPolicyInstances(defaultAccountID)
	if err != nil {
		return nil, err
	}
	type xmlTPI struct {
		XMLName              xml.Name `xml:"TrafficPolicyInstance"`
		Id                   string   `xml:"Id"`
		HostedZoneId         string   `xml:"HostedZoneId"`
		Name                 string   `xml:"Name"`
		TrafficPolicyId      string   `xml:"TrafficPolicyId"`
		TrafficPolicyVersion int      `xml:"TrafficPolicyVersion"`
		TTL                  int      `xml:"TTL"`
		State                string   `xml:"State"`
	}
	type response struct {
		XMLName                xml.Name `xml:"ListTrafficPolicyInstancesResponse"`
		TrafficPolicyInstances []xmlTPI `xml:"TrafficPolicyInstances>TrafficPolicyInstance"`
	}
	resp := response{}
	for _, tpi := range tpis {
		resp.TrafficPolicyInstances = append(resp.TrafficPolicyInstances, xmlTPI{
			Id:                   tpi.ID,
			HostedZoneId:         tpi.HostedZoneID,
			Name:                 tpi.Name,
			TrafficPolicyId:      tpi.TrafficPolicyID,
			TrafficPolicyVersion: tpi.Version,
			TTL:                  tpi.TTL,
			State:                tpi.State,
		})
	}
	return xmlResp(http.StatusOK, resp)
}

func (p *Provider) listTrafficPolicyInstancesByPolicy(_ *http.Request) (*plugin.Response, error) {
	tpis, err := p.store.ListTrafficPolicyInstances(defaultAccountID)
	if err != nil {
		return nil, err
	}
	type xmlTPI struct {
		XMLName              xml.Name `xml:"TrafficPolicyInstance"`
		Id                   string   `xml:"Id"`
		HostedZoneId         string   `xml:"HostedZoneId"`
		Name                 string   `xml:"Name"`
		TrafficPolicyId      string   `xml:"TrafficPolicyId"`
		TrafficPolicyVersion int      `xml:"TrafficPolicyVersion"`
		TTL                  int      `xml:"TTL"`
		State                string   `xml:"State"`
	}
	type response struct {
		XMLName                xml.Name `xml:"ListTrafficPolicyInstancesResponse"`
		TrafficPolicyInstances []xmlTPI `xml:"TrafficPolicyInstances>TrafficPolicyInstance"`
	}
	resp := response{}
	for _, tpi := range tpis {
		resp.TrafficPolicyInstances = append(resp.TrafficPolicyInstances, xmlTPI{
			Id:                   tpi.ID,
			HostedZoneId:         tpi.HostedZoneID,
			Name:                 tpi.Name,
			TrafficPolicyId:      tpi.TrafficPolicyID,
			TrafficPolicyVersion: tpi.Version,
			TTL:                  tpi.TTL,
			State:                tpi.State,
		})
	}
	return xmlResp(http.StatusOK, resp)
}

func trafficPolicyInstanceIDFromPath(path string) string {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	for i, p := range parts {
		if p == "trafficpolicyinstance" && i+1 < len(parts) {
			return parts[i+1]
		}
	}
	return ""
}

func (p *Provider) getTrafficPolicyInstance(req *http.Request) (*plugin.Response, error) {
	id := trafficPolicyInstanceIDFromPath(req.URL.Path)
	if id == "" {
		return r53Error("InvalidInput", "missing traffic policy instance ID", http.StatusBadRequest), nil
	}
	tpi, err := p.store.GetTrafficPolicyInstance(id, defaultAccountID)
	if err != nil {
		if err == ErrTrafficPolicyInstanceNotFound {
			return r53Error("NoSuchTrafficPolicyInstance", "traffic policy instance not found", http.StatusNotFound), nil
		}
		return nil, err
	}
	type xmlTPI struct {
		XMLName              xml.Name `xml:"TrafficPolicyInstance"`
		Id                   string   `xml:"Id"`
		HostedZoneId         string   `xml:"HostedZoneId"`
		Name                 string   `xml:"Name"`
		TrafficPolicyId      string   `xml:"TrafficPolicyId"`
		TrafficPolicyVersion int      `xml:"TrafficPolicyVersion"`
		TTL                  int      `xml:"TTL"`
		State                string   `xml:"State"`
	}
	type response struct {
		XMLName               xml.Name `xml:"GetTrafficPolicyInstanceResponse"`
		TrafficPolicyInstance xmlTPI   `xml:"TrafficPolicyInstance"`
	}
	return xmlResp(http.StatusOK, response{
		TrafficPolicyInstance: xmlTPI{
			Id:                   tpi.ID,
			HostedZoneId:         tpi.HostedZoneID,
			Name:                 tpi.Name,
			TrafficPolicyId:      tpi.TrafficPolicyID,
			TrafficPolicyVersion: tpi.Version,
			TTL:                  tpi.TTL,
			State:                tpi.State,
		},
	})
}

// --- Cidr Collection handlers ---

func (p *Provider) createCidrCollection(req *http.Request) (*plugin.Response, error) {
	type cidrBlock struct {
		Cidr     string `xml:"Cidr"`
		Location string `xml:"Location"`
	}
	type input struct {
		XMLName    xml.Name    `xml:"CreateCidrCollectionRequest"`
		Name       string      `xml:"Name"`
		CidrBlocks []cidrBlock `xml:"CidrBlocks>CidrBlock"`
		Comment    string      `xml:"Comment"`
	}
	var in input
	if err := xml.NewDecoder(req.Body).Decode(&in); err != nil {
		return r53Error("MalformedInput", "failed to parse request body", http.StatusBadRequest), nil
	}
	if in.Name == "" {
		return r53Error("InvalidInput", "Name is required", http.StatusBadRequest), nil
	}
	ccID := generateID()
	var cidrs []string
	for _, cb := range in.CidrBlocks {
		cidrs = append(cidrs, cb.Cidr)
	}
	cc := &CidrCollection{
		CidrCollectionID: ccID,
		Name:             in.Name,
		Cidrs:            cidrs,
		State:            "Created",
		AccountID:        defaultAccountID,
		CreatedAt:        time.Now(),
	}
	if err := p.store.CreateCidrCollection(cc); err != nil {
		return nil, err
	}
	type xmlCidrCollection struct {
		XMLName          xml.Name `xml:"CidrCollection"`
		CidrCollectionId string   `xml:"CidrCollectionId"`
		Name             string   `xml:"Name"`
		State            string   `xml:"State"`
	}
	type response struct {
		XMLName        xml.Name          `xml:"CreateCidrCollectionResponse"`
		CidrCollection xmlCidrCollection `xml:"CidrCollection"`
		Location       string            `xml:"Location"`
	}
	return xmlResp(http.StatusCreated, response{
		CidrCollection: xmlCidrCollection{
			CidrCollectionId: ccID,
			Name:             in.Name,
			State:            "Created",
		},
		Location: fmt.Sprintf("/2013-04-01/cidrcollection/%s", ccID),
	})
}

func (p *Provider) changeCidrCollection(req *http.Request) (*plugin.Response, error) {
	type xmlCidr struct {
		Cidr     string `xml:"Cidr"`
		Location string `xml:"Location"`
	}
	type input struct {
		XMLName          xml.Name `xml:"ChangeCidrCollectionRequest"`
		CidrCollectionId string   `xml:"CidrCollectionId"`
		Comment          string   `xml:"Comment"`
	}
	var in input
	if err := xml.NewDecoder(req.Body).Decode(&in); err != nil {
		return r53Error("MalformedInput", "failed to parse request body", http.StatusBadRequest), nil
	}
	if in.CidrCollectionId == "" {
		return r53Error("InvalidInput", "CidrCollectionId is required", http.StatusBadRequest), nil
	}
	type response struct {
		XMLName          xml.Name `xml:"ChangeCidrCollectionResponse"`
		CidrCollectionId string   `xml:"CidrCollectionId"`
	}
	return xmlResp(http.StatusOK, response{
		CidrCollectionId: in.CidrCollectionId,
	})
}

func (p *Provider) listCidrCollections(_ *http.Request) (*plugin.Response, error) {
	ccList, err := p.store.ListCidrCollections(defaultAccountID)
	if err != nil {
		return nil, err
	}
	type xmlCidrCollection struct {
		CidrCollectionId string `xml:"CidrCollectionId"`
		Name             string `xml:"Name"`
		State            string `xml:"State"`
	}
	type response struct {
		XMLName         xml.Name            `xml:"ListCidrCollectionsResponse"`
		CidrCollections []xmlCidrCollection `xml:"CidrCollections>CidrCollection"`
		IsTruncated     bool                `xml:"IsTruncated"`
		MaxItems        int                 `xml:"MaxItems"`
		NextId          string              `xml:"NextCidrCollectionId"`
	}
	resp := response{IsTruncated: false, MaxItems: 100}
	for _, cc := range ccList {
		resp.CidrCollections = append(resp.CidrCollections, xmlCidrCollection{
			CidrCollectionId: cc.CidrCollectionID,
			Name:             cc.Name,
			State:            cc.State,
		})
	}
	return xmlResp(http.StatusOK, resp)
}

func (p *Provider) listCidrBlocks(_ *http.Request) (*plugin.Response, error) {
	type xmlCidr struct {
		Cidr     string `xml:"Cidr"`
		Location string `xml:"Location"`
	}
	type response struct {
		XMLName     xml.Name  `xml:"ListCidrBlocksResponse"`
		CidrBlocks  []xmlCidr `xml:"CidrBlocks>CidrBlock"`
		IsTruncated bool      `xml:"IsTruncated"`
		NextCidr    string    `xml:"NextCidr"`
	}
	return xmlResp(http.StatusOK, response{
		CidrBlocks: []xmlCidr{
			{Cidr: "10.0.0.0/8", Location: "us-east-1"},
		},
	})
}

func (p *Provider) listCidrLocations(_ *http.Request) (*plugin.Response, error) {
	type response struct {
		XMLName xml.Name `xml:"ListCidrLocationsResponse"`
	}
	return xmlResp(http.StatusOK, response{})
}

func (p *Provider) deleteCidrCollection(req *http.Request) (*plugin.Response, error) {
	id := cidrCollectionIDFromPath(req.URL.Path)
	if id == "" {
		return r53Error("InvalidInput", "missing CIDR collection ID", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteCidrCollection(id, defaultAccountID); err != nil {
		if err == ErrCidrCollectionNotFound {
			return r53Error("NoSuchCidrCollection", "CIDR collection not found", http.StatusNotFound), nil
		}
		return nil, err
	}
	type response struct {
		XMLName xml.Name `xml:"DeleteCidrCollectionResponse"`
	}
	return xmlResp(http.StatusOK, response{})
}

func cidrCollectionIDFromPath(path string) string {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	for i, p := range parts {
		if p == "cidrcollection" && i+1 < len(parts) {
			return parts[i+1]
		}
	}
	return ""
}

// --- Reusable Delegation Set handlers ---

func (p *Provider) createReusableDelegationSet(req *http.Request) (*plugin.Response, error) {
	type input struct {
		XMLName       xml.Name `xml:"CreateReusableDelegationSetRequest"`
		Name          string   `xml:"Name"`
		UseCustomName bool     `xml:"UseCustomName"`
	}
	var in input
	if err := xml.NewDecoder(req.Body).Decode(&in); err != nil {
		return r53Error("MalformedInput", "failed to parse request body", http.StatusBadRequest), nil
	}
	dsID := generateID()
	keyIds := []string{"ABCDEF0123456789", "GHIJKL0123456789"}
	ds := &ReusableDelegationSet{
		DelegationSetID: dsID,
		Name:            in.Name,
		KeyIds:          keyIds,
		State:           "Complete",
		AccountID:       defaultAccountID,
		CreatedAt:       time.Now(),
	}
	if err := p.store.CreateReusableDelegationSet(ds); err != nil {
		return nil, err
	}
	type xmlNameServer struct {
		NameServer string `xml:"NameServer"`
	}
	type xmlDelegationSet struct {
		DelegationSetId string        `xml:"DelegationSetId"`
		Name            string        `xml:"Name"`
		State           string        `xml:"State"`
		NameServers     xmlNameServer `xml:"NameServers"`
	}
	type response struct {
		XMLName       xml.Name         `xml:"CreateReusableDelegationSetResponse"`
		DelegationSet xmlDelegationSet `xml:"DelegationSet"`
		Location      string           `xml:"Location"`
	}
	return xmlResp(http.StatusCreated, response{
		DelegationSet: xmlDelegationSet{
			DelegationSetId: dsID,
			Name:            in.Name,
			State:           "Complete",
			NameServers:     xmlNameServer{NameServer: "ns-1.devcloud.internal"},
		},
		Location: fmt.Sprintf("/2013-04-01/delegationset/%s", dsID),
	})
}

func (p *Provider) getReusableDelegationSet(req *http.Request) (*plugin.Response, error) {
	id := delegationSetIDFromPath(req.URL.Path)
	if id == "" {
		return r53Error("InvalidInput", "missing delegation set ID", http.StatusBadRequest), nil
	}
	ds, err := p.store.GetReusableDelegationSet(id, defaultAccountID)
	if err != nil {
		if err == ErrReusableDelegationSetNotFound {
			return r53Error("NoSuchReusableDelegationSet", "delegation set not found", http.StatusNotFound), nil
		}
		return nil, err
	}
	type xmlNameServer struct {
		NameServer string `xml:"NameServer"`
	}
	type xmlDelegationSet struct {
		DelegationSetId string          `xml:"DelegationSetId"`
		Name            string          `xml:"Name"`
		State           string          `xml:"State"`
		NameServers     []xmlNameServer `xml:"NameServers>NameServer"`
	}
	type response struct {
		XMLName       xml.Name         `xml:"GetReusableDelegationSetResponse"`
		DelegationSet xmlDelegationSet `xml:"DelegationSet"`
	}
	return xmlResp(http.StatusOK, response{
		DelegationSet: xmlDelegationSet{
			DelegationSetId: ds.DelegationSetID,
			Name:            ds.Name,
			State:           ds.State,
			NameServers: []xmlNameServer{
				{NameServer: "ns-1.devcloud.internal"},
				{NameServer: "ns-2.devcloud.internal"},
			},
		},
	})
}

func (p *Provider) listReusableDelegationSets(_ *http.Request) (*plugin.Response, error) {
	dsList, err := p.store.ListReusableDelegationSets(defaultAccountID)
	if err != nil {
		return nil, err
	}
	type xmlNameServer struct {
		NameServer string `xml:"NameServer"`
	}
	type xmlDelegationSet struct {
		DelegationSetId string          `xml:"DelegationSetId"`
		Name            string          `xml:"Name"`
		State           string          `xml:"State"`
		NameServers     []xmlNameServer `xml:"NameServers>NameServer"`
	}
	type response struct {
		XMLName        xml.Name           `xml:"ListReusableDelegationSetsResponse"`
		DelegationSets []xmlDelegationSet `xml:"ReusableDelegationSets>ReusableDelegationSet"`
		IsTruncated    bool               `xml:"IsTruncated"`
		MaxItems       int                `xml:"MaxItems"`
		NextId         string             `xml:"NextDelegationSetId"`
	}
	resp := response{IsTruncated: false, MaxItems: 100}
	for _, ds := range dsList {
		resp.DelegationSets = append(resp.DelegationSets, xmlDelegationSet{
			DelegationSetId: ds.DelegationSetID,
			Name:            ds.Name,
			State:           ds.State,
			NameServers: []xmlNameServer{
				{NameServer: "ns-1.devcloud.internal"},
			},
		})
	}
	return xmlResp(http.StatusOK, resp)
}

func (p *Provider) deleteReusableDelegationSet(req *http.Request) (*plugin.Response, error) {
	id := delegationSetIDFromPath(req.URL.Path)
	if id == "" {
		return r53Error("InvalidInput", "missing delegation set ID", http.StatusBadRequest), nil
	}
	if err := p.store.DeleteReusableDelegationSet(id, defaultAccountID); err != nil {
		if err == ErrReusableDelegationSetNotFound {
			return r53Error("NoSuchReusableDelegationSet", "delegation set not found", http.StatusNotFound), nil
		}
		return nil, err
	}
	type response struct {
		XMLName xml.Name `xml:"DeleteReusableDelegationSetResponse"`
	}
	return xmlResp(http.StatusOK, response{})
}

func delegationSetIDFromPath(path string) string {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	for i, p := range parts {
		if p == "delegationset" && i+1 < len(parts) {
			return parts[i+1]
		}
	}
	return ""
}

// --- VPC Association handlers ---

func (p *Provider) associateVPC(req *http.Request) (*plugin.Response, error) {
	zoneID := zoneIDFromPath(req.URL.Path)
	type changeInfo struct {
		Id          string `xml:"Id"`
		Status      string `xml:"Status"`
		SubmittedAt string `xml:"SubmittedAt"`
	}
	type response struct {
		XMLName    xml.Name   `xml:"AssociateVPCWithHostedZoneResponse"`
		ChangeInfo changeInfo `xml:"ChangeInfo"`
	}
	return xmlResp(http.StatusOK, response{
		ChangeInfo: changeInfo{
			Id:          "/change/C" + zoneID,
			Status:      "INSYNC",
			SubmittedAt: time.Now().UTC().Format(time.RFC3339),
		},
	})
}

func (p *Provider) disassociateVPC(req *http.Request) (*plugin.Response, error) {
	zoneID := zoneIDFromPath(req.URL.Path)
	type changeInfo struct {
		Id          string `xml:"Id"`
		Status      string `xml:"Status"`
		SubmittedAt string `xml:"SubmittedAt"`
	}
	type response struct {
		XMLName    xml.Name   `xml:"DisassociateVPCFromHostedZoneResponse"`
		ChangeInfo changeInfo `xml:"ChangeInfo"`
	}
	return xmlResp(http.StatusOK, response{
		ChangeInfo: changeInfo{
			Id:          "/change/C" + zoneID,
			Status:      "INSYNC",
			SubmittedAt: time.Now().UTC().Format(time.RFC3339),
		},
	})
}
