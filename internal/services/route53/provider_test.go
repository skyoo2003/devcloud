// SPDX-License-Identifier: Apache-2.0

// internal/services/route53/provider_test.go
package route53

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestProvider(t *testing.T) *Provider {
	t.Helper()
	p := &Provider{}
	err := p.Init(plugin.PluginConfig{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	return p
}

func TestHostedZoneCRUD(t *testing.T) {
	p := newTestProvider(t)

	createBody := `<?xml version="1.0" encoding="UTF-8"?>
<CreateHostedZoneRequest>
  <Name>example.com.</Name>
  <CallerReference>ref-1</CallerReference>
  <HostedZoneConfig><Comment>test zone</Comment></HostedZoneConfig>
</CreateHostedZoneRequest>`

	// CreateHostedZone
	req := httptest.NewRequest(http.MethodPost, "/2013-04-01/hostedzone", strings.NewReader(createBody))
	resp, err := p.HandleRequest(context.Background(), "CreateHostedZone", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusCreated, resp.StatusCode)

	// parse zone ID from response
	type createResp struct {
		HostedZone struct {
			Id string `xml:"Id"`
		} `xml:"HostedZone"`
	}
	var cr createResp
	require.NoError(t, xml.Unmarshal(resp.Body, &cr))
	fullID := cr.HostedZone.Id // e.g. /hostedzone/ZXXX
	zoneID := strings.TrimPrefix(fullID, "/hostedzone/")
	require.NotEmpty(t, zoneID)

	// GetHostedZone
	req = httptest.NewRequest(http.MethodGet, "/2013-04-01/hostedzone/"+zoneID, nil)
	resp, err = p.HandleRequest(context.Background(), "GetHostedZone", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(resp.Body), "example.com.")

	// ListHostedZones
	req = httptest.NewRequest(http.MethodGet, "/2013-04-01/hostedzone", nil)
	resp, err = p.HandleRequest(context.Background(), "ListHostedZones", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(resp.Body), zoneID)

	// DeleteHostedZone
	req = httptest.NewRequest(http.MethodDelete, "/2013-04-01/hostedzone/"+zoneID, nil)
	resp, err = p.HandleRequest(context.Background(), "DeleteHostedZone", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// GetHostedZone after delete → 404
	req = httptest.NewRequest(http.MethodGet, "/2013-04-01/hostedzone/"+zoneID, nil)
	resp, err = p.HandleRequest(context.Background(), "GetHostedZone", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestChangeResourceRecordSets(t *testing.T) {
	p := newTestProvider(t)

	// create zone
	createBody := `<?xml version="1.0"?><CreateHostedZoneRequest><Name>test.com.</Name><CallerReference>r1</CallerReference></CreateHostedZoneRequest>`
	req := httptest.NewRequest(http.MethodPost, "/2013-04-01/hostedzone", strings.NewReader(createBody))
	resp, err := p.HandleRequest(context.Background(), "CreateHostedZone", req)
	require.NoError(t, err)
	var cr struct {
		HostedZone struct {
			Id string `xml:"Id"`
		} `xml:"HostedZone"`
	}
	require.NoError(t, xml.Unmarshal(resp.Body, &cr))
	zoneID := strings.TrimPrefix(cr.HostedZone.Id, "/hostedzone/")

	// UPSERT A record
	changeBatch := `<?xml version="1.0"?>
<ChangeResourceRecordSetsRequest>
  <ChangeBatch>
    <Changes>
      <Change>
        <Action>UPSERT</Action>
        <ResourceRecordSet>
          <Name>www.test.com.</Name>
          <Type>A</Type>
          <TTL>300</TTL>
          <ResourceRecords>
            <ResourceRecord><Value>1.2.3.4</Value></ResourceRecord>
          </ResourceRecords>
        </ResourceRecordSet>
      </Change>
    </Changes>
  </ChangeBatch>
</ChangeResourceRecordSetsRequest>`
	req = httptest.NewRequest(http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/rrset", strings.NewReader(changeBatch))
	resp, err = p.HandleRequest(context.Background(), "ChangeResourceRecordSets", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(resp.Body), "INSYNC")

	// UPSERT again (update TTL)
	changeBatch2 := strings.ReplaceAll(changeBatch, "<TTL>300</TTL>", "<TTL>600</TTL>")
	req = httptest.NewRequest(http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/rrset", strings.NewReader(changeBatch2))
	resp, err = p.HandleRequest(context.Background(), "ChangeResourceRecordSets", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// DELETE record
	deleteBody := `<?xml version="1.0"?>
<ChangeResourceRecordSetsRequest>
  <ChangeBatch>
    <Changes>
      <Change>
        <Action>DELETE</Action>
        <ResourceRecordSet>
          <Name>www.test.com.</Name>
          <Type>A</Type>
          <TTL>600</TTL>
          <ResourceRecords>
            <ResourceRecord><Value>1.2.3.4</Value></ResourceRecord>
          </ResourceRecords>
        </ResourceRecordSet>
      </Change>
    </Changes>
  </ChangeBatch>
</ChangeResourceRecordSetsRequest>`
	req = httptest.NewRequest(http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/rrset", strings.NewReader(deleteBody))
	resp, err = p.HandleRequest(context.Background(), "ChangeResourceRecordSets", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestListRecordSets(t *testing.T) {
	p := newTestProvider(t)

	// create zone
	createBody := `<?xml version="1.0"?><CreateHostedZoneRequest><Name>list.com.</Name><CallerReference>r2</CallerReference></CreateHostedZoneRequest>`
	req := httptest.NewRequest(http.MethodPost, "/2013-04-01/hostedzone", strings.NewReader(createBody))
	resp, err := p.HandleRequest(context.Background(), "CreateHostedZone", req)
	require.NoError(t, err)
	var cr struct {
		HostedZone struct {
			Id string `xml:"Id"`
		} `xml:"HostedZone"`
	}
	require.NoError(t, xml.Unmarshal(resp.Body, &cr))
	zoneID := strings.TrimPrefix(cr.HostedZone.Id, "/hostedzone/")

	for _, name := range []string{"a.list.com.", "b.list.com.", "c.list.com."} {
		body := fmt.Sprintf(`<?xml version="1.0"?><ChangeResourceRecordSetsRequest><ChangeBatch><Changes><Change>
			<Action>CREATE</Action>
			<ResourceRecordSet><Name>%s</Name><Type>A</Type><TTL>60</TTL>
			<ResourceRecords><ResourceRecord><Value>10.0.0.1</Value></ResourceRecord></ResourceRecords>
			</ResourceRecordSet></Change></Changes></ChangeBatch></ChangeResourceRecordSetsRequest>`, name)
		req = httptest.NewRequest(http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/rrset", strings.NewReader(body))
		resp, err = p.HandleRequest(context.Background(), "ChangeResourceRecordSets", req)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
	}

	// list
	req = httptest.NewRequest(http.MethodGet, "/2013-04-01/hostedzone/"+zoneID+"/rrset", nil)
	resp, err = p.HandleRequest(context.Background(), "ListResourceRecordSets", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body := string(resp.Body)
	assert.Contains(t, body, "a.list.com.")
	assert.Contains(t, body, "b.list.com.")
	assert.Contains(t, body, "c.list.com.")
}

func TestDNSSECHandlers(t *testing.T) {
	p := newTestProvider(t)

	// Create zone first
	createBody := `<?xml version="1.0"?><CreateHostedZoneRequest><Name>dnssec-test.com.</Name><CallerReference>r-dnssec</CallerReference></CreateHostedZoneRequest>`
	req := httptest.NewRequest(http.MethodPost, "/2013-04-01/hostedzone", strings.NewReader(createBody))
	resp, err := p.HandleRequest(context.Background(), "CreateHostedZone", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusCreated, resp.StatusCode)

	var cr struct {
		HostedZone struct {
			Id string `xml:"Id"`
		} `xml:"HostedZone"`
	}
	require.NoError(t, xml.Unmarshal(resp.Body, &cr))
	zoneID := strings.TrimPrefix(cr.HostedZone.Id, "/hostedzone/")

	// GetDNSSEC (should return NotEnabled initially)
	req = httptest.NewRequest(http.MethodGet, "/2013-04-01/hostedzone/"+zoneID+"/dnssec", nil)
	resp, err = p.HandleRequest(context.Background(), "GetDNSSEC", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(resp.Body), "NOT_SIGNING")

	// EnableDNSSEC
	req = httptest.NewRequest(http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/enable-dnssec", nil)
	resp, err = p.HandleRequest(context.Background(), "EnableHostedZoneDNSSEC", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(resp.Body), "INSYNC")

	// GetDNSSEC after enable
	req = httptest.NewRequest(http.MethodGet, "/2013-04-01/hostedzone/"+zoneID+"/dnssec", nil)
	resp, err = p.HandleRequest(context.Background(), "GetDNSSEC", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(resp.Body), "SIGNING")

	// DisableDNSSEC
	req = httptest.NewRequest(http.MethodPost, "/2013-04-01/hostedzone/"+zoneID+"/disable-dnssec", nil)
	resp, err = p.HandleRequest(context.Background(), "DisableHostedZoneDNSSEC", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(resp.Body), "INSYNC")

	// GetDNSSEC after disable
	req = httptest.NewRequest(http.MethodGet, "/2013-04-01/hostedzone/"+zoneID+"/dnssec", nil)
	resp, err = p.HandleRequest(context.Background(), "GetDNSSEC", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(resp.Body), "NOT_SIGNING")
}

func TestKeySigningKeyHandlers(t *testing.T) {
	p := newTestProvider(t)

	// Create zone first
	createBody := `<?xml version="1.0"?><CreateHostedZoneRequest><Name>ksk-test.com.</Name><CallerReference>r-ksk</CallerReference></CreateHostedZoneRequest>`
	req := httptest.NewRequest(http.MethodPost, "/2013-04-01/hostedzone", strings.NewReader(createBody))
	resp, err := p.HandleRequest(context.Background(), "CreateHostedZone", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusCreated, resp.StatusCode)

	var cr struct {
		HostedZone struct {
			Id string `xml:"Id"`
		} `xml:"HostedZone"`
	}
	require.NoError(t, xml.Unmarshal(resp.Body, &cr))
	zoneID := strings.TrimPrefix(cr.HostedZone.Id, "/hostedzone/")

	// CreateKeySigningKey
	createKSK := `<?xml version="1.0"?><CreateKeySigningKeyRequest><Name>my-ksk</Name><HostedZoneId>/hostedzone/` + zoneID + `</HostedZoneId><Use>SigningOnly</Use><Algorithm>ECDSA_P256_SHA256</Algorithm><KeySpec>ECDSA_P256</KeySpec></CreateKeySigningKeyRequest>`
	req = httptest.NewRequest(http.MethodPost, "/2013-04-01/keysigningkey", strings.NewReader(createKSK))
	resp, err = p.HandleRequest(context.Background(), "CreateKeySigningKey", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusCreated, resp.StatusCode)

	var createResp struct {
		KeySigningKey struct {
			Name  string `xml:"Name"`
			KeyId string `xml:"KeyId"`
			State string `xml:"State"`
		} `xml:"KeySigningKey"`
	}
	require.NoError(t, xml.Unmarshal(resp.Body, &createResp))
	assert.Equal(t, "Pending", createResp.KeySigningKey.State)

	// ListKeySigningKeys
	req = httptest.NewRequest(http.MethodGet, "/2013-04-01/keysigningkey", nil)
	resp, err = p.HandleRequest(context.Background(), "ListKeySigningKeys", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(resp.Body), "my-ksk")
	assert.Contains(t, string(resp.Body), zoneID)

	// ActivateKeySigningKey
	req = httptest.NewRequest(http.MethodPost, "/2013-04-01/keysigningkey/"+zoneID+"/my-ksk/activate", nil)
	resp, err = p.HandleRequest(context.Background(), "ActivateKeySigningKey", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// DeactivateKeySigningKey
	req = httptest.NewRequest(http.MethodPost, "/2013-04-01/keysigningkey/"+zoneID+"/my-ksk/deactivate", nil)
	resp, err = p.HandleRequest(context.Background(), "DeactivateKeySigningKey", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// DeleteKeySigningKey
	req = httptest.NewRequest(http.MethodDelete, "/2013-04-01/keysigningkey/"+zoneID+"/my-ksk", nil)
	resp, err = p.HandleRequest(context.Background(), "DeleteKeySigningKey", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestTrafficPolicyInstanceHandlers(t *testing.T) {
	p := newTestProvider(t)

	// Create zone first
	createBody := `<?xml version="1.0"?><CreateHostedZoneRequest><Name>tppi-test.com.</Name><CallerReference>r-tppi</CallerReference></CreateHostedZoneRequest>`
	req := httptest.NewRequest(http.MethodPost, "/2013-04-01/hostedzone", strings.NewReader(createBody))
	resp, err := p.HandleRequest(context.Background(), "CreateHostedZone", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusCreated, resp.StatusCode)

	var cr struct {
		HostedZone struct {
			Id string `xml:"Id"`
		} `xml:"HostedZone"`
	}
	require.NoError(t, xml.Unmarshal(resp.Body, &cr))
	zoneID := strings.TrimPrefix(cr.HostedZone.Id, "/hostedzone/")

	// CreateTrafficPolicyInstance
	createTPBody := `<?xml version="1.0"?><CreateTrafficPolicyInstanceRequest><Name>my-instance</Name><HostedZoneId>/hostedzone/` + zoneID + `</HostedZoneId><TrafficPolicyId>policy-001</TrafficPolicyId><TrafficPolicyVersion>1</TrafficPolicyVersion><TTL>300</TTL></CreateTrafficPolicyInstanceRequest>`
	req = httptest.NewRequest(http.MethodPost, "/2013-04-01/trafficpolicyinstance", strings.NewReader(createTPBody))
	resp, err = p.HandleRequest(context.Background(), "CreateTrafficPolicyInstance", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusCreated, resp.StatusCode)

	var createResp struct {
		TrafficPolicyInstance struct {
			Id string `xml:"Id"`
		} `xml:"TrafficPolicyInstance"`
	}
	require.NoError(t, xml.Unmarshal(resp.Body, &createResp))
	tpiID := createResp.TrafficPolicyInstance.Id
	assert.NotEmpty(t, tpiID)

	// GetTrafficPolicyInstance
	req = httptest.NewRequest(http.MethodGet, "/2013-04-01/trafficpolicyinstance/"+tpiID, nil)
	resp, err = p.HandleRequest(context.Background(), "GetTrafficPolicyInstance", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(resp.Body), "my-instance")

	// ListTrafficPolicyInstances
	req = httptest.NewRequest(http.MethodGet, "/2013-04-01/trafficpolicyinstances", nil)
	resp, err = p.HandleRequest(context.Background(), "ListTrafficPolicyInstances", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(resp.Body), "my-instance")

	// UpdateTrafficPolicyInstance
	updateBody := `<?xml version="1.0"?><UpdateTrafficPolicyInstanceRequest><Name>updated-instance</Name><TrafficPolicyId>policy-002</TrafficPolicyId><TrafficPolicyVersion>1</TrafficPolicyVersion><TTL>600</TTL></UpdateTrafficPolicyInstanceRequest>`
	req = httptest.NewRequest(http.MethodPost, "/2013-04-01/trafficpolicyinstance/"+tpiID, strings.NewReader(updateBody))
	resp, err = p.HandleRequest(context.Background(), "UpdateTrafficPolicyInstance", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(resp.Body), "updated-instance")

	// DeleteTrafficPolicyInstance
	req = httptest.NewRequest(http.MethodDelete, "/2013-04-01/trafficpolicyinstance/"+tpiID, nil)
	resp, err = p.HandleRequest(context.Background(), "DeleteTrafficPolicyInstance", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Get after delete → 404
	req = httptest.NewRequest(http.MethodGet, "/2013-04-01/trafficpolicyinstance/"+tpiID, nil)
	resp, err = p.HandleRequest(context.Background(), "GetTrafficPolicyInstance", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestCidrCollectionHandlers(t *testing.T) {
	p := newTestProvider(t)

	// CreateCidrCollection
	createBody := `<?xml version="1.0"?><CreateCidrCollectionRequest><Name>my-cidr-collection</Name><CidrBlocks><CidrBlock><Cidr>10.0.0.0/8</Cidr><Location>us-east-1</Location></CidrBlock><CidrBlock><Cidr>192.168.0.0/16</Cidr><Location>us-west-2</Location></CidrBlock></CidrBlocks></CreateCidrCollectionRequest>`
	req := httptest.NewRequest(http.MethodPost, "/2013-04-01/cidrcollection", strings.NewReader(createBody))
	resp, err := p.HandleRequest(context.Background(), "CreateCidrCollection", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusCreated, resp.StatusCode)

	var createResp struct {
		Collection struct {
			Id   string `xml:"Id"`
			Name string `xml:"Name"`
		} `xml:"Collection"`
	}
	require.NoError(t, xml.Unmarshal(resp.Body, &createResp))
	ccID := createResp.Collection.Id
	assert.Equal(t, "my-cidr-collection", createResp.Collection.Name)

	// ListCidrCollections
	req = httptest.NewRequest(http.MethodGet, "/2013-04-01/cidrcollection", nil)
	resp, err = p.HandleRequest(context.Background(), "ListCidrCollections", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(resp.Body), "my-cidr-collection")
	assert.Contains(t, string(resp.Body), ccID)

	// ListCidrBlocks
	req = httptest.NewRequest(http.MethodGet, "/2013-04-01/cidrcollection/"+ccID+"/cidrblocks", nil)
	resp, err = p.HandleRequest(context.Background(), "ListCidrBlocks", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// ListCidrLocations
	req = httptest.NewRequest(http.MethodGet, "/2013-04-01/cidrcollection/"+ccID+"/locations", nil)
	resp, err = p.HandleRequest(context.Background(), "ListCidrLocations", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// DeleteCidrCollection
	req = httptest.NewRequest(http.MethodDelete, "/2013-04-01/cidrcollection/"+ccID, nil)
	resp, err = p.HandleRequest(context.Background(), "DeleteCidrCollection", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// List after delete should not contain it
	req = httptest.NewRequest(http.MethodGet, "/2013-04-01/cidrcollection", nil)
	resp, err = p.HandleRequest(context.Background(), "ListCidrCollections", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.NotContains(t, string(resp.Body), "my-cidr-collection")
}

func TestReusableDelegationSetHandlers(t *testing.T) {
	p := newTestProvider(t)

	// CreateReusableDelegationSet
	createBody := `<?xml version="1.0"?><CreateReusableDelegationSetRequest><Name>my-delegation-set</Name></CreateReusableDelegationSetRequest>`
	req := httptest.NewRequest(http.MethodPost, "/2013-04-01/delegationset", strings.NewReader(createBody))
	resp, err := p.HandleRequest(context.Background(), "CreateReusableDelegationSet", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusCreated, resp.StatusCode)

	var createResp struct {
		DelegationSet struct {
			Id              string   `xml:"Id"`
			CallerReference string   `xml:"CallerReference"`
			NameServers     []string `xml:"NameServers>NameServer"`
		} `xml:"DelegationSet"`
	}
	require.NoError(t, xml.Unmarshal(resp.Body, &createResp))
	dsID := createResp.DelegationSet.Id
	assert.Equal(t, "rds-ref-1", createResp.DelegationSet.CallerReference)

	// GetReusableDelegationSet
	req = httptest.NewRequest(http.MethodGet, "/2013-04-01/delegationset/"+dsID, nil)
	resp, err = p.HandleRequest(context.Background(), "GetReusableDelegationSet", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(resp.Body), dsID)

	// ListReusableDelegationSets
	req = httptest.NewRequest(http.MethodGet, "/2013-04-01/delegationset", nil)
	resp, err = p.HandleRequest(context.Background(), "ListReusableDelegationSets", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, string(resp.Body), dsID)

	// DeleteReusableDelegationSet
	req = httptest.NewRequest(http.MethodDelete, "/2013-04-01/delegationset/"+dsID, nil)
	resp, err = p.HandleRequest(context.Background(), "DeleteReusableDelegationSet", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Get after delete → 404
	req = httptest.NewRequest(http.MethodGet, "/2013-04-01/delegationset/"+dsID, nil)
	resp, err = p.HandleRequest(context.Background(), "GetReusableDelegationSet", req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}
