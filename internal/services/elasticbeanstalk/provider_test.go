// SPDX-License-Identifier: Apache-2.0

package elasticbeanstalk

import (
	"context"
	"encoding/xml"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/skyoo2003/devcloud/internal/plugin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestProvider(t *testing.T) *Provider {
	t.Helper()
	p := &Provider{}
	require.NoError(t, p.Init(plugin.PluginConfig{DataDir: t.TempDir()}))
	t.Cleanup(func() { _ = p.Shutdown(context.Background()) })
	return p
}

func callQuery(t *testing.T, p *Provider, action string, params map[string]string) *plugin.Response {
	t.Helper()
	form := url.Values{}
	form.Set("Action", action)
	for k, v := range params {
		form.Set(k, v)
	}
	req := httptest.NewRequest("POST", "/", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := p.HandleRequest(context.Background(), "", req)
	require.NoError(t, err)
	return resp
}

func TestApplicationCRUD(t *testing.T) {
	p := newTestProvider(t)

	// Create
	resp := callQuery(t, p, "CreateApplication", map[string]string{
		"ApplicationName": "my-app",
		"Description":     "test app",
	})
	assert.Equal(t, 200, resp.StatusCode, string(resp.Body))

	type createResp struct {
		Application struct {
			ApplicationName string `xml:"ApplicationName"`
			ApplicationArn  string `xml:"ApplicationArn"`
			Description     string `xml:"Description"`
		} `xml:"CreateApplicationResult>Application"`
	}
	var cr createResp
	require.NoError(t, xml.Unmarshal(resp.Body, &cr))
	assert.Equal(t, "my-app", cr.Application.ApplicationName)
	assert.Contains(t, cr.Application.ApplicationArn, "arn:aws:elasticbeanstalk")
	assert.Equal(t, "test app", cr.Application.Description)

	// Describe
	descHttpResp := callQuery(t, p, "DescribeApplications", map[string]string{
		"ApplicationNames.member.1": "my-app",
	})
	assert.Equal(t, 200, descHttpResp.StatusCode)

	type appListResp struct {
		Applications []struct {
			ApplicationName string `xml:"ApplicationName"`
		} `xml:"DescribeApplicationsResult>Applications>member"`
	}
	var dr appListResp
	require.NoError(t, xml.Unmarshal(descHttpResp.Body, &dr))
	require.Len(t, dr.Applications, 1)
	assert.Equal(t, "my-app", dr.Applications[0].ApplicationName)

	// Update
	updateHttpResp := callQuery(t, p, "UpdateApplication", map[string]string{
		"ApplicationName": "my-app",
		"Description":     "updated desc",
	})
	assert.Equal(t, 200, updateHttpResp.StatusCode, string(updateHttpResp.Body))

	type appUpdateResp struct {
		Application struct {
			Description string `xml:"Description"`
		} `xml:"UpdateApplicationResult>Application"`
	}
	var ur appUpdateResp
	require.NoError(t, xml.Unmarshal(updateHttpResp.Body, &ur))
	assert.Equal(t, "updated desc", ur.Application.Description)

	// Delete
	delResp := callQuery(t, p, "DeleteApplication", map[string]string{
		"ApplicationName": "my-app",
	})
	assert.Equal(t, 200, delResp.StatusCode, string(delResp.Body))

	// Should be gone
	descHttpResp2 := callQuery(t, p, "DescribeApplications", map[string]string{
		"ApplicationNames.member.1": "my-app",
	})
	var dr2 appListResp
	require.NoError(t, xml.Unmarshal(descHttpResp2.Body, &dr2))
	assert.Empty(t, dr2.Applications)
}

func TestApplicationVersionCRUD(t *testing.T) {
	p := newTestProvider(t)

	callQuery(t, p, "CreateApplication", map[string]string{"ApplicationName": "ver-app"})

	// Create version
	resp := callQuery(t, p, "CreateApplicationVersion", map[string]string{
		"ApplicationName": "ver-app",
		"VersionLabel":    "v1.0",
		"Description":     "first version",
	})
	assert.Equal(t, 200, resp.StatusCode, string(resp.Body))

	type createVersionResp struct {
		Version struct {
			ApplicationName string `xml:"ApplicationName"`
			VersionLabel    string `xml:"VersionLabel"`
			Status          string `xml:"Status"`
		} `xml:"CreateApplicationVersionResult>ApplicationVersion"`
	}
	var cv createVersionResp
	require.NoError(t, xml.Unmarshal(resp.Body, &cv))
	assert.Equal(t, "ver-app", cv.Version.ApplicationName)
	assert.Equal(t, "v1.0", cv.Version.VersionLabel)
	assert.Equal(t, "PROCESSED", cv.Version.Status)

	// Describe versions
	descResp := callQuery(t, p, "DescribeApplicationVersions", map[string]string{
		"ApplicationName": "ver-app",
	})
	assert.Equal(t, 200, descResp.StatusCode)

	type descVersionsResp struct {
		Versions []struct {
			VersionLabel string `xml:"VersionLabel"`
		} `xml:"DescribeApplicationVersionsResult>ApplicationVersions>member"`
	}
	var dv descVersionsResp
	require.NoError(t, xml.Unmarshal(descResp.Body, &dv))
	require.Len(t, dv.Versions, 1)
	assert.Equal(t, "v1.0", dv.Versions[0].VersionLabel)

	// Update version
	updateResp := callQuery(t, p, "UpdateApplicationVersion", map[string]string{
		"ApplicationName": "ver-app",
		"VersionLabel":    "v1.0",
		"Description":     "updated",
	})
	assert.Equal(t, 200, updateResp.StatusCode)

	// Delete version
	delResp := callQuery(t, p, "DeleteApplicationVersion", map[string]string{
		"ApplicationName": "ver-app",
		"VersionLabel":    "v1.0",
	})
	assert.Equal(t, 200, delResp.StatusCode)

	// Should be gone
	descResp2 := callQuery(t, p, "DescribeApplicationVersions", map[string]string{
		"ApplicationName": "ver-app",
	})
	var dv2 descVersionsResp
	require.NoError(t, xml.Unmarshal(descResp2.Body, &dv2))
	assert.Empty(t, dv2.Versions)
}

func TestEnvironmentCRUD(t *testing.T) {
	p := newTestProvider(t)

	callQuery(t, p, "CreateApplication", map[string]string{"ApplicationName": "env-app"})

	// Create environment
	resp := callQuery(t, p, "CreateEnvironment", map[string]string{
		"ApplicationName": "env-app",
		"EnvironmentName": "my-env",
		"CNAMEPrefix":     "my-env",
		"Description":     "test env",
	})
	assert.Equal(t, 200, resp.StatusCode, string(resp.Body))

	type createEnvResp struct {
		EnvironmentId   string `xml:"CreateEnvironmentResult>EnvironmentId"`
		EnvironmentName string `xml:"CreateEnvironmentResult>EnvironmentName"`
		ApplicationName string `xml:"CreateEnvironmentResult>ApplicationName"`
		Status          string `xml:"CreateEnvironmentResult>Status"`
		Health          string `xml:"CreateEnvironmentResult>Health"`
		CNAME           string `xml:"CreateEnvironmentResult>CNAME"`
	}
	var ce createEnvResp
	require.NoError(t, xml.Unmarshal(resp.Body, &ce))
	assert.Equal(t, "my-env", ce.EnvironmentName)
	assert.Equal(t, "env-app", ce.ApplicationName)
	assert.Equal(t, "Ready", ce.Status)
	assert.Equal(t, "Green", ce.Health)
	assert.Contains(t, ce.CNAME, "elasticbeanstalk.com")
	envID := ce.EnvironmentId

	// Describe environments
	descResp := callQuery(t, p, "DescribeEnvironments", map[string]string{
		"ApplicationName": "env-app",
	})
	assert.Equal(t, 200, descResp.StatusCode)

	type descEnvsResp struct {
		Environments []struct {
			EnvironmentName string `xml:"EnvironmentName"`
		} `xml:"DescribeEnvironmentsResult>Environments>member"`
	}
	var de descEnvsResp
	require.NoError(t, xml.Unmarshal(descResp.Body, &de))
	require.Len(t, de.Environments, 1)
	assert.Equal(t, "my-env", de.Environments[0].EnvironmentName)

	// Terminate environment
	termResp := callQuery(t, p, "TerminateEnvironment", map[string]string{
		"EnvironmentId": envID,
	})
	assert.Equal(t, 200, termResp.StatusCode, string(termResp.Body))

	type termEnvResp struct {
		Status string `xml:"TerminateEnvironmentResult>Status"`
	}
	var te termEnvResp
	require.NoError(t, xml.Unmarshal(termResp.Body, &te))
	assert.Equal(t, "Terminated", te.Status)
}

func TestConfigTemplateCRUD(t *testing.T) {
	p := newTestProvider(t)

	callQuery(t, p, "CreateApplication", map[string]string{"ApplicationName": "tmpl-app"})

	// Create template
	resp := callQuery(t, p, "CreateConfigurationTemplate", map[string]string{
		"ApplicationName":   "tmpl-app",
		"TemplateName":      "my-template",
		"Description":       "test template",
		"SolutionStackName": "64bit Amazon Linux 2023 v4.0.0 running Docker",
	})
	assert.Equal(t, 200, resp.StatusCode, string(resp.Body))

	type createTmplResp struct {
		ApplicationName string `xml:"CreateConfigurationTemplateResult>ApplicationName"`
		TemplateName    string `xml:"CreateConfigurationTemplateResult>TemplateName"`
		Description     string `xml:"CreateConfigurationTemplateResult>Description"`
	}
	var ct createTmplResp
	require.NoError(t, xml.Unmarshal(resp.Body, &ct))
	assert.Equal(t, "tmpl-app", ct.ApplicationName)
	assert.Equal(t, "my-template", ct.TemplateName)
	assert.Equal(t, "test template", ct.Description)

	// Describe configuration settings
	descResp := callQuery(t, p, "DescribeConfigurationSettings", map[string]string{
		"ApplicationName": "tmpl-app",
		"TemplateName":    "my-template",
	})
	assert.Equal(t, 200, descResp.StatusCode, string(descResp.Body))

	type descSettingsResp struct {
		Settings []struct {
			TemplateName string `xml:"TemplateName"`
		} `xml:"DescribeConfigurationSettingsResult>ConfigurationSettings>member"`
	}
	var ds descSettingsResp
	require.NoError(t, xml.Unmarshal(descResp.Body, &ds))
	require.Len(t, ds.Settings, 1)
	assert.Equal(t, "my-template", ds.Settings[0].TemplateName)

	// Update template
	updateResp := callQuery(t, p, "UpdateConfigurationTemplate", map[string]string{
		"ApplicationName": "tmpl-app",
		"TemplateName":    "my-template",
		"Description":     "updated template",
	})
	assert.Equal(t, 200, updateResp.StatusCode)

	// Delete template
	delResp := callQuery(t, p, "DeleteConfigurationTemplate", map[string]string{
		"ApplicationName": "tmpl-app",
		"TemplateName":    "my-template",
	})
	assert.Equal(t, 200, delResp.StatusCode)

	// Should be gone
	errResp := callQuery(t, p, "DescribeConfigurationSettings", map[string]string{
		"ApplicationName": "tmpl-app",
		"TemplateName":    "my-template",
	})
	assert.Equal(t, 400, errResp.StatusCode)
}

func TestTags(t *testing.T) {
	p := newTestProvider(t)

	callQuery(t, p, "CreateApplication", map[string]string{"ApplicationName": "tag-app"})

	arn := "arn:aws:elasticbeanstalk:us-east-1:000000000000:application/tag-app"

	// Add tags
	addResp := callQuery(t, p, "UpdateTagsForResource", map[string]string{
		"ResourceArn":              arn,
		"TagsToAdd.member.1.Key":   "env",
		"TagsToAdd.member.1.Value": "prod",
		"TagsToAdd.member.2.Key":   "team",
		"TagsToAdd.member.2.Value": "backend",
	})
	assert.Equal(t, 200, addResp.StatusCode, string(addResp.Body))

	// List tags
	listResp := callQuery(t, p, "ListTagsForResource", map[string]string{
		"ResourceArn": arn,
	})
	assert.Equal(t, 200, listResp.StatusCode, string(listResp.Body))

	type listTagsResp struct {
		Tags []struct {
			Key   string `xml:"Key"`
			Value string `xml:"Value"`
		} `xml:"ListTagsForResourceResult>ResourceTags>member"`
	}
	var lt listTagsResp
	require.NoError(t, xml.Unmarshal(listResp.Body, &lt))
	assert.Len(t, lt.Tags, 2)

	tagMap := make(map[string]string)
	for _, tag := range lt.Tags {
		tagMap[tag.Key] = tag.Value
	}
	assert.Equal(t, "prod", tagMap["env"])
	assert.Equal(t, "backend", tagMap["team"])

	// Remove a tag
	removeResp := callQuery(t, p, "UpdateTagsForResource", map[string]string{
		"ResourceArn":           arn,
		"TagsToRemove.member.1": "team",
	})
	assert.Equal(t, 200, removeResp.StatusCode)

	// Verify only one tag remains
	listResp2 := callQuery(t, p, "ListTagsForResource", map[string]string{
		"ResourceArn": arn,
	})
	var lt2 listTagsResp
	require.NoError(t, xml.Unmarshal(listResp2.Body, &lt2))
	assert.Len(t, lt2.Tags, 1)
	assert.Equal(t, "env", lt2.Tags[0].Key)
}
