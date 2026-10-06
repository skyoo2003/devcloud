// SPDX-License-Identifier: Apache-2.0
package sns

import (
	"encoding/xml"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSMSSettingsUnitHasResultWrapper(t *testing.T) {
	p := newTestProvider(t)
	r := snsCall(t, p, "SetSMSAttributes", map[string]string{"attributes.entry.1.key": "DefaultSMSType", "attributes.entry.1.value": "Transactional"})
	require.Equal(t, 200, r.StatusCode)
	var v struct {
		Result *struct{} `xml:"SetSMSAttributesResult"`
	}
	require.NoError(t, xml.Unmarshal(r.Body, &v))
	require.NotNil(t, v.Result)
}
