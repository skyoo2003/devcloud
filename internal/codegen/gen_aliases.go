// SPDX-License-Identifier: Apache-2.0

// internal/codegen/gen_aliases.go
package codegen

import (
	"sort"
	"strings"

	"github.com/skyoo2003/devcloud/internal/codegen/ir"
)

// aliasesTemplateData is the data model for aliases.go.tmpl.
type aliasesTemplateData struct {
	Aliases    []aliasEntry
	Collisions []string
}

type aliasEntry struct {
	Alias     string
	ServiceID string
}

// BuildAliases derives the alias table the gateway routes with: every name a
// caller might use for a service, mapped to its DevCloud service ID.
//
// It returns the table and the sorted list of aliases more than one service
// claimed. A contested alias is left OUT of the table rather than resolved,
// because both answers are wrong for one of the claimants and picking silently
// sends that service's traffic to the other one — "es" names both opensearch and
// elasticsearchservice, "rds" names rds, docdb and neptune. The gateway holds the
// resolutions, being the only layer that can see the request and tell them
// apart; a collision with no resolution there is a test failure, not a default.
func BuildAliases(models []*ir.Model) (map[string]string, []string) {
	claims := make(map[string]map[string]bool)

	claim := func(alias, serviceID string) {
		// Spaces come from SDK ids ("CloudWatch Logs"); the wire never has them.
		alias = strings.ToLower(strings.ReplaceAll(alias, " ", ""))
		if alias == "" || serviceID == "" {
			return
		}
		if claims[alias] == nil {
			claims[alias] = make(map[string]bool)
		}
		claims[alias][serviceID] = true
	}

	for _, m := range models {
		id := m.ServiceID
		claim(id, id)
		claim(m.ShapeName, id)
		// The X-Amz-Target prefix carries an API date ("Logs_20140328"), and
		// older SDKs send the bare name. Both have to route.
		if base, _, found := strings.Cut(m.ShapeName, "_"); found {
			claim(base, id)
		}
		claim(m.SigningName, id)
		claim(m.EndpointPrefix, id)
		claim(m.ARNNamespace, id)
		claim(m.CloudFormationName, id)
		claim(m.SDKID, id)
	}

	table := make(map[string]string, len(claims))
	var collisions []string
	for alias, claimants := range claims {
		if len(claimants) > 1 {
			collisions = append(collisions, alias)
			continue
		}
		for serviceID := range claimants {
			table[alias] = serviceID
		}
	}
	sort.Strings(collisions)
	return table, collisions
}

// GenerateAliases renders the alias table into Go source.
func (g *Generator) GenerateAliases(table map[string]string, collisions []string) (string, error) {
	entries := make([]aliasEntry, 0, len(table))
	for alias, serviceID := range table {
		entries = append(entries, aliasEntry{Alias: alias, ServiceID: serviceID})
	}
	// Sorted so a regeneration diff shows model changes and nothing else.
	sort.Slice(entries, func(i, j int) bool { return entries[i].Alias < entries[j].Alias })

	return g.renderTemplate("aliases.go.tmpl", aliasesTemplateData{
		Aliases:    entries,
		Collisions: collisions,
	})
}
