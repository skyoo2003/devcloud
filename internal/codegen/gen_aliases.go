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
	Siblings   []siblingEntry
}

type siblingEntry struct {
	SigningName string
	ServiceIDs  []string
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
		switch {
		case len(claimants) == 1:
			for serviceID := range claimants {
				table[alias] = serviceID
			}
		default:
			// Every runtime split-out signs with its parent's name —
			// sagemaker-runtime as "sagemaker", forecastquery as "forecast" —
			// which makes the parent's own identifier look contested. It is not:
			// when exactly one claimant IS the alias, that service is naming
			// itself and the rest are borrowing. Resolving that here is reading
			// the models, not picking a winner.
			if owner, ok := selfNamedClaimant(alias, claimants); ok {
				table[alias] = owner
				continue
			}
			collisions = append(collisions, alias)
		}
	}
	sort.Strings(collisions)
	return table, collisions
}

// selfNamedClaimant returns the one claimant whose service ID is the alias, if
// there is exactly one. Hyphens are ignored on the alias side: a service ID
// never contains one, but the names services publish do ("bedrock-agentcore"),
// and that punctuation is not a difference in identity.
//
// Exactly one, deliberately. Two services that both name themselves the same
// thing is a genuine conflict and must stay contested.
func selfNamedClaimant(alias string, claimants map[string]bool) (string, bool) {
	bare := strings.ReplaceAll(alias, "-", "")
	var owner string
	var found int
	for serviceID := range claimants {
		if serviceID == alias || serviceID == bare {
			owner = serviceID
			found++
		}
	}
	if found != 1 {
		return "", false
	}
	return owner, true
}

// BuildSigningSiblings groups services by the SigV4 signing name they sign
// with, keeping only the names more than one service shares.
//
// AWS splits a service's data plane, runtime, or successor version into its own
// SDK client but leaves it signing with the parent's name: mediastore-data
// signs "mediastore", service-catalog-appregistry signs "servicecatalog",
// sagemaker-runtime signs "sagemaker". 18 signing names are shared this way,
// across 50 services.
//
// The alias table can only answer with one service per name, so it answers with
// the parent and every child becomes registered-but-unreachable for any caller
// that routes by credential scope. The gateway needs the whole group to pick the
// member that actually models the request — which is why this is derived here
// rather than hand-listed there, where it would grow an entry per service and
// rot exactly like the switch BuildAliases replaced.
func BuildSigningSiblings(models []*ir.Model) map[string][]string {
	groups := make(map[string][]string)
	for _, m := range models {
		name := strings.ToLower(strings.ReplaceAll(m.SigningName, " ", ""))
		if name == "" || m.ServiceID == "" {
			continue
		}
		groups[name] = append(groups[name], m.ServiceID)
	}
	for name, ids := range groups {
		if len(ids) < 2 {
			delete(groups, name)
			continue
		}
		sort.Strings(ids)
		groups[name] = ids
	}
	return groups
}

// GenerateAliases renders the alias table into Go source.
func (g *Generator) GenerateAliases(table map[string]string, collisions []string, siblings map[string][]string) (string, error) {
	entries := make([]aliasEntry, 0, len(table))
	for alias, serviceID := range table {
		entries = append(entries, aliasEntry{Alias: alias, ServiceID: serviceID})
	}
	// Sorted so a regeneration diff shows model changes and nothing else.
	sort.Slice(entries, func(i, j int) bool { return entries[i].Alias < entries[j].Alias })

	sibs := make([]siblingEntry, 0, len(siblings))
	for name, ids := range siblings {
		sibs = append(sibs, siblingEntry{SigningName: name, ServiceIDs: ids})
	}
	sort.Slice(sibs, func(i, j int) bool { return sibs[i].SigningName < sibs[j].SigningName })

	return g.renderTemplate("aliases.go.tmpl", aliasesTemplateData{
		Aliases:    entries,
		Collisions: collisions,
		Siblings:   sibs,
	})
}
