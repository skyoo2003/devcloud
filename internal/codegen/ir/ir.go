// SPDX-License-Identifier: Apache-2.0

// Package ir defines the intermediate representation that sits between an API
// description format and the code generator.
//
// Nothing in this package names Smithy, AWS, or any other provider. A
// ModelSource (see internal/codegen) reads one format — Smithy today, OpenAPI
// or Protobuf later — and produces an *ir.Model; the generators read only the
// IR. That is the whole point of the split: adding a second CSP means adding a
// source, not editing every template and generator.
//
// The IR is deliberately close to the shape of an AWS-style API because that is
// the only surface DevCloud serves today. Where a future format needs something
// this type cannot express, add a field — the IR is an internal type with no
// compatibility promise (see docs/compatibility-policy.md).
package ir

// DefaultProvider is the provider a model belongs to when its source does not
// say. It mirrors plugin.DefaultProvider, which is the runtime half of the same
// convention; the two packages do not import each other, so the constant is
// repeated rather than shared.
const DefaultProvider = "aws"

// Model is one service's API, normalized away from whatever format described it.
type Model struct {
	// Provider is the CSP this service belongs to, e.g. "aws". Empty means
	// DefaultProvider.
	Provider string
	// ServiceName is the human-readable service name, e.g. "AmazonDynamoDB".
	ServiceName string
	// ServiceID is the stable lowercase identifier used as a package name and
	// registry key, e.g. "dynamodb".
	ServiceID string
	// Protocol is the wire protocol identifier, matching the values of
	// plugin.ProtocolType (e.g. "json-1.0", "rest-xml").
	Protocol string

	// The identifiers below are the other names this service answers to. A
	// caller addresses a service by one of them — in an X-Amz-Target prefix, a
	// SigV4 credential scope, a host prefix — and the gateway has to map all of
	// them back to ServiceID. Deriving that mapping from the model is what keeps
	// it from being a hand-maintained switch; see codegen.BuildAliases.
	//
	// They are named for what they are, not for the format they came from: a
	// source with no equivalent leaves them empty, which is a valid model.

	// ShapeName is the service shape's own name, e.g. "DynamoDB_20120810" or
	// "RekognitionService". For the JSON protocols this is the X-Amz-Target
	// prefix, which is why it cannot be reconstructed from ServiceID.
	ShapeName string
	// SigningName is the name that appears in a SigV4 credential scope, e.g.
	// "logs" for CloudWatch Logs. Several services share one.
	SigningName string
	// EndpointPrefix is the hostname prefix, e.g. "streams.dynamodb".
	EndpointPrefix string
	// ARNNamespace is the service part of an ARN, e.g. "dynamodb".
	ARNNamespace string
	// CloudFormationName is the name used in CloudFormation resource types.
	CloudFormationName string
	// SDKID is the human-readable SDK identifier, e.g. "CloudWatch Logs". It
	// carries spaces; the wire never does.
	SDKID string

	Operations []Operation
	Shapes     map[string]*Shape
}

// ProviderID returns the model's provider, defaulting to DefaultProvider.
func (m *Model) ProviderID() string {
	if m.Provider == "" {
		return DefaultProvider
	}
	return m.Provider
}

type Operation struct {
	Name       string
	InputName  string
	OutputName string
	Errors     []string
	HTTPMethod string
	HTTPUri    string
}

type Shape struct {
	Name        string
	Type        ShapeType
	Members     []Member
	ListMember  *MemberRef
	KeyMember   *MemberRef
	ValueMember *MemberRef
	EnumValues  []string
	ErrorTrait  *ErrorTrait
}

type ShapeType string

const (
	ShapeStructure ShapeType = "structure"
	ShapeList      ShapeType = "list"
	ShapeMap       ShapeType = "map"
	ShapeEnum      ShapeType = "enum"
	ShapeString    ShapeType = "string"
	ShapeInteger   ShapeType = "integer"
	ShapeLong      ShapeType = "long"
	ShapeBoolean   ShapeType = "boolean"
	ShapeBlob      ShapeType = "blob"
	ShapeTimestamp ShapeType = "timestamp"
	ShapeDouble    ShapeType = "double"
	ShapeFloat     ShapeType = "float"
	ShapeUnion     ShapeType = "union"
	ShapeDocument  ShapeType = "document"
	ShapeIntEnum   ShapeType = "intEnum"
)

type Member struct {
	Name        string
	TargetName  string
	GoType      string
	Required    bool
	HTTPLabel   bool
	HTTPHeader  string
	HTTPQuery   string
	HTTPPayload bool
}

type MemberRef struct {
	TargetName string
	GoType     string
}

type ErrorTrait struct {
	Type       string // "client" or "server"
	HTTPStatus int
}
