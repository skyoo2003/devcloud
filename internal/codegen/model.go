// SPDX-License-Identifier: Apache-2.0

// internal/codegen/model.go
package codegen

type SmithyModel struct {
	ServiceName string
	ServiceID   string
	Protocol    string
	Operations  []Operation
	Shapes      map[string]*Shape
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
