// SPDX-License-Identifier: Apache-2.0

// internal/codegen/scan_handverified.go
package codegen

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// opNamePattern is the shape of an AWS operation name: PascalCase, letters and
// digits only. It rejects the odd non-operation literal that shares a dispatch
// switch with real operations (HTTP verbs, content types, protocol tokens).
var opNamePattern = regexp.MustCompile(`^[A-Z][A-Za-z0-9]{3,}$`)

// pathRoutedOps lists the operations of providers that route on HTTP method and
// path instead of an operation name, so no operation-name literal appears in
// their HandleRequest for the scan to find. Keep in sync by hand when these
// providers gain operations — the fidelity manifest test fails loudly if a
// fourth provider starts routing this way, since it would resolve to an empty
// operation set.
//
// ponytail: hand-maintained for three services; if the list keeps growing,
// derive the mapping from the generated OperationRoutes table instead.
var pathRoutedOps = map[string][]string{
	"s3": {
		"AbortMultipartUpload", "CompleteMultipartUpload", "CopyObject", "CreateBucket",
		"CreateMultipartUpload", "DeleteBucket", "DeleteBucketCors", "DeleteBucketPolicy",
		"DeleteBucketTagging", "DeleteObject", "DeleteObjectTagging", "DeleteObjects",
		"GetBucketAcl", "GetBucketCors", "GetBucketLocation",
		"GetBucketNotificationConfiguration", "GetBucketPolicy", "GetBucketTagging",
		"GetBucketVersioning", "GetObject", "GetObjectTagging", "HeadBucket", "HeadObject",
		"ListBuckets", "ListMultipartUploads", "ListObjects", "ListObjectsV2", "ListParts",
		"PutBucketAcl", "PutBucketCors", "PutBucketNotificationConfiguration",
		"PutBucketPolicy", "PutBucketTagging", "PutBucketVersioning", "PutObject",
		"PutObjectTagging", "UploadPart",
	},
	"lambda": {
		"AddPermission", "CreateAlias", "CreateEventSourceMapping", "CreateFunction",
		"DeleteAlias", "DeleteEventSourceMapping", "DeleteFunction", "GetAlias",
		"GetEventSourceMapping", "GetFunction", "GetPolicy", "Invoke", "ListAliases",
		"ListEventSourceMappings", "ListFunctions", "ListTags", "ListVersionsByFunction",
		"PublishVersion", "RemovePermission", "TagResource", "UntagResource",
		"UpdateAlias", "UpdateEventSourceMapping", "UpdateFunctionCode",
		"UpdateFunctionConfiguration",
	},
	// InvokeModel and InvokeModelWithResponseStream belong to bedrock-runtime in
	// AWS but are served by this provider, so they are hand-verified even though
	// bedrock.json does not declare them. The manifest's universe is
	// model ∪ hand-verified for exactly this case.
	"bedrock": {
		"CreateGuardrail", "CreateModelCustomizationJob", "DeleteCustomModel",
		"DeleteGuardrail", "GetCustomModel", "GetFoundationModel", "GetGuardrail",
		"GetModelCustomizationJob", "InvokeModel", "InvokeModelWithResponseStream",
		"ListCustomModels", "ListFoundationModels", "ListGuardrails",
		"ListModelCustomizationJobs", "ListTagsForResource",
		"StopModelCustomizationJob", "TagResource", "UntagResource", "UpdateGuardrail",
	},
}

// ProviderScan is what a service's source tells us about how it serves requests.
type ProviderScan struct {
	// Operations are the operation names the provider dispatches by hand.
	Operations []string
	// EngineWired reports whether the provider hands the operations it does not
	// implement to the generic CRUD engine, by returning plugin.ErrUnhandledOp.
	// Only a wired provider's CRUD-shaped operations are actually served, so
	// this is what separates a truthful auto-crud label from a claim: the CRUD
	// registry says an operation is *classifiable*, never that it is reachable.
	EngineWired bool
}

// ScanProviders returns, per registered service ID, the operations its provider
// dispatches by hand.
//
// The scan is deliberately narrow: it reads only the case literals of the keyed
// switches inside a provider's HandleRequest, keyed by the receiver type that
// Register binds to the service ID. That scoping is what makes a literal scan
// trustworthy — attribute-path switches in helpers (identitystore matches on
// "DisplayName") and HTTP-verb switches in path resolvers (pipes matches on
// "POST") live outside HandleRequest and never contribute, and a package
// registering two services (iam also registers sts) splits by receiver type
// rather than by guesswork.
//
// It deliberately does *not* intersect with the Smithy model: providers serve
// operations no in-tree model declares, and discarding them would understate
// the surface. dynamodbstreams alone dispatches 22 operations against a model
// that declares 4.
func ScanProviders(servicesDir string) (map[string]ProviderScan, error) {
	entries, err := os.ReadDir(servicesDir)
	if err != nil {
		return nil, fmt.Errorf("read services dir: %w", err)
	}

	result := make(map[string]ProviderScan)
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		pkg, err := scanServiceDir(filepath.Join(servicesDir, entry.Name()))
		if err != nil {
			return nil, err
		}
		for id, recvType := range pkg.serviceTypes {
			result[id] = ProviderScan{
				Operations:  opsFor(id, pkg.dispatchedOps(recvType)),
				EngineWired: pkg.wiresCRUDEngine(recvType),
			}
		}
	}
	return result, nil
}

// opsFor resolves one service's operations, preferring the hand-maintained list
// for providers whose dispatch carries no operation-name literals.
func opsFor(serviceID string, scanned map[string]bool) []string {
	if declared, ok := pathRoutedOps[serviceID]; ok {
		return append([]string(nil), declared...)
	}
	ops := make([]string, 0, len(scanned))
	for op := range scanned {
		ops = append(ops, op)
	}
	sort.Strings(ops)
	return ops
}

// packageScan is one service package's providers, keyed by receiver type name.
type packageScan struct {
	// serviceTypes maps a registered service ID to the provider type Register
	// returns for it.
	serviceTypes map[string]string
	// methods maps a provider type to its methods by name, so the scan can
	// follow HandleRequest into whatever it delegates dispatch to.
	methods map[string]map[string]*ast.FuncDecl
}

func scanServiceDir(dir string) (packageScan, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return packageScan{}, fmt.Errorf("read %s: %w", dir, err)
	}

	pkg := packageScan{
		serviceTypes: make(map[string]string),
		methods:      make(map[string]map[string]*ast.FuncDecl),
	}
	fset := token.NewFileSet()

	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		path := filepath.Join(dir, name)
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return packageScan{}, fmt.Errorf("parse %s: %w", path, err)
		}

		ast.Inspect(file, func(n ast.Node) bool {
			switch node := n.(type) {
			case *ast.CallExpr:
				if id, typeName, ok := registeredService(node); ok {
					pkg.serviceTypes[id] = typeName
				}
			case *ast.FuncDecl:
				if recv, ok := receiverType(node); ok {
					if pkg.methods[recv] == nil {
						pkg.methods[recv] = make(map[string]*ast.FuncDecl)
					}
					pkg.methods[recv][node.Name.Name] = node
				}
			}
			return true
		})
	}
	return pkg, nil
}

// dispatchedOps collects the operations a provider type dispatches, starting at
// its HandleRequest and following the methods it delegates to. Not every
// provider switches in HandleRequest itself: sqs branches on protocol into
// handleQueryRequest/handleJSONRequest, and kafka wraps handleRequest to
// camelCase the response.
func (p packageScan) dispatchedOps(recvType string) map[string]bool {
	ops := make(map[string]bool)
	visited := make(map[string]bool)

	var walk func(name string)
	walk = func(name string) {
		fn := p.methods[recvType][name]
		if fn == nil || fn.Body == nil || visited[name] {
			return
		}
		visited[name] = true

		collectCaseLiterals(fn.Body, ops)
		for _, delegate := range delegateCalls(fn) {
			walk(delegate)
		}
	}
	walk("HandleRequest")
	return ops
}

// wiresCRUDEngine reports whether a provider type opts into the generic CRUD
// engine, by returning plugin.ErrUnhandledOp anywhere in its dispatch.
//
// It walks the same methods as dispatchedOps, and for the same reason: scoping
// to one receiver's dispatch is what stops a package registering two services
// (iam also registers sts) from crediting one with the other's wiring.
func (p packageScan) wiresCRUDEngine(recvType string) bool {
	visited := make(map[string]bool)
	wired := false

	var walk func(name string)
	walk = func(name string) {
		fn := p.methods[recvType][name]
		if fn == nil || fn.Body == nil || visited[name] || wired {
			return
		}
		visited[name] = true

		ast.Inspect(fn.Body, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "ErrUnhandledOp" {
				return true
			}
			if ident, isIdent := sel.X.(*ast.Ident); isIdent && ident.Name == "plugin" {
				wired = true
			}
			return true
		})
		for _, delegate := range delegateCalls(fn) {
			walk(delegate)
		}
	}
	walk("HandleRequest")
	return wired
}

// collectCaseLiterals reads the case literals of every keyed switch in a body.
// Keyed is the point: `switch op { case "GetItem": }` dispatches, whereas a
// tagless `switch { case remaining == 0: }` is control flow whose cases are
// expressions, not operation names.
func collectCaseLiterals(body *ast.BlockStmt, ops map[string]bool) {
	ast.Inspect(body, func(n ast.Node) bool {
		sw, ok := n.(*ast.SwitchStmt)
		if !ok || sw.Tag == nil {
			return true
		}
		for _, stmt := range sw.Body.List {
			clause, ok := stmt.(*ast.CaseClause)
			if !ok {
				continue
			}
			for _, expr := range clause.List {
				if lit, ok := stringLiteral(expr); ok && opNamePattern.MatchString(lit) {
					ops[lit] = true
				}
			}
		}
		return true
	})
}

// delegateCalls returns the names of same-receiver methods a function hands
// dispatch off to, so the scan can follow them.
//
// Inside a dispatch switch only the default clause counts. `case "GetApi":
// p.getApi(…)` is a per-operation handler, and following it would drag every
// helper's internal switch into the operation set; `default: p.handleStub(op)`
// is the rest of the dispatch, and appsync serves 37 operations through one.
func delegateCalls(fn *ast.FuncDecl) []string {
	recv := receiverName(fn)
	if recv == "" {
		return nil
	}
	var names []string
	var visit func(n ast.Node) bool
	visit = func(n ast.Node) bool {
		switch node := n.(type) {
		case *ast.SwitchStmt:
			for _, stmt := range node.Body.List {
				clause, ok := stmt.(*ast.CaseClause)
				if !ok || clause.List != nil {
					continue
				}
				for _, s := range clause.Body {
					ast.Inspect(s, visit)
				}
			}
			return false
		case *ast.TypeSwitchStmt:
			return false
		case *ast.CallExpr:
			if sel, ok := node.Fun.(*ast.SelectorExpr); ok {
				if ident, ok := sel.X.(*ast.Ident); ok && ident.Name == recv {
					names = append(names, sel.Sel.Name)
				}
			}
		}
		return true
	}
	ast.Inspect(fn.Body, visit)
	return names
}

// receiverName returns "p" from `func (p *SQSProvider) …`.
func receiverName(fn *ast.FuncDecl) string {
	if fn.Recv == nil || len(fn.Recv.List) != 1 || len(fn.Recv.List[0].Names) != 1 {
		return ""
	}
	return fn.Recv.List[0].Names[0].Name
}

// receiverType returns "SQSProvider" from `func (p *SQSProvider) …`.
func receiverType(fn *ast.FuncDecl) (string, bool) {
	if fn.Recv == nil || len(fn.Recv.List) != 1 {
		return "", false
	}
	expr := fn.Recv.List[0].Type
	if star, ok := expr.(*ast.StarExpr); ok {
		expr = star.X
	}
	ident, ok := expr.(*ast.Ident)
	if !ok {
		return "", false
	}
	return ident.Name, true
}

// registeredService extracts ("sqs", "SQSProvider") from
// `plugin.DefaultRegistry.Register("sqs", func() plugin.ServicePlugin { return &SQSProvider{} })`.
func registeredService(call *ast.CallExpr) (serviceID, typeName string, ok bool) {
	sel, isSel := call.Fun.(*ast.SelectorExpr)
	if !isSel || sel.Sel.Name != "Register" || len(call.Args) != 2 {
		return "", "", false
	}
	serviceID, ok = stringLiteral(call.Args[0])
	if !ok {
		return "", "", false
	}
	factory, isFunc := call.Args[1].(*ast.FuncLit)
	if !isFunc || factory.Body == nil {
		return "", "", false
	}
	for _, stmt := range factory.Body.List {
		ret, isRet := stmt.(*ast.ReturnStmt)
		if !isRet || len(ret.Results) != 1 {
			continue
		}
		if name, found := constructedType(ret.Results[0]); found {
			return serviceID, name, true
		}
	}
	return "", "", false
}

// constructedType returns "SQSProvider" from `&SQSProvider{…}` or `NewSQSProvider()`.
func constructedType(expr ast.Expr) (string, bool) {
	if unary, ok := expr.(*ast.UnaryExpr); ok {
		expr = unary.X
	}
	switch node := expr.(type) {
	case *ast.CompositeLit:
		if ident, ok := node.Type.(*ast.Ident); ok {
			return ident.Name, true
		}
	case *ast.CallExpr:
		if ident, ok := node.Fun.(*ast.Ident); ok {
			return strings.TrimPrefix(ident.Name, "New"), true
		}
	}
	return "", false
}

func stringLiteral(expr ast.Expr) (string, bool) {
	lit, ok := expr.(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return "", false
	}
	s, err := strconv.Unquote(lit.Value)
	if err != nil {
		return "", false
	}
	return s, true
}
