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
// digits only. It is a pre-filter, not the decision — for a service with a
// Smithy model the scan intersects with the model's operations, which discards
// any string that merely looks like an operation name.
var opNamePattern = regexp.MustCompile(`^[A-Z][A-Za-z0-9]{3,}$`)

// pathRoutedOps lists the operations of providers that route on HTTP method and
// path instead of an operation name, so no operation-name literal appears in
// their source for the scan to find. Keep in sync by hand when these providers
// gain operations — the fidelity manifest test fails loudly if a fourth provider
// starts routing this way, since it would resolve to an empty operation set.
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
	// InvokeModel belongs to bedrock-runtime in AWS but is served by this
	// provider, so it is hand-verified even though bedrock.json does not declare
	// it. The manifest's universe is model ∪ hand-verified for exactly this case.
	"bedrock": {
		"CreateGuardrail", "CreateModelCustomizationJob", "DeleteCustomModel",
		"DeleteGuardrail", "GetCustomModel", "GetFoundationModel", "GetGuardrail",
		"GetModelCustomizationJob", "InvokeModel", "ListCustomModels",
		"ListFoundationModels", "ListGuardrails", "ListModelCustomizationJobs",
		"ListTagsForResource", "StopModelCustomizationJob", "TagResource",
		"UntagResource", "UpdateGuardrail",
	},
}

// ScanHandVerified returns, per registered service ID, the operation names that
// service's provider handles by hand. The scan collects the string literals of
// every case clause in a service package, then keeps only those that name an
// operation in modelOps[serviceID]; a service with no model keeps every literal
// that looks like an operation name.
//
// Intersecting with the model is what makes a crude literal scan safe: unrelated
// switches (protocol constants, expression tokens) contribute strings that no
// model declares, and a directory registering two services (iam also registers
// sts) splits correctly because each model claims only its own operations.
func ScanHandVerified(servicesDir string, modelOps map[string][]string) (map[string][]string, error) {
	entries, err := os.ReadDir(servicesDir)
	if err != nil {
		return nil, fmt.Errorf("read services dir: %w", err)
	}

	result := make(map[string][]string)
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		dir := filepath.Join(servicesDir, entry.Name())
		ids, literals, err := scanServiceDir(dir)
		if err != nil {
			return nil, err
		}
		for _, id := range ids {
			result[id] = selectOps(id, literals, modelOps[id])
		}
	}
	return result, nil
}

// selectOps narrows a package's case literals to the operations of one service.
func selectOps(serviceID string, literals map[string]bool, modelOperations []string) []string {
	if declared, ok := pathRoutedOps[serviceID]; ok {
		return append([]string(nil), declared...)
	}

	var ops []string
	if len(modelOperations) > 0 {
		for _, op := range modelOperations {
			if literals[op] {
				ops = append(ops, op)
			}
		}
	} else {
		for lit := range literals {
			ops = append(ops, lit)
		}
	}
	sort.Strings(ops)
	return ops
}

// scanServiceDir returns the service IDs a package registers and every
// operation-shaped string literal used as a switch case in it.
func scanServiceDir(dir string) (ids []string, literals map[string]bool, err error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, nil, fmt.Errorf("read %s: %w", dir, err)
	}

	fset := token.NewFileSet()
	literals = make(map[string]bool)
	seenID := make(map[string]bool)

	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, 0)
		if err != nil {
			return nil, nil, fmt.Errorf("parse %s: %w", filepath.Join(dir, name), err)
		}

		ast.Inspect(file, func(n ast.Node) bool {
			switch node := n.(type) {
			case *ast.CallExpr:
				if id, ok := registeredServiceID(node); ok && !seenID[id] {
					seenID[id] = true
					ids = append(ids, id)
				}
			case *ast.CaseClause:
				for _, expr := range node.List {
					if lit, ok := stringLiteral(expr); ok && opNamePattern.MatchString(lit) {
						literals[lit] = true
					}
				}
			}
			return true
		})
	}
	sort.Strings(ids)
	return ids, literals, nil
}

// registeredServiceID extracts "sqs" from plugin.DefaultRegistry.Register("sqs", …).
func registeredServiceID(call *ast.CallExpr) (string, bool) {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "Register" || len(call.Args) == 0 {
		return "", false
	}
	return stringLiteral(call.Args[0])
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
