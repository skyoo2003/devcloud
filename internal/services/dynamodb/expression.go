// SPDX-License-Identifier: Apache-2.0

package dynamodb

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"
)

// ---- Tokenizer ----

type tokenKind int

const (
	tokIdent   tokenKind = iota // identifier or keyword: AND, OR, NOT, BETWEEN, IN, attribute names
	tokValue                    // expression value placeholder like :val
	tokNameRef                  // expression name reference like #n
	tokOp                       // operator: =, <>, <, <=, >, >=
	tokLParen                   // (
	tokRParen                   // )
	tokComma                    // ,
	tokEOF
)

type token struct {
	kind tokenKind
	val  string
}

type tokenizer struct {
	input []rune
	pos   int
}

func newTokenizer(s string) *tokenizer {
	return &tokenizer{input: []rune(s)}
}

func (t *tokenizer) peek() rune {
	if t.pos >= len(t.input) {
		return 0
	}
	return t.input[t.pos]
}

func (t *tokenizer) skipWS() {
	for t.pos < len(t.input) && unicode.IsSpace(t.input[t.pos]) {
		t.pos++
	}
}

func (t *tokenizer) nextToken() token {
	t.skipWS()
	if t.pos >= len(t.input) {
		return token{kind: tokEOF}
	}
	r := t.peek()

	// Value placeholder: :xxx
	if r == ':' {
		t.pos++
		start := t.pos
		for t.pos < len(t.input) && (unicode.IsLetter(t.input[t.pos]) || unicode.IsDigit(t.input[t.pos]) || t.input[t.pos] == '_') {
			t.pos++
		}
		return token{kind: tokValue, val: ":" + string(t.input[start:t.pos])}
	}

	// Name reference: #xxx
	if r == '#' {
		t.pos++
		start := t.pos
		for t.pos < len(t.input) && (unicode.IsLetter(t.input[t.pos]) || unicode.IsDigit(t.input[t.pos]) || t.input[t.pos] == '_') {
			t.pos++
		}
		return token{kind: tokNameRef, val: "#" + string(t.input[start:t.pos])}
	}

	// Operators
	if r == '<' {
		t.pos++
		if t.pos < len(t.input) && t.input[t.pos] == '>' {
			t.pos++
			return token{kind: tokOp, val: "<>"}
		}
		if t.pos < len(t.input) && t.input[t.pos] == '=' {
			t.pos++
			return token{kind: tokOp, val: "<="}
		}
		return token{kind: tokOp, val: "<"}
	}
	if r == '>' {
		t.pos++
		if t.pos < len(t.input) && t.input[t.pos] == '=' {
			t.pos++
			return token{kind: tokOp, val: ">="}
		}
		return token{kind: tokOp, val: ">"}
	}
	if r == '=' {
		t.pos++
		return token{kind: tokOp, val: "="}
	}
	if r == '(' {
		t.pos++
		return token{kind: tokLParen, val: "("}
	}
	if r == ')' {
		t.pos++
		return token{kind: tokRParen, val: ")"}
	}
	if r == ',' {
		t.pos++
		return token{kind: tokComma, val: ","}
	}

	// Identifier (attribute name, function name, keyword)
	if unicode.IsLetter(r) || r == '_' {
		start := t.pos
		for t.pos < len(t.input) && (unicode.IsLetter(t.input[t.pos]) || unicode.IsDigit(t.input[t.pos]) || t.input[t.pos] == '_' || t.input[t.pos] == '.') {
			t.pos++
		}
		return token{kind: tokIdent, val: string(t.input[start:t.pos])}
	}

	// Unknown: skip
	t.pos++
	return token{kind: tokIdent, val: string(r)}
}

func (t *tokenizer) tokenizeAll() []token {
	var tokens []token
	for {
		tok := t.nextToken()
		tokens = append(tokens, tok)
		if tok.kind == tokEOF {
			break
		}
	}
	return tokens
}

// ---- Parser / Evaluator ----

type exprParser struct {
	tokens  []token
	pos     int
	nameMap map[string]string
	valMap  map[string]*AttributeValue
	item    Item
}

func (p *exprParser) peek() token {
	if p.pos >= len(p.tokens) {
		return token{kind: tokEOF}
	}
	return p.tokens[p.pos]
}

func (p *exprParser) consume() token {
	t := p.tokens[p.pos]
	p.pos++
	return t
}

// peekUpperVal returns the upper-case string of the next token's value without consuming.
func (p *exprParser) peekUpperVal() string {
	return strings.ToUpper(p.peek().val)
}

// parseExpr is the top-level OR expression.
func (p *exprParser) parseExpr() bool {
	left := p.parseAnd()
	for p.peekUpperVal() == "OR" {
		p.consume()
		right := p.parseAnd()
		left = left || right
	}
	return left
}

func (p *exprParser) parseAnd() bool {
	left := p.parseNot()
	for p.peekUpperVal() == "AND" {
		p.consume()
		right := p.parseNot()
		left = left && right
	}
	return left
}

func (p *exprParser) parseNot() bool {
	if strings.ToUpper(p.peek().val) == "NOT" {
		p.consume()
		return !p.parsePrimary()
	}
	return p.parsePrimary()
}

func (p *exprParser) parsePrimary() bool {
	tok := p.peek()

	// Parenthesized expression
	if tok.kind == tokLParen {
		p.consume()
		val := p.parseExpr()
		if p.peek().kind == tokRParen {
			p.consume()
		}
		return val
	}

	// Function call: begins_with, contains, attribute_exists, attribute_not_exists, size
	if tok.kind == tokIdent {
		upper := strings.ToUpper(tok.val)
		switch upper {
		case "BEGINS_WITH":
			p.consume()
			return p.evalBeginsWithFunc()
		case "CONTAINS":
			p.consume()
			return p.evalContainsFunc()
		case "ATTRIBUTE_EXISTS":
			p.consume()
			return p.evalAttributeExistsFunc(true)
		case "ATTRIBUTE_NOT_EXISTS":
			p.consume()
			return p.evalAttributeExistsFunc(false)
		case "SIZE":
			// size() can be used as LHS of comparison, fall through to comparison
		}
	}

	// Comparison: lhs op rhs / BETWEEN / IN
	return p.parseComparison()
}

// resolveAttrName resolves a token to a real attribute name.
func (p *exprParser) resolveAttrName(tok token) string {
	if tok.kind == tokNameRef {
		if resolved, ok := p.nameMap[tok.val]; ok {
			return resolved
		}
		return tok.val
	}
	return tok.val
}

// parsePathToken reads one path token (ident or name-ref).
func (p *exprParser) parsePathToken() (string, bool) {
	tok := p.peek()
	if tok.kind == tokIdent || tok.kind == tokNameRef {
		p.consume()
		return p.resolveAttrName(tok), true
	}
	return "", false
}

func (p *exprParser) evalBeginsWithFunc() bool {
	if p.peek().kind != tokLParen {
		return false
	}
	p.consume()
	path, _ := p.parsePathToken()
	if p.peek().kind == tokComma {
		p.consume()
	}
	valTok := p.consume() // value token
	if p.peek().kind == tokRParen {
		p.consume()
	}
	av := p.item[path]
	if av == nil || av.S == nil {
		return false
	}
	prefix := p.resolveValueTok(valTok)
	if prefix != nil && prefix.S != nil {
		return strings.HasPrefix(*av.S, *prefix.S)
	}
	return false
}

func (p *exprParser) evalContainsFunc() bool {
	if p.peek().kind != tokLParen {
		return false
	}
	p.consume()
	path, _ := p.parsePathToken()
	if p.peek().kind == tokComma {
		p.consume()
	}
	valTok := p.consume()
	if p.peek().kind == tokRParen {
		p.consume()
	}
	av := p.item[path]
	if av == nil {
		return false
	}
	search := p.resolveValueTok(valTok)
	if search == nil {
		return false
	}
	if av.S != nil && search.S != nil {
		return strings.Contains(*av.S, *search.S)
	}
	if av.SS != nil && search.S != nil {
		for _, s := range av.SS {
			if s == *search.S {
				return true
			}
		}
	}
	if av.NS != nil && search.N != nil {
		for _, n := range av.NS {
			if n == *search.N {
				return true
			}
		}
	}
	return false
}

func (p *exprParser) evalAttributeExistsFunc(shouldExist bool) bool {
	if p.peek().kind != tokLParen {
		return !shouldExist
	}
	p.consume()
	path, _ := p.parsePathToken()
	if p.peek().kind == tokRParen {
		p.consume()
	}
	_, exists := p.item[path]
	if shouldExist {
		return exists
	}
	return !exists
}

// sizeOf returns the "size" of an AttributeValue as defined by DynamoDB.
func sizeOf(av *AttributeValue) (float64, bool) {
	if av == nil {
		return 0, false
	}
	if av.S != nil {
		return float64(len(*av.S)), true
	}
	if av.N != nil {
		return float64(len(*av.N)), true
	}
	if av.B != nil {
		return float64(len(av.B)), true
	}
	if av.SS != nil {
		return float64(len(av.SS)), true
	}
	if av.NS != nil {
		return float64(len(av.NS)), true
	}
	if av.L != nil {
		return float64(len(av.L)), true
	}
	if av.M != nil {
		return float64(len(av.M)), true
	}
	return 0, false
}

// parseComparison handles: path OP value, path BETWEEN v AND v, path IN (v1, v2, ...)
// Also handles size(path) OP value
func (p *exprParser) parseComparison() bool {
	// Check for size() function on LHS
	isSizeFunc := false
	var lhsPath string

	tok := p.peek()
	if tok.kind == tokIdent && strings.ToUpper(tok.val) == "SIZE" {
		// peek ahead for '('
		if p.pos+1 < len(p.tokens) && p.tokens[p.pos+1].kind == tokLParen {
			isSizeFunc = true
			p.consume() // consume "size"
			p.consume() // consume "("
			pathTok := p.consume()
			lhsPath = p.resolveAttrName(pathTok)
			if p.peek().kind == tokRParen {
				p.consume()
			}
		}
	}

	if !isSizeFunc {
		pathTok := p.consume()
		lhsPath = p.resolveAttrName(pathTok)
	}

	opTok := p.peek()

	// BETWEEN
	if opTok.kind == tokIdent && strings.ToUpper(opTok.val) == "BETWEEN" {
		p.consume()
		loTok := p.consume()
		if p.peek().kind == tokIdent && strings.ToUpper(p.peek().val) == "AND" {
			p.consume()
		}
		hiTok := p.consume()
		av := p.item[lhsPath]
		if av == nil {
			return false
		}
		lo := p.resolveValueTok(loTok)
		hi := p.resolveValueTok(hiTok)
		return compareBetween(av, lo, hi)
	}

	// IN
	if opTok.kind == tokIdent && strings.ToUpper(opTok.val) == "IN" {
		p.consume()
		if p.peek().kind == tokLParen {
			p.consume()
		}
		var vals []*AttributeValue
		for p.peek().kind != tokRParen && p.peek().kind != tokEOF {
			vt := p.consume()
			vals = append(vals, p.resolveValueTok(vt))
			if p.peek().kind == tokComma {
				p.consume()
			}
		}
		if p.peek().kind == tokRParen {
			p.consume()
		}
		av := p.item[lhsPath]
		if av == nil {
			return false
		}
		for _, v := range vals {
			if compareAttr(av, v) == 0 {
				return true
			}
		}
		return false
	}

	// Standard comparison operator
	if opTok.kind != tokOp {
		return false
	}
	p.consume()
	rhsTok := p.consume()
	rhs := p.resolveValueTok(rhsTok)

	if isSizeFunc {
		sz, ok := sizeOf(p.item[lhsPath])
		if !ok {
			return false
		}
		rhsNum := float64(0)
		if rhs != nil && rhs.N != nil {
			rhsNum, _ = strconv.ParseFloat(*rhs.N, 64)
		}
		return compareNumbers(sz, rhsNum, opTok.val)
	}

	av := p.item[lhsPath]
	if av == nil {
		return false
	}
	cmp := compareAttr(av, rhs)
	return applyOp(cmp, opTok.val)
}

func (p *exprParser) resolveValueTok(tok token) *AttributeValue {
	if tok.kind == tokValue {
		if av, ok := p.valMap[tok.val]; ok {
			return av
		}
		return nil
	}
	// Literal ident — look up in item
	name := p.resolveAttrName(tok)
	return p.item[name]
}

// compareAttr compares two AttributeValues. Returns -1, 0, or 1.
func compareAttr(a, b *AttributeValue) int {
	if a == nil || b == nil {
		return -1
	}
	// Numeric comparison
	if a.N != nil && b.N != nil {
		af, _ := strconv.ParseFloat(*a.N, 64)
		bf, _ := strconv.ParseFloat(*b.N, 64)
		if af < bf {
			return -1
		}
		if af > bf {
			return 1
		}
		return 0
	}
	// String comparison
	if a.S != nil && b.S != nil {
		if *a.S < *b.S {
			return -1
		}
		if *a.S > *b.S {
			return 1
		}
		return 0
	}
	// Boolean comparison
	if a.BOOL != nil && b.BOOL != nil {
		if *a.BOOL == *b.BOOL {
			return 0
		}
		if !*a.BOOL {
			return -1
		}
		return 1
	}
	return -1
}

func applyOp(cmp int, op string) bool {
	switch op {
	case "=":
		return cmp == 0
	case "<>":
		return cmp != 0
	case "<":
		return cmp < 0
	case "<=":
		return cmp <= 0
	case ">":
		return cmp > 0
	case ">=":
		return cmp >= 0
	}
	return false
}

func compareNumbers(a, b float64, op string) bool {
	var cmp int
	if a < b {
		cmp = -1
	} else if a > b {
		cmp = 1
	}
	return applyOp(cmp, op)
}

func compareBetween(av, lo, hi *AttributeValue) bool {
	if av == nil || lo == nil || hi == nil {
		return false
	}
	return compareAttr(av, lo) >= 0 && compareAttr(av, hi) <= 0
}

// ---- Public API ----

// EvaluateFilterExpression evaluates a DynamoDB filter expression against an item.
// Returns true if the item matches the expression.
func EvaluateFilterExpression(expr string, exprNames map[string]string, exprValues map[string]*AttributeValue, item Item) bool {
	if expr == "" {
		return true
	}
	if exprNames == nil {
		exprNames = map[string]string{}
	}
	if exprValues == nil {
		exprValues = map[string]*AttributeValue{}
	}
	tokens := newTokenizer(expr).tokenizeAll()
	p := &exprParser{
		tokens:  tokens,
		nameMap: exprNames,
		valMap:  exprValues,
		item:    item,
	}
	return p.parseExpr()
}

// EvaluateConditionExpression evaluates a condition expression, returning an error if the condition is not met.
func EvaluateConditionExpression(expr string, exprNames map[string]string, exprValues map[string]*AttributeValue, item Item) error {
	if expr == "" {
		return nil
	}
	if !EvaluateFilterExpression(expr, exprNames, exprValues, item) {
		return fmt.Errorf("ConditionalCheckFailedException: The conditional request failed")
	}
	return nil
}

// ApplyProjectionExpression returns a new item containing only the projected attributes.
func ApplyProjectionExpression(expr string, exprNames map[string]string, item Item) Item {
	if expr == "" {
		return item
	}
	if exprNames == nil {
		exprNames = map[string]string{}
	}
	result := make(Item)
	parts := strings.Split(expr, ",")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		// Resolve name reference
		name := part
		if strings.HasPrefix(part, "#") {
			if resolved, ok := exprNames[part]; ok {
				name = resolved
			}
		}
		if av, ok := item[name]; ok {
			result[name] = av
		}
	}
	return result
}
