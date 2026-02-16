package traceql

import (
	"fmt"
	"strconv"
	"time"
)

// Parse parses a TraceQL query string into a RootExpr AST.
func Parse(input string) (*RootExpr, error) {
	l := newLexer(input)
	tokens, err := l.tokenize()
	if err != nil {
		return nil, fmt.Errorf("lexer error: %w", err)
	}

	p := &parser{
		tokens: tokens,
		pos:    0,
	}

	root, err := p.parseRoot()
	if err != nil {
		return nil, err
	}

	if !p.atEnd() {
		return nil, fmt.Errorf("unexpected token %s at position %d", p.current().val, p.current().pos)
	}

	return root, nil
}

// parser is a recursive-descent parser for TraceQL.
type parser struct {
	tokens []token
	pos    int
}

func (p *parser) current() token {
	if p.pos >= len(p.tokens) {
		return token{typ: tokenEOF}
	}
	return p.tokens[p.pos]
}

func (p *parser) peek() tokenType {
	return p.current().typ
}

func (p *parser) peekAt(offset int) tokenType {
	idx := p.pos + offset
	if idx >= len(p.tokens) {
		return tokenEOF
	}
	return p.tokens[idx].typ
}

func (p *parser) advance() token {
	t := p.current()
	p.pos++
	return t
}

func (p *parser) expect(typ tokenType) (token, error) {
	t := p.current()
	if t.typ != typ {
		return t, fmt.Errorf("expected %d but got %q at position %d", typ, t.val, t.pos)
	}
	p.pos++
	return t, nil
}

func (p *parser) atEnd() bool {
	return p.peek() == tokenEOF
}

// ---------------------------------------------------------------------------
// Grammar: root
// ---------------------------------------------------------------------------

// parseRoot parses the top-level query.
// root = spansetPipeline [hints]
//
//	| spansetPipeline "|" metricsAggregation [hints]
//	| spansetPipelineExpression [hints]
func (p *parser) parseRoot() (*RootExpr, error) {
	pipeline, err := p.parsePipeline()
	if err != nil {
		return nil, err
	}

	root := &RootExpr{Pipeline: *pipeline}

	// Optional hints: with(...)
	if p.peek() == tokenWith {
		hints, err := p.parseHints()
		if err != nil {
			return nil, err
		}
		root.Hints = hints
	}

	return root, nil
}

// ---------------------------------------------------------------------------
// Grammar: pipeline
// ---------------------------------------------------------------------------

// parsePipeline parses a pipeline of elements separated by |
// pipeline = element ("|" element)*
func (p *parser) parsePipeline() (*Pipeline, error) {
	first, err := p.parsePipelineElement()
	if err != nil {
		return nil, err
	}

	elements := []PipelineElement{first}

	for p.peek() == tokenPipe {
		p.advance() // consume |
		elem, err := p.parsePipelineElement()
		if err != nil {
			return nil, err
		}
		elements = append(elements, elem)
	}

	return &Pipeline{Elements: elements}, nil
}

// parsePipelineElement parses a single pipeline element.
func (p *parser) parsePipelineElement() (PipelineElement, error) {
	switch p.peek() {
	case tokenOpenBrace:
		return p.parseSpansetExpression()
	case tokenOpenParen:
		return p.parseWrappedScalarFilter()
	case tokenCount, tokenMin, tokenMax, tokenSum, tokenAvg:
		return p.parseAggregateOrScalarFilter()
	case tokenRate, tokenCountOverTime, tokenMinOverTime, tokenMaxOverTime,
		tokenAvgOverTime, tokenSumOverTime, tokenQuantileOverTime, tokenHistogramOverTime:
		return p.parseMetricsAggregate()
	case tokenBy:
		return p.parseGroupOperation()
	case tokenCoalesce:
		return p.parseCoalesceOperation()
	case tokenSelect:
		return p.parseSelectOperation()
	case tokenTopK, tokenBottomK:
		// Second stage -- treat as a metrics aggregate for simplicity
		return p.parseMetricsAggregate()
	default:
		return nil, fmt.Errorf("unexpected token %q at position %d, expected pipeline element", p.current().val, p.current().pos)
	}
}

// ---------------------------------------------------------------------------
// Grammar: spanset expressions
// ---------------------------------------------------------------------------

// parseSpansetExpression parses spanset expressions with potential &&/|| between them.
func (p *parser) parseSpansetExpression() (PipelineElement, error) {
	lhs, err := p.parseSpansetPrimary()
	if err != nil {
		return nil, err
	}

	for {
		op, ok := p.trySpansetOperator()
		if !ok {
			break
		}

		rhs, err := p.parseSpansetPrimary()
		if err != nil {
			return nil, err
		}

		lhs = &SpansetOperation{
			Op:  op,
			LHS: lhs,
			RHS: rhs,
		}
	}

	return lhs, nil
}

// trySpansetOperator tries to consume a spanset-level operator.
// These are && and || when they appear between { } blocks, and structural operators.
func (p *parser) trySpansetOperator() (Operator, bool) {
	switch p.peek() {
	case tokenAnd:
		// Determine if this is a spanset-level && (between two {})
		p.advance()
		return OpSpansetAnd, true
	case tokenOr:
		p.advance()
		return OpSpansetUnion, true
	case tokenGt:
		// Could be structural child operator between spansets
		if p.peekAt(1) == tokenOpenBrace {
			p.advance()
			return OpSpansetChild, true
		}
		return OpNone, false
	case tokenLt:
		if p.peekAt(1) == tokenOpenBrace {
			p.advance()
			return OpSpansetParent, true
		}
		return OpNone, false
	case tokenGtGt:
		p.advance()
		return OpSpansetDescendant, true
	case tokenLtLt:
		p.advance()
		return OpSpansetAncestor, true
	case tokenTilde:
		p.advance()
		return OpSpansetSibling, true
	case tokenBangGt:
		p.advance()
		return OpSpansetNotChild, true
	case tokenBangLt:
		p.advance()
		return OpSpansetNotParent, true
	case tokenBangGtGt:
		p.advance()
		return OpSpansetNotDescendant, true
	case tokenBangLtLt:
		p.advance()
		return OpSpansetNotAncestor, true
	case tokenAmpGt:
		p.advance()
		return OpSpansetUnionChild, true
	case tokenAmpLt:
		p.advance()
		return OpSpansetUnionParent, true
	case tokenAmpGtGt:
		p.advance()
		return OpSpansetUnionDescendant, true
	case tokenAmpLtLt:
		p.advance()
		return OpSpansetUnionAncestor, true
	case tokenAmpTilde:
		p.advance()
		return OpSpansetUnionSibling, true
	default:
		return OpNone, false
	}
}

// parseSpansetPrimary parses a single { filter } block.
func (p *parser) parseSpansetPrimary() (PipelineElement, error) {
	if _, err := p.expect(tokenOpenBrace); err != nil {
		return nil, fmt.Errorf("expected '{': %w", err)
	}

	// Empty spanset filter: { }
	if p.peek() == tokenCloseBrace {
		p.advance()
		return &SpansetFilter{Expression: nil}, nil
	}

	expr, err := p.parseFieldExpression()
	if err != nil {
		return nil, err
	}

	if _, err := p.expect(tokenCloseBrace); err != nil {
		return nil, fmt.Errorf("expected '}': %w", err)
	}

	return &SpansetFilter{Expression: expr}, nil
}

// ---------------------------------------------------------------------------
// Grammar: scalar filter (e.g. count() > 5)
// ---------------------------------------------------------------------------

// parseAggregateOrScalarFilter parses aggregate functions and optional comparison.
// This handles both standalone aggregates in pipelines and scalar filters.
func (p *parser) parseAggregateOrScalarFilter() (PipelineElement, error) {
	agg, err := p.parseAggregate()
	if err != nil {
		return nil, err
	}

	// Check for comparison operator
	if op, ok := p.tryComparisonOp(); ok {
		rhs, err := p.parseScalarOperand()
		if err != nil {
			return nil, err
		}
		return &ScalarFilter{
			Op:  op,
			LHS: agg,
			RHS: rhs,
		}, nil
	}

	return agg, nil
}

// parseWrappedScalarFilter handles ( pipeline ) op value.
func (p *parser) parseWrappedScalarFilter() (PipelineElement, error) {
	if _, err := p.expect(tokenOpenParen); err != nil {
		return nil, err
	}

	pipeline, err := p.parsePipeline()
	if err != nil {
		return nil, err
	}

	if _, err := p.expect(tokenCloseParen); err != nil {
		return nil, fmt.Errorf("expected ')': %w", err)
	}

	// The wrapped pipeline should end with an aggregate, then we compare
	if op, ok := p.tryComparisonOp(); ok {
		rhs, err := p.parseScalarOperand()
		if err != nil {
			return nil, err
		}

		// Extract the last element as the LHS scalar expression
		if len(pipeline.Elements) > 0 {
			lastElem := pipeline.Elements[len(pipeline.Elements)-1]
			if scalar, ok := lastElem.(ScalarExpression); ok {
				// Remove last element from pipeline, prepend pipeline
				pipeline.Elements = pipeline.Elements[:len(pipeline.Elements)-1]
				filter := &ScalarFilter{
					Op:  op,
					LHS: scalar,
					RHS: rhs,
				}
				// If pipeline still has elements, combine them
				if len(pipeline.Elements) > 0 {
					pipeline.Elements = append(pipeline.Elements, filter)
					return pipeline, nil
				}
				return filter, nil
			}
		}

		return nil, fmt.Errorf("expected scalar expression before comparison operator")
	}

	// Just a grouped pipeline
	if len(pipeline.Elements) == 1 {
		return pipeline.Elements[0], nil
	}
	return pipeline, nil
}

func (p *parser) parseAggregate() (*Aggregate, error) {
	t := p.advance()
	var op AggregateOp
	switch t.typ {
	case tokenCount:
		op = AggregateCount
	case tokenMin:
		op = AggregateMin
	case tokenMax:
		op = AggregateMax
	case tokenSum:
		op = AggregateSum
	case tokenAvg:
		op = AggregateAvg
	default:
		return nil, fmt.Errorf("expected aggregate function, got %q", t.val)
	}

	if _, err := p.expect(tokenOpenParen); err != nil {
		return nil, fmt.Errorf("expected '(' after %s: %w", t.val, err)
	}

	var expr FieldExpression
	if p.peek() != tokenCloseParen {
		var err error
		expr, err = p.parseFieldExpression()
		if err != nil {
			return nil, err
		}
	}

	if _, err := p.expect(tokenCloseParen); err != nil {
		return nil, fmt.Errorf("expected ')' after aggregate args: %w", err)
	}

	return &Aggregate{Op: op, Expression: expr}, nil
}

func (p *parser) parseScalarOperand() (ScalarExpression, error) {
	s, err := p.parseStatic()
	if err != nil {
		return nil, err
	}
	return s, nil
}

func (p *parser) tryComparisonOp() (Operator, bool) {
	switch p.peek() {
	case tokenEq:
		p.advance()
		return OpEqual, true
	case tokenNeq:
		p.advance()
		return OpNotEqual, true
	case tokenGt:
		p.advance()
		return OpGreater, true
	case tokenGte:
		p.advance()
		return OpGreaterEqual, true
	case tokenLt:
		p.advance()
		return OpLess, true
	case tokenLte:
		p.advance()
		return OpLessEqual, true
	default:
		return OpNone, false
	}
}

// ---------------------------------------------------------------------------
// Grammar: metrics aggregate
// ---------------------------------------------------------------------------

func (p *parser) parseMetricsAggregate() (PipelineElement, error) {
	t := p.advance()
	var op MetricsAggregateOp
	switch t.typ {
	case tokenRate:
		op = MetricsAggregateRate
	case tokenCountOverTime:
		op = MetricsAggregateCountOverTime
	case tokenMinOverTime:
		op = MetricsAggregateMinOverTime
	case tokenMaxOverTime:
		op = MetricsAggregateMaxOverTime
	case tokenAvgOverTime:
		op = MetricsAggregateAvgOverTime
	case tokenSumOverTime:
		op = MetricsAggregateSumOverTime
	case tokenQuantileOverTime:
		op = MetricsAggregateQuantileOverTime
	case tokenHistogramOverTime:
		op = MetricsAggregateHistogramOverTime
	default:
		return nil, fmt.Errorf("expected metrics function, got %q", t.val)
	}

	if _, err := p.expect(tokenOpenParen); err != nil {
		return nil, fmt.Errorf("expected '(' after %s: %w", t.val, err)
	}

	ma := &MetricsAggregate{Op: op}

	// Parse args: optional field expression and float args
	if p.peek() != tokenCloseParen {
		// First arg could be a field expression or float
		if p.peek() == tokenFloat || p.peek() == tokenInteger {
			// Numeric arg (e.g., quantile value)
			v, err := p.parseFloat()
			if err != nil {
				return nil, err
			}
			ma.Floats = append(ma.Floats, v)
		} else if p.peek() != tokenCloseParen {
			expr, err := p.parseFieldExpression()
			if err != nil {
				return nil, err
			}
			ma.Attr = expr
		}

		// Additional comma-separated args
		for p.peek() == tokenComma {
			p.advance()
			if p.peek() == tokenFloat || p.peek() == tokenInteger {
				v, err := p.parseFloat()
				if err != nil {
					return nil, err
				}
				ma.Floats = append(ma.Floats, v)
			} else {
				expr, err := p.parseFieldExpression()
				if err != nil {
					return nil, err
				}
				ma.Attr = expr
			}
		}
	}

	if _, err := p.expect(tokenCloseParen); err != nil {
		return nil, fmt.Errorf("expected ')' after metrics args: %w", err)
	}

	// Optional by(...)
	if p.peek() == tokenBy {
		p.advance()
		if _, err := p.expect(tokenOpenParen); err != nil {
			return nil, fmt.Errorf("expected '(' after by: %w", err)
		}
		for {
			attr, err := p.parseAttribute()
			if err != nil {
				return nil, err
			}
			ma.By = append(ma.By, *attr)
			if p.peek() != tokenComma {
				break
			}
			p.advance()
		}
		if _, err := p.expect(tokenCloseParen); err != nil {
			return nil, fmt.Errorf("expected ')' after by args: %w", err)
		}
	}

	return ma, nil
}

func (p *parser) parseFloat() (float64, error) {
	t := p.advance()
	return strconv.ParseFloat(t.val, 64)
}

// ---------------------------------------------------------------------------
// Grammar: group, coalesce, select
// ---------------------------------------------------------------------------

func (p *parser) parseGroupOperation() (*GroupOperation, error) {
	p.advance() // consume 'by'
	if _, err := p.expect(tokenOpenParen); err != nil {
		return nil, fmt.Errorf("expected '(' after by: %w", err)
	}
	expr, err := p.parseFieldExpression()
	if err != nil {
		return nil, err
	}
	if _, err := p.expect(tokenCloseParen); err != nil {
		return nil, fmt.Errorf("expected ')' after by arg: %w", err)
	}
	return &GroupOperation{Expression: expr}, nil
}

func (p *parser) parseCoalesceOperation() (*CoalesceOperation, error) {
	p.advance() // consume 'coalesce'
	if _, err := p.expect(tokenOpenParen); err != nil {
		return nil, err
	}
	if _, err := p.expect(tokenCloseParen); err != nil {
		return nil, err
	}
	return &CoalesceOperation{}, nil
}

func (p *parser) parseSelectOperation() (*SelectOperation, error) {
	p.advance() // consume 'select'
	if _, err := p.expect(tokenOpenParen); err != nil {
		return nil, err
	}
	var attrs []Attribute
	for {
		attr, err := p.parseAttribute()
		if err != nil {
			return nil, err
		}
		attrs = append(attrs, *attr)
		if p.peek() != tokenComma {
			break
		}
		p.advance()
	}
	if _, err := p.expect(tokenCloseParen); err != nil {
		return nil, err
	}
	return &SelectOperation{Attrs: attrs}, nil
}

// ---------------------------------------------------------------------------
// Grammar: hints
// ---------------------------------------------------------------------------

func (p *parser) parseHints() (*Hints, error) {
	p.advance() // consume 'with'
	if _, err := p.expect(tokenOpenParen); err != nil {
		return nil, err
	}
	var hints []*Hint
	for p.peek() != tokenCloseParen {
		name := p.advance()
		if name.typ != tokenIdent {
			return nil, fmt.Errorf("expected hint name, got %q", name.val)
		}
		if _, err := p.expect(tokenEq); err != nil {
			return nil, err
		}
		val, err := p.parseStatic()
		if err != nil {
			return nil, err
		}
		hints = append(hints, &Hint{Name: name.val, Value: *val})
		if p.peek() == tokenComma {
			p.advance()
		}
	}
	if _, err := p.expect(tokenCloseParen); err != nil {
		return nil, err
	}
	return &Hints{Hints: hints}, nil
}

// ---------------------------------------------------------------------------
// Grammar: field expressions (precedence climbing)
// ---------------------------------------------------------------------------

// parseFieldExpression is the entry point for field-level expressions.
func (p *parser) parseFieldExpression() (FieldExpression, error) {
	return p.parseOr()
}

func (p *parser) parseOr() (FieldExpression, error) {
	lhs, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for p.peek() == tokenOr {
		p.advance()
		rhs, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		lhs = &BinaryOperation{Op: OpOr, LHS: lhs, RHS: rhs}
	}
	return lhs, nil
}

func (p *parser) parseAnd() (FieldExpression, error) {
	lhs, err := p.parseComparison()
	if err != nil {
		return nil, err
	}
	for p.peek() == tokenAnd {
		p.advance()
		rhs, err := p.parseComparison()
		if err != nil {
			return nil, err
		}
		lhs = &BinaryOperation{Op: OpAnd, LHS: lhs, RHS: rhs}
	}
	return lhs, nil
}

func (p *parser) parseComparison() (FieldExpression, error) {
	lhs, err := p.parseAddition()
	if err != nil {
		return nil, err
	}

	switch p.peek() {
	case tokenEq:
		p.advance()
		// Check for nil
		if p.peek() == tokenNil {
			p.advance()
			return &UnaryOperation{Op: OpNotExists, Expression: lhs}, nil
		}
		rhs, err := p.parseAddition()
		if err != nil {
			return nil, err
		}
		return &BinaryOperation{Op: OpEqual, LHS: lhs, RHS: rhs}, nil
	case tokenNeq:
		p.advance()
		if p.peek() == tokenNil {
			p.advance()
			return &UnaryOperation{Op: OpExists, Expression: lhs}, nil
		}
		rhs, err := p.parseAddition()
		if err != nil {
			return nil, err
		}
		return &BinaryOperation{Op: OpNotEqual, LHS: lhs, RHS: rhs}, nil
	case tokenGt:
		p.advance()
		rhs, err := p.parseAddition()
		if err != nil {
			return nil, err
		}
		return &BinaryOperation{Op: OpGreater, LHS: lhs, RHS: rhs}, nil
	case tokenGte:
		p.advance()
		rhs, err := p.parseAddition()
		if err != nil {
			return nil, err
		}
		return &BinaryOperation{Op: OpGreaterEqual, LHS: lhs, RHS: rhs}, nil
	case tokenLt:
		p.advance()
		rhs, err := p.parseAddition()
		if err != nil {
			return nil, err
		}
		return &BinaryOperation{Op: OpLess, LHS: lhs, RHS: rhs}, nil
	case tokenLte:
		p.advance()
		rhs, err := p.parseAddition()
		if err != nil {
			return nil, err
		}
		return &BinaryOperation{Op: OpLessEqual, LHS: lhs, RHS: rhs}, nil
	case tokenRegex:
		p.advance()
		rhs, err := p.parseAddition()
		if err != nil {
			return nil, err
		}
		return &BinaryOperation{Op: OpRegex, LHS: lhs, RHS: rhs}, nil
	case tokenNotRegex:
		p.advance()
		rhs, err := p.parseAddition()
		if err != nil {
			return nil, err
		}
		return &BinaryOperation{Op: OpNotRegex, LHS: lhs, RHS: rhs}, nil
	}

	return lhs, nil
}

func (p *parser) parseAddition() (FieldExpression, error) {
	lhs, err := p.parseMultiplication()
	if err != nil {
		return nil, err
	}
	for p.peek() == tokenPlus || p.peek() == tokenMinus {
		t := p.advance()
		rhs, err := p.parseMultiplication()
		if err != nil {
			return nil, err
		}
		op := OpAdd
		if t.typ == tokenMinus {
			op = OpSub
		}
		lhs = &BinaryOperation{Op: op, LHS: lhs, RHS: rhs}
	}
	return lhs, nil
}

func (p *parser) parseMultiplication() (FieldExpression, error) {
	lhs, err := p.parsePower()
	if err != nil {
		return nil, err
	}
	for p.peek() == tokenStar || p.peek() == tokenSlash || p.peek() == tokenPercent {
		t := p.advance()
		rhs, err := p.parsePower()
		if err != nil {
			return nil, err
		}
		var op Operator
		switch t.typ {
		case tokenStar:
			op = OpMult
		case tokenSlash:
			op = OpDiv
		case tokenPercent:
			op = OpMod
		}
		lhs = &BinaryOperation{Op: op, LHS: lhs, RHS: rhs}
	}
	return lhs, nil
}

func (p *parser) parsePower() (FieldExpression, error) {
	lhs, err := p.parseUnary()
	if err != nil {
		return nil, err
	}
	if p.peek() == tokenCaret {
		p.advance()
		rhs, err := p.parsePower() // right-associative
		if err != nil {
			return nil, err
		}
		return &BinaryOperation{Op: OpPower, LHS: lhs, RHS: rhs}, nil
	}
	return lhs, nil
}

func (p *parser) parseUnary() (FieldExpression, error) {
	if p.peek() == tokenBang {
		p.advance()
		expr, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		return &UnaryOperation{Op: OpNot, Expression: expr}, nil
	}
	if p.peek() == tokenMinus {
		p.advance()
		expr, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		// Fold into static if possible
		if s, ok := expr.(*Static); ok && s.Type == TypeInt {
			return &Static{Type: TypeInt, IntVal: -s.IntVal}, nil
		}
		if s, ok := expr.(*Static); ok && s.Type == TypeFloat {
			return &Static{Type: TypeFloat, FloatVal: -s.FloatVal}, nil
		}
		return &UnaryOperation{Op: OpSub, Expression: expr}, nil
	}
	return p.parsePrimary()
}

func (p *parser) parsePrimary() (FieldExpression, error) {
	switch p.peek() {
	case tokenOpenParen:
		p.advance()
		expr, err := p.parseFieldExpression()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(tokenCloseParen); err != nil {
			return nil, fmt.Errorf("expected ')': %w", err)
		}
		return expr, nil

	case tokenInteger, tokenFloat, tokenString, tokenDuration, tokenTrue, tokenFalse, tokenNil:
		return p.parseStatic()

	case tokenDot:
		return p.parseAttribute()

	case tokenIdent:
		// Identifiers can be: intrinsics (status, kind, duration, name, ...),
		// status values (error, ok, unset), kind values (server, client, ...),
		// or scoped/unscoped attribute names.
		return p.parseIdentOrAttribute()

	default:
		return nil, fmt.Errorf("unexpected token %q at position %d", p.current().val, p.current().pos)
	}
}

// ---------------------------------------------------------------------------
// Grammar: attributes
// ---------------------------------------------------------------------------

// parseAttribute parses attribute references starting with '.'
func (p *parser) parseAttribute() (*Attribute, error) {
	switch p.peek() {
	case tokenDot:
		// Unscoped: .attribute.name
		p.advance()
		name, err := p.parseDottedName()
		if err != nil {
			return nil, err
		}
		return &Attribute{Name: name, Scope: AttributeScopeNone}, nil

	case tokenIdent:
		return p.parseIdentOrAttributeAsAttr()

	default:
		return nil, fmt.Errorf("expected attribute, got %q", p.current().val)
	}
}

// parseIdentOrAttribute handles identifiers that could be intrinsics, scoped attrs,
// status/kind values, or unscoped intrinsics.
func (p *parser) parseIdentOrAttribute() (FieldExpression, error) {
	name := p.current().val

	// Check if this is a status enum value (error, ok, unset)
	// Only treat as a status value if it's NOT followed by something that
	// makes it look like an attribute (dot, colon, comparison operator on LHS)
	if sv, ok := statusValues[name]; ok {
		// If the next token after is a comparison or logical operator or close brace,
		// this is a value on the RHS. If it's followed by a dot or colon, it's an attribute.
		if p.peekAt(1) != tokenDot && p.peekAt(1) != tokenColon {
			p.advance()
			return &Static{Type: TypeStatus, StatusVal: sv}, nil
		}
	}

	// Check if this is a kind enum value
	if kv, ok := kindValues[name]; ok {
		if p.peekAt(1) != tokenDot && p.peekAt(1) != tokenColon {
			p.advance()
			return &Static{Type: TypeKind, KindVal: kv}, nil
		}
	}

	attr, err := p.parseIdentOrAttributeAsAttr()
	if err != nil {
		return nil, err
	}
	return attr, nil
}

// statusValues and kindValues map identifier strings to their typed Static values.
var statusValues = map[string]Status{
	"error": StatusError,
	"ok":    StatusOk,
	"unset": StatusUnset,
}

var kindValues = map[string]Kind{
	"unspecified": KindUnspecified,
	"internal":    KindInternal,
	"client":      KindClient,
	"server":      KindServer,
	"producer":    KindProducer,
	"consumer":    KindConsumer,
}

func (p *parser) parseIdentOrAttributeAsAttr() (*Attribute, error) {
	t := p.advance() // consume the identifier
	name := t.val

	// Check for scoped intrinsics: span:id, trace:id, event:name, etc.
	if p.peek() == tokenColon {
		p.advance()           // consume ':'
		suffix := p.advance() // consume the intrinsic name
		fullName := name + ":" + suffix.val
		if intr := LookupIntrinsic(fullName); intr != IntrinsicNone {
			return &Attribute{Intrinsic: intr}, nil
		}
		return nil, fmt.Errorf("unknown scoped intrinsic %q", fullName)
	}

	// Check for parent prefix BEFORE checking intrinsics, because
	// "parent" is both an intrinsic and a scope prefix.
	parent := false
	if name == "parent" {
		if p.peek() == tokenDot {
			p.advance() // consume '.'
			parent = true
			name = p.advance().val // next identifier (resource, span, etc.)
		} else {
			// Standalone "parent" intrinsic
			return &Attribute{Intrinsic: IntrinsicParent}, nil
		}
	}

	// Check if this is an unscoped intrinsic (for non-parent names)
	if !parent {
		if intr := LookupIntrinsic(name); intr != IntrinsicNone {
			return &Attribute{Intrinsic: intr}, nil
		}
	}

	// Check for scope prefix: resource.attr, span.attr
	scope := AttributeScopeNone
	switch name {
	case "resource":
		if p.peek() == tokenDot {
			p.advance()
			scope = AttributeScopeResource
			attrName, err := p.parseDottedName()
			if err != nil {
				return nil, err
			}
			return &Attribute{Name: attrName, Scope: scope, Parent: parent}, nil
		}
	case "span":
		if p.peek() == tokenDot {
			p.advance()
			scope = AttributeScopeSpan
			attrName, err := p.parseDottedName()
			if err != nil {
				return nil, err
			}
			return &Attribute{Name: attrName, Scope: scope, Parent: parent}, nil
		}
	case "event":
		if p.peek() == tokenDot {
			p.advance()
			scope = AttributeScopeEvent
			attrName, err := p.parseDottedName()
			if err != nil {
				return nil, err
			}
			return &Attribute{Name: attrName, Scope: scope, Parent: parent}, nil
		}
	case "link":
		if p.peek() == tokenDot {
			p.advance()
			scope = AttributeScopeLink
			attrName, err := p.parseDottedName()
			if err != nil {
				return nil, err
			}
			return &Attribute{Name: attrName, Scope: scope, Parent: parent}, nil
		}
	case "instrumentation":
		if p.peek() == tokenDot {
			p.advance()
			scope = AttributeScopeInstrumentation
			attrName, err := p.parseDottedName()
			if err != nil {
				return nil, err
			}
			return &Attribute{Name: attrName, Scope: scope, Parent: parent}, nil
		}
	}

	// Just a plain attribute name (used in by(), select(), etc.)
	if parent {
		return &Attribute{Name: name, Scope: scope, Parent: true}, nil
	}

	// If we're here and the name is still a scope word without a dot following,
	// it might just be an identifier being used as an attribute name
	return &Attribute{Name: name, Scope: scope}, nil
}

// parseDottedName reads a dotted identifier like "http.status_code" or "service.name"
func (p *parser) parseDottedName() (string, error) {
	t := p.advance()
	if t.typ != tokenIdent && !isKeywordIdentifier(t.typ) {
		return "", fmt.Errorf("expected identifier, got %q", t.val)
	}
	name := t.val

	for p.peek() == tokenDot {
		// Look ahead: is the next thing an identifier?
		if p.peekAt(1) == tokenIdent || isKeywordIdentifier(p.peekAt(1)) {
			p.advance() // consume '.'
			next := p.advance()
			name += "." + next.val
		} else {
			break
		}
	}

	return name, nil
}

// isKeywordIdentifier returns true if a token type is a keyword that can also appear as an identifier.
func isKeywordIdentifier(t tokenType) bool {
	switch t {
	case tokenStatus, tokenKind,
		tokenStatusError, tokenStatusOk, tokenStatusUnset,
		tokenKindUnspecified, tokenKindInternal, tokenKindClient, tokenKindServer,
		tokenKindProducer, tokenKindConsumer,
		tokenCount, tokenMin, tokenMax, tokenSum, tokenAvg,
		tokenTrue, tokenFalse:
		return true
	}
	return false
}

// ---------------------------------------------------------------------------
// Grammar: statics (literals)
// ---------------------------------------------------------------------------

func (p *parser) parseStatic() (*Static, error) {
	t := p.advance()
	switch t.typ {
	case tokenInteger:
		v, err := strconv.ParseInt(t.val, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid integer %q: %w", t.val, err)
		}
		return &Static{Type: TypeInt, IntVal: v}, nil
	case tokenFloat:
		v, err := strconv.ParseFloat(t.val, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid float %q: %w", t.val, err)
		}
		return &Static{Type: TypeFloat, FloatVal: v}, nil
	case tokenString:
		return &Static{Type: TypeString, StringVal: t.val}, nil
	case tokenDuration:
		d, err := parseDuration(t.val)
		if err != nil {
			return nil, fmt.Errorf("invalid duration %q: %w", t.val, err)
		}
		return &Static{Type: TypeDuration, DurationVal: d}, nil
	case tokenTrue:
		return &Static{Type: TypeBoolean, BoolVal: true}, nil
	case tokenFalse:
		return &Static{Type: TypeBoolean, BoolVal: false}, nil
	case tokenNil:
		return &Static{Type: TypeNil}, nil
	default:
		return nil, fmt.Errorf("expected literal value, got %q", t.val)
	}
}

func (p *parser) parseStatusLiteral() (*Static, error) {
	t := p.advance()
	switch t.typ {
	case tokenStatusError:
		return &Static{Type: TypeStatus, StatusVal: StatusError}, nil
	case tokenStatusOk:
		return &Static{Type: TypeStatus, StatusVal: StatusOk}, nil
	case tokenStatusUnset:
		return &Static{Type: TypeStatus, StatusVal: StatusUnset}, nil
	default:
		return nil, fmt.Errorf("expected status literal, got %q", t.val)
	}
}

func (p *parser) parseKindLiteral() (*Static, error) {
	t := p.advance()
	switch t.typ {
	case tokenKindUnspecified:
		return &Static{Type: TypeKind, KindVal: KindUnspecified}, nil
	case tokenKindInternal:
		return &Static{Type: TypeKind, KindVal: KindInternal}, nil
	case tokenKindClient:
		return &Static{Type: TypeKind, KindVal: KindClient}, nil
	case tokenKindServer:
		return &Static{Type: TypeKind, KindVal: KindServer}, nil
	case tokenKindProducer:
		return &Static{Type: TypeKind, KindVal: KindProducer}, nil
	case tokenKindConsumer:
		return &Static{Type: TypeKind, KindVal: KindConsumer}, nil
	default:
		return nil, fmt.Errorf("expected kind literal, got %q", t.val)
	}
}

// parseDuration handles Go-style durations plus 'd' for days.
func parseDuration(s string) (time.Duration, error) {
	// Handle 'd' (day) suffix by converting to hours
	if len(s) > 0 && s[len(s)-1] == 'd' {
		// Extract the number before 'd'
		numStr := s[:len(s)-1]
		v, err := strconv.ParseFloat(numStr, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid day duration: %w", err)
		}
		return time.Duration(v * 24 * float64(time.Hour)), nil
	}

	// Handle 'ns' by checking for it specifically since 'n' alone would be ambiguous
	// Go's time.ParseDuration handles ns, us/µs, ms, s, m, h natively
	return time.ParseDuration(s)
}
