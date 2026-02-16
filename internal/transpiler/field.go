package transpiler

import (
	"fmt"
	"strings"

	"github.com/hacktohell/opex/internal/traceql"
)

// transpileFieldExpr converts a FieldExpression to a SQL expression string.
func (t *transpiler) transpileFieldExpr(expr traceql.FieldExpression) (string, error) {
	switch e := expr.(type) {
	case *traceql.BinaryOperation:
		return t.transpileBinaryOp(e)
	case *traceql.UnaryOperation:
		return t.transpileUnaryOp(e)
	case *traceql.Static:
		return staticToSQL(e), nil
	case *traceql.Attribute:
		return attributeToSQL(e), nil
	default:
		return "", fmt.Errorf("unsupported field expression type: %T", expr)
	}
}

// transpileBinaryOp converts a binary operation to SQL.
func (t *transpiler) transpileBinaryOp(b *traceql.BinaryOperation) (string, error) {
	// Special case: attribute compared to a typed value needs type coercion
	if attr, ok := b.LHS.(*traceql.Attribute); ok {
		if static, ok := b.RHS.(*traceql.Static); ok {
			return t.transpileAttributeComparison(attr, b.Op, static)
		}
	}

	lhs, err := t.transpileFieldExpr(b.LHS)
	if err != nil {
		return "", err
	}
	rhs, err := t.transpileFieldExpr(b.RHS)
	if err != nil {
		return "", err
	}

	op := operatorToSQL(b.Op)

	// Special case for regex operators
	switch b.Op {
	case traceql.OpRegex:
		return fmt.Sprintf("match(%s, %s)", lhs, rhs), nil
	case traceql.OpNotRegex:
		return fmt.Sprintf("NOT match(%s, %s)", lhs, rhs), nil
	}

	return fmt.Sprintf("(%s %s %s)", lhs, op, rhs), nil
}

// transpileUnaryOp converts a unary operation to SQL.
func (t *transpiler) transpileUnaryOp(u *traceql.UnaryOperation) (string, error) {
	switch u.Op {
	case traceql.OpExists:
		return t.transpileExists(u.Expression, true)
	case traceql.OpNotExists:
		return t.transpileExists(u.Expression, false)
	case traceql.OpNot:
		expr, err := t.transpileFieldExpr(u.Expression)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("NOT (%s)", expr), nil
	case traceql.OpSub:
		expr, err := t.transpileFieldExpr(u.Expression)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("(-%s)", expr), nil
	default:
		return "", fmt.Errorf("unsupported unary operator: %s", u.Op)
	}
}

// transpileExists generates SQL for != nil (exists) and = nil (not exists).
func (t *transpiler) transpileExists(expr traceql.FieldExpression, exists bool) (string, error) {
	attr, ok := expr.(*traceql.Attribute)
	if !ok {
		return "", fmt.Errorf("existence check requires an attribute, got %T", expr)
	}

	notPrefix := ""
	if !exists {
		notPrefix = "NOT "
	}

	switch attr.Scope {
	case traceql.AttributeScopeSpan:
		return fmt.Sprintf("%smapContains(SpanAttributes, '%s')", notPrefix, attr.Name), nil
	case traceql.AttributeScopeResource:
		return fmt.Sprintf("%smapContains(ResourceAttributes, '%s')", notPrefix, attr.Name), nil
	case traceql.AttributeScopeNone:
		if exists {
			return fmt.Sprintf("(mapContains(SpanAttributes, '%s') OR mapContains(ResourceAttributes, '%s'))",
				attr.Name, attr.Name), nil
		}
		return fmt.Sprintf("(NOT mapContains(SpanAttributes, '%s') AND NOT mapContains(ResourceAttributes, '%s'))",
			attr.Name, attr.Name), nil
	default:
		return fmt.Sprintf("%smapContains(SpanAttributes, '%s')", notPrefix, attr.Name), nil
	}
}

// transpileAttributeComparison handles the common case of attribute op static
// with proper type coercion for map values.
func (t *transpiler) transpileAttributeComparison(attr *traceql.Attribute, op traceql.Operator, static *traceql.Static) (string, error) {
	// Intrinsic attributes have first-class columns, no coercion needed
	if attr.Intrinsic != traceql.IntrinsicNone {
		col := intrinsicColumnSQL(attr.Intrinsic)
		val := staticToSQLForColumn(static, attr.Intrinsic)
		opStr := operatorToSQL(op)

		switch op {
		case traceql.OpRegex:
			return fmt.Sprintf("match(%s, %s)", col, val), nil
		case traceql.OpNotRegex:
			return fmt.Sprintf("NOT match(%s, %s)", col, val), nil
		default:
			return fmt.Sprintf("%s %s %s", col, opStr, val), nil
		}
	}

	opStr := operatorToSQL(op)
	val := staticToSQL(static)

	// For map attributes, we need type coercion based on the static type
	switch attr.Scope {
	case traceql.AttributeScopeSpan:
		col := mapAccessSQL("SpanAttributes", attr.Name, static.Type)
		if op == traceql.OpRegex {
			return fmt.Sprintf("match(%s, %s)", col, val), nil
		}
		if op == traceql.OpNotRegex {
			return fmt.Sprintf("NOT match(%s, %s)", col, val), nil
		}
		return fmt.Sprintf("%s %s %s", col, opStr, val), nil

	case traceql.AttributeScopeResource:
		// Special case: resource.service.name is a first-class column
		if attr.Name == "service.name" {
			if op == traceql.OpRegex {
				return fmt.Sprintf("match(ServiceName, %s)", val), nil
			}
			if op == traceql.OpNotRegex {
				return fmt.Sprintf("NOT match(ServiceName, %s)", val), nil
			}
			return fmt.Sprintf("ServiceName %s %s", opStr, val), nil
		}
		col := mapAccessSQL("ResourceAttributes", attr.Name, static.Type)
		if op == traceql.OpRegex {
			return fmt.Sprintf("match(%s, %s)", col, val), nil
		}
		if op == traceql.OpNotRegex {
			return fmt.Sprintf("NOT match(%s, %s)", col, val), nil
		}
		return fmt.Sprintf("%s %s %s", col, opStr, val), nil

	case traceql.AttributeScopeNone:
		// Unscoped: check both SpanAttributes and ResourceAttributes
		spanCol := mapAccessSQL("SpanAttributes", attr.Name, static.Type)
		resCol := mapAccessSQL("ResourceAttributes", attr.Name, static.Type)

		// Special case: service.name is a first-class column
		if attr.Name == "service.name" {
			if op == traceql.OpRegex {
				return fmt.Sprintf("(match(ServiceName, %s) OR match(%s, %s))",
					val, spanCol, val), nil
			}
			if op == traceql.OpNotRegex {
				return fmt.Sprintf("(NOT match(ServiceName, %s) AND NOT match(%s, %s))",
					val, spanCol, val), nil
			}
			return fmt.Sprintf("(ServiceName %s %s OR %s %s %s)",
				opStr, val, spanCol, opStr, val), nil
		}

		if op == traceql.OpRegex {
			return fmt.Sprintf("(match(%s, %s) OR match(%s, %s))", spanCol, val, resCol, val), nil
		}
		if op == traceql.OpNotRegex {
			return fmt.Sprintf("(NOT match(%s, %s) AND NOT match(%s, %s))", spanCol, val, resCol, val), nil
		}
		return fmt.Sprintf("(%s %s %s OR %s %s %s)",
			spanCol, opStr, val, resCol, opStr, val), nil

	default:
		col := mapAccessSQL("SpanAttributes", attr.Name, static.Type)
		return fmt.Sprintf("%s %s %s", col, opStr, val), nil
	}
}

// mapAccessSQL returns the SQL to access a map value with optional type coercion.
func mapAccessSQL(mapCol, key string, valType traceql.StaticType) string {
	access := fmt.Sprintf("%s['%s']", mapCol, key)
	switch valType {
	case traceql.TypeInt:
		return fmt.Sprintf("toInt64OrZero(%s)", access)
	case traceql.TypeFloat:
		return fmt.Sprintf("toFloat64OrZero(%s)", access)
	case traceql.TypeBoolean:
		// Booleans are stored as 'true'/'false' strings
		return access
	default:
		return access
	}
}

// attributeToSQL converts an Attribute to its SQL column representation.
func attributeToSQL(attr *traceql.Attribute) string {
	if attr.Intrinsic != traceql.IntrinsicNone {
		return intrinsicColumnSQL(attr.Intrinsic)
	}

	switch attr.Scope {
	case traceql.AttributeScopeSpan:
		return fmt.Sprintf("SpanAttributes['%s']", attr.Name)
	case traceql.AttributeScopeResource:
		// Special case
		if attr.Name == "service.name" {
			return "ServiceName"
		}
		return fmt.Sprintf("ResourceAttributes['%s']", attr.Name)
	case traceql.AttributeScopeNone:
		if attr.Name == "service.name" {
			return "ServiceName"
		}
		// For unscoped, prefer SpanAttributes as the column reference.
		// The comparison function handles the OR fallback.
		return fmt.Sprintf("SpanAttributes['%s']", attr.Name)
	default:
		return fmt.Sprintf("SpanAttributes['%s']", attr.Name)
	}
}

// intrinsicColumnSQL maps intrinsic attributes to ClickHouse columns.
func intrinsicColumnSQL(i traceql.Intrinsic) string {
	switch i {
	case traceql.IntrinsicDuration:
		return "Duration"
	case traceql.IntrinsicName:
		return "SpanName"
	case traceql.IntrinsicStatus:
		return "StatusCode"
	case traceql.IntrinsicStatusMessage:
		return "StatusMessage"
	case traceql.IntrinsicKind:
		return "SpanKind"
	case traceql.IntrinsicTraceID:
		return "TraceId"
	case traceql.IntrinsicSpanID:
		return "SpanId"
	case traceql.IntrinsicParentID:
		return "ParentSpanId"
	case traceql.IntrinsicInstrumentationName:
		return "ScopeName"
	case traceql.IntrinsicInstrumentationVersion:
		return "ScopeVersion"
	case traceql.IntrinsicTraceRootService:
		// This requires a subquery, return a placeholder
		return "ServiceName" // simplified; full implementation needs subquery
	case traceql.IntrinsicTraceRootSpan:
		return "SpanName" // simplified
	case traceql.IntrinsicTraceDuration:
		return "Duration" // simplified
	case traceql.IntrinsicSpanStartTime:
		return "Timestamp"
	default:
		return "SpanName"
	}
}

// staticToSQL converts a Static value to its SQL literal.
func staticToSQL(s *traceql.Static) string {
	switch s.Type {
	case traceql.TypeNil:
		return "NULL"
	case traceql.TypeInt:
		return fmt.Sprintf("%d", s.IntVal)
	case traceql.TypeFloat:
		return fmt.Sprintf("%g", s.FloatVal)
	case traceql.TypeString:
		return fmt.Sprintf("'%s'", escapeSQL(s.StringVal))
	case traceql.TypeBoolean:
		if s.BoolVal {
			return "'true'"
		}
		return "'false'"
	case traceql.TypeDuration:
		// Duration is stored in nanoseconds
		return fmt.Sprintf("%d", s.DurationVal.Nanoseconds())
	case traceql.TypeStatus:
		return fmt.Sprintf("'%s'", statusToClickHouse(s.StatusVal))
	case traceql.TypeKind:
		return fmt.Sprintf("'%s'", kindToClickHouse(s.KindVal))
	default:
		return "NULL"
	}
}

// staticToSQLForColumn converts a Static to SQL appropriate for comparison
// with a specific intrinsic column.
func staticToSQLForColumn(s *traceql.Static, intrinsic traceql.Intrinsic) string {
	// For duration intrinsics, ensure we output nanoseconds
	if intrinsic == traceql.IntrinsicDuration || intrinsic == traceql.IntrinsicTraceDuration {
		if s.Type == traceql.TypeDuration {
			return fmt.Sprintf("%d", s.DurationVal.Nanoseconds())
		}
	}
	return staticToSQL(s)
}

// statusToClickHouse maps our Status enum to ClickHouse OTEL column values.
func statusToClickHouse(s traceql.Status) string {
	switch s {
	case traceql.StatusError:
		return "STATUS_CODE_ERROR"
	case traceql.StatusOk:
		return "STATUS_CODE_OK"
	case traceql.StatusUnset:
		return "STATUS_CODE_UNSET"
	default:
		return "STATUS_CODE_UNSET"
	}
}

// kindToClickHouse maps our Kind enum to ClickHouse OTEL column values.
func kindToClickHouse(k traceql.Kind) string {
	switch k {
	case traceql.KindServer:
		return "SPAN_KIND_SERVER"
	case traceql.KindClient:
		return "SPAN_KIND_CLIENT"
	case traceql.KindInternal:
		return "SPAN_KIND_INTERNAL"
	case traceql.KindProducer:
		return "SPAN_KIND_PRODUCER"
	case traceql.KindConsumer:
		return "SPAN_KIND_CONSUMER"
	case traceql.KindUnspecified:
		return "SPAN_KIND_UNSPECIFIED"
	default:
		return "SPAN_KIND_UNSPECIFIED"
	}
}

// operatorToSQL converts an Operator to its SQL equivalent.
func operatorToSQL(op traceql.Operator) string {
	switch op {
	case traceql.OpEqual:
		return "="
	case traceql.OpNotEqual:
		return "!="
	case traceql.OpGreater:
		return ">"
	case traceql.OpGreaterEqual:
		return ">="
	case traceql.OpLess:
		return "<"
	case traceql.OpLessEqual:
		return "<="
	case traceql.OpAnd:
		return "AND"
	case traceql.OpOr:
		return "OR"
	case traceql.OpAdd:
		return "+"
	case traceql.OpSub:
		return "-"
	case traceql.OpMult:
		return "*"
	case traceql.OpDiv:
		return "/"
	case traceql.OpMod:
		return "%"
	case traceql.OpPower:
		return "^"
	case traceql.OpIn:
		return "IN"
	case traceql.OpNotIn:
		return "NOT IN"
	default:
		return "="
	}
}

// aggregateToSQL converts an Aggregate to its SQL function call.
func aggregateToSQL(a *traceql.Aggregate) string {
	switch a.Op {
	case traceql.AggregateCount:
		return "count(*)"
	case traceql.AggregateMin:
		if a.Expression != nil {
			attr, ok := a.Expression.(*traceql.Attribute)
			if ok {
				return fmt.Sprintf("min(%s)", attributeToSQL(attr))
			}
		}
		return "min(Duration)"
	case traceql.AggregateMax:
		if a.Expression != nil {
			attr, ok := a.Expression.(*traceql.Attribute)
			if ok {
				return fmt.Sprintf("max(%s)", attributeToSQL(attr))
			}
		}
		return "max(Duration)"
	case traceql.AggregateSum:
		if a.Expression != nil {
			attr, ok := a.Expression.(*traceql.Attribute)
			if ok {
				return fmt.Sprintf("sum(%s)", attributeToSQL(attr))
			}
		}
		return "sum(Duration)"
	case traceql.AggregateAvg:
		if a.Expression != nil {
			attr, ok := a.Expression.(*traceql.Attribute)
			if ok {
				return fmt.Sprintf("avg(%s)", attributeToSQL(attr))
			}
		}
		return "avg(Duration)"
	default:
		return "count(*)"
	}
}

func escapeSQL(s string) string {
	return strings.ReplaceAll(s, "'", "\\'")
}
