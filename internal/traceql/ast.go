// Package traceql implements a TraceQL parser producing a fully-exported AST.
//
// The grammar follows Grafana Tempo's TraceQL specification:
// https://grafana.com/docs/tempo/latest/traceql/
package traceql

import (
	"fmt"
	"strings"
	"time"
)

// ---------------------------------------------------------------------------
// Enums
// ---------------------------------------------------------------------------

// Operator represents a TraceQL operator.
type Operator int

// Operator constants for TraceQL expressions.
const (
	// OpNone represents no operator.
	OpNone Operator = iota
	// OpAdd is the addition operator.
	OpAdd
	// OpSub is the subtraction operator.
	OpSub
	// OpDiv is the division operator.
	OpDiv
	// OpMod is the modulo operator.
	OpMod
	// OpMult is the multiplication operator.
	OpMult
	// OpPower is the exponentiation operator.
	OpPower
	// OpEqual is the equality operator.
	OpEqual
	// OpNotEqual is the inequality operator.
	OpNotEqual
	// OpRegex is the regex match operator.
	OpRegex
	// OpNotRegex is the negated regex match operator.
	OpNotRegex
	// OpGreater is the greater-than operator.
	OpGreater
	// OpGreaterEqual is the greater-than-or-equal operator.
	OpGreaterEqual
	// OpLess is the less-than operator.
	OpLess
	// OpLessEqual is the less-than-or-equal operator.
	OpLessEqual
	// OpAnd is the logical AND operator.
	OpAnd
	// OpOr is the logical OR operator.
	OpOr
	// OpNot is the logical NOT operator.
	OpNot
	// OpSpansetAnd is the spanset-level AND operator.
	OpSpansetAnd
	// OpSpansetUnion is the spanset-level UNION operator.
	OpSpansetUnion
	// OpSpansetChild is the structural child operator.
	OpSpansetChild
	// OpSpansetParent is the structural parent operator.
	OpSpansetParent
	// OpSpansetDescendant is the structural descendant operator.
	OpSpansetDescendant
	// OpSpansetAncestor is the structural ancestor operator.
	OpSpansetAncestor
	// OpSpansetSibling is the structural sibling operator.
	OpSpansetSibling
	// OpSpansetNotChild is the negated structural child operator.
	OpSpansetNotChild
	// OpSpansetNotParent is the negated structural parent operator.
	OpSpansetNotParent
	// OpSpansetNotDescendant is the negated structural descendant operator.
	OpSpansetNotDescendant
	// OpSpansetNotAncestor is the negated structural ancestor operator.
	OpSpansetNotAncestor
	// OpSpansetNotSibling is the negated structural sibling operator.
	OpSpansetNotSibling
	// OpSpansetUnionChild is the union structural child operator.
	OpSpansetUnionChild
	// OpSpansetUnionParent is the union structural parent operator.
	OpSpansetUnionParent
	// OpSpansetUnionDescendant is the union structural descendant operator.
	OpSpansetUnionDescendant
	// OpSpansetUnionAncestor is the union structural ancestor operator.
	OpSpansetUnionAncestor
	// OpSpansetUnionSibling is the union structural sibling operator.
	OpSpansetUnionSibling
	// OpExists is the exists operator (post AST-rewrite).
	OpExists
	// OpNotExists is the not-exists operator (post AST-rewrite).
	OpNotExists
	// OpIn is the IN operator (post AST-rewrite).
	OpIn
	// OpNotIn is the NOT IN operator (post AST-rewrite).
	OpNotIn
)

var operatorStrings = map[Operator]string{
	OpAdd: "+", OpSub: "-", OpDiv: "/", OpMod: "%", OpMult: "*", OpPower: "^",
	OpEqual: "=", OpNotEqual: "!=", OpRegex: "=~", OpNotRegex: "!~",
	OpGreater: ">", OpGreaterEqual: ">=", OpLess: "<", OpLessEqual: "<=",
	OpAnd: "&&", OpOr: "||", OpNot: "!",
	OpSpansetAnd: "&&", OpSpansetUnion: "||",
	OpSpansetChild: ">", OpSpansetParent: "<",
	OpSpansetDescendant: ">>", OpSpansetAncestor: "<<",
	OpSpansetSibling:  "~",
	OpSpansetNotChild: "!>", OpSpansetNotParent: "!<",
	OpSpansetNotDescendant: "!>>", OpSpansetNotAncestor: "!<<",
	OpSpansetNotSibling: "!~",
	OpSpansetUnionChild: "&>", OpSpansetUnionParent: "&<",
	OpSpansetUnionDescendant: "&>>", OpSpansetUnionAncestor: "&<<",
	OpSpansetUnionSibling: "&~",
	OpExists:              "!= nil", OpNotExists: "= nil",
	OpIn: "IN", OpNotIn: "NOT IN",
}

func (op Operator) String() string {
	if s, ok := operatorStrings[op]; ok {
		return s
	}
	return "?"
}

// StaticType represents the type of a Static value.
type StaticType int

// StaticType constants for literal values.
const (
	// TypeNil represents a nil value.
	TypeNil StaticType = iota
	// TypeInt represents an integer value.
	TypeInt
	// TypeFloat represents a floating-point value.
	TypeFloat
	// TypeString represents a string value.
	TypeString
	// TypeBoolean represents a boolean value.
	TypeBoolean
	// TypeDuration represents a duration value.
	TypeDuration
	// TypeStatus represents a span status value.
	TypeStatus
	// TypeKind represents a span kind value.
	TypeKind
	// TypeIntArray represents an integer array value.
	TypeIntArray
	// TypeFloatArray represents a floating-point array value.
	TypeFloatArray
	// TypeStringArray represents a string array value.
	TypeStringArray
	// TypeBooleanArray represents a boolean array value.
	TypeBooleanArray
)

func (t StaticType) String() string {
	switch t {
	case TypeNil:
		return "nil"
	case TypeInt:
		return "int"
	case TypeFloat:
		return "float"
	case TypeString:
		return "string"
	case TypeBoolean:
		return "boolean"
	case TypeDuration:
		return "duration"
	case TypeStatus:
		return "status"
	case TypeKind:
		return "kind"
	case TypeIntArray:
		return "int[]"
	case TypeFloatArray:
		return "float[]"
	case TypeStringArray:
		return "string[]"
	case TypeBooleanArray:
		return "boolean[]"
	default:
		return "unknown"
	}
}

// Status represents span status.
type Status int

// Status constants for span status values.
const (
	// StatusError represents an error status.
	StatusError Status = iota
	// StatusOk represents a successful status.
	StatusOk
	// StatusUnset represents an unset status.
	StatusUnset
)

func (s Status) String() string {
	switch s {
	case StatusError:
		return "error"
	case StatusOk:
		return "ok"
	case StatusUnset:
		return "unset"
	default:
		return "unknown"
	}
}

// Kind represents span kind.
type Kind int

// Kind constants for span kind values.
const (
	// KindUnspecified represents an unspecified span kind.
	KindUnspecified Kind = iota
	// KindInternal represents an internal span kind.
	KindInternal
	// KindClient represents a client span kind.
	KindClient
	// KindServer represents a server span kind.
	KindServer
	// KindProducer represents a producer span kind.
	KindProducer
	// KindConsumer represents a consumer span kind.
	KindConsumer
)

func (k Kind) String() string {
	switch k {
	case KindUnspecified:
		return "unspecified"
	case KindInternal:
		return "internal"
	case KindClient:
		return "client"
	case KindServer:
		return "server"
	case KindProducer:
		return "producer"
	case KindConsumer:
		return "consumer"
	default:
		return "unknown"
	}
}

// AttributeScope represents the scope of an attribute.
type AttributeScope int

// AttributeScope constants for attribute lookup scope.
const (
	// AttributeScopeNone represents no specific scope.
	AttributeScopeNone AttributeScope = iota
	// AttributeScopeResource represents the resource scope.
	AttributeScopeResource
	// AttributeScopeSpan represents the span scope.
	AttributeScopeSpan
	// AttributeScopeEvent represents the event scope.
	AttributeScopeEvent
	// AttributeScopeLink represents the link scope.
	AttributeScopeLink
	// AttributeScopeInstrumentation represents the instrumentation scope.
	AttributeScopeInstrumentation
)

func (s AttributeScope) String() string {
	switch s {
	case AttributeScopeNone:
		return "none"
	case AttributeScopeResource:
		return "resource"
	case AttributeScopeSpan:
		return "span"
	case AttributeScopeEvent:
		return "event"
	case AttributeScopeLink:
		return "link"
	case AttributeScopeInstrumentation:
		return "instrumentation"
	default:
		return "unknown"
	}
}

// Intrinsic represents a built-in attribute.
type Intrinsic int

// Intrinsic constants for built-in span attributes.
const (
	// IntrinsicNone represents no intrinsic.
	IntrinsicNone Intrinsic = iota
	// IntrinsicDuration represents span duration.
	IntrinsicDuration
	// IntrinsicName represents span name.
	IntrinsicName
	// IntrinsicStatus represents span status.
	IntrinsicStatus
	// IntrinsicStatusMessage represents span status message.
	IntrinsicStatusMessage
	// IntrinsicKind represents span kind.
	IntrinsicKind
	// IntrinsicChildCount represents span child count.
	IntrinsicChildCount
	// IntrinsicTraceRootService represents root service name.
	IntrinsicTraceRootService
	// IntrinsicTraceRootSpan represents root span name.
	IntrinsicTraceRootSpan
	// IntrinsicTraceDuration represents trace duration.
	IntrinsicTraceDuration
	// IntrinsicTraceID represents trace ID.
	IntrinsicTraceID
	// IntrinsicSpanID represents span ID.
	IntrinsicSpanID
	// IntrinsicParentID represents parent span ID.
	IntrinsicParentID
	// IntrinsicEventName represents event name.
	IntrinsicEventName
	// IntrinsicEventTimeSinceStart represents event time since span start.
	IntrinsicEventTimeSinceStart
	// IntrinsicLinkSpanID represents link span ID.
	IntrinsicLinkSpanID
	// IntrinsicLinkTraceID represents link trace ID.
	IntrinsicLinkTraceID
	// IntrinsicInstrumentationName represents instrumentation library name.
	IntrinsicInstrumentationName
	// IntrinsicInstrumentationVersion represents instrumentation library version.
	IntrinsicInstrumentationVersion
	// IntrinsicParent represents parent span.
	IntrinsicParent
	// IntrinsicSpanStartTime represents span start time.
	IntrinsicSpanStartTime
	// IntrinsicNestedSetParent is Tempo's nested-set parent index.
	// Root spans have nestedSetParent = -1; non-root spans >= 0.
	IntrinsicNestedSetParent
)

var intrinsicStrings = map[Intrinsic]string{
	IntrinsicDuration:               "duration",
	IntrinsicName:                   "name",
	IntrinsicStatus:                 "status",
	IntrinsicStatusMessage:          "statusMessage",
	IntrinsicKind:                   "kind",
	IntrinsicChildCount:             "childCount",
	IntrinsicTraceRootService:       "rootServiceName",
	IntrinsicTraceRootSpan:          "rootName",
	IntrinsicTraceDuration:          "traceDuration",
	IntrinsicTraceID:                "trace:id",
	IntrinsicSpanID:                 "span:id",
	IntrinsicParentID:               "span:parentID",
	IntrinsicEventName:              "event:name",
	IntrinsicEventTimeSinceStart:    "event:timeSinceStart",
	IntrinsicLinkSpanID:             "link:spanID",
	IntrinsicLinkTraceID:            "link:traceID",
	IntrinsicInstrumentationName:    "instrumentation:name",
	IntrinsicInstrumentationVersion: "instrumentation:version",
	IntrinsicParent:                 "parent",
	IntrinsicSpanStartTime:          "spanStartTime",
	IntrinsicNestedSetParent:        "nestedSetParent",
}

func (i Intrinsic) String() string {
	if s, ok := intrinsicStrings[i]; ok {
		return s
	}
	return "none"
}

// AggregateOp represents a span-level aggregate function.
type AggregateOp int

// AggregateOp constants for span-level aggregate functions.
const (
	// AggregateCount represents the count() aggregate.
	AggregateCount AggregateOp = iota
	// AggregateMax represents the max() aggregate.
	AggregateMax
	// AggregateMin represents the min() aggregate.
	AggregateMin
	// AggregateSum represents the sum() aggregate.
	AggregateSum
	// AggregateAvg represents the avg() aggregate.
	AggregateAvg
)

func (a AggregateOp) String() string {
	switch a {
	case AggregateCount:
		return "count"
	case AggregateMax:
		return "max"
	case AggregateMin:
		return "min"
	case AggregateSum:
		return "sum"
	case AggregateAvg:
		return "avg"
	default:
		return "unknown"
	}
}

// MetricsAggregateOp represents a metrics pipeline function.
type MetricsAggregateOp int

// MetricsAggregateOp constants for metrics pipeline functions.
const (
	// MetricsAggregateRate represents the rate() function.
	MetricsAggregateRate MetricsAggregateOp = iota
	// MetricsAggregateCountOverTime represents the count_over_time() function.
	MetricsAggregateCountOverTime
	// MetricsAggregateMinOverTime represents the min_over_time() function.
	MetricsAggregateMinOverTime
	// MetricsAggregateMaxOverTime represents the max_over_time() function.
	MetricsAggregateMaxOverTime
	// MetricsAggregateAvgOverTime represents the avg_over_time() function.
	MetricsAggregateAvgOverTime
	// MetricsAggregateSumOverTime represents the sum_over_time() function.
	MetricsAggregateSumOverTime
	// MetricsAggregateQuantileOverTime represents the quantile_over_time() function.
	MetricsAggregateQuantileOverTime
	// MetricsAggregateHistogramOverTime represents the histogram_over_time() function.
	MetricsAggregateHistogramOverTime
)

func (m MetricsAggregateOp) String() string {
	switch m {
	case MetricsAggregateRate:
		return "rate"
	case MetricsAggregateCountOverTime:
		return "count_over_time"
	case MetricsAggregateMinOverTime:
		return "min_over_time"
	case MetricsAggregateMaxOverTime:
		return "max_over_time"
	case MetricsAggregateAvgOverTime:
		return "avg_over_time"
	case MetricsAggregateSumOverTime:
		return "sum_over_time"
	case MetricsAggregateQuantileOverTime:
		return "quantile_over_time"
	case MetricsAggregateHistogramOverTime:
		return "histogram_over_time"
	default:
		return "unknown"
	}
}

// ---------------------------------------------------------------------------
// AST Node Types -- all fields exported
// ---------------------------------------------------------------------------

// RootExpr is the top-level AST node.
type RootExpr struct {
	Pipeline Pipeline
	Hints    *Hints
}

// Hints holds query hints.
type Hints struct {
	Hints []*Hint
}

func (h *Hints) String() string {
	if h == nil || len(h.Hints) == 0 {
		return ""
	}
	parts := make([]string, len(h.Hints))
	for i, hint := range h.Hints {
		parts[i] = fmt.Sprintf("%s=%s", hint.Name, hint.Value.String())
	}
	return "with(" + strings.Join(parts, ", ") + ")"
}

// Hint is a single query hint key=value.
type Hint struct {
	Name  string
	Value Static
}

// Pipeline is an ordered sequence of pipeline elements.
type Pipeline struct {
	Elements []PipelineElement
}

func (Pipeline) pipelineElement() {}

func (p Pipeline) String() string {
	parts := make([]string, len(p.Elements))
	for i, e := range p.Elements {
		parts[i] = e.String()
	}
	return strings.Join(parts, " | ")
}

// PipelineElement is the interface for anything in a pipeline.
type PipelineElement interface {
	fmt.Stringer
	pipelineElement()
}

// FieldExpression is the interface for span-level expressions.
type FieldExpression interface {
	fmt.Stringer
	fieldExpression()
}

// ScalarExpression is the interface for scalar (aggregate) expressions.
type ScalarExpression interface {
	fmt.Stringer
	scalarExpression()
}

// ---------------------------------------------------------------------------
// Pipeline Elements
// ---------------------------------------------------------------------------

// SpansetFilter represents { expression }.
type SpansetFilter struct {
	Expression FieldExpression
}

func (SpansetFilter) pipelineElement() {}
func (f SpansetFilter) String() string {
	return fmt.Sprintf("{ %s }", f.Expression)
}

// SpansetOperation represents LHS op RHS at the spanset level (&&, ||, structural).
type SpansetOperation struct {
	Op  Operator
	LHS PipelineElement
	RHS PipelineElement
}

func (SpansetOperation) pipelineElement() {}
func (o SpansetOperation) String() string {
	return fmt.Sprintf("(%s %s %s)", o.LHS, o.Op, o.RHS)
}

// ScalarFilter compares a scalar expression against another (e.g. count() > 5).
type ScalarFilter struct {
	Op  Operator
	LHS ScalarExpression
	RHS ScalarExpression
}

func (ScalarFilter) pipelineElement() {}
func (f ScalarFilter) String() string {
	return fmt.Sprintf("%s %s %s", f.LHS, f.Op, f.RHS)
}

// Aggregate represents an aggregate function: count(), min(expr), max(expr), etc.
type Aggregate struct {
	Op         AggregateOp
	Expression FieldExpression // nil for count()
}

func (Aggregate) pipelineElement()  {}
func (Aggregate) scalarExpression() {}
func (a Aggregate) String() string {
	if a.Expression == nil {
		return fmt.Sprintf("%s()", a.Op)
	}
	return fmt.Sprintf("%s(%s)", a.Op, a.Expression)
}

// GroupOperation represents by(expr).
type GroupOperation struct {
	Expression FieldExpression
}

func (GroupOperation) pipelineElement() {}
func (g GroupOperation) String() string {
	return fmt.Sprintf("by(%s)", g.Expression)
}

// CoalesceOperation represents coalesce().
type CoalesceOperation struct{}

func (CoalesceOperation) pipelineElement() {}
func (CoalesceOperation) String() string   { return "coalesce()" }

// SelectOperation represents select(attr1, attr2, ...).
type SelectOperation struct {
	Attrs []Attribute
}

func (SelectOperation) pipelineElement() {}
func (s SelectOperation) String() string {
	parts := make([]string, len(s.Attrs))
	for i, a := range s.Attrs {
		parts[i] = a.String()
	}
	return fmt.Sprintf("select(%s)", strings.Join(parts, ", "))
}

// MetricsAggregate represents a metrics pipeline function like rate(), count_over_time(), etc.
type MetricsAggregate struct {
	Op     MetricsAggregateOp
	By     []Attribute
	Attr   FieldExpression // The field being aggregated (e.g., duration in quantile_over_time(duration, 0.95))
	Floats []float64       // Extra numeric args (e.g., 0.95 for quantile)
}

func (MetricsAggregate) pipelineElement() {}
func (m MetricsAggregate) String() string {
	var args []string
	if m.Attr != nil {
		args = append(args, m.Attr.String())
	}
	for _, f := range m.Floats {
		args = append(args, fmt.Sprintf("%g", f))
	}
	by := ""
	if len(m.By) > 0 {
		byParts := make([]string, len(m.By))
		for i, a := range m.By {
			byParts[i] = a.String()
		}
		by = " by(" + strings.Join(byParts, ", ") + ")"
	}
	return fmt.Sprintf("%s(%s)%s", m.Op, strings.Join(args, ", "), by)
}

// ---------------------------------------------------------------------------
// Field Expressions
// ---------------------------------------------------------------------------

// BinaryOperation represents LHS op RHS.
type BinaryOperation struct {
	Op  Operator
	LHS FieldExpression
	RHS FieldExpression
}

func (BinaryOperation) fieldExpression() {}
func (b BinaryOperation) String() string {
	return fmt.Sprintf("(%s %s %s)", b.LHS, b.Op, b.RHS)
}

// UnaryOperation represents op expression (e.g., !expr, -expr).
type UnaryOperation struct {
	Op         Operator
	Expression FieldExpression
}

func (UnaryOperation) fieldExpression() {}
func (u UnaryOperation) String() string {
	return fmt.Sprintf("%s%s", u.Op, u.Expression)
}

// Static represents a literal value.
type Static struct {
	Type        StaticType
	IntVal      int64
	FloatVal    float64
	StringVal   string
	BoolVal     bool
	DurationVal time.Duration
	StatusVal   Status
	KindVal     Kind
	// Array variants
	IntArrayVal     []int64
	FloatArrayVal   []float64
	StringArrayVal  []string
	BooleanArrayVal []bool
}

func (Static) fieldExpression()  {}
func (Static) scalarExpression() {}

func (s Static) String() string {
	switch s.Type {
	case TypeNil:
		return "nil"
	case TypeInt:
		return fmt.Sprintf("%d", s.IntVal)
	case TypeFloat:
		return fmt.Sprintf("%g", s.FloatVal)
	case TypeString:
		return fmt.Sprintf("%q", s.StringVal)
	case TypeBoolean:
		if s.BoolVal {
			return "true"
		}
		return "false"
	case TypeDuration:
		return s.DurationVal.String()
	case TypeStatus:
		return s.StatusVal.String()
	case TypeKind:
		return s.KindVal.String()
	default:
		return "?"
	}
}

// Attribute represents a reference to a span/resource/intrinsic attribute.
type Attribute struct {
	Name      string
	Scope     AttributeScope
	Parent    bool
	Intrinsic Intrinsic
}

func (Attribute) fieldExpression() {}

func (a Attribute) String() string {
	if a.Intrinsic != IntrinsicNone {
		return a.Intrinsic.String()
	}
	prefix := ""
	if a.Parent {
		prefix = "parent."
	}
	switch a.Scope {
	case AttributeScopeResource:
		return prefix + "resource." + a.Name
	case AttributeScopeSpan:
		return prefix + "span." + a.Name
	case AttributeScopeEvent:
		return prefix + "event." + a.Name
	case AttributeScopeLink:
		return prefix + "link." + a.Name
	case AttributeScopeInstrumentation:
		return prefix + "instrumentation." + a.Name
	default:
		return prefix + "." + a.Name
	}
}

// ScalarOperation represents arithmetic between two scalars (e.g., count() + 1).
type ScalarOperation struct {
	Op  Operator
	LHS ScalarExpression
	RHS ScalarExpression
}

func (ScalarOperation) scalarExpression() {}
func (o ScalarOperation) String() string {
	return fmt.Sprintf("(%s %s %s)", o.LHS, o.Op, o.RHS)
}

// ---------------------------------------------------------------------------
// Intrinsic lookup
// ---------------------------------------------------------------------------

var intrinsicFromString = map[string]Intrinsic{
	"duration":        IntrinsicDuration,
	"name":            IntrinsicName,
	"status":          IntrinsicStatus,
	"statusMessage":   IntrinsicStatusMessage,
	"kind":            IntrinsicKind,
	"childCount":      IntrinsicChildCount,
	"rootServiceName": IntrinsicTraceRootService,
	"rootName":        IntrinsicTraceRootSpan,
	"traceDuration":   IntrinsicTraceDuration,
	// Scoped intrinsics
	"trace:id":                IntrinsicTraceID,
	"trace:rootService":       IntrinsicTraceRootService,
	"trace:rootName":          IntrinsicTraceRootSpan,
	"trace:duration":          IntrinsicTraceDuration,
	"span:id":                 IntrinsicSpanID,
	"span:parentID":           IntrinsicParentID,
	"span:duration":           IntrinsicDuration,
	"span:name":               IntrinsicName,
	"span:status":             IntrinsicStatus,
	"span:statusMessage":      IntrinsicStatusMessage,
	"span:kind":               IntrinsicKind,
	"span:childCount":         IntrinsicChildCount,
	"event:name":              IntrinsicEventName,
	"event:timeSinceStart":    IntrinsicEventTimeSinceStart,
	"link:spanID":             IntrinsicLinkSpanID,
	"link:traceID":            IntrinsicLinkTraceID,
	"instrumentation:name":    IntrinsicInstrumentationName,
	"instrumentation:version": IntrinsicInstrumentationVersion,
	"parent":                  IntrinsicParent,
	"nestedSetParent":         IntrinsicNestedSetParent,
}

// LookupIntrinsic returns the Intrinsic for a given string, or IntrinsicNone.
func LookupIntrinsic(s string) Intrinsic {
	if i, ok := intrinsicFromString[s]; ok {
		return i
	}
	return IntrinsicNone
}
