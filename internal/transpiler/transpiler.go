// Package transpiler converts TraceQL AST to ClickHouse SQL queries.
package transpiler

import (
	"fmt"
	"strings"
	"time"

	"github.com/hacktohell/opex/internal/traceql"
)

// TranspileResult holds the generated SQL and metadata.
type TranspileResult struct {
	SQL  string
	Args []any
}

// TranspileOptions configures query generation.
type TranspileOptions struct {
	Table string
	Start time.Time
	End   time.Time
	Limit int

	// NoLimit disables the LIMIT clause entirely. When true, the Limit
	// field is ignored and no LIMIT is appended to the generated SQL.
	// Use this for subqueries that feed into aggregations where an
	// artificial cap would produce incorrect results.
	NoLimit bool

	// UsePrewhere uses PREWHERE instead of WHERE for time range filters.
	// PREWHERE reads only indexed columns first, then applies the rest,
	// which can significantly reduce I/O for large tables.
	UsePrewhere bool

	// SampleRate enables ClickHouse SAMPLE clause (0 < rate <= 1.0).
	// A value of 0 disables sampling.
	SampleRate float64
}

// Transpile converts a TraceQL RootExpr into a ClickHouse SQL query
// that returns matching TraceId values.
func Transpile(root *traceql.RootExpr, opts TranspileOptions) (*TranspileResult, error) {
	if opts.Table == "" {
		opts.Table = "otel_traces"
	}
	if !opts.NoLimit && opts.Limit <= 0 {
		opts.Limit = 20
	}

	// Extract hints from the AST and apply to options
	applyHints(root, &opts)

	t := &transpiler{
		opts:   opts,
		args:   nil,
		cteIdx: 0,
	}

	sql, err := t.transpilePipeline(&root.Pipeline)
	if err != nil {
		return nil, err
	}

	return &TranspileResult{SQL: sql, Args: t.args}, nil
}

// TranspileFilterConditions extracts WHERE-clause SQL fragments from a pipeline
// of SpansetFilter elements. Unlike Transpile(), it does not wrap the result in
// a SELECT statement — it returns only the boolean condition(s) ANDed together.
// This is used by metrics queries that need to embed span-level filter predicates
// directly in their own WHERE clause.
//
// Returns an empty string if the pipeline has no filterable elements.
func TranspileFilterConditions(pipeline *traceql.Pipeline, opts TranspileOptions) (string, error) {
	if pipeline == nil || len(pipeline.Elements) == 0 {
		return "", nil
	}

	t := &transpiler{opts: opts}

	var conditions []string
	for _, elem := range pipeline.Elements {
		sf, ok := elem.(*traceql.SpansetFilter)
		if !ok {
			// Non-filter elements (aggregates, structural operators, etc.)
			// are silently skipped — only SpansetFilter predicates can be
			// converted to WHERE conditions.
			continue
		}
		if sf.Expression == nil {
			continue
		}
		cond, err := t.transpileFieldExpr(sf.Expression)
		if err != nil {
			return "", fmt.Errorf("transpiling filter condition: %w", err)
		}
		conditions = append(conditions, cond)
	}

	if len(conditions) == 0 {
		return "", nil
	}
	return strings.Join(conditions, " AND "), nil
}

// applyHints reads query hints from the AST (e.g., with(sample=0.1))
// and applies them to the transpile options.
func applyHints(root *traceql.RootExpr, opts *TranspileOptions) {
	if root.Hints == nil {
		return
	}
	for _, h := range root.Hints.Hints {
		switch h.Name {
		case "sample":
			switch h.Value.Type {
			case traceql.TypeFloat:
				opts.SampleRate = h.Value.FloatVal
			case traceql.TypeInt:
				opts.SampleRate = float64(h.Value.IntVal) / 100.0
			case traceql.TypeBoolean:
				// sample=true from Grafana drilldown; no specific rate requested
			}
		case "prewhere":
			if h.Value.Type == traceql.TypeBoolean {
				opts.UsePrewhere = h.Value.BoolVal
			}
		}
	}
}

// transpiler holds state during a single transpilation.
type transpiler struct {
	opts   TranspileOptions
	args   []any
	cteIdx int
}

// nextCTE returns the next CTE alias name.
func (t *transpiler) nextCTE() string {
	t.cteIdx++
	return fmt.Sprintf("stage%d", t.cteIdx)
}

// timeFilter returns the time range WHERE clause fragment.
func (t *transpiler) timeFilter() string {
	if t.opts.Start.IsZero() && t.opts.End.IsZero() {
		return ""
	}
	if t.opts.Start.IsZero() {
		return fmt.Sprintf("Timestamp <= fromUnixTimestamp64Nano(%d)", t.opts.End.UnixNano())
	}
	if t.opts.End.IsZero() {
		return fmt.Sprintf("Timestamp >= fromUnixTimestamp64Nano(%d)", t.opts.Start.UnixNano())
	}
	return fmt.Sprintf("Timestamp >= fromUnixTimestamp64Nano(%d) AND Timestamp <= fromUnixTimestamp64Nano(%d)",
		t.opts.Start.UnixNano(), t.opts.End.UnixNano())
}

// needsSpanRows reports whether the given pipeline element needs span-level
// rows (not just distinct TraceIds) from its upstream CTE. Aggregates and
// metrics aggregates need to count/measure individual spans.
func needsSpanRows(elem traceql.PipelineElement) bool {
	switch elem.(type) {
	case *traceql.ScalarFilter, *traceql.Aggregate, *traceql.MetricsAggregate:
		return true
	default:
		return false
	}
}

// transpilePipeline handles the full pipeline.
func (t *transpiler) transpilePipeline(p *traceql.Pipeline) (string, error) {
	if len(p.Elements) == 0 {
		return fmt.Sprintf("SELECT DISTINCT TraceId FROM %s WHERE %s%s",
			t.opts.Table, t.mustTimeFilter(), t.limitClause()), nil
	}

	// Single element pipeline (most common case)
	if len(p.Elements) == 1 {
		return t.transpileElement(p.Elements[0], "", false)
	}

	// Multi-stage pipeline: use CTEs
	var ctes []string
	prevCTE := ""

	for i, elem := range p.Elements {
		isLast := i == len(p.Elements)-1

		// Look ahead: if the next element needs span-level rows,
		// this element must produce them (no DISTINCT).
		spanLevel := false
		if !isLast && needsSpanRows(p.Elements[i+1]) {
			spanLevel = true
		}

		sql, err := t.transpileElement(elem, prevCTE, spanLevel)
		if err != nil {
			return "", fmt.Errorf("pipeline stage %d: %w", i, err)
		}

		if isLast {
			// Last stage: just use the SQL directly
			if len(ctes) == 0 {
				return sql, nil
			}
			return fmt.Sprintf("WITH %s\n%s", strings.Join(ctes, ",\n"), sql), nil
		}

		// Wrap this stage as a CTE
		cteName := t.nextCTE()
		ctes = append(ctes, fmt.Sprintf("%s AS (\n  %s\n)", cteName, sql))
		prevCTE = cteName
	}

	// Should not reach here
	return "", fmt.Errorf("empty pipeline")
}

// transpileElement dispatches to the appropriate handler.
// spanLevel indicates whether the element should produce span-level rows
// (all matching spans) rather than distinct TraceIds. This is set when
// the next pipeline stage needs to aggregate over individual spans.
func (t *transpiler) transpileElement(elem traceql.PipelineElement, prevCTE string, spanLevel bool) (string, error) {
	switch e := elem.(type) {
	case *traceql.SpansetFilter:
		return t.transpileSpansetFilter(e, prevCTE, spanLevel)
	case *traceql.SpansetOperation:
		return t.transpileSpansetOperation(e, prevCTE)
	case *traceql.ScalarFilter:
		return t.transpileScalarFilter(e, prevCTE)
	case *traceql.Aggregate:
		return t.transpileAggregate(e, prevCTE)
	case *traceql.GroupOperation:
		return "", fmt.Errorf("GroupOperation must be part of a pipeline, not standalone")
	case *traceql.CoalesceOperation:
		// Coalesce is a no-op in SQL context (traces are already grouped by TraceId)
		if prevCTE != "" {
			if spanLevel {
				return fmt.Sprintf("SELECT * FROM %s", prevCTE), nil
			}
			return fmt.Sprintf("SELECT DISTINCT TraceId FROM %s", prevCTE), nil
		}
		return fmt.Sprintf("SELECT DISTINCT TraceId FROM %s WHERE %s", t.opts.Table, t.mustTimeFilter()), nil
	case *traceql.MetricsAggregate:
		return t.transpileMetricsAggregate(e, prevCTE)
	case *traceql.Pipeline:
		return t.transpilePipeline(e)
	default:
		return "", fmt.Errorf("unsupported pipeline element type: %T", elem)
	}
}

// sampleClause returns the SAMPLE clause if sampling is enabled.
func (t *transpiler) sampleClause() string {
	if t.opts.SampleRate > 0 && t.opts.SampleRate < 1.0 {
		return fmt.Sprintf(" SAMPLE %g", t.opts.SampleRate)
	}
	return ""
}

func (t *transpiler) mustTimeFilter() string {
	tf := t.timeFilter()
	if tf == "" {
		return "1=1"
	}
	return tf
}

// limitClause returns " LIMIT N" or "" if NoLimit is set.
func (t *transpiler) limitClause() string {
	if t.opts.NoLimit {
		return ""
	}
	return fmt.Sprintf(" LIMIT %d", t.opts.Limit)
}

// transpileSpansetFilter generates SQL for a { expression } filter.
// When spanLevel is true, the query returns all matching span rows (SELECT *)
// instead of SELECT DISTINCT TraceId. This is needed when the next pipeline
// stage aggregates over individual spans (e.g., count(), avg(duration)).
func (t *transpiler) transpileSpansetFilter(f *traceql.SpansetFilter, prevCTE string, spanLevel bool) (string, error) {
	var timeConditions []string
	var filterConditions []string

	// Separate time range conditions from filter conditions for PREWHERE support
	if tf := t.timeFilter(); tf != "" {
		timeConditions = append(timeConditions, tf)
	}

	// Add the filter expression
	if f.Expression != nil {
		exprSQL, err := t.transpileFieldExpr(f.Expression)
		if err != nil {
			return "", err
		}
		filterConditions = append(filterConditions, exprSQL)
	}

	// Build the SAMPLE clause
	sampleClause := t.sampleClause()

	// Determine SELECT and LIMIT based on whether downstream needs span rows.
	// Span-level CTEs use a large safety limit to prevent unbounded scans
	// while preserving enough rows for correct aggregation.
	selectClause := "SELECT DISTINCT TraceId"
	lc := t.limitClause()
	if spanLevel {
		selectClause = "SELECT *"
		if !t.opts.NoLimit {
			safetyLimit := t.opts.Limit * 1000
			if safetyLimit < 100000 {
				safetyLimit = 100000
			}
			lc = fmt.Sprintf(" LIMIT %d", safetyLimit)
		}
	}

	// Build the query with optional PREWHERE
	if t.opts.UsePrewhere && len(timeConditions) > 0 {
		prewhere := strings.Join(timeConditions, " AND ")
		where := "1=1"
		if len(filterConditions) > 0 {
			where = strings.Join(filterConditions, " AND ")
		}

		if prevCTE != "" {
			return fmt.Sprintf("%s FROM %s%s PREWHERE %s WHERE TraceId IN (SELECT TraceId FROM %s) AND %s%s",
				selectClause, t.opts.Table, sampleClause, prewhere, prevCTE, where, lc), nil
		}
		return fmt.Sprintf("%s FROM %s%s PREWHERE %s WHERE %s%s",
			selectClause, t.opts.Table, sampleClause, prewhere, where, lc), nil
	}

	// Standard path: combine all conditions into WHERE
	allConditions := make([]string, 0, len(timeConditions)+len(filterConditions))
	allConditions = append(allConditions, timeConditions...)
	allConditions = append(allConditions, filterConditions...)
	where := "1=1"
	if len(allConditions) > 0 {
		where = strings.Join(allConditions, " AND ")
	}

	if prevCTE != "" {
		return fmt.Sprintf("%s FROM %s%s WHERE TraceId IN (SELECT TraceId FROM %s) AND %s%s",
			selectClause, t.opts.Table, sampleClause, prevCTE, where, lc), nil
	}

	return fmt.Sprintf("%s FROM %s%s WHERE %s%s",
		selectClause, t.opts.Table, sampleClause, where, lc), nil
}

// transpileSpansetOperation generates SQL for && and || between spansets.
func (t *transpiler) transpileSpansetOperation(op *traceql.SpansetOperation, prevCTE string) (string, error) {
	switch op.Op {
	case traceql.OpSpansetAnd, traceql.OpSpansetUnion:
		return t.transpileSetOperation(op, prevCTE)

	case traceql.OpSpansetChild:
		return t.transpileStructuralChild(op, prevCTE)
	case traceql.OpSpansetParent:
		return t.transpileStructuralParent(op, prevCTE)
	case traceql.OpSpansetDescendant:
		return t.transpileStructuralDescendant(op, prevCTE)
	case traceql.OpSpansetAncestor:
		return t.transpileStructuralAncestor(op, prevCTE)
	case traceql.OpSpansetSibling:
		return t.transpileStructuralSibling(op, prevCTE)

	case traceql.OpSpansetNotChild:
		return t.transpileStructuralNot(op, traceql.OpSpansetChild, prevCTE)
	case traceql.OpSpansetNotParent:
		return t.transpileStructuralNot(op, traceql.OpSpansetParent, prevCTE)
	case traceql.OpSpansetNotDescendant:
		return t.transpileStructuralNot(op, traceql.OpSpansetDescendant, prevCTE)
	case traceql.OpSpansetNotAncestor:
		return t.transpileStructuralNot(op, traceql.OpSpansetAncestor, prevCTE)
	case traceql.OpSpansetNotSibling:
		return t.transpileStructuralNot(op, traceql.OpSpansetSibling, prevCTE)

	case traceql.OpSpansetUnionChild:
		return t.transpileStructuralUnion(op, traceql.OpSpansetChild, prevCTE)
	case traceql.OpSpansetUnionParent:
		return t.transpileStructuralUnion(op, traceql.OpSpansetParent, prevCTE)
	case traceql.OpSpansetUnionDescendant:
		return t.transpileStructuralUnion(op, traceql.OpSpansetDescendant, prevCTE)
	case traceql.OpSpansetUnionAncestor:
		return t.transpileStructuralUnion(op, traceql.OpSpansetAncestor, prevCTE)
	case traceql.OpSpansetUnionSibling:
		return t.transpileStructuralUnion(op, traceql.OpSpansetSibling, prevCTE)

	default:
		return "", fmt.Errorf("unsupported spanset operator: %s", op.Op)
	}
}

// transpileSetOperation handles && (INTERSECT) and || (UNION) between spansets.
func (t *transpiler) transpileSetOperation(op *traceql.SpansetOperation, prevCTE string) (string, error) {
	lhsSQL, err := t.transpileElement(op.LHS, prevCTE, false)
	if err != nil {
		return "", fmt.Errorf("spanset operation LHS: %w", err)
	}
	rhsSQL, err := t.transpileElement(op.RHS, prevCTE, false)
	if err != nil {
		return "", fmt.Errorf("spanset operation RHS: %w", err)
	}

	switch op.Op {
	case traceql.OpSpansetAnd:
		return fmt.Sprintf("%s\nINTERSECT\n%s", lhsSQL, rhsSQL), nil
	case traceql.OpSpansetUnion:
		return fmt.Sprintf("%s\nUNION DISTINCT\n%s", lhsSQL, rhsSQL), nil
	default:
		return "", fmt.Errorf("unsupported set operator: %s", op.Op)
	}
}

// extractFilterCondition extracts the WHERE condition SQL from a SpansetFilter element.
// It returns the condition string and an error. For non-SpansetFilter elements,
// it falls back to a subquery approach.
func (t *transpiler) extractFilterCondition(elem traceql.PipelineElement) (string, error) {
	sf, ok := elem.(*traceql.SpansetFilter)
	if !ok {
		return "", fmt.Errorf("structural operators require spanset filters, got %T", elem)
	}
	if sf.Expression == nil {
		return "1=1", nil
	}
	return t.transpileFieldExpr(sf.Expression)
}

// transpileStructuralChild generates SQL for { LHS } > { RHS } (direct child).
// Finds traces where a span matching LHS is the direct parent of a span matching RHS.
func (t *transpiler) transpileStructuralChild(op *traceql.SpansetOperation, prevCTE string) (string, error) {
	lhsCond, err := t.extractFilterCondition(op.LHS)
	if err != nil {
		return "", fmt.Errorf("structural child LHS: %w", err)
	}
	rhsCond, err := t.extractFilterCondition(op.RHS)
	if err != nil {
		return "", fmt.Errorf("structural child RHS: %w", err)
	}

	tf := t.mustTimeFilter()
	lhsTimeFilter := t.aliasedTimeFilter("p")
	rhsTimeFilter := t.aliasedTimeFilter("c")

	// Fall back to unaliased time filter if both are empty
	_ = tf

	prevFilter := ""
	if prevCTE != "" {
		prevFilter = fmt.Sprintf(" AND p.TraceId IN (SELECT TraceId FROM %s)", prevCTE)
	}

	return fmt.Sprintf(
		"SELECT DISTINCT p.TraceId FROM %s p "+
			"JOIN %s c ON p.TraceId = c.TraceId AND p.SpanId = c.ParentSpanId "+
			"WHERE %s AND %s AND %s AND %s%s%s",
		t.opts.Table, t.opts.Table,
		t.replaceColumnsWithAlias(lhsCond, "p"),
		t.replaceColumnsWithAlias(rhsCond, "c"),
		lhsTimeFilter,
		rhsTimeFilter,
		prevFilter,
		t.limitClause(),
	), nil
}

// transpileStructuralParent generates SQL for { LHS } < { RHS } (direct parent).
// Finds traces where a span matching LHS has a direct parent matching RHS.
// This is the reverse of child: LHS is the child, RHS is the parent.
func (t *transpiler) transpileStructuralParent(op *traceql.SpansetOperation, prevCTE string) (string, error) {
	lhsCond, err := t.extractFilterCondition(op.LHS)
	if err != nil {
		return "", fmt.Errorf("structural parent LHS: %w", err)
	}
	rhsCond, err := t.extractFilterCondition(op.RHS)
	if err != nil {
		return "", fmt.Errorf("structural parent RHS: %w", err)
	}

	lhsTimeFilter := t.aliasedTimeFilter("c")
	rhsTimeFilter := t.aliasedTimeFilter("p")

	prevFilter := ""
	if prevCTE != "" {
		prevFilter = fmt.Sprintf(" AND c.TraceId IN (SELECT TraceId FROM %s)", prevCTE)
	}

	return fmt.Sprintf(
		"SELECT DISTINCT c.TraceId FROM %s c "+
			"JOIN %s p ON c.TraceId = p.TraceId AND c.ParentSpanId = p.SpanId "+
			"WHERE %s AND %s AND %s AND %s%s%s",
		t.opts.Table, t.opts.Table,
		t.replaceColumnsWithAlias(lhsCond, "c"),
		t.replaceColumnsWithAlias(rhsCond, "p"),
		lhsTimeFilter,
		rhsTimeFilter,
		prevFilter,
		t.limitClause(),
	), nil
}

// transpileStructuralDescendant generates SQL for { LHS } >> { RHS } (descendant).
// Finds traces where a span matching RHS is a descendant (at any depth) of a span matching LHS.
// Uses a recursive CTE to walk the ancestor chain.
func (t *transpiler) transpileStructuralDescendant(op *traceql.SpansetOperation, prevCTE string) (string, error) {
	lhsCond, err := t.extractFilterCondition(op.LHS)
	if err != nil {
		return "", fmt.Errorf("structural descendant LHS: %w", err)
	}
	rhsCond, err := t.extractFilterCondition(op.RHS)
	if err != nil {
		return "", fmt.Errorf("structural descendant RHS: %w", err)
	}

	tf := t.mustTimeFilter()

	// Recursive CTE: start from ancestor spans (LHS), walk down to find descendants (RHS)
	prevFilter := ""
	if prevCTE != "" {
		prevFilter = fmt.Sprintf(" AND TraceId IN (SELECT TraceId FROM %s)", prevCTE)
	}
	prevFilterAlias := ""
	if prevCTE != "" {
		prevFilterAlias = fmt.Sprintf(" AND d.TraceId IN (SELECT TraceId FROM %s)", prevCTE)
	}

	return fmt.Sprintf(
		"WITH RECURSIVE ancestors AS ("+
			"SELECT TraceId, SpanId FROM %s WHERE %s AND %s%s"+
			" UNION ALL "+
			"SELECT t.TraceId, t.SpanId FROM %s t "+
			"JOIN ancestors a ON t.ParentSpanId = a.SpanId AND t.TraceId = a.TraceId"+
			") "+
			"SELECT DISTINCT d.TraceId FROM %s d "+
			"JOIN ancestors a ON d.TraceId = a.TraceId AND d.ParentSpanId = a.SpanId "+
			"WHERE %s AND %s%s%s",
		t.opts.Table, lhsCond, tf, prevFilter,
		t.opts.Table,
		t.opts.Table,
		rhsCond, tf, prevFilterAlias,
		t.limitClause(),
	), nil
}

// transpileStructuralAncestor generates SQL for { LHS } << { RHS } (ancestor).
// Finds traces where a span matching RHS is an ancestor of a span matching LHS.
// This is the reverse of descendant.
func (t *transpiler) transpileStructuralAncestor(op *traceql.SpansetOperation, prevCTE string) (string, error) {
	lhsCond, err := t.extractFilterCondition(op.LHS)
	if err != nil {
		return "", fmt.Errorf("structural ancestor LHS: %w", err)
	}
	rhsCond, err := t.extractFilterCondition(op.RHS)
	if err != nil {
		return "", fmt.Errorf("structural ancestor RHS: %w", err)
	}

	tf := t.mustTimeFilter()

	// Recursive CTE: start from descendant spans (LHS), walk up to find ancestors (RHS)
	prevFilter := ""
	if prevCTE != "" {
		prevFilter = fmt.Sprintf(" AND TraceId IN (SELECT TraceId FROM %s)", prevCTE)
	}
	prevFilterAlias := ""
	if prevCTE != "" {
		prevFilterAlias = fmt.Sprintf(" AND d.TraceId IN (SELECT TraceId FROM %s)", prevCTE)
	}

	return fmt.Sprintf(
		"WITH RECURSIVE descendants AS ("+
			"SELECT TraceId, SpanId, ParentSpanId FROM %s WHERE %s AND %s%s"+
			" UNION ALL "+
			"SELECT t.TraceId, t.SpanId, t.ParentSpanId FROM %s t "+
			"JOIN descendants d ON t.SpanId = d.ParentSpanId AND t.TraceId = d.TraceId"+
			") "+
			"SELECT DISTINCT d.TraceId FROM descendants d "+
			"JOIN %s t ON d.TraceId = t.TraceId AND d.ParentSpanId = t.SpanId "+
			"WHERE %s AND %s%s%s",
		t.opts.Table, lhsCond, tf, prevFilter,
		t.opts.Table,
		t.opts.Table,
		rhsCond, tf, prevFilterAlias,
		t.limitClause(),
	), nil
}

// transpileStructuralSibling generates SQL for { LHS } ~ { RHS } (sibling).
// Finds traces where spans matching LHS and RHS share the same parent.
func (t *transpiler) transpileStructuralSibling(op *traceql.SpansetOperation, prevCTE string) (string, error) {
	lhsCond, err := t.extractFilterCondition(op.LHS)
	if err != nil {
		return "", fmt.Errorf("structural sibling LHS: %w", err)
	}
	rhsCond, err := t.extractFilterCondition(op.RHS)
	if err != nil {
		return "", fmt.Errorf("structural sibling RHS: %w", err)
	}

	lhsTimeFilter := t.aliasedTimeFilter("s1")
	rhsTimeFilter := t.aliasedTimeFilter("s2")

	prevFilter := ""
	if prevCTE != "" {
		prevFilter = fmt.Sprintf(" AND s1.TraceId IN (SELECT TraceId FROM %s)", prevCTE)
	}

	return fmt.Sprintf(
		"SELECT DISTINCT s1.TraceId FROM %s s1 "+
			"JOIN %s s2 ON s1.TraceId = s2.TraceId "+
			"AND s1.ParentSpanId = s2.ParentSpanId "+
			"WHERE %s AND %s AND s1.ParentSpanId != '' AND s1.SpanId != s2.SpanId AND %s AND %s%s%s",
		t.opts.Table, t.opts.Table,
		t.replaceColumnsWithAlias(lhsCond, "s1"),
		t.replaceColumnsWithAlias(rhsCond, "s2"),
		lhsTimeFilter,
		rhsTimeFilter,
		prevFilter,
		t.limitClause(),
	), nil
}

// transpileStructuralNot wraps a structural operation with NOT IN to negate it.
// E.g., { LHS } !> { RHS } returns traces that do NOT satisfy { LHS } > { RHS }.
func (t *transpiler) transpileStructuralNot(op *traceql.SpansetOperation, positiveOp traceql.Operator, prevCTE string) (string, error) {
	// Build the positive structural query
	positiveQuery, err := t.transpileSpansetOperation(&traceql.SpansetOperation{
		Op:  positiveOp,
		LHS: op.LHS,
		RHS: op.RHS,
	}, prevCTE)
	if err != nil {
		return "", fmt.Errorf("negated structural: %w", err)
	}

	// Get all traces from LHS, then exclude those matching the structural condition
	lhsSQL, err := t.transpileElement(op.LHS, "", false)
	if err != nil {
		return "", fmt.Errorf("negated structural LHS: %w", err)
	}

	return fmt.Sprintf(
		"%s\nEXCEPT\n%s",
		lhsSQL, positiveQuery,
	), nil
}

// transpileStructuralUnion wraps a structural operation with UNION to combine
// the LHS trace set with the structural match set.
// E.g., { LHS } &> { RHS } returns traces from LHS UNION traces that satisfy { LHS } > { RHS }.
func (t *transpiler) transpileStructuralUnion(op *traceql.SpansetOperation, positiveOp traceql.Operator, prevCTE string) (string, error) {
	positiveQuery, err := t.transpileSpansetOperation(&traceql.SpansetOperation{
		Op:  positiveOp,
		LHS: op.LHS,
		RHS: op.RHS,
	}, prevCTE)
	if err != nil {
		return "", fmt.Errorf("union structural: %w", err)
	}

	lhsSQL, err := t.transpileElement(op.LHS, "", false)
	if err != nil {
		return "", fmt.Errorf("union structural LHS: %w", err)
	}

	return fmt.Sprintf(
		"%s\nUNION DISTINCT\n%s",
		lhsSQL, positiveQuery,
	), nil
}

// aliasedTimeFilter returns the time filter with a table alias prefix.
func (t *transpiler) aliasedTimeFilter(alias string) string {
	if t.opts.Start.IsZero() && t.opts.End.IsZero() {
		return "1=1"
	}
	if t.opts.Start.IsZero() {
		return fmt.Sprintf("%s.Timestamp <= fromUnixTimestamp64Nano(%d)", alias, t.opts.End.UnixNano())
	}
	if t.opts.End.IsZero() {
		return fmt.Sprintf("%s.Timestamp >= fromUnixTimestamp64Nano(%d)", alias, t.opts.Start.UnixNano())
	}
	return fmt.Sprintf("%s.Timestamp >= fromUnixTimestamp64Nano(%d) AND %s.Timestamp <= fromUnixTimestamp64Nano(%d)",
		alias, t.opts.Start.UnixNano(), alias, t.opts.End.UnixNano())
}

// replaceColumnsWithAlias prefixes known ClickHouse column names with a table alias.
// This is needed for JOINs where both sides reference the same table.
// It skips replacements inside single-quoted string literals to avoid corrupting
// user-provided values that happen to match column names.
func (t *transpiler) replaceColumnsWithAlias(sql, alias string) string {
	replacer := strings.NewReplacer(
		"SpanAttributes[", alias+".SpanAttributes[",
		"ResourceAttributes[", alias+".ResourceAttributes[",
		"ServiceName", alias+".ServiceName",
		"SpanName", alias+".SpanName",
		"Duration", alias+".Duration",
		"StatusCode", alias+".StatusCode",
		"StatusMessage", alias+".StatusMessage",
		"SpanKind", alias+".SpanKind",
		"TraceId", alias+".TraceId",
		"SpanId", alias+".SpanId",
		"if(ParentSpanId", "if("+alias+".ParentSpanId",
		"ParentSpanId", alias+".ParentSpanId",
		"ScopeName", alias+".ScopeName",
		"ScopeVersion", alias+".ScopeVersion",
		"Timestamp", alias+".Timestamp",
	)
	return replaceOutsideStrings(sql, replacer)
}

// replaceOutsideStrings applies a strings.Replacer only to segments of the SQL
// that are outside single-quoted string literals. This prevents column name
// replacement from corrupting user-provided string values.
func replaceOutsideStrings(sql string, replacer *strings.Replacer) string {
	var result strings.Builder
	result.Grow(len(sql))

	i := 0
	for i < len(sql) {
		quoteIdx := strings.IndexByte(sql[i:], '\'')
		if quoteIdx == -1 {
			// No more quotes — replace the rest and done.
			result.WriteString(replacer.Replace(sql[i:]))
			break
		}

		// Replace the segment before the quote.
		result.WriteString(replacer.Replace(sql[i : i+quoteIdx]))
		i += quoteIdx

		// Find the closing quote, handling escaped quotes (\').
		j := i + 1
		for j < len(sql) {
			if sql[j] == '\\' {
				j += 2 // skip escaped character
				continue
			}
			if sql[j] == '\'' {
				j++ // include closing quote
				break
			}
			j++
		}

		// Write the quoted string verbatim (no replacements).
		result.WriteString(sql[i:j])
		i = j
	}

	return result.String()
}

// transpileScalarFilter generates SQL for aggregate comparisons like count() > 5.
// When prevCTE is set, the CTE already contains span-level rows with the correct
// filters applied, so we can aggregate directly from it.
func (t *transpiler) transpileScalarFilter(f *traceql.ScalarFilter, prevCTE string) (string, error) {
	// LHS should be an Aggregate
	aggSQL, err := t.transpileScalarExpr(f.LHS)
	if err != nil {
		return "", err
	}

	rhsSQL, err := t.transpileScalarExpr(f.RHS)
	if err != nil {
		return "", err
	}

	opSQL := operatorToSQL(f.Op)

	if prevCTE != "" {
		// The upstream CTE already contains span-level rows with proper filters.
		return fmt.Sprintf("SELECT TraceId FROM %s GROUP BY TraceId HAVING %s %s %s%s",
			prevCTE, aggSQL, opSQL, rhsSQL, t.limitClause()), nil
	}

	// No previous CTE: query the raw table (single-stage pipeline)
	where := t.mustTimeFilter()
	return fmt.Sprintf("SELECT TraceId FROM %s WHERE %s GROUP BY TraceId HAVING %s %s %s%s",
		t.opts.Table, where, aggSQL, opSQL, rhsSQL, t.limitClause()), nil
}

// transpileAggregate generates SQL for a standalone aggregate in a pipeline.
// When prevCTE is set, the CTE already contains span-level rows.
func (t *transpiler) transpileAggregate(a *traceql.Aggregate, prevCTE string) (string, error) {
	aggSQL := aggregateToSQL(a)

	if prevCTE != "" {
		return fmt.Sprintf("SELECT TraceId, %s AS agg_value FROM %s GROUP BY TraceId%s",
			aggSQL, prevCTE, t.limitClause()), nil
	}

	where := t.mustTimeFilter()
	return fmt.Sprintf("SELECT TraceId, %s AS agg_value FROM %s WHERE %s GROUP BY TraceId%s",
		aggSQL, t.opts.Table, where, t.limitClause()), nil
}

// transpileScalarExpr converts a ScalarExpression to SQL.
func (t *transpiler) transpileScalarExpr(expr traceql.ScalarExpression) (string, error) {
	switch e := expr.(type) {
	case *traceql.Aggregate:
		return aggregateToSQL(e), nil
	case *traceql.Static:
		return staticToSQL(e), nil
	case *traceql.ScalarOperation:
		lhs, err := t.transpileScalarExpr(e.LHS)
		if err != nil {
			return "", err
		}
		rhs, err := t.transpileScalarExpr(e.RHS)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("(%s %s %s)", lhs, operatorToSQL(e.Op), rhs), nil
	default:
		return "", fmt.Errorf("unsupported scalar expression type: %T", expr)
	}
}

// transpileMetricsAggregate generates SQL for metrics pipeline functions.
// When prevCTE is set, the CTE already contains span-level rows.
func (t *transpiler) transpileMetricsAggregate(m *traceql.MetricsAggregate, prevCTE string) (string, error) {
	// Determine the aggregation SQL
	var aggExpr string
	switch m.Op {
	case traceql.MetricsAggregateRate:
		aggExpr = "count(*)"
	case traceql.MetricsAggregateCountOverTime:
		aggExpr = "count(*)"
	case traceql.MetricsAggregateMinOverTime:
		if m.Attr != nil {
			attrSQL, err := t.transpileFieldExpr(m.Attr)
			if err != nil {
				return "", err
			}
			aggExpr = fmt.Sprintf("min(%s)", attrSQL)
		} else {
			aggExpr = "min(Duration)"
		}
	case traceql.MetricsAggregateMaxOverTime:
		if m.Attr != nil {
			attrSQL, err := t.transpileFieldExpr(m.Attr)
			if err != nil {
				return "", err
			}
			aggExpr = fmt.Sprintf("max(%s)", attrSQL)
		} else {
			aggExpr = "max(Duration)"
		}
	case traceql.MetricsAggregateAvgOverTime:
		if m.Attr != nil {
			attrSQL, err := t.transpileFieldExpr(m.Attr)
			if err != nil {
				return "", err
			}
			aggExpr = fmt.Sprintf("avg(%s)", attrSQL)
		} else {
			aggExpr = "avg(Duration)"
		}
	case traceql.MetricsAggregateSumOverTime:
		if m.Attr != nil {
			attrSQL, err := t.transpileFieldExpr(m.Attr)
			if err != nil {
				return "", err
			}
			aggExpr = fmt.Sprintf("sum(%s)", attrSQL)
		} else {
			aggExpr = "sum(Duration)"
		}
	case traceql.MetricsAggregateQuantileOverTime:
		q := 0.5
		if len(m.Floats) > 0 {
			q = m.Floats[0]
		}
		attrCol := "Duration"
		if m.Attr != nil {
			var err error
			attrCol, err = t.transpileFieldExpr(m.Attr)
			if err != nil {
				return "", err
			}
		}
		aggExpr = fmt.Sprintf("quantile(%g)(%s)", q, attrCol)
	case traceql.MetricsAggregateHistogramOverTime:
		attrCol := "Duration"
		if m.Attr != nil {
			var err error
			attrCol, err = t.transpileFieldExpr(m.Attr)
			if err != nil {
				return "", err
			}
		}
		aggExpr = fmt.Sprintf("histogram(10)(%s)", attrCol)
	default:
		return "", fmt.Errorf("unsupported metrics aggregate: %s", m.Op)
	}

	// Build GROUP BY with by() attributes
	var groupByParts []string
	var selectLabels []string
	for _, attr := range m.By {
		col := attributeToSQL(&attr)
		groupByParts = append(groupByParts, col)
		selectLabels = append(selectLabels, fmt.Sprintf("%s AS label_%s", col, sanitizeAlias(attr.String())))
	}

	selectParts := []string{aggExpr + " AS value"}
	selectParts = append(selectParts, selectLabels...)

	groupBy := ""
	if len(groupByParts) > 0 {
		groupBy = " GROUP BY " + strings.Join(groupByParts, ", ")
	}

	if prevCTE != "" {
		// The upstream CTE already contains span-level rows with proper filters.
		return fmt.Sprintf("SELECT %s FROM %s%s%s",
			strings.Join(selectParts, ", "), prevCTE, groupBy, t.limitClause()), nil
	}

	where := t.mustTimeFilter()
	return fmt.Sprintf("SELECT %s FROM %s WHERE %s%s%s",
		strings.Join(selectParts, ", "), t.opts.Table, where, groupBy, t.limitClause()), nil
}

func sanitizeAlias(s string) string {
	r := strings.NewReplacer(".", "_", ":", "_", " ", "_")
	return r.Replace(s)
}
