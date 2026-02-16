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
	if opts.Limit <= 0 {
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

// transpilePipeline handles the full pipeline.
func (t *transpiler) transpilePipeline(p *traceql.Pipeline) (string, error) {
	if len(p.Elements) == 0 {
		return fmt.Sprintf("SELECT DISTINCT TraceId FROM %s WHERE %s LIMIT %d",
			t.opts.Table, t.mustTimeFilter(), t.opts.Limit), nil
	}

	// Single element pipeline (most common case)
	if len(p.Elements) == 1 {
		return t.transpileElement(p.Elements[0], "")
	}

	// Multi-stage pipeline: use CTEs
	var ctes []string
	prevCTE := ""

	for i, elem := range p.Elements {
		isLast := i == len(p.Elements)-1

		sql, err := t.transpileElement(elem, prevCTE)
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
func (t *transpiler) transpileElement(elem traceql.PipelineElement, prevCTE string) (string, error) {
	switch e := elem.(type) {
	case *traceql.SpansetFilter:
		return t.transpileSpansetFilter(e, prevCTE)
	case *traceql.SpansetOperation:
		return t.transpileSpansetOperation(e)
	case *traceql.ScalarFilter:
		return t.transpileScalarFilter(e, prevCTE)
	case *traceql.Aggregate:
		return t.transpileAggregate(e, prevCTE)
	case *traceql.GroupOperation:
		return "", fmt.Errorf("GroupOperation must be part of a pipeline, not standalone")
	case *traceql.CoalesceOperation:
		// Coalesce is a no-op in SQL context (traces are already grouped by TraceId)
		if prevCTE != "" {
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

// transpileSpansetFilter generates SQL for a { expression } filter.
func (t *transpiler) transpileSpansetFilter(f *traceql.SpansetFilter, prevCTE string) (string, error) {
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

	// Build the query with optional PREWHERE
	if t.opts.UsePrewhere && len(timeConditions) > 0 {
		prewhere := strings.Join(timeConditions, " AND ")
		where := "1=1"
		if len(filterConditions) > 0 {
			where = strings.Join(filterConditions, " AND ")
		}

		if prevCTE != "" {
			return fmt.Sprintf("SELECT DISTINCT TraceId FROM %s%s PREWHERE %s WHERE TraceId IN (SELECT TraceId FROM %s) AND %s LIMIT %d",
				t.opts.Table, sampleClause, prewhere, prevCTE, where, t.opts.Limit), nil
		}
		return fmt.Sprintf("SELECT DISTINCT TraceId FROM %s%s PREWHERE %s WHERE %s LIMIT %d",
			t.opts.Table, sampleClause, prewhere, where, t.opts.Limit), nil
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
		return fmt.Sprintf("SELECT DISTINCT TraceId FROM %s%s WHERE TraceId IN (SELECT TraceId FROM %s) AND %s LIMIT %d",
			t.opts.Table, sampleClause, prevCTE, where, t.opts.Limit), nil
	}

	return fmt.Sprintf("SELECT DISTINCT TraceId FROM %s%s WHERE %s LIMIT %d",
		t.opts.Table, sampleClause, where, t.opts.Limit), nil
}

// transpileSpansetOperation generates SQL for && and || between spansets.
func (t *transpiler) transpileSpansetOperation(op *traceql.SpansetOperation) (string, error) {
	switch op.Op {
	case traceql.OpSpansetAnd, traceql.OpSpansetUnion:
		return t.transpileSetOperation(op)

	case traceql.OpSpansetChild:
		return t.transpileStructuralChild(op)
	case traceql.OpSpansetParent:
		return t.transpileStructuralParent(op)
	case traceql.OpSpansetDescendant:
		return t.transpileStructuralDescendant(op)
	case traceql.OpSpansetAncestor:
		return t.transpileStructuralAncestor(op)
	case traceql.OpSpansetSibling:
		return t.transpileStructuralSibling(op)

	case traceql.OpSpansetNotChild:
		return t.transpileStructuralNot(op, traceql.OpSpansetChild)
	case traceql.OpSpansetNotParent:
		return t.transpileStructuralNot(op, traceql.OpSpansetParent)
	case traceql.OpSpansetNotDescendant:
		return t.transpileStructuralNot(op, traceql.OpSpansetDescendant)
	case traceql.OpSpansetNotAncestor:
		return t.transpileStructuralNot(op, traceql.OpSpansetAncestor)
	case traceql.OpSpansetNotSibling:
		return t.transpileStructuralNot(op, traceql.OpSpansetSibling)

	case traceql.OpSpansetUnionChild:
		return t.transpileStructuralUnion(op, traceql.OpSpansetChild)
	case traceql.OpSpansetUnionParent:
		return t.transpileStructuralUnion(op, traceql.OpSpansetParent)
	case traceql.OpSpansetUnionDescendant:
		return t.transpileStructuralUnion(op, traceql.OpSpansetDescendant)
	case traceql.OpSpansetUnionAncestor:
		return t.transpileStructuralUnion(op, traceql.OpSpansetAncestor)
	case traceql.OpSpansetUnionSibling:
		return t.transpileStructuralUnion(op, traceql.OpSpansetSibling)

	default:
		return "", fmt.Errorf("unsupported spanset operator: %s", op.Op)
	}
}

// transpileSetOperation handles && (INTERSECT) and || (UNION) between spansets.
func (t *transpiler) transpileSetOperation(op *traceql.SpansetOperation) (string, error) {
	lhsSQL, err := t.transpileElement(op.LHS, "")
	if err != nil {
		return "", fmt.Errorf("spanset operation LHS: %w", err)
	}
	rhsSQL, err := t.transpileElement(op.RHS, "")
	if err != nil {
		return "", fmt.Errorf("spanset operation RHS: %w", err)
	}

	switch op.Op {
	case traceql.OpSpansetAnd:
		return fmt.Sprintf("%s\nINTERSECT\n%s", lhsSQL, rhsSQL), nil
	case traceql.OpSpansetUnion:
		return fmt.Sprintf("%s\nUNION\n%s", lhsSQL, rhsSQL), nil
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
func (t *transpiler) transpileStructuralChild(op *traceql.SpansetOperation) (string, error) {
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

	return fmt.Sprintf(
		"SELECT DISTINCT p.TraceId FROM %s p "+
			"JOIN %s c ON p.TraceId = c.TraceId AND p.SpanId = c.ParentSpanId "+
			"WHERE %s AND %s AND %s AND %s LIMIT %d",
		t.opts.Table, t.opts.Table,
		t.replaceColumnsWithAlias(lhsCond, "p"),
		t.replaceColumnsWithAlias(rhsCond, "c"),
		lhsTimeFilter,
		rhsTimeFilter,
		t.opts.Limit,
	), nil
}

// transpileStructuralParent generates SQL for { LHS } < { RHS } (direct parent).
// Finds traces where a span matching LHS has a direct parent matching RHS.
// This is the reverse of child: LHS is the child, RHS is the parent.
func (t *transpiler) transpileStructuralParent(op *traceql.SpansetOperation) (string, error) {
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

	return fmt.Sprintf(
		"SELECT DISTINCT c.TraceId FROM %s c "+
			"JOIN %s p ON c.TraceId = p.TraceId AND c.ParentSpanId = p.SpanId "+
			"WHERE %s AND %s AND %s AND %s LIMIT %d",
		t.opts.Table, t.opts.Table,
		t.replaceColumnsWithAlias(lhsCond, "c"),
		t.replaceColumnsWithAlias(rhsCond, "p"),
		lhsTimeFilter,
		rhsTimeFilter,
		t.opts.Limit,
	), nil
}

// transpileStructuralDescendant generates SQL for { LHS } >> { RHS } (descendant).
// Finds traces where a span matching RHS is a descendant (at any depth) of a span matching LHS.
// Uses a recursive CTE to walk the ancestor chain.
func (t *transpiler) transpileStructuralDescendant(op *traceql.SpansetOperation) (string, error) {
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
	return fmt.Sprintf(
		"WITH RECURSIVE ancestors AS ("+
			"SELECT TraceId, SpanId FROM %s WHERE %s AND %s"+
			" UNION ALL "+
			"SELECT t.TraceId, t.SpanId FROM %s t "+
			"JOIN ancestors a ON t.ParentSpanId = a.SpanId AND t.TraceId = a.TraceId"+
			") "+
			"SELECT DISTINCT d.TraceId FROM %s d "+
			"JOIN ancestors a ON d.TraceId = a.TraceId AND d.ParentSpanId = a.SpanId "+
			"WHERE %s AND %s LIMIT %d",
		t.opts.Table, lhsCond, tf,
		t.opts.Table,
		t.opts.Table,
		rhsCond, tf,
		t.opts.Limit,
	), nil
}

// transpileStructuralAncestor generates SQL for { LHS } << { RHS } (ancestor).
// Finds traces where a span matching RHS is an ancestor of a span matching LHS.
// This is the reverse of descendant.
func (t *transpiler) transpileStructuralAncestor(op *traceql.SpansetOperation) (string, error) {
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
	return fmt.Sprintf(
		"WITH RECURSIVE descendants AS ("+
			"SELECT TraceId, SpanId, ParentSpanId FROM %s WHERE %s AND %s"+
			" UNION ALL "+
			"SELECT t.TraceId, t.SpanId, t.ParentSpanId FROM %s t "+
			"JOIN descendants d ON t.SpanId = d.ParentSpanId AND t.TraceId = d.TraceId"+
			") "+
			"SELECT DISTINCT d.TraceId FROM descendants d "+
			"JOIN %s t ON d.TraceId = t.TraceId AND d.ParentSpanId = t.SpanId "+
			"WHERE %s AND %s LIMIT %d",
		t.opts.Table, lhsCond, tf,
		t.opts.Table,
		t.opts.Table,
		rhsCond, tf,
		t.opts.Limit,
	), nil
}

// transpileStructuralSibling generates SQL for { LHS } ~ { RHS } (sibling).
// Finds traces where spans matching LHS and RHS share the same parent.
func (t *transpiler) transpileStructuralSibling(op *traceql.SpansetOperation) (string, error) {
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

	return fmt.Sprintf(
		"SELECT DISTINCT s1.TraceId FROM %s s1 "+
			"JOIN %s s2 ON s1.TraceId = s2.TraceId "+
			"AND s1.ParentSpanId = s2.ParentSpanId "+
			"AND s1.SpanId != s2.SpanId "+
			"WHERE %s AND %s AND s1.ParentSpanId != '' AND %s AND %s LIMIT %d",
		t.opts.Table, t.opts.Table,
		t.replaceColumnsWithAlias(lhsCond, "s1"),
		t.replaceColumnsWithAlias(rhsCond, "s2"),
		lhsTimeFilter,
		rhsTimeFilter,
		t.opts.Limit,
	), nil
}

// transpileStructuralNot wraps a structural operation with NOT IN to negate it.
// E.g., { LHS } !> { RHS } returns traces that do NOT satisfy { LHS } > { RHS }.
func (t *transpiler) transpileStructuralNot(op *traceql.SpansetOperation, positiveOp traceql.Operator) (string, error) {
	// Build the positive structural query
	positiveQuery, err := t.transpileSpansetOperation(&traceql.SpansetOperation{
		Op:  positiveOp,
		LHS: op.LHS,
		RHS: op.RHS,
	})
	if err != nil {
		return "", fmt.Errorf("negated structural: %w", err)
	}

	// Get all traces from LHS, then exclude those matching the structural condition
	lhsSQL, err := t.transpileElement(op.LHS, "")
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
func (t *transpiler) transpileStructuralUnion(op *traceql.SpansetOperation, positiveOp traceql.Operator) (string, error) {
	positiveQuery, err := t.transpileSpansetOperation(&traceql.SpansetOperation{
		Op:  positiveOp,
		LHS: op.LHS,
		RHS: op.RHS,
	})
	if err != nil {
		return "", fmt.Errorf("union structural: %w", err)
	}

	lhsSQL, err := t.transpileElement(op.LHS, "")
	if err != nil {
		return "", fmt.Errorf("union structural LHS: %w", err)
	}

	return fmt.Sprintf(
		"%s\nUNION\n%s",
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
func (t *transpiler) replaceColumnsWithAlias(sql, alias string) string {
	// Replace known column references with aliased versions.
	// We must be careful not to replace columns inside function calls
	// or string literals. A simple approach works for our generated SQL.
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
		"ParentSpanId", alias+".ParentSpanId",
		"ScopeName", alias+".ScopeName",
		"ScopeVersion", alias+".ScopeVersion",
		"Timestamp", alias+".Timestamp",
	)
	return replacer.Replace(sql)
}

// transpileScalarFilter generates SQL for aggregate comparisons like count() > 5.
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

	from := t.opts.Table
	where := t.mustTimeFilter()
	if prevCTE != "" {
		from = prevCTE
		where = "1=1"
	}

	return fmt.Sprintf("SELECT TraceId FROM %s WHERE %s GROUP BY TraceId HAVING %s %s %s LIMIT %d",
		from, where, aggSQL, opSQL, rhsSQL, t.opts.Limit), nil
}

// transpileAggregate generates SQL for a standalone aggregate in a pipeline.
func (t *transpiler) transpileAggregate(a *traceql.Aggregate, prevCTE string) (string, error) {
	aggSQL := aggregateToSQL(a)

	from := t.opts.Table
	where := t.mustTimeFilter()
	if prevCTE != "" {
		from = prevCTE
		where = "1=1"
	}

	return fmt.Sprintf("SELECT TraceId, %s AS agg_value FROM %s WHERE %s GROUP BY TraceId LIMIT %d",
		aggSQL, from, where, t.opts.Limit), nil
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
func (t *transpiler) transpileMetricsAggregate(m *traceql.MetricsAggregate, prevCTE string) (string, error) {
	from := t.opts.Table
	where := t.mustTimeFilter()
	if prevCTE != "" {
		from = prevCTE
		where = "1=1"
	}

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

	return fmt.Sprintf("SELECT %s FROM %s WHERE %s%s LIMIT %d",
		strings.Join(selectParts, ", "), from, where, groupBy, t.opts.Limit), nil
}

func sanitizeAlias(s string) string {
	r := strings.NewReplacer(".", "_", ":", "_", " ", "_")
	return r.Replace(s)
}
