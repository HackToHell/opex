package traceql

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

// Token types
type tokenType int

const (
	tokenEOF tokenType = iota
	tokenError

	// Delimiters
	tokenOpenBrace    // {
	tokenCloseBrace   // }
	tokenOpenParen    // (
	tokenCloseParen   // )
	tokenOpenBracket  // [
	tokenCloseBracket // ]
	tokenPipe         // |
	tokenComma        // ,
	tokenDot          // .

	// Operators
	tokenEq       // =
	tokenNeq      // !=
	tokenRegex    // =~
	tokenNotRegex // !~
	tokenGt       // >
	tokenGte      // >=
	tokenLt       // <
	tokenLte      // <=
	tokenPlus     // +
	tokenMinus    // -
	tokenStar     // *
	tokenSlash    // /
	tokenPercent  // %
	tokenCaret    // ^
	tokenAnd      // &&
	tokenOr       // ||
	tokenBang     // !
	tokenGtGt     // >>
	tokenLtLt     // <<
	tokenTilde    // ~
	tokenBangGt   // !>
	tokenBangLt   // !<
	tokenBangGtGt // !>>
	tokenBangLtLt // !<<
	tokenAmpGt    // &>
	tokenAmpLt    // &<
	tokenAmpGtGt  // &>>
	tokenAmpLtLt  // &<<
	tokenAmpTilde // &~

	// Literals
	tokenInteger  // 123
	tokenFloat    // 1.23
	tokenString   // "hello"
	tokenDuration // 1s, 500ms, 1h30m

	// Identifiers and keywords
	tokenIdent // arbitrary identifier
	tokenColon // : (for scoped intrinsics like span:id)

	// Keywords
	tokenTrue
	tokenFalse
	tokenNil
	tokenStatus
	tokenKind

	// Status values
	tokenStatusError
	tokenStatusOk
	tokenStatusUnset

	// Kind values
	tokenKindUnspecified
	tokenKindInternal
	tokenKindClient
	tokenKindServer
	tokenKindProducer
	tokenKindConsumer

	// Aggregate functions
	tokenCount
	tokenMin
	tokenMax
	tokenSum
	tokenAvg

	// Metrics functions
	tokenRate
	tokenCountOverTime
	tokenMinOverTime
	tokenMaxOverTime
	tokenAvgOverTime
	tokenSumOverTime
	tokenQuantileOverTime
	tokenHistogramOverTime

	// Pipeline keywords
	tokenBy
	tokenCoalesce
	tokenSelect
	tokenWith
	tokenTopK
	tokenBottomK
)

// token is a lexer token.
type token struct {
	typ tokenType
	val string
	pos int
}

func (t token) String() string {
	if t.typ == tokenEOF {
		return "EOF"
	}
	if t.typ == tokenError {
		return fmt.Sprintf("ERROR(%s)", t.val)
	}
	return fmt.Sprintf("%q", t.val)
}

// keywords maps identifier strings to their token types.
// NOTE: status, kind, and their values (error, ok, server, client, etc.)
// are context-dependent. They are always lexed as identifiers and the parser
// resolves them based on context (intrinsic vs enum literal).
var keywords = map[string]tokenType{
	"true":                tokenTrue,
	"false":               tokenFalse,
	"nil":                 tokenNil,
	"count":               tokenCount,
	"min":                 tokenMin,
	"max":                 tokenMax,
	"sum":                 tokenSum,
	"avg":                 tokenAvg,
	"rate":                tokenRate,
	"count_over_time":     tokenCountOverTime,
	"min_over_time":       tokenMinOverTime,
	"max_over_time":       tokenMaxOverTime,
	"avg_over_time":       tokenAvgOverTime,
	"sum_over_time":       tokenSumOverTime,
	"quantile_over_time":  tokenQuantileOverTime,
	"histogram_over_time": tokenHistogramOverTime,
	"by":                  tokenBy,
	"coalesce":            tokenCoalesce,
	"select":              tokenSelect,
	"with":                tokenWith,
	"topk":                tokenTopK,
	"bottomk":             tokenBottomK,
	// Scoped names used as identifiers
	"duration":        tokenIdent,
	"name":            tokenIdent,
	"statusMessage":   tokenIdent,
	"childCount":      tokenIdent,
	"rootServiceName": tokenIdent,
	"rootName":        tokenIdent,
	"traceDuration":   tokenIdent,
	"spanStartTime":   tokenIdent,
	"nestedSetParent": tokenIdent,
	"resource":        tokenIdent,
	"span":            tokenIdent,
	"event":           tokenIdent,
	"link":            tokenIdent,
	"instrumentation": tokenIdent,
	"parent":          tokenIdent,
	"trace":           tokenIdent,
	"nestedSetLeft":   tokenIdent,
	"nestedSetRight":  tokenIdent,
}

// lexer tokenizes a TraceQL input string.
type lexer struct {
	input  string
	pos    int
	tokens []token
}

func newLexer(input string) *lexer {
	return &lexer{input: input}
}

func (l *lexer) tokenize() ([]token, error) {
	for {
		l.skipWhitespace()
		if l.pos >= len(l.input) {
			l.tokens = append(l.tokens, token{typ: tokenEOF, pos: l.pos})
			return l.tokens, nil
		}

		ch := l.peek()

		switch {
		case ch == '{':
			l.emit(tokenOpenBrace, "{")
		case ch == '}':
			l.emit(tokenCloseBrace, "}")
		case ch == '(':
			l.emit(tokenOpenParen, "(")
		case ch == ')':
			l.emit(tokenCloseParen, ")")
		case ch == '[':
			l.emit(tokenOpenBracket, "[")
		case ch == ']':
			l.emit(tokenCloseBracket, "]")
		case ch == ',':
			l.emit(tokenComma, ",")
		case ch == '+':
			l.emit(tokenPlus, "+")
		case ch == '-':
			// Could be negative number or minus operator
			if l.pos+1 < len(l.input) && (l.input[l.pos+1] >= '0' && l.input[l.pos+1] <= '9') && l.shouldBeNumber() {
				tok, err := l.readNumber()
				if err != nil {
					return nil, err
				}
				l.tokens = append(l.tokens, tok)
				continue
			}
			l.emit(tokenMinus, "-")
		case ch == '*':
			l.emit(tokenStar, "*")
		case ch == '/':
			l.emit(tokenSlash, "/")
		case ch == '%':
			l.emit(tokenPercent, "%")
		case ch == '^':
			l.emit(tokenCaret, "^")
		case ch == '~':
			l.emit(tokenTilde, "~")
		case ch == '.':
			l.emit(tokenDot, ".")
		case ch == ':':
			l.emit(tokenColon, ":")
		case ch == '|':
			l.advance()
			if l.pos < len(l.input) && l.input[l.pos] == '|' {
				l.pos++
				l.tokens = append(l.tokens, token{typ: tokenOr, val: "||", pos: l.pos - 2})
			} else {
				l.tokens = append(l.tokens, token{typ: tokenPipe, val: "|", pos: l.pos - 1})
			}
			continue
		case ch == '&':
			l.advance()
			if l.pos < len(l.input) {
				switch l.input[l.pos] {
				case '&':
					l.pos++
					l.tokens = append(l.tokens, token{typ: tokenAnd, val: "&&", pos: l.pos - 2})
				case '>':
					l.pos++
					if l.pos < len(l.input) && l.input[l.pos] == '>' {
						l.pos++
						l.tokens = append(l.tokens, token{typ: tokenAmpGtGt, val: "&>>", pos: l.pos - 3})
					} else {
						l.tokens = append(l.tokens, token{typ: tokenAmpGt, val: "&>", pos: l.pos - 2})
					}
				case '<':
					l.pos++
					if l.pos < len(l.input) && l.input[l.pos] == '<' {
						l.pos++
						l.tokens = append(l.tokens, token{typ: tokenAmpLtLt, val: "&<<", pos: l.pos - 3})
					} else {
						l.tokens = append(l.tokens, token{typ: tokenAmpLt, val: "&<", pos: l.pos - 2})
					}
				case '~':
					l.pos++
					l.tokens = append(l.tokens, token{typ: tokenAmpTilde, val: "&~", pos: l.pos - 2})
				default:
					return nil, fmt.Errorf("unexpected character after '&' at position %d", l.pos)
				}
			} else {
				return nil, fmt.Errorf("unexpected end of input after '&' at position %d", l.pos-1)
			}
			continue
		case ch == '=':
			l.advance()
			if l.pos < len(l.input) && l.input[l.pos] == '~' {
				l.pos++
				l.tokens = append(l.tokens, token{typ: tokenRegex, val: "=~", pos: l.pos - 2})
			} else {
				l.tokens = append(l.tokens, token{typ: tokenEq, val: "=", pos: l.pos - 1})
			}
			continue
		case ch == '!':
			l.advance()
			if l.pos < len(l.input) {
				switch l.input[l.pos] {
				case '=':
					l.pos++
					l.tokens = append(l.tokens, token{typ: tokenNeq, val: "!=", pos: l.pos - 2})
				case '~':
					l.pos++
					l.tokens = append(l.tokens, token{typ: tokenNotRegex, val: "!~", pos: l.pos - 2})
				case '>':
					l.pos++
					if l.pos < len(l.input) && l.input[l.pos] == '>' {
						l.pos++
						l.tokens = append(l.tokens, token{typ: tokenBangGtGt, val: "!>>", pos: l.pos - 3})
					} else {
						l.tokens = append(l.tokens, token{typ: tokenBangGt, val: "!>", pos: l.pos - 2})
					}
				case '<':
					l.pos++
					if l.pos < len(l.input) && l.input[l.pos] == '<' {
						l.pos++
						l.tokens = append(l.tokens, token{typ: tokenBangLtLt, val: "!<<", pos: l.pos - 3})
					} else {
						l.tokens = append(l.tokens, token{typ: tokenBangLt, val: "!<", pos: l.pos - 2})
					}
				default:
					l.tokens = append(l.tokens, token{typ: tokenBang, val: "!", pos: l.pos - 1})
				}
			} else {
				l.tokens = append(l.tokens, token{typ: tokenBang, val: "!", pos: l.pos - 1})
			}
			continue
		case ch == '>':
			l.advance()
			if l.pos < len(l.input) {
				switch l.input[l.pos] {
				case '=':
					l.pos++
					l.tokens = append(l.tokens, token{typ: tokenGte, val: ">=", pos: l.pos - 2})
				case '>':
					l.pos++
					l.tokens = append(l.tokens, token{typ: tokenGtGt, val: ">>", pos: l.pos - 2})
				default:
					l.tokens = append(l.tokens, token{typ: tokenGt, val: ">", pos: l.pos - 1})
				}
			} else {
				l.tokens = append(l.tokens, token{typ: tokenGt, val: ">", pos: l.pos - 1})
			}
			continue
		case ch == '<':
			l.advance()
			if l.pos < len(l.input) {
				switch l.input[l.pos] {
				case '=':
					l.pos++
					l.tokens = append(l.tokens, token{typ: tokenLte, val: "<=", pos: l.pos - 2})
				case '<':
					l.pos++
					l.tokens = append(l.tokens, token{typ: tokenLtLt, val: "<<", pos: l.pos - 2})
				default:
					l.tokens = append(l.tokens, token{typ: tokenLt, val: "<", pos: l.pos - 1})
				}
			} else {
				l.tokens = append(l.tokens, token{typ: tokenLt, val: "<", pos: l.pos - 1})
			}
			continue
		case ch == '"':
			tok, err := l.readString()
			if err != nil {
				return nil, err
			}
			l.tokens = append(l.tokens, tok)
			continue
		case ch >= '0' && ch <= '9':
			tok, err := l.readNumber()
			if err != nil {
				return nil, err
			}
			l.tokens = append(l.tokens, tok)
			continue
		case isIdentStart(ch):
			tok := l.readIdentifier()
			l.tokens = append(l.tokens, tok)
			continue
		default:
			return nil, fmt.Errorf("unexpected character %q at position %d", string(ch), l.pos)
		}
	}
}

func (l *lexer) peek() byte {
	return l.input[l.pos]
}

func (l *lexer) advance() {
	l.pos++
}

func (l *lexer) emit(typ tokenType, val string) {
	l.tokens = append(l.tokens, token{typ: typ, val: val, pos: l.pos})
	l.pos += len(val)
}

func (l *lexer) skipWhitespace() {
	for l.pos < len(l.input) {
		r, size := utf8.DecodeRuneInString(l.input[l.pos:])
		if !unicode.IsSpace(r) {
			break
		}
		l.pos += size
	}
}

// shouldBeNumber determines if a '-' should be treated as the start of a negative number.
func (l *lexer) shouldBeNumber() bool {
	if len(l.tokens) == 0 {
		return true
	}
	last := l.tokens[len(l.tokens)-1].typ
	// '-' is a negative sign after operators and opening delimiters
	switch last {
	case tokenEq, tokenNeq, tokenGt, tokenGte, tokenLt, tokenLte,
		tokenRegex, tokenNotRegex, tokenPlus, tokenMinus, tokenStar,
		tokenSlash, tokenPercent, tokenCaret, tokenAnd, tokenOr,
		tokenOpenParen, tokenOpenBrace, tokenComma, tokenPipe:
		return true
	}
	return false
}

func (l *lexer) readString() (token, error) {
	start := l.pos
	l.pos++ // skip opening "
	var sb strings.Builder
	for l.pos < len(l.input) {
		ch := l.input[l.pos]
		if ch == '\\' {
			l.pos++
			if l.pos >= len(l.input) {
				return token{}, fmt.Errorf("unterminated string escape at position %d", l.pos)
			}
			esc := l.input[l.pos]
			switch esc {
			case '"', '\\':
				sb.WriteByte(esc)
			case 'n':
				sb.WriteByte('\n')
			case 't':
				sb.WriteByte('\t')
			case 'r':
				sb.WriteByte('\r')
			default:
				sb.WriteByte('\\')
				sb.WriteByte(esc)
			}
			l.pos++
			continue
		}
		if ch == '"' {
			l.pos++ // skip closing "
			return token{typ: tokenString, val: sb.String(), pos: start}, nil
		}
		sb.WriteByte(ch)
		l.pos++
	}
	return token{}, fmt.Errorf("unterminated string starting at position %d", start)
}

func (l *lexer) readNumber() (token, error) {
	start := l.pos
	if l.pos < len(l.input) && l.input[l.pos] == '-' {
		l.pos++
	}
	for l.pos < len(l.input) && l.input[l.pos] >= '0' && l.input[l.pos] <= '9' {
		l.pos++
	}

	isFloat := false
	if l.pos < len(l.input) && l.input[l.pos] == '.' {
		// Check that next char is a digit (not a method call like 123.method)
		if l.pos+1 < len(l.input) && l.input[l.pos+1] >= '0' && l.input[l.pos+1] <= '9' {
			isFloat = true
			l.pos++ // skip '.'
			for l.pos < len(l.input) && l.input[l.pos] >= '0' && l.input[l.pos] <= '9' {
				l.pos++
			}
		}
	}

	numStr := l.input[start:l.pos]

	// Check for duration suffix
	if l.pos < len(l.input) && isDurationSuffix(l.input[l.pos]) {
		// Read the full duration (may have multiple parts like 1h30m)
		for l.pos < len(l.input) && (isDurationSuffix(l.input[l.pos]) || (l.input[l.pos] >= '0' && l.input[l.pos] <= '9') || l.input[l.pos] == '.') {
			l.pos++
		}
		return token{typ: tokenDuration, val: l.input[start:l.pos], pos: start}, nil
	}

	if isFloat {
		return token{typ: tokenFloat, val: numStr, pos: start}, nil
	}
	return token{typ: tokenInteger, val: numStr, pos: start}, nil
}

func isDurationSuffix(ch byte) bool {
	return ch == 'n' || ch == 'u' || ch == 'µ' || ch == 'm' || ch == 's' || ch == 'h' || ch == 'd'
}

func (l *lexer) readIdentifier() token {
	start := l.pos
	for l.pos < len(l.input) {
		r, size := utf8.DecodeRuneInString(l.input[l.pos:])
		if !isIdentPart(r) {
			break
		}
		l.pos += size
	}
	val := l.input[start:l.pos]

	// Check for compound keywords with underscores
	if typ, ok := keywords[val]; ok {
		if typ == tokenIdent {
			return token{typ: tokenIdent, val: val, pos: start}
		}
		return token{typ: typ, val: val, pos: start}
	}

	return token{typ: tokenIdent, val: val, pos: start}
}

func isIdentStart(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_'
}

func isIdentPart(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_'
}
