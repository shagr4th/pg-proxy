package main

import (
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"
	"unicode"

	"github.com/DataDog/go-sqllexer"
)

type SqlTranslator interface {
	Polyfill() (string, string)
	Translate(query string, polyfilled bool, withPlaceHolder bool) (*SqlQuery, error)
	RenameColumn(index int, column string) string
}

type SqlEnclosure struct {
	Start *SqlToken
	End   *SqlToken
	Heads []*SqlToken
}

type SqlToken struct {
	query               *SqlQuery
	Type                sqllexer.TokenType
	Value               string
	Index               int
	Enclosure           *SqlEnclosure
	Enclosing           *SqlEnclosure
	Next                *SqlToken
	Prev                *SqlToken
	PlaceHolderPosition int
}

type SqlQuery struct {
	tokens               []*SqlToken
	separators           []*SqlToken
	Transformed          bool
	PlaceholderPositions []int
}

type isoTranslator struct {
}

func IsoTranslator() SqlTranslator {
	return &isoTranslator{}
}

func (t *isoTranslator) Polyfill() (string, string) {
	return "", ""
}

func (t *isoTranslator) RenameColumn(index int, column string) string {
	return column
}

func (t *isoTranslator) Translate(query string, polyfilled bool, withPlaceHolder bool) (*SqlQuery, error) {
	return nil, nil
}

func ParseSql(query string, dbms sqllexer.DBMSType) (*SqlQuery, error) {
	var lexer = sqllexer.New(query, sqllexer.WithDBMS(dbms))

	q := &SqlQuery{
		tokens: make([]*SqlToken, 0),
	}

	for {
		lexerToken := lexer.Scan()
		if lexerToken.Type == sqllexer.EOF {
			break
		} else if lexerToken.Type == sqllexer.INCOMPLETE_STRING {
			return nil, fmt.Errorf("{compat} Can't fully parse: %s (incomplete token: %s)", query, lexerToken.Value)
		} else if lexerToken.Type == sqllexer.ERROR {
			return nil, fmt.Errorf("{compat} Can't fully parse: %s (error token: %s)", query, lexerToken.Value)
		}
		token := &SqlToken{
			query: q,
			Type:  lexerToken.Type,
			Value: lexerToken.Value,
		}
		q.tokens = append(q.tokens, token)
	}

	if len(q.tokens) == 0 {
		return nil, errors.New("{compat} No tokens")
	}

	q.reindex()
	bindIndex := 0
	for i, token := range q.tokens {
		if token.Type == sqllexer.OPERATOR && token.EqualFold("?") &&
			(token == q.tokens[len(q.tokens)-1] || q.tokens[i+1].Type == sqllexer.SPACE ||
				q.tokens[i+1].Type == sqllexer.OPERATOR ||
				q.tokens[i+1].Type == sqllexer.PUNCTUATION) {
			bindIndex++
			token.PlaceHolderPosition = bindIndex
		}
	}
	return q, nil
}

func (q *SqlQuery) Split() []*SqlQuery {
	query := &SqlQuery{
		tokens: make([]*SqlToken, 0),
	}
	queries := []*SqlQuery{query}
	removal := 0
	for _, token := range q.tokens {
		if token.Type == sqllexer.PUNCTUATION && token.Value == ";" {
			query = &SqlQuery{
				tokens: make([]*SqlToken, 0),
			}
			queries = append(queries, query)
			removal = len(queries) - 1
		} else {
			query.tokens = append(query.tokens, &SqlToken{
				query: query,
				Type:  token.Type,
				Value: token.Value,
			})
			if token.Type != sqllexer.SPACE && token.Type != sqllexer.MULTILINE_COMMENT && token.Type != sqllexer.COMMENT {
				removal = -1
			}
		}
	}
	if removal > -1 {
		queries = queries[0:removal]
	}
	for _, line := range queries {
		line.reindex()
	}
	return queries
}

func (q *SqlQuery) reindex() {
	var enclosure *SqlEnclosure = nil
	addHead := false
	lastValid := 0
	for i, token := range q.tokens {
		if token.Type != sqllexer.SPACE {
			for j := lastValid; j <= i; j++ {
				if j < i {
					q.tokens[j].Next = token
				}
				if j > lastValid {
					q.tokens[j].Prev = q.tokens[lastValid]
				}
			}
			lastValid = i
		} else if lastValid == 0 {
			token.Prev = nil
		}
		token.Index = i
		token.Enclosure = enclosure
		if addHead && enclosure != nil && token.Type != sqllexer.SPACE {
			enclosure.Heads = append(enclosure.Heads, token)
			addHead = false
		}
		if token.EqualFold("(") {
			enclosure = &SqlEnclosure{
				Start: token,
				Heads: make([]*SqlToken, 0),
			}
			addHead = true
			token.Enclosing = enclosure
		} else if enclosure != nil && enclosure.Start != nil && token.EqualFold(")") {
			enclosure.End = token
			enclosure = enclosure.Start.Enclosure
			addHead = false
		} else if token.EqualFold(",") {
			addHead = true
		} else if token.EqualFold(";") && token.Type == sqllexer.PUNCTUATION {
			q.separators = append(q.separators, token)
		}
	}
	for i := len(q.tokens) - 1; i >= 0; i-- {
		q.tokens[i].Next = nil
		if q.tokens[i].Type != sqllexer.SPACE {
			break
		}
	}
}

/*
Retourne le premier token
*/
func (q *SqlQuery) First() *SqlToken {
	if len(q.tokens) > 0 {
		return q.tokens[0]
	}
	return nil
}

/*
Retourne une requête SQL
*/
func (q *SqlQuery) Sql() string {
	n := 0
	for i, token := range q.tokens {
		if token.Type == sqllexer.SPACE {
			token.SetValue(" ")
			// formatage plus sympa:
			if i > 0 && q.tokens[i-1].EqualFold("(") {
				token.SetValue("")
			} else if i < len(q.tokens)-1 && q.tokens[i+1].EqualFold(")") {
				token.SetValue("")
			}
		}
		n += len(token.Value)
	}

	var b strings.Builder
	b.Grow(n)
	for _, token := range q.tokens {
		b.WriteString(token.Value)
	}
	return strings.TrimSpace(b.String())
}

/*
Retourne le dernier token de la requête
*/
func (t *SqlToken) Last() *SqlToken {
	for _, qs := range t.query.separators {
		if qs.Index >= t.Index {
			return qs.Prev
		}
	}
	return t.query.tokens[len(t.query.tokens)-1]
}

/*
Retourne vrai si c'est un séparateur de requête
*/
func (t *SqlToken) IsSeparator() bool {
	return slices.Contains(t.query.separators, t)
}

/*
Change le type du token
*/
func (t *SqlToken) Set(typ sqllexer.TokenType, value string) *SqlToken {
	t.Type = typ
	t.SetValue(value)
	return t
}

/*
Change le contenu textuel du token
*/
func (t *SqlToken) SetValue(value string) *SqlToken {
	t.Value = value
	t.query.Transformed = true
	return t
}

func (t *SqlToken) Search(value string, until *SqlToken, sameEnclosure bool) *SqlToken {
	if until == nil && t.Enclosure != nil {
		until = t.Enclosure.End
	}

	lastTokenIndex := t.Last().Index + 1
	for i := t.Index + 1; i >= 0 && i < lastTokenIndex && t.query.tokens[i] != until; i = i + 1 {
		if (!sameEnclosure || t.query.tokens[i].Enclosure == t.Enclosure) && t.query.tokens[i].EqualFold(value) {
			return t.query.tokens[i]
		}
	}
	return nil
}

func (t *SqlToken) EqualFold(values ...string) bool {
	for _, value := range values {
		if len(t.Value) == len(value) && strings.EqualFold(t.Value, value) {
			return true
		}
	}
	return false
}

func (t *SqlToken) Append(tokens ...string) *SqlToken {
	array := make([]*SqlToken, len(tokens))
	for i, keyword := range tokens {
		typ := sqllexer.IDENT
		if keyword == " " || keyword == "\n" || keyword == "\t" {
			typ = sqllexer.SPACE
		} else if keyword == "(" || keyword == ")" || keyword == "," || keyword == ";" || keyword == "." || keyword == ":" || keyword == "[" || keyword == "]" || keyword == "{" || keyword == "}" {
			// cf isPunctuation in sqllexer_utils.go
			typ = sqllexer.PUNCTUATION
		} else if strings.HasPrefix(keyword, "'") && strings.HasSuffix(keyword, "'") {
			typ = sqllexer.STRING
		} else if keyword == "+" || keyword == "-" || keyword == "*" || keyword == "/" || keyword == "=" || keyword == "<" || keyword == ">" || keyword == "!" || keyword == "&" || keyword == "|" || keyword == "^" || keyword == "%" || keyword == "~" || keyword == "?" || keyword == "@" || keyword == "::" || keyword == "#" {
			// cf isOperator in sqllexer_utils.go
			typ = sqllexer.OPERATOR
		} else if keyword == "0" || keyword == "1" { // lol
			typ = sqllexer.NUMBER
		} else if keyword == "AS" {
			typ = sqllexer.ALIAS_INDICATOR
		} else if keyword == "ON" {
			typ = sqllexer.KEYWORD
		}
		array[i] = &SqlToken{
			query: t.query,
			Type:  typ,
			Value: keyword,
		}
	}
	return t.Paste(array...)
}

func (t *SqlToken) PasteSubQuery(subQuery *SqlQuery) *SqlToken {
	array := make([]*SqlToken, len(subQuery.tokens))
	for i := range array {
		array[i] = &SqlToken{
			query: t.query,
			Type:  subQuery.tokens[i].Type,
			Value: subQuery.tokens[i].Value,
		}
	}
	return t.Paste(array...)
}

func (t *SqlToken) Replace(tokens ...*SqlToken) *SqlToken {
	if len(tokens) == 0 {
		return t
	}
	t.Type = tokens[0].Type
	t.Value = tokens[0].Value
	tokens = tokens[1:]
	return t.Paste(tokens...)
}

func (t *SqlToken) Paste(tokens ...*SqlToken) *SqlToken {
	if len(tokens) == 0 {
		return t
	}
	t.query.Transformed = true
	t.query.tokens = slices.Insert(t.query.tokens, t.Index+1, tokens...)
	t.query.reindex()
	return t.query.tokens[t.Index+len(tokens)]
}

func (t *SqlToken) StartsWith(prefix string) bool {
	return (strings.HasPrefix(strings.ToLower(t.Value), prefix)) ||
		(t.Type == sqllexer.QUOTED_IDENT && len(t.Value) > 1 && strings.HasPrefix(strings.ToLower(t.Value[1:]), prefix))
}

/*
Coupe jusqu'au token "until" (si nil, jusqu'à la fin) et retourne la partie coupée
*/
func (t *SqlToken) Cut(until *SqlToken) []*SqlToken {
	to := -1
	if until != nil {
		to = slices.Index(t.query.tokens, until)
	}
	if to == -1 {
		to = t.Last().Index + 1
	}
	var cut []*SqlToken = nil
	if t.Index > -1 && to > t.Index {
		t.query.Transformed = true
		cut = make([]*SqlToken, to-t.Index)
		copy(cut, t.query.tokens[t.Index:to])
		t.query.tokens = slices.Delete(t.query.tokens, t.Index, to)
		t.query.reindex()
	}
	return cut
}

func (t *SqlToken) EnclosingFunction() (*SqlToken, int) {
	argIndex := -1
	if t == nil || t.Enclosure == nil {
		return nil, argIndex
	}
	var enclosurePrevToken = t.Enclosure.Start.Prev
	for {
		if enclosurePrevToken != nil && enclosurePrevToken.Next != nil && enclosurePrevToken.Next.Enclosing != nil {
			// le token potentialFunction est une fonction appelante
			for i, param := range enclosurePrevToken.Next.Enclosing.Heads {
				if t.Index >= param.Index {
					argIndex = i
				}
			}
		}
		if enclosurePrevToken != nil && (enclosurePrevToken.Type == sqllexer.OPERATOR || enclosurePrevToken.Type == sqllexer.PUNCTUATION) {
			// c'est possiblement une double enclosure, du genre "mafonction((xxx))", ou "mafonction(... op (xxx))", ou "mafonction(..., (xxx))"
			if enclosurePrevToken.Enclosure != nil {
				// on privilégie l'enclosure si y'en a une
				enclosurePrevToken = enclosurePrevToken.Enclosure.Start.Prev
			} else {
				// sinon on tente le token précédent
				enclosurePrevToken = enclosurePrevToken.Prev
			}
		} else {
			break
		}
	}
	return enclosurePrevToken, argIndex
}

func IsMixedCase(str string) bool {
	var hasLower bool
	var hasUpper bool
	for _, c := range str {
		if unicode.IsLetter(c) {
			if unicode.IsLower(c) {
				hasLower = true
				if hasUpper {
					return true
				}
			} else if unicode.IsUpper(c) {
				hasUpper = true
				if hasLower {
					return true
				}
			}
		}
	}
	return false
}

type withFatal interface {
	Fatal(args ...any)
}

func AssertNoError(t withFatal, err error, args ...string) {
	if err != nil {
		t.Fatal(err, args)
	}
}

func AssertEquals[K comparable](t withFatal, message string, expected K, got K) {
	if expected != got {
		t.Fatal(fmt.Errorf("expected %v, got %v", expected, got), message)
	}
}

func Exec(db *sql.DB, prepare bool, query string, args ...any) (sql.Result, error) {
	var res sql.Result
	var err error
	var stmt *sql.Stmt
	if prepare {
		stmt, err = db.Prepare(query)
		if err != nil {
			return nil, err
		}
		defer stmt.Close()
		res, err = stmt.Exec(args...)
	} else {
		res, err = db.Exec(query, args...)
	}
	return res, err
}

func Query[K comparable](db *sql.DB, query string, args ...any) ([]*K, error) {
	stmt, err := db.Prepare(query)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	rows, err := stmt.Query(args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := []*K{}
	for rows.Next() {
		var res K
		err = rows.Scan(&res)
		results = append(results, &res)
	}
	return results, err
}

func AssertSqlRowCount[K comparable](t withFatal, db *sql.DB, query string, expectedRowCount int, args ...any) []*K {
	results, err := Query[K](db, query, args...)
	AssertNoError(t, err, query)
	AssertEquals(t, query, expectedRowCount, len(results))
	return results
}

func AssertSqlQuery[K comparable](t withFatal, db *sql.DB, query string, expectedResults []K, args ...any) ([]*K, error) {
	result := AssertSqlRowCount[K](t, db, query, len(expectedResults), args...)
	for i := range result {
		AssertEquals(t, query, expectedResults[i], *result[i])
	}
	return result, nil
}

func AssertSqlExec(t withFatal, db *sql.DB, prepare bool, query string, expectedRowsAffected int64, args ...any) int64 {
	res, err := Exec(db, prepare, query, args...)
	AssertNoError(t, err, query)
	rowsAffected, err := res.RowsAffected()
	AssertNoError(t, err, query)
	AssertEquals(t, query, expectedRowsAffected, rowsAffected)
	return rowsAffected
}
