package sqlite

import (
	"embed"
	"log"
	"schenker/pg-proxy/sqlutils"
	"strconv"
	"strings"

	"github.com/DataDog/go-sqllexer"
)

//go:embed polyfills/*
var polyFiles embed.FS

type sqliteTranslator struct {
	polyfills *sqlutils.Polyfills
}

func SqliteTranslator() *sqliteTranslator {
	compat, _ := polyFiles.ReadFile("polyfills/sqlite.sql")

	polyfills := &sqlutils.Polyfills{
		SystemCheck:   "select randomblob(8)",
		SystemCreate:  string(compat),
		SessionCreate: "",
	}
	return &sqliteTranslator{
		polyfills: polyfills,
	}
}

func (v *sqliteTranslator) Polyfills() *sqlutils.Polyfills {
	return v.polyfills
}

func (v *sqliteTranslator) RenameRowField(index int, rowField string) string {
	return rowField
}

func (t *sqliteTranslator) Translate(query string) (*sqlutils.Query, error) {
	query = strings.TrimSpace(query)
	// sqllexer sqlite's parsing should be compatible with the MySql type (see source code about backticks)
	parsed, err := sqlutils.ParseSql(query, sqllexer.DBMSMySQL)
	if err != nil {
		log.Printf("SQLLEXER FAIL: %v\n", err)
		return nil, err
	}
	first := parsed.First()
	if first.EqualFold("PRAGMA", "VACUUM") {
		parsed, err = sqlutils.ParseSql("SET my.dummy = 'dummy'", sqllexer.DBMSMySQL)
		return parsed, err
	} else if first.EqualFold("DROP") {
		objectToken := first.Next
		if objectToken.EqualFold("VIEW") || objectToken.EqualFold("TABLE") {
			objectToken.Last().Append(" ", "CASCADE")
		}
	}
	positional := 1
	token := first
	for { // après la gestion des commandes SQL, parcours de chaque token non vide
		token = token.Next
		if token == nil {
			break // fin du parcours
		}
		if token.Type == sqllexer.QUOTED_IDENT {
			token.SetValue(strings.ReplaceAll(token.Value, "`", "\""))
		}
		if token.EqualFold("sql") {
			token.Set(sqllexer.QUOTED_IDENT, "\"sql\"")
		}
		if (token.Type == sqllexer.IDENT || token.Type == sqllexer.QUOTED_IDENT) && strings.Contains(strings.ToLower(token.Value), "_rowid_") {
			token.SetValue(strings.ReplaceAll(token.Value, "_rowid_", "ctid"))
		} else if token.Value == "\"_rowid_\"" {
			token.SetValue("\"ctid\"")
		}
		if token.Type == sqllexer.IDENT && !token.EqualFold("boolean") && sqlutils.IsMixedCase(token.Value) {
			parts := strings.Split(token.Value, ".")
			for i := range parts {
				if sqlutils.IsMixedCase(parts[i]) {
					parts[i] = "\"" + parts[i] + "\""
				}
			}
			token.Set(sqllexer.QUOTED_IDENT, strings.Join(parts, "."))
		}
		if token.EqualFold("IS") {
			nextToken := token.Next
			if nextToken != nil {
				if nextToken.EqualFold("NOT") {
					nullToken := nextToken.Next
					if nullToken != nil && nullToken.Type != sqllexer.NULL {
						nextToken.Cut(nullToken)
						token.Set(sqllexer.OPERATOR, "!=")
					}
				} else if nextToken.Type != sqllexer.NULL {
					token.Set(sqllexer.OPERATOR, "!=")
				}
			}
		} else if token.EqualFold("DEFAULT") && first.EqualFold("CREATE") {
			// TODO: improve target
			strToken := token.Next
			if strToken != nil && strToken.Type == sqllexer.QUOTED_IDENT {
				strToken.Set(sqllexer.STRING, strings.ReplaceAll(strToken.Value, "\"", "'"))
			}
		} else if token.EqualFold("UNIQUE") && first.EqualFold("CREATE") {
			if token.Prev != nil && token.Prev.EqualFold("TEXT") {
				token.Prev.SetValue("VARCHAR").Append("(", "255", ")")
			}
		} else if token.EqualFold("JOIN") {
			current := token
			join := current
			for {
				nextPotentialJoin := current.Search("JOIN", join, true)
				on := current.Search("ON", nextPotentialJoin, true)
				if on == nil {
					afterOnLessJoin := join.Next
					if afterOnLessJoin != nil {
						if afterOnLessJoin.Type == sqllexer.IDENT || afterOnLessJoin.Type == sqllexer.QUOTED_IDENT {
							afterOnLessJoin.Append(" ", "ON", " ", "1", " ", "=", " ", "1")
						}
					}
				}
				if nextPotentialJoin == nil {
					break
				}
				join = nextPotentialJoin
				current = join
			}
		} else if token.EqualFold("json_each") {
			token.SetValue("sqlite_json_each")
		} else if token.EqualFold("json_object") {
			token.SetValue("json_build_object")
		} else if token.EqualFold("strftime") {
			if token.Next != nil && token.Next.Enclosing != nil && len(token.Next.Enclosing.Heads) > 0 && token.Next.Enclosing.Heads[0].Type == sqllexer.STRING {
				enclosure := token.Next.Enclosing
				value := enclosure.Heads[0].Value
				value = strings.ReplaceAll(value, "%Y", "YYYY")
				value = strings.ReplaceAll(value, "%m", "mm")
				value = strings.ReplaceAll(value, "%d", "dd")
				value = strings.ReplaceAll(value, "%H", "HH24")
				value = strings.ReplaceAll(value, "%M", "MM")
				value = strings.ReplaceAll(value, "%f", "ss")

				enclosure.Heads[0].SetValue(value)
				if len(enclosure.Heads) == 1 {
					token.SetValue("to_char")
					token.Next.Append("NOW", "(", ")", ",", " ")

				} else if len(enclosure.Heads) == 2 {
					token.Cut(enclosure.Heads[0])
					enclosure.Heads[0].Cut(enclosure.Heads[1])
					enclosure.End.Cut(enclosure.End.Next)
					token = token.Next
				}
			}
		}
		if token.Value == "?" {
			token.Set(sqllexer.POSITIONAL_PARAMETER, strings.ReplaceAll(token.Value, "?", "$"+strconv.Itoa(positional)))
			positional++
		}
		if token.Type == sqllexer.OPERATOR && len(token.Value) > 1 && strings.HasSuffix(token.Value, "?") {
			token.SetValue(token.Value[0 : len(token.Value)-1]).Append("?")
		}
		if token.EqualFold("==") {
			token.SetValue("=")
		} else if token.EqualFold("<>") {
			token.SetValue("!=")
		}
	}
	return parsed, nil
}
