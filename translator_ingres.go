package main

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/DataDog/go-sqllexer"
)

const ingresStaticFixedCharFeature = false // on active ?

type ingresTranslator struct {
}

func IngresTranslator() SqlTranslator {
	return &ingresTranslator{}
}

func (v *ingresTranslator) Polyfill() (string, string) {
	return `SELECT 'a'+'b' /*NOTRANSLATION*/`, `/*NOTRANSLATION*/
-- OPERATEURS GERANT LE SYMBOLE '+' AVEC DES CHAINES DE CARACTERES

-- avec un TEXT
CREATE OPERATOR public.+ (LEFTARG = text, RIGHTARG = text, FUNCTION = textcat);

-- avec un INTEGER
CREATE OR REPLACE FUNCTION public.add_integer_text (integer, text) RETURNS numeric LANGUAGE sql IMMUTABLE STRICT RETURN $1 + $2::numeric;
CREATE OPERATOR public.+ (LEFTARG = integer, RIGHTARG = text, FUNCTION = public.add_integer_text);
CREATE OR REPLACE FUNCTION public.add_text_integer (text, integer) RETURNS numeric LANGUAGE sql IMMUTABLE STRICT RETURN $1::numeric + $2;
CREATE OPERATOR public.+ (LEFTARG = text, RIGHTARG = integer, FUNCTION = public.add_text_integer);

-- avec un SMALLINT
CREATE OR REPLACE FUNCTION public.add_smallint_text (smallint, text) RETURNS numeric LANGUAGE sql IMMUTABLE STRICT RETURN $1 + $2::numeric;
CREATE OPERATOR public.+ (LEFTARG = smallint, RIGHTARG = text, FUNCTION = public.add_smallint_text);
CREATE OR REPLACE FUNCTION public.add_text_smallint (text, smallint) RETURNS numeric LANGUAGE sql IMMUTABLE STRICT RETURN $1::numeric + $2;
CREATE OPERATOR public.+ (LEFTARG = text, RIGHTARG = smallint, FUNCTION = public.add_text_smallint);

-- avec un BIGINT
CREATE OR REPLACE FUNCTION public.add_bigint_text (bigint, text) RETURNS numeric LANGUAGE sql IMMUTABLE STRICT RETURN $1 + $2::numeric;
CREATE OPERATOR public.+ (LEFTARG = bigint, RIGHTARG = text, FUNCTION = public.add_bigint_text);
CREATE OR REPLACE FUNCTION public.add_text_bigint (text, bigint) RETURNS numeric LANGUAGE sql IMMUTABLE STRICT RETURN $1::numeric + $2;
CREATE OPERATOR public.+ (LEFTARG = text, RIGHTARG = bigint, FUNCTION = public.add_text_bigint);

-- avec un REAL
CREATE OR REPLACE FUNCTION public.add_real_text (real, text) RETURNS numeric LANGUAGE sql IMMUTABLE STRICT RETURN $1 + $2::numeric;
CREATE OPERATOR public.+ (LEFTARG = real, RIGHTARG = text, FUNCTION = public.add_real_text);
CREATE OR REPLACE FUNCTION public.add_text_real (text, real) RETURNS numeric LANGUAGE sql IMMUTABLE STRICT RETURN $1::numeric + $2;
CREATE OPERATOR public.+ (LEFTARG = text, RIGHTARG = real, FUNCTION = public.add_text_real);

-- avec un NUMERIC
CREATE OR REPLACE FUNCTION public.add_numeric_text (numeric, text) RETURNS numeric LANGUAGE sql IMMUTABLE STRICT RETURN $1 + $2::numeric;
CREATE OPERATOR public.+ (LEFTARG = numeric, RIGHTARG = text, FUNCTION = public.add_numeric_text);
CREATE OR REPLACE FUNCTION public.add_text_numeric (text, numeric) RETURNS numeric LANGUAGE sql IMMUTABLE STRICT RETURN $1::numeric + $2;
CREATE OPERATOR public.+ (LEFTARG = text, RIGHTARG = numeric, FUNCTION = public.add_text_numeric);
`
}

func (v *ingresTranslator) RenameColumn(index int, column string) (string, error) {
	if column == "?column?" || column == "substring" || column == "trunc" || column == "date" || column == "to_char" || column == "trim" {
		column = fmt.Sprintf("col%d", index+1)
	}
	return column, nil
}

func (v *ingresTranslator) Translate(query string, polyfilled bool, withPlaceHolder bool) (*SqlQuery, error) {
	query = strings.TrimSpace(query)
	if strings.Contains(query, "/*NOTRANSLATION*/") {
		return nil, nil
	}
	parsed, err := ParseSql(query, sqllexer.DBMSOracle) // Oracle car c'est uniquement utilisé dans sqllexer.go pour gérer les positional parameters ':x', supportés aussi sous Ingres
	if err != nil {
		return nil, err
	}

	token := parsed.First()
	var lastDDLToken *SqlToken = nil

	// check des commandes
	if token.EqualFold("CREATE", "DECLARE", "ALTER") {
		lastDDLToken = parsed.Last()
		if token.EqualFold("DECLARE") {
			token.SetValue("CREATE")
		}
		afterCREATE := token.Next
		if afterCREATE != nil && afterCREATE.EqualFold("GLOBAL") {
			afterCREATE.Cut(afterCREATE.Next)
		}
		WITH := token.Search("WITH", nil, true)
		if WITH != nil {
			cutted := WITH.Cut(nil) // TODO: convertir ce qui peut l'être ?
			if slices.ContainsFunc(cutted, func(tok *SqlToken) bool {
				return tok.EqualFold("NOJOURNALING")
			}) {
				TABLE := token.Search("TABLE", nil, true)
				UNLOGGED := token.Search("UNLOGGED", nil, true)
				if UNLOGGED == nil {
					TABLE.Prev.Append(" ", "UNLOGGED")
				}
			}
		}
		AS := token.Search("AS", nil, true)
		if AS != nil {
			ON := AS.Search("ON", nil, true)
			for ON != nil {
				Next := ON.Next
				if Next != nil && Next.EqualFold("COMMIT") {
					break
				}
				// on va chercher le token suivant "ON" (pour ne pas confondre avec un ON de jointure)
				ON = ON.Search("ON", nil, true)
			}
			if ON != nil {
				// inversion du create table ... as ... on commit ...
				cutted := AS.Cut(ON)
				if cutted != nil {
					parsed.Last().Append(" ").Paste(cutted...)
				}
				if len(cutted) > 0 {
					lastDDLToken = cutted[0]
				}
			}
		}
	} else if token.EqualFold("UPDATE") {
		FROM := token.Search("FROM", nil, true)
		SET := token.Search("SET", nil, true)
		if FROM != nil && SET != nil {

			WHERE := SET.Search("WHERE", nil, true)
			SETafterFROM := FROM.Search("SET", WHERE, true) // recherche en partant du FROM

			// on élimine d'abord les aliases des clauses SET (non supportés par PG)
			var tableName *SqlToken
			if SETafterFROM != nil {
				tableName = FROM.Prev
			} else {
				tableName = SET.Prev
			}
			if tableName != nil {
				alias := strings.ToLower(tableName.Value) + "."
				current := SET.Next
				for {
					if current == nil || current == WHERE || current == FROM {
						break
					}
					if current.StartsWith(alias) {
						current.SetValue(current.Value[len(alias):])
					}
					current = current.Next
				}
			}

			// cas du update ... from ... set
			if SETafterFROM != nil {
				fromTokens := FROM.Cut(SETafterFROM)
				if len(fromTokens) > 0 {
					newPos := parsed.Last()
					if WHERE != nil {
						newPos = WHERE.Prev
					}
					newPos.Append(" ").Paste(fromTokens...)
				}
			}
		}
	} else if token.EqualFold("SET") {
		LOCKMODE := token.Search("LOCKMODE", nil, true)
		if LOCKMODE != nil {
			next := token.Next
			if next != nil {
				next.Cut(nil)
				token.Append(" ", "pg.dummy", "=", "1")
			}
		}
	} else if token.EqualFold("MODIFY") {
		token.SetValue("SET")
		next := token.Next
		if next != nil {
			next.Cut(nil)
			token.Append(" ", "pg.dummy", "=", "1")
		}
	} else if token.EqualFold("COPY") {
		INTO := token.Search("INTO", nil, true)
		if INTO != nil {
			INTO.SetValue(INTO.Value[2:])
		}
	}

	for { // après la gestion des commandes SQL, parcours de chaque token non vide
		token = token.Next
		if token == nil {
			break // fin du parcours
		} else if token.Type == sqllexer.IDENT && token.StartsWith("session.") {
			token.SetValue(token.Value[8:])
			continue
		} else if token.Type == sqllexer.IDENT && token.EqualFold("iitables") {
			token.SetValue(`(select t.table_name, t.table_schema as table_owner, null as create_date, null as alter_date,
case t.table_type when 'VIEW' then 'V' else 'T' end as table_type,
-1 as num_rows, -1 as number_pages, '' as location_name from information_schema.tables t
union
select i.tablename as table_name, i.schemaname as table_owner, null as create_date, null as alter_date,
'I' as table_type, -1 as num_rows, -1 as number_pages, '' as location_name from pg_indexes i) as iitables`)
			continue
		} else if token.Type == sqllexer.IDENT && token.EqualFold("iicolumns") {
			token.SetValue(`(select c.table_name, c.table_schema as table_owner, c.column_name, case c.is_nullable when 'YES' then 'Y' else 'N' end as column_nulls,
c.ordinal_position as column_sequence, c.data_type as column_datatype, c.character_maximum_length as column_length, c.numeric_scale as column_scale, 0 as key_sequence
from information_schema.columns c) as iicolumns`)
			continue
		} else if token.Type == sqllexer.IDENT && strings.HasSuffix(strings.ToLower(token.Value), ".nextval") {
			sequence := token.Value[0:(len(token.Value) - 8)]
			token.Set(sqllexer.FUNCTION, "nextval").Append("(", "'"+sequence+"'", ")")
			continue
		} else if token.EqualFold("ingresdate") {
			token.SetValue(token.Value[6:]) // alias de ingresdate
			continue
		} else if token.EqualFold("sysdate") {
			token.SetValue("current_timestamp")
			continue
		} else if token.EqualFold("+") && !polyfilled {
			// Sans opérateur custom (dispo dans le polyfill), PG ne supporte pas la concaténation avec des +, on tente de transformer en || quand on est sûr de maniper des string
			leftToken := token.Prev
			rightToken := token.Next
			if rightToken == nil || leftToken == nil {
				continue
			}

			ingresFunctionCharAsParam := func(t *SqlToken, argIndex int) bool {
				return argIndex == 0 && t.EqualFold("rtrim", "ltrim",
					"substring", "substr", "trim", "pad", "lpad", "rpad", "lshift", "rshift", "left",
					"right", "date_format", "lowercase", "uppercase", "lower", "upper", "squeeze")
			}

			ingresFunctionReturnsChar := func(t *SqlToken) bool {
				nextToken := t.Next
				if nextToken == nil || nextToken.Enclosing == nil {
					return false
				}
				return t.EqualFold("rtrim", "ltrim", "char", "to_char", "charextract", "format",
					"substring", "substr", "trim", "pad", "lpad", "rpad", "lshift", "rshift", "left",
					"right", "date_format", "lowercase", "uppercase", "lower", "upper", "squeeze") ||
					// ifnull(YYY, 'toto') est une string function à cause de 'toto' :
					(t.EqualFold("ifnull", "coalesce") && len(nextToken.Enclosing.Heads) == 2 && nextToken.Enclosing.Heads[1].Type == sqllexer.STRING)
			}

			hasStrings := false
			if leftToken.Type == sqllexer.STRING || // le membre de gauche est un char
				rightToken.Type == sqllexer.STRING || // le membre de droite est un char
				ingresFunctionReturnsChar(rightToken) { // le membre de droite est un appel a une fonction qui retourne un type char
				hasStrings = true
			}
			if !hasStrings {
				topFunction, argumentIndex := token.EnclosingFunction()
				// l'appel qui englobe l'argument contenant le "+" est une fonction string qui prend une string en paramètre, ca a du sens de remplacer par un || aussi
				hasStrings = topFunction != nil && ingresFunctionCharAsParam(topFunction, argumentIndex)
			}
			if !hasStrings && leftToken.EqualFold(")") {
				// le membre de gauche est un appel a une fonction qui retourne un type char
				leftFunction, _ := leftToken.EnclosingFunction()
				hasStrings = leftFunction != nil && ingresFunctionReturnsChar(leftFunction)
			}
			if !hasStrings && leftToken.EqualFold("text") {
				// le membre de gauche est qqchose qui est suffixé par ::text
				leftLeftToken := leftToken.Prev
				if leftLeftToken != nil {
					hasStrings = leftLeftToken.EqualFold("::")
				}

			}
			if hasStrings {
				token = token.SetValue("|").Append("|")
			}
			continue
		} else if token.Type == sqllexer.BIND_PARAMETER || (token.Type == sqllexer.POSITIONAL_PARAMETER && withPlaceHolder) { // @x ou :x
			value := token.Value[1:]
			parameterIndex, err := strconv.Atoi(value)
			if err != nil {
				if withPlaceHolder {
					token.Set(sqllexer.BIND_PARAMETER, token.Value)
				} else {
					// https://docs.actian.com/ingres/12.0/index.html#page/Upgrade/Named_Parameters_in_Parameterized_Queries_in_.NE.htm
					// named parameter non nativement supporté par postgres
					return nil, fmt.Errorf("named parameter '%s' not supported in postgres: %v", value, err)
				}
			} else {
				// un nombre, donc positional parameter
				if !withPlaceHolder {
					token.Set(sqllexer.POSITIONAL_PARAMETER, "$"+strconv.Itoa(parameterIndex))
				} else {
					token.Set(sqllexer.OPERATOR, "?")
					if parsed.PlaceholderPositions == nil {
						parsed.PlaceholderPositions = make([]int, 0)
					}
					parsed.PlaceholderPositions = append(parsed.PlaceholderPositions, parameterIndex)
				}
			}
			continue
		} else if token.Type == sqllexer.OPERATOR && token.EqualFold("?") {
			if token.PlaceHolderPosition > 0 {
				if !withPlaceHolder {
					token.Set(sqllexer.POSITIONAL_PARAMETER, "$"+strconv.Itoa(token.PlaceHolderPosition))
				} else {
					if parsed.PlaceholderPositions == nil {
						parsed.PlaceholderPositions = make([]int, 0)
					}
					parsed.PlaceholderPositions = append(parsed.PlaceholderPositions, token.PlaceHolderPosition)
				}
			} else if token.Next != nil {
				if token.Next.Type == sqllexer.NUMBER {
					if !withPlaceHolder {
						token.Set(sqllexer.POSITIONAL_PARAMETER, "$"+token.Next.Value)
					} else {
						parameterIndex, err := strconv.Atoi(token.Next.Value)
						if err != nil {
							return nil, fmt.Errorf("named parameter '%s' not supported in postgres: %v", token.Next.Value, err)
						}
						token.Set(sqllexer.OPERATOR, "?")
						if parsed.PlaceholderPositions == nil {
							parsed.PlaceholderPositions = make([]int, 0)
						}
						parsed.PlaceholderPositions = append(parsed.PlaceholderPositions, parameterIndex)
					}
					token.Next.Cut(token.Next.Next)
					token.Append(" ")
				} else if withPlaceHolder {
					token.Set(sqllexer.BIND_PARAMETER, token.Value)
				} else {
					return nil, fmt.Errorf("named parameter '%s' not supported in postgres", token.Next.Value)
				}
			}
			continue
		} else if token.Next == nil || token.Next.Enclosing == nil {
			// ca n'est pas une fonction potentielle, or tout les checks suivants ne sont que des fonctions
			continue
		}
		enclosure := token.Next.Enclosing

		if token.EqualFold("charextract") && len(enclosure.Heads) == 2 && enclosure.Heads[1].Prev != nil {
			// utilisation de substring a la place de charextract
			// charextract(x, n) -> substring((x)::text, n, 1)
			token.SetValue("substring").Append("(")
			beforeComma := enclosure.Heads[1].Prev.Prev
			if beforeComma != nil {
				beforeComma.Append(")", "::", "text")
				enclosure.End.Prev.Append(",", "1")
			}

		} else if token.EqualFold("char") || token.EqualFold("vchar") {
			if len(enclosure.Heads) == 2 && enclosure.Heads[1].Prev != nil {
				// il y a une virgule, c'est un char(xxx, n) en mode extraction des 'n' premiers caractères
				// on fait select char(XXX, 2) -> select substring ((XXX)::text, 1, 2)
				token.SetValue("substring").Append("(")
				beforeComma := enclosure.Heads[1].Prev.Prev
				if beforeComma != nil {
					beforeComma.Append(")", "::", "text", ",", " ", "1")
				}

			} else if len(enclosure.Heads) == 1 {
				afterEnclosure := enclosure.End.Next
				if (token.Prev != nil && token.Prev.EqualFold("::")) ||
					(lastDDLToken != nil && lastDDLToken.Index > token.Index && (afterEnclosure == nil || !afterEnclosure.EqualFold("AS"))) {
					// sauf si on est en create/declare/alter ou char(10) peut etre confondu avec un type de colonne
					continue
				}
				if token.Prev != nil && token.Prev.EqualFold("AS") {
					castFunction, argumentIndex := token.EnclosingFunction()
					if castFunction.EqualFold("CAST") && argumentIndex == 0 {
						// ou si on a fait un "CAST(xxx AS CHAR(10))"
						continue
					}
				}
				// utilisation de (1)::text à la place de char(1)
				token.Cut(token.Next) // suppression token de fonction
				textType := "text"
				if ingresStaticFixedCharFeature {
					currentToken := enclosure.Heads[0]
					for {
						if currentToken == enclosure.End {
							break
						}
						if textType != "text" || currentToken.Type != sqllexer.STRING {
							// n'importe quel autre token qu'un STRING dans l'argument passé à char() annule cette feature
							textType = "text"
							break
						} else {
							textType = fmt.Sprintf("char(%d)", len(currentToken.Value)-2) // on enlève les quotes pour calculer la taille fixe
						}
						currentToken = currentToken.Next
					}
				}
				enclosure.End.Append("::", textType)
			}

		} else if token.EqualFold("date_part") && len(enclosure.Heads) == 2 {
			// gestion des unités non supportées dans Postgres
			if enclosure.Heads[0].EqualFold("'mo'") {
				enclosure.Heads[0].SetValue("'month'")
			}
			// en remplace le deuxième argument si besoin
			if enclosure.Heads[1].EqualFold("'now'") {
				enclosure.Heads[1].Set(sqllexer.IDENT, "current_timestamp")

			} else if enclosure.Heads[1].EqualFold("'today'") {
				enclosure.Heads[1].Set(sqllexer.IDENT, "current_date")
			}
		} else if token.EqualFold("squeeze") {
			token.SetValue("trim")
		} else if token.EqualFold("right", "left") && len(enclosure.Heads) == 2 && enclosure.Heads[1].Prev != nil {
			// right(x, 2) => right(format('%s', x), 2)
			enclosure.Start.Append("format", "(", "'%s'", ",")
			beforeComma := enclosure.Heads[1].Prev.Prev
			if beforeComma != nil {
				beforeComma.Append(")")
			}
		} else if token.EqualFold("date_format") && len(enclosure.Heads) == 2 && enclosure.Heads[1].Type == sqllexer.STRING {
			token.SetValue("to_char")
			// https://docs.actian.com/ingres/10s/index.html#page/SQLRef/Date_and_Time_Functions.htm
			// https://www.postgresql.org/docs/current/functions-formatting.html#FUNCTIONS-FORMATTING-TABLE
			newValue := strings.ReplaceAll(enclosure.Heads[1].Value, "%Y", "YYYY")
			newValue = strings.ReplaceAll(newValue, "%M", "Month")
			newValue = strings.ReplaceAll(newValue, "%m", "MM")
			newValue = strings.ReplaceAll(newValue, "%d", "DD")
			newValue = strings.ReplaceAll(newValue, "%j", "DDD")
			newValue = strings.ReplaceAll(newValue, "%b", "Mon")
			newValue = strings.ReplaceAll(newValue, "%s", "US")
			newValue = strings.ReplaceAll(newValue, "%H", "HH24")
			newValue = strings.ReplaceAll(newValue, "%h", "HH12")
			newValue = strings.ReplaceAll(newValue, "%i", "MI")
			newValue = strings.ReplaceAll(newValue, "%S", "SS")
			newValue = strings.ReplaceAll(newValue, "%s", "SS")
			newValue = strings.ReplaceAll(newValue, "%w", "ID")
			enclosure.Heads[1].SetValue(newValue)
		} else if token.EqualFold("date") && len(enclosure.Heads) == 1 {
			/* PG ne garde pas les H:M:S avec date(""), mais dans ingres, date("2012-12-01 16:55:15") retourne un timestamp avec les H:M:S */
			token.Cut(token.Next) // suppression token de fonction
			if strings.EqualFold(enclosure.Heads[0].Value, "'today'") || strings.EqualFold(enclosure.Heads[0].Value, "sysdate") {
				enclosure.End.Append("::", "date") // cas particulier ou c'est interprété comme une date et non un timestamp
			} else if strings.EqualFold(enclosure.Heads[0].Value, "'now'") {
				enclosure.End.Append("::", "timestamptz") // cas particulier ou c'est interprété comme un timestamp avec timezone
			} else {
				enclosure.End.Append("::", "timestamp")
			}
		} else if token.EqualFold("ifnull") {
			token.SetValue("COALESCE")
		} else if token.EqualFold("LOWERCASE", "UPPERCASE") {
			token.SetValue(token.Value[0:5])
		} else if token.EqualFold("dbmsinfo") && len(enclosure.Heads) == 1 {
			// Au cas ou : https://docs.actian.com/ingres/11.0/index.html#page/SQLRef/DBMSINFO_Function--Return_Information_About_the.htm
			if enclosure.Heads[0].EqualFold("'session_id'") {
				token.SetValue("pg_backend_pid")
			} else if enclosure.Heads[0].EqualFold("'_version'") || true {
				token.SetValue("version")
			}
			enclosure.Heads[0].Cut(enclosure.End)
		} else if token.EqualFold("decimal") && len(enclosure.Heads) == 3 && enclosure.Heads[1].Prev != nil {
			token.SetValue("trunc")
			enclosure.Start.Append("(")
			if enclosure.Heads[1].Prev.Prev != nil {
				enclosure.Heads[1].Prev.Prev.Append(")", "::", "numeric")
			}
			enclosure.Heads[1].Cut(enclosure.Heads[2])
		} else if token.EqualFold("TIMESTAMPADD") && len(enclosure.Heads) == 3 {
			token.Cut(token.Next) // suppression token de fonction
			// TIMESTAMPADD(HOUR, 1, SYSDATE) => (current_timestamp + CAST(((1)::text||'HOUR') AS INTERVAL))
			enclosure.Heads[0].Cut(enclosure.Heads[1])
			addition := enclosure.Heads[1].Cut(enclosure.Heads[2].Prev)
			enclosure.Heads[2].Prev.Cut(enclosure.Heads[2])
			enclosure.End.Prev.
				Append(" ", "+", " ", "CAST", "(", "(", "(").
				Paste(addition...).
				Append(")", "::", "text", "|", "|", "'"+enclosure.Heads[0].Value+"'", ")", " ", "AS", " ", "INTERVAL", ")")
		}
	}
	return parsed, nil
}
