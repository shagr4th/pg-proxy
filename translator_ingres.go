package main

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/DataDog/go-sqllexer"
)

type ingresTranslator struct {
}

func IngresTranslator() SqlTranslator {
	return &ingresTranslator{}
}

func (t *ingresTranslator) IsCopyLocal() bool {
	return true
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

func (v *ingresTranslator) RenameColumn(index int, column string) string {
	if column == "?column?" || column == "substring" || column == "trunc" || column == "bpchar" || column == "date" || column == "to_char" || column == "trim" {
		column = fmt.Sprintf("col%d", index+1)
	}
	return column
}

func (v *ingresTranslator) Translate(query string, configuration TranslationConfiguration) (*SqlQuery, error) {
	query = strings.TrimSpace(query)
	if strings.Contains(query, "/*NOTRANSLATION*/") {
		return nil, nil
	}
	parsed, err := ParseSql(query, sqllexer.DBMSOracle) // Oracle car c'est uniquement utilisé dans sqllexer.go pour gérer les positional parameters ':x', supportés aussi sous Ingres
	if err != nil {
		return nil, err
	}

	token := parsed.First()
	for {
		token, err = v.singleQueryTranslate(parsed, token, configuration)
		if err != nil {
			return nil, err
		}
		if token == nil {
			break
		}
	}
	return parsed, nil
}

func (v *ingresTranslator) singleQueryTranslate(parsed *SqlQuery, token *SqlToken, configuration TranslationConfiguration) (*SqlToken, error) {
	var lastDDLToken *SqlToken = nil
	var copyToken *SqlToken = nil
	var copyIntoToken *SqlToken = nil

	if token == nil {
		return token, nil
	}

	// check des commandes
	if token.EqualFold("CREATE", "DECLARE", "ALTER") {
		lastDDLToken = token.Last()
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
			if cutted != nil && slices.ContainsFunc(cutted, func(tok *SqlToken) bool {
				return tok.EqualFold("NOJOURNALING")
			}) {
				TABLE := token.Search("TABLE", nil, true)
				UNLOGGED := token.Search("UNLOGGED", nil, true)
				if TABLE != nil && UNLOGGED == nil && TABLE.Prev != nil {
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
					token.Last().Append(" ").Paste(cutted...)
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
					newPos := token.Last()
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
				key := "dummy"
				value := "1"
				if next.Search("nolock", nil, true) != nil {
					key = "readlock"
					value = "'nolock'"
				}
				next.Cut(nil)
				token.Append(" ", "pg."+key, "=", value)
			}
		}
	} else if token.EqualFold("MODIFY") {
		tableToken := token.Next
		if tableToken != nil {
			WITH := token.Search("WITH", nil, true)
			if WITH != nil {
				_ = WITH.Cut(nil) // TODO: convertir ce qui peut l'être ?
			}
			on := tableToken.Search("ON", nil, true)
			if on != nil {
				token.SetValue("CREATE")
				unique := tableToken.Search("UNIQUE", nil, true)
				if unique != nil {
					tableToken.Cut(unique)
				} else {
					tableToken.Cut(on)
				}
				if on.Prev != nil {
					on.Prev.Append(" ", "INDEX", " ", "IF", " ", "NOT", " ", "EXISTS", " ", fmt.Sprintf("midx_%s", tableToken.Value))
				}
				on.Append(" ", tableToken.Value, "(").Last().Append(")")
			}
		}
	} else if token.EqualFold("COPY") {
		copyToken = token
		copyIntoToken = token.Search("INTO", nil, true)
		if copyIntoToken != nil {
			copyIntoToken.SetValue(copyIntoToken.Value[2:])
			if v.IsCopyLocal() && copyIntoToken.Next != nil && copyIntoToken.Next.Type == sqllexer.STRING {
				parsed.CopyTo = copyIntoToken.Next.Value[1 : len(copyIntoToken.Next.Value)-1]
				copyIntoToken.Next.Set(sqllexer.IDENT, "STDOUT")
			}
		} else {
			FROM := token.Search("FROM", nil, true)
			if v.IsCopyLocal() && FROM != nil && FROM.Next != nil && FROM.Next.Type == sqllexer.STRING {
				parsed.CopyFrom = FROM.Next.Value[1 : len(FROM.Next.Value)-1]
				FROM.Next.Set(sqllexer.IDENT, "STDIN")
			}
		}
		tableToken := token.Search("TABLE", nil, true)
		if tableToken != nil && tableToken.Next != nil {
			tableToken.Cut(tableToken.Next)
		}
	} else if token.EqualFold("EXECUTE") {
		if token.Next != nil && token.Next.EqualFold("PROCEDURE") {
			token.Next.Cut(token.Next.Next)
		}
		token.SetValue("CALL")
		if token.Next != nil && token.Next.Next != nil && token.Next.Next.Enclosing != nil && len(token.Next.Next.Enclosing.Heads) > 0 {
			equals := token.Next.Next.Enclosing.Heads[0]
			for {
				equals := equals.Search("=", token.Next.Next.Enclosing.End, true)
				if equals == nil {
					break
				}
				equals.SetValue("=>")
			}
		}
	}

	for { // après la gestion des commandes SQL, parcours de chaque token non vide
		token = token.Next
		if token == nil {
			return token, nil // fin du parcours
		} else if token.IsSeparator() { // plusieurs commandes dans la même requête
			return token.Next, nil
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
		} else if token.EqualFold("+") && !configuration.TargetPolyfilled {
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
				return t.EqualFold("rtrim", "ltrim", "char", "vchar", "varchar", "to_char", "charextract", "format",
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
		} else if token.Type == sqllexer.BIND_PARAMETER || (token.Type == sqllexer.POSITIONAL_PARAMETER && configuration.WithPlaceHolder) { // @x ou :x
			value := token.Value[1:]
			parameterIndex, err := strconv.Atoi(value)
			if err != nil {
				if configuration.WithPlaceHolder {
					token.Set(sqllexer.BIND_PARAMETER, token.Value)
				} else {
					// https://docs.actian.com/ingres/12.0/index.html#page/Upgrade/Named_Parameters_in_Parameterized_Queries_in_.NE.htm
					// named parameter non nativement supporté par postgres
					return nil, fmt.Errorf("named parameter '%s' not supported in postgres: %v", value, err)
				}
			} else {
				// un nombre, donc positional parameter
				if !configuration.WithPlaceHolder {
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
				if !configuration.WithPlaceHolder {
					token.Set(sqllexer.POSITIONAL_PARAMETER, "$"+strconv.Itoa(token.PlaceHolderPosition))
				} else {
					if parsed.PlaceholderPositions == nil {
						parsed.PlaceholderPositions = make([]int, 0)
					}
					parsed.PlaceholderPositions = append(parsed.PlaceholderPositions, token.PlaceHolderPosition)
				}
			} else if token.Next != nil {
				if token.Next.Type == sqllexer.NUMBER {
					if !configuration.WithPlaceHolder {
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
				} else if configuration.WithPlaceHolder {
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
			// utilisation de substring a la place de charextract, avec format() pour émuler le blank padded avec un fixed char
			// charextract(x, n) -> substring(format('%s', x), n, 1)
			token.SetValue("substring")
			beforeComma := enclosure.Heads[1].Prev.Prev
			if beforeComma != nil {
				enclosure.Heads[0].Prev.Append("format", "(", "'%s'", ",")
				beforeComma.Append(")")
				if enclosure.End != nil && enclosure.End.Prev != nil {
					enclosure.End.Prev.Append(",", "1")
				}
			}

		} else if token.EqualFold("char") || token.EqualFold("vchar") || token.EqualFold("varchar") ||
			token.EqualFold("smallint") || token.EqualFold("int") { // Fonctions de cast
			castToType := strings.ToLower(token.Value)
			switch castToType {
			case "char":
				castToType = "bpchar"
			case "vchar":
				castToType = "varchar"
			}
			isChar := strings.HasSuffix(castToType, "char")
			if len(enclosure.Heads) == 2 && enclosure.Heads[1].Prev != nil && isChar {
				// il y a une virgule, c'est un char(xxx, n) en mode longueur des 'n' premiers caractères
				// on fait select char(XXX, 2) -> select (XXX)::char(2)
				secondArg := enclosure.Heads[1].Prev.Cut(enclosure.End)
				if len(secondArg) > 0 {
					secondArg = secondArg[1:]
				}
				token.SetValue(" ")
				enclosure.End.Append("::", castToType, "(").Paste(secondArg...).Append(")")

			} else if len(enclosure.Heads) == 1 {
				afterEnclosure := enclosure.End.Next
				if (token.Prev != nil && token.Prev.EqualFold("::")) ||
					(lastDDLToken != nil && lastDDLToken.Index > token.Index && (afterEnclosure == nil || !afterEnclosure.EqualFold("AS"))) {
					// sauf si on est en create/declare/alter ou char(10) peut etre confondu avec un type de colonne
					continue
				}
				if token.Prev != nil && token.Prev.EqualFold("AS") {
					castFunction, argumentIndex := token.EnclosingFunction()
					if castFunction != nil && castFunction.EqualFold("CAST") && argumentIndex == 0 {
						// ou si on a fait un "CAST(xxx AS CHAR(10))"
						continue
					}
				}
				// utilisation de (1)::type à la place de type(1)
				token.Cut(token.Next) // suppression token de fonction
				enclosure.End.Append("::", castToType)
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
			// TODO ? : right(xxx, 6) => substr(xxx, octet_length(xxx) - 6 + 1)
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
			head2Prev := enclosure.Heads[2].Prev
			if head2Prev != nil {
				addition := enclosure.Heads[1].Cut(head2Prev)
				head2Prev.Cut(enclosure.Heads[2])
				if enclosure.End != nil && enclosure.End.Prev != nil {
					enclosure.End.Prev.
						Append(" ", "+", " ", "CAST", "(", "(", "format", "(", "'%s'", ",").
						Paste(addition...).
						Append(")", "|", "|", "'"+enclosure.Heads[0].Value+"'", ")", " ", "AS", " ", "INTERVAL", ")")
				}
			}
		} else if token.Prev != nil && token.Prev == copyToken {
			// https://docs.actian.com/openroad/6.2/index.html#page/LangRef/Copy_Statement.htm
			// https://www.postgresql.org/docs/current/sql-copy.html

			// on est sur un COPY TABLE (...), on convertit les définitions de colonne vers une syntaxe compatible Postgres
			// COPY TABLE (col1 type, col2 type)
			// =>
			// COPY (SELECT col1, col2 FROM TABLE)
			var delimiter string
			var globalNull string
			for i := 0; i < len(enclosure.Heads); i++ {
				endOfHead := enclosure.End
				if i < len(enclosure.Heads)-1 {
					endOfHead = enclosure.Heads[i+1].Prev
				}
				hasEquals := enclosure.Heads[i].Search("=", endOfHead, true)
				//columnName := enclosure.Heads[i].Value
				var columnWithNull string
				var columnTypeToken *SqlToken = nil
				var columnSize int = -1
				var columnDummy = false
				if hasEquals != nil {
					cutted := hasEquals.Cut(endOfHead)
					for _, c := range cutted {
						if c.EqualFold("=") || c.Type == sqllexer.SPACE {
							continue
						}
						val := strings.ToLower(c.Value)
						if delimiter == "" && strings.HasSuffix(val, "tab") {
							delimiter = "'\\t'"
						} else if delimiter == "" && strings.HasSuffix(val, "colon") {
							delimiter = "':'"
						} else if delimiter == "" && strings.HasSuffix(val, "ssv") {
							delimiter = "';'"
						} else if delimiter == "" && (strings.HasSuffix(val, "comma") || strings.HasSuffix(val, "csv")) {
							delimiter = "','"
						} else if strings.HasSuffix(val, "'") && strings.Contains(val[:len(val)-1], "'") {
							coldelimiter := c.Value[strings.LastIndex(val[:len(val)-1], "'"):]
							if strings.HasPrefix(coldelimiter, "'d") {
								columnDummy = true
								coldelimiter = "'" + coldelimiter[len(coldelimiter)-2:]
							}
							if delimiter == "" {
								delimiter = coldelimiter
							}
						}

						if c.EqualFold("with") && c.Next != nil && c.Next.EqualFold("null") && c.Next.Next != nil &&
							c.Next.Next.EqualFold("(") && c.Next.Next.Next != nil && c.Next.Next.Next.Type == sqllexer.STRING {
							columnWithNull = c.Next.Next.Next.Value
							break
						}
						if columnTypeToken == nil {
							columnTypeToken = c
							columnDummy = columnDummy || strings.HasPrefix(columnTypeToken.Value, "d")
						} else if columnSize == -1 && c.Type == sqllexer.NUMBER {
							columnSize, _ = strconv.Atoi(c.Value)
						}
					}
				}
				if globalNull == "" && columnWithNull != "" {
					globalNull = columnWithNull
				}
				if columnDummy && endOfHead != nil {
					enclosure.Heads[i].Cut(endOfHead.Next)
				}
			}
			format := "csv"
			if len(enclosure.Heads) == 0 { // Unformatted Copying sous Ingres
				format = "binary"
				if enclosure.Start != nil && enclosure.End != nil { // on enlève les ()
					enclosure.Start.Cut(enclosure.End.Next)
				}
			} else if copyIntoToken != nil {
				token.Cut(token.Next)
				enclosure.Start.Append("SELECT", " ")
				if enclosure.End != nil && enclosure.End.Prev != nil {
					enclosure.End.Prev.Append(" ", "FROM", " ", token.Value)
				}
			}
			copyWithToken := token.Search("WITH", nil, true)
			if copyWithToken != nil {
				_ = copyWithToken.Cut(nil) // TODO: convertir ce qui peut l'être ?
			}

			t := token.Last().Append(" ", "WITH", "(", "FORMAT", " ", format)
			if delimiter != "" {
				t = t.Append(",", "DELIMITER", " ", fmt.Sprintf("E%s", delimiter))
			}
			if globalNull != "" {
				t = t.Append(",", "NULL", " ", globalNull)
			}
			t = t.Append(")")

		}
	}
}
