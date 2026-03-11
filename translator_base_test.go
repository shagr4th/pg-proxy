package main

import (
	"testing"
)

func TestEnclosingFunction(t *testing.T) {
	for _, query := range []string{"SELECT TRUNC(1+(112.1))", "SELECT TRUNC(112.1)", "SELECT TRUNC((112.1))", "SELECT TRUNC(((112.1)))", "SELECT (TRUNC(1+(112.1)))"} {
		parsed, err := ParseSql(query, "none")
		AssertNoError(t, err, query)
		NUM := parsed.First().Search("112.1", nil, false)
		AssertEquals(t, true, NUM != nil, "IS NUM")

		TRUNC, pos := NUM.EnclosingFunction()
		AssertEquals(t, true, TRUNC != nil, "IS TRUNC")
		AssertEquals(t, "TRUNC", TRUNC.Value, "IS TRUNC")
		AssertEquals(t, 0, pos, "IS TRUNC")
	}

	for _, query := range []string{"SELECT TRUNC(112.123, (2))", "SELECT TRUNC(112.123, 1+(2))"} {
		parsed, err := ParseSql(query, "none")
		AssertNoError(t, err, query)
		NUM := parsed.First().Search("2", nil, false)
		AssertEquals(t, true, NUM != nil, "IS NUM")

		TRUNC, pos := NUM.EnclosingFunction()
		AssertEquals(t, true, TRUNC != nil, "IS TRUNC")
		AssertEquals(t, "TRUNC", TRUNC.Value, "IS TRUNC")
		AssertEquals(t, 1, pos, "IS TRUNC")
	}

	query := "SELECT CASE TOTO = 1 WHEN 'A' THEN 'B' END, 'A'"
	parsed, err := ParseSql(query, "none")
	AssertNoError(t, err, query)
	TOTO := parsed.First().Search("TOTO", nil, false)
	AssertEquals(t, "CASE", TOTO.Enclosure.Start.Value, query)
	AssertEquals(t, 3, len(TOTO.Enclosure.Heads))
	A := parsed.First().Search("'A'", nil, false)
	AssertEquals(t, "CASE", A.Enclosure.Start.Value, query)
	A = A.Search("'A'", nil, false)
	AssertEquals(t, nil, A.Enclosure, query)

	query = "SELECT CASE TOTO = 1 WHEN 'A' THEN 'B' ELSE 'C' END, 'A'"
	parsed, err = ParseSql(query, "none")
	AssertNoError(t, err, query)
	A = parsed.First().Search("'A'", nil, false)
	AssertEquals(t, "CASE", A.Enclosure.Start.Value, query)
	AssertEquals(t, 4, len(A.Enclosure.Heads), query)
}

func TestSeparators(t *testing.T) {
	parsed, err := ParseSql("SELECT 1; SELECT 1 + 2;;", "none")
	AssertEquals(t, 3, len(parsed.separators), "Separators count")
	AssertNoError(t, err)
	found := parsed.First().Search("1", nil, true)
	AssertEquals(t, "1", found.Value, "First query search")
	separator := found.Next
	AssertEquals(t, true, separator.IsSeparator(), "Check Separator")
	AssertEquals(t, found, separator.Last(), "Last is the same")
	found = separator.Next.Search("2", nil, true)
	AssertEquals(t, "2", found.Value, "Second query search")
}

func TestMixedCase(t *testing.T) {
	AssertEquals(t, IsMixedCase("AB"), false, "AB")
	AssertEquals(t, IsMixedCase("aB"), true, "aB")
	AssertEquals(t, IsMixedCase("ab"), false, "ab")
	AssertEquals(t, IsMixedCase("A.B"), false, "A.B")
}
