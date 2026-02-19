package main

import (
	"testing"
)

func TestEnclosingFunction(t *testing.T) {
	for _, query := range []string{"SELECT TRUNC(1+(112.1))", "SELECT TRUNC(112.1)", "SELECT TRUNC((112.1))", "SELECT TRUNC(((112.1)))", "SELECT (TRUNC(1+(112.1)))"} {
		parsed, err := ParseSql(query, "none")
		AssertNoError(t, err, query)
		NUM := parsed.First().Search("112.1", nil, false)
		AssertEquals(t, "IS NUM", true, NUM != nil)

		TRUNC, pos := NUM.EnclosingFunction()
		AssertEquals(t, "IS TRUNC", true, TRUNC != nil)
		AssertEquals(t, "IS TRUNC", "TRUNC", TRUNC.Value)
		AssertEquals(t, "IS TRUNC", 0, pos)
	}

	for _, query := range []string{"SELECT TRUNC(112.123, (2))", "SELECT TRUNC(112.123, 1+(2))"} {
		parsed, err := ParseSql(query, "none")
		AssertNoError(t, err, query)
		NUM := parsed.First().Search("2", nil, false)
		AssertEquals(t, "IS NUM", true, NUM != nil)

		TRUNC, pos := NUM.EnclosingFunction()
		AssertEquals(t, "IS TRUNC", true, TRUNC != nil)
		AssertEquals(t, "IS TRUNC", "TRUNC", TRUNC.Value)
		AssertEquals(t, "IS TRUNC", 1, pos)
	}
}

func TestSeparators(t *testing.T) {
	parsed, err := ParseSql("SELECT 1; SELECT 1 + 2;;", "none")
	AssertEquals(t, "Separators count", 3, len(parsed.separators))
	AssertNoError(t, err)
	found := parsed.First().Search("1", nil, true)
	AssertEquals(t, "First query search", "1", found.Value)
	separator := found.Next
	AssertEquals(t, "Check Separator", true, separator.IsSeparator())
	AssertEquals(t, "Last is the same", found, separator.Last())
	found = separator.Next.Search("2", nil, true)
	AssertEquals(t, "Second query search", "2", found.Value)
}

func TestMixedCase(t *testing.T) {
	AssertEquals(t, "AB", IsMixedCase("AB"), false)
	AssertEquals(t, "aB", IsMixedCase("aB"), true)
	AssertEquals(t, "ab", IsMixedCase("ab"), false)
	AssertEquals(t, "A.B", IsMixedCase("A.B"), false)
}
