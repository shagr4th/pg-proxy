package sqlite

import (
	"strings"
	"testing"
)

var TestQueries = []struct {
	query    string
	expected string
}{
	{
		query:    `SELECT "users".* FROM "users" ORDER BY "users"."_rowid_" DESC LIMIT 40`,
		expected: `SELECT "users".* FROM "users" ORDER BY "users"."ctid" DESC LIMIT 40`,
	},
	{
		query:    "SELECT \"users\".* FROM \"users\" ORDER BY `users._rowid_` DESC LIMIT 40",
		expected: `SELECT "users".* FROM "users" ORDER BY "users.ctid" DESC LIMIT 40`,
	},
	{
		query:    "SELECT `demo2`.* FROM `demo2` WHERE `demo2`.`active` = 0 LIMIT 1",
		expected: `SELECT "demo2".* FROM "demo2" WHERE "demo2"."active" = 0 LIMIT 1`,
	},
	{
		query:    "SELECT a.id, a.username, a.email, a.emailVisibility FROM a",
		expected: `SELECT a.id, a.username, a.email, a."emailVisibility" FROM a`,
	},
	{
		query:    "SELECT * FROM `TEST` WHERE A IS NOT '' COLLATE NOCASE",
		expected: "SELECT * FROM \"TEST\" WHERE A != '' COLLATE NOCASE",
	},
	{
		query:    "ALTER TABLE `demo5` DROP COLUMN `file`",
		expected: "ALTER TABLE \"demo5\" DROP COLUMN \"file\"",
	},
	{
		query:    "CREATE VIEW `_temp_HORWZC` AS SELECT * FROM (select a.id from users as a left join demo1)",
		expected: "CREATE VIEW \"_temp_HORWZC\" AS SELECT * FROM (select a.id from users as a left join demo1 on 1 = 1)",
	},
	{
		query:    "SELECT `demo1`.* FROM `demo1` WHERE ((`demo1`.`id` IS NOT '' AND `demo1`.`id` IS NOT NULL)) LIMIT 1",
		expected: "SELECT \"demo1\".* FROM \"demo1\" WHERE ((\"demo1\".\"id\" != '' AND \"demo1\".\"id\" IS NOT NULL)) LIMIT 1",
	},
	{
		query:    "SELECT count(*) FROM `_collections` WHERE (LOWER(`name`) = ?) AND (`id`<>?) LIMIT 1",
		expected: "SELECT count(*) FROM \"_collections\" WHERE (LOWER(\"name\") = $1) AND (\"id\"!=$2) LIMIT 1",
	},
	{
		query:    "INSERT INTO TB VALUES ($1, $2)",
		expected: "INSERT INTO TB VALUES ($1, $2)",
	},
	{
		query:    "select 1 where strftime('%Y-%m-%d %H:%M:%fZ') == ?",
		expected: "SELECT 1 WHERE to_char(now(), 'YYYY-mm-dd HH24:MM:ssZ') = $1",
	},
	{
		query:    "select strftime('%Y-%m-%d %H:00:00Z', `created`)",
		expected: "SELECT \"created\"",
	},
	{
		query:    "drop view view1",
		expected: "drop view view1 CASCADE",
	},
}

func TestTranslateLexer(t *testing.T) {
	for _, query := range TestQueries {
		translator := SqliteTranslator()
		res, err := translator.Translate(query.query)
		if err != nil {
			t.Fatal(err)
		}
		final := res.Sql()
		if res == nil || !strings.EqualFold(strings.TrimSpace(query.expected), strings.TrimSpace(final)) {
			t.Logf("\nfailed translation test\nquery    : %s\nexpected : %s\ngot      : %s", query.query, query.expected, final)
		}
	}
}
