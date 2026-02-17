package main

import (
	"database/sql"
	"fmt"
	"math"
	"net"
	"os"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DataDog/go-sqllexer"
	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	_ "github.com/jackc/pgx/v5/stdlib"
	_ "github.com/lib/pq"
)

const TestProxyPort = 5432
const TestDatabasePort = TestProxyPort + 1
const TestDatabaseName = "ingres"
const TestDatabaseEncoding = "fr_FR.UTF-8"
const TestUsername = "user"
const TestPassword = "pass"

func TestIngres(t *testing.T) {
	pgPath := fmt.Sprintf("./pg-%s-%s-%s", TestDatabaseName, TestUsername, TestDatabaseEncoding)
	postgres := embeddedpostgres.NewDatabase(
		embeddedpostgres.DefaultConfig().
			Version(embeddedpostgres.V17).
			BinariesPath(pgPath + "/extracted").
			RuntimePath(pgPath + "/runtime").
			DataPath(pgPath + "/data").
			CachePath(pgPath).
			Logger(nil).
			Locale(TestDatabaseEncoding).
			Database(TestDatabaseName).
			Username(TestUsername).
			Password(TestPassword).
			Port(TestDatabasePort))
	AssertNoError(t, postgres.Start())
	defer func() {
		AssertNoError(t, postgres.Stop())
	}()

	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", TestProxyPort))
	AssertNoError(t, err)

	translator := IngresTranslator()
	cnxStr := fmt.Sprintf("postgres://%s:%s@localhost:%d/%s@localhost:%d?sslmode=require", TestUsername, TestPassword, TestProxyPort, TestDatabaseName, TestDatabasePort) // sslmode=disable

	proxyConfig := &ProxyConfig{
		SqlTranslator:   translator,
		Verbose:         0,
		CertificateFile: "dummy.crt", // on utilise "require" dans cnxStr, donc on force le SSL en passant un faux certificat, mais sans clé privée (il fera un self signed)
		StartupParameters: map[string]string{
			"datestyle": "iso,us", // forcage du datestyle pour simuler le date_format=US par défaut dans la base Ingres
		},
	}
	server, err := proxyConfig.NewServer()
	AssertNoError(t, err)
	go server.Serve(ln)
	defer func() {
		server.Shutdown()
	}()

	connectionsTest := func(name string, url string, profile bool) {
		numThreads := 50
		latency := "0.200"
		start := time.Now()
		var wg sync.WaitGroup
		if profile {
			//runtime.SetCPUProfileRate(50)
			cpuProfiler := "cpuprofile.prof"
			f, _ := os.Create(cpuProfiler)
			defer f.Close()
			err = pprof.StartCPUProfile(f)
			if err != nil {
				t.Fatal(err)
			}
		}
		for range numThreads {
			wg.Go(func() {
				//t.Run(fmt.Sprintf("loadtest-rawthread-%d", i), func(t *testing.T) {
				db, err := sql.Open("postgres", url)
				AssertNoError(t, err)
				defer db.Close()
				AssertSqlQuery(t, db, "SELECT pg_sleep("+latency+")", []string{""})
				//})
			})
		}
		wg.Wait()
		if profile {
			pprof.StopCPUProfile()
		}
		t.Logf("Time spent for %d %s threads : %d ms.\n", numThreads, name, time.Since(start).Milliseconds())
	}
	connectionsTest("raw", fmt.Sprintf("postgres://%s:%s@localhost:%d/%s?sslmode=disable", TestUsername, TestPassword, TestDatabasePort, TestDatabaseName), false)
	connectionsTest("proxy", fmt.Sprintf("postgres://%s:%s@localhost:%d/%s@localhost:%d?sslmode=disable", TestUsername, TestPassword, TestProxyPort, TestDatabaseName, TestDatabasePort), false)

	for _, driver := range []string{"postgres", "pgx"} { // test 2 drivers différents
		t.Logf("Testing driver %s", driver)
		db, err := sql.Open(driver, cnxStr)
		AssertNoError(t, err)
		defer db.Close()

		AssertSqlQuery(t, db, "SELECT 'a' + 'b'", []string{"ab"})
		AssertSqlQuery(t, db, "SELECT 'a' + $1", []string{"ab"}, "b")
		AssertSqlQuery(t, db, "SELECT 'a' + ?", []string{"ab"}, "b")
		AssertSqlQuery(t, db, "SELECT 'a' + ?1", []string{"ab"}, "b")
		AssertSqlQuery(t, db, "SELECT 'a' + @1", []string{"ab"}, "b")
		AssertSqlQuery(t, db, "SELECT 'a' + :1", []string{"ab"}, "b")
		_, err = Query[string](db, "SELECT 'a' + :name", sql.Named("name", "b"))
		if err == nil || !strings.Contains(err.Error(), `pg-proxy error: named parameter 'name' not supported in postgres: strconv.Atoi: parsing "name": invalid syntax`) {
			t.Fatal("Unexpected nil or wrong error: " + err.Error())
		}
		AssertSqlQuery(t, db, "SELECT 2 + ('5' + '2')", []string{"54"})
		AssertSqlQuery(t, db, "SELECT 3 + ('5' + '2') + 9.5", []string{"64.5"})

		AssertSqlExec(t, db, true, "DROP TABLE IF EXISTS TABLE1", 0)
		AssertSqlExec(t, db, true, "DROP TABLE IF EXISTS TABLE2", 0)
		AssertSqlExec(t, db, true, "CREATE TABLE TABLE1 (COLUMN1 TEXT, heuremaj CHAR(6)) WITH NORECOVERY", 0)
		AssertSqlExec(t, db, true, "DECLARE TABLE TABLE2 (COLUMN2 CHAR(10))", 0)

		AssertSqlQuery(t, db, "SELECT char($1)", []string{"A"}, "A")
		AssertSqlQuery(t, db, "SELECT charextract('ABC', 2)", []string{"B"})
		AssertSqlQuery(t, db, "SELECT charextract (charextract('ABC', 2), 1)", []string{"B"})
		AssertSqlQuery(t, db, "SELECT charextract (456, 2)", []string{"5"})
		AssertSqlQuery(t, db, "SELECT date_part('mo', TO_DATE('20170503','YYYYMMDD'))", []int{5})
		AssertSqlExec(t, db, true, "drop table if exists test_table4", 0)
		AssertSqlExec(t, db, true, "drop table if exists test_table5", 0)
		AssertSqlExec(t, db, true, "drop table if exists test_table6", 0)

		AssertSqlExec(t, db, true, "INSERT INTO TABLE1 VALUES ('dummy', '100100')", 1)
		AssertSqlExec(t, db, true, "INSERT INTO TABLE2 VALUES ('dummy')", 1)
		testQuery := "SELECT t.column1, charextract (456, 2), charextract (456, 2) as overr1, 10 from Table1 t where colUMN1 = 'dummy'"
		rows, err := db.Query(testQuery)
		AssertNoError(t, err)
		defer rows.Close()
		columns, err := rows.Columns()
		AssertNoError(t, err)
		AssertEquals(t, testQuery, "column1", columns[0])
		AssertEquals(t, testQuery, "col2", columns[1])
		AssertEquals(t, testQuery, "overr1", columns[2])
		AssertEquals(t, testQuery, "col4", columns[3])

		AssertSqlExec(t, db, false, "COPY TABLE2 INTO '/tmp/test'", 1) // false = pas de TX possible pour un COPY, donc pas de prepare avant l'exec

		AssertSqlQuery(t, db, "select table_name from iitables where table_owner = 'public' order by table_name", []string{"table1", "table2"})
		AssertSqlQuery(t, db, "select table_name from iicolumns where column_name = 'heuremaj' order by table_name", []string{"table1"})
		AssertSqlQuery(t, db, "select upper(COLUMN1+COLUMN1) from TABLE1", []string{"DUMMYDUMMY"})
		AssertSqlQuery(t, db, "select upper(COLUMN1+COLUMN1) + '-' + upper(COLUMN1+COLUMN1) from TABLE1", []string{"DUMMYDUMMY-DUMMYDUMMY"})
		AssertSqlQuery(t, db, "select upper(COLUMN1) + COLUMN1 from TABLE1", []string{"DUMMYdummy"})
		AssertSqlQuery(t, db, "select char(COLUMN1, 1) + COLUMN1 from TABLE1", []string{"ddummy"})
		AssertSqlQuery(t, db, "select COLUMN1 +upper(COLUMN1)  from TABLE1", []string{"dummyDUMMY"})
		AssertSqlQuery(t, db, "select COLUMN1 + char('A')  from TABLE1", []string{"dummyA"})
		AssertSqlQuery(t, db, "select COLUMN1 + char('A' + 'B')  from TABLE1", []string{"dummyAB"})
		AssertSqlQuery(t, db, "select t.HEUREMAJ + COLUMN1  from TABLE1 t", []string{"100100dummy"})
		AssertSqlQuery(t, db, "select COLUMN1 + HEUREMAJ from TABLE1 t", []string{"dummy100100"})
		testQuery = "select substring(COLUMN1,(COLUMN1+COLUMN1)) from TABLE1"
		parsed, err := translator.Translate(testQuery, true, false)
		AssertNoError(t, err)
		AssertEquals(t, testQuery, false, parsed.Transformed)

		AssertSqlExec(t, db, true, "UPDATE TABLE1 FROM TABLE2 SET COLUMN1 = charextract(TABLE2.COLUMN2, 2)", 1)
		AssertSqlExec(t, db, true, "UPDATE TABLE1 FROM TABLE2 SET COLUMN1 = '1' WHERE COLUMN1 = 'u'", 1)
		AssertSqlExec(t, db, true, "UPDATE TABLE1 FROM (SELECT $1 AS CC) AS T SET COLUMN1 = charextract(T.CC, $2)", 1, "ABC", 3)
		AssertSqlQuery(t, db, "SELECT COLUMN1 FROM TABLE1 LIMIT 1", []string{"C"})
		AssertSqlExec(t, db, true, "UPDATE TABLE1 FROM (SELECT ? AS CC) AS T SET COLUMN1 =? + charextract(T.CC, ?)", 1, "ABC", "X", 3) // test inversion ordre des param placeholder
		AssertSqlQuery(t, db, "SELECT COLUMN1 FROM TABLE1 LIMIT 1", []string{"XC"})

		AssertSqlExec(t, db, true, "DROP TABLE TABLE1", 0)
		AssertSqlExec(t, db, true, "DROP TABLE TABLE2", 0)

		now := time.Now()

		testQuery = "select date('today')" // pareil que current_date
		timeResults := AssertSqlRowCount[time.Time](t, db, testQuery, 1)
		timeResults2 := AssertSqlRowCount[time.Time](t, db, "select current_date", 1)
		AssertEquals(t, testQuery, (*timeResults2[0]).Unix(), (*timeResults[0]).Unix())
		AssertSqlQuery(t, db, "select char(date('today'))", []string{now.Format("2006-01-02")})
		AssertSqlQuery(t, db, "select date_part('minutes','now')", []string{now.Format("4")})
		AssertSqlQuery(t, db, "select date_part('minutes','today')", []string{"0"})

		testQuery = "select date('2012-12-01 16:55:15')"
		timeResults = AssertSqlRowCount[time.Time](t, db, testQuery, 1)
		AssertEquals(t, testQuery, 16, timeResults[0].Hour())
		AssertEquals(t, testQuery, 55, timeResults[0].Minute())
		testQuery = "select date_part('minutes', DATE('23-Oct-1998 12:33'))"
		intResults := AssertSqlRowCount[int](t, db, testQuery, 1)
		AssertEquals(t, testQuery, 33, *intResults[0])

		testQuery = "select date('today') - 1"
		timeResults = AssertSqlRowCount[time.Time](t, db, testQuery, 1)
		AssertEquals(t, testQuery, now.AddDate(0, 0, -1).Month(), timeResults[0].Month())
		AssertEquals(t, testQuery, now.AddDate(0, 0, -1).Day(), timeResults[0].Day())
		testQuery = "SELECT TO_DATE('2012-12-01', 'YYYY-MM-DD')"
		timeResults = AssertSqlRowCount[time.Time](t, db, testQuery, 1)
		AssertEquals(t, testQuery, time.December, timeResults[0].Month())
		AssertEquals(t, testQuery, 1, timeResults[0].Day())
		testQuery = "SELECT TO_DATE('2012-Dec', 'YYYY-MON')"
		timeResults = AssertSqlRowCount[time.Time](t, db, testQuery, 1)
		AssertEquals(t, testQuery, time.December, timeResults[0].Month())
		AssertEquals(t, testQuery, 1, timeResults[0].Day())

		res, err := translator.Translate("create temporary table session_tmp_param as select char(par.param2,2) as produit_taxation , substr(par.libre,1,2) as produit_facturation from jdev_param par where par.societe = $1           and par.param1 = 'VEN' on commit preserve rows", true, false)
		if err != nil {
			t.Fatal("Unexpected error")
		}
		if res.Sql() != "create temporary table session_tmp_param on commit preserve rows as select substring((par.param2)::text, 1,2) as produit_taxation , substr(par.libre,1,2) as produit_facturation from jdev_param par where par.societe = $1 and par.param1 = 'VEN'" {
			t.Fatal("Unexpected query translation : " + res.Sql())
		}

		AssertSqlExec(t, db, true, "Set lockmode session where readlock=nolock", 0)
		AssertSqlExec(t, db, true, "create table test_table4 (etat char(10), societe char(10))", 0)
		AssertSqlExec(t, db, true, "create table test_table5 as select * from test_table4 with location = (ii_commercial), nojournaling", 0)
		AssertSqlExec(t, db, true, "create table test_table6 as select char('00        ') as prestation", 1)
		AssertSqlExec(t, db, true, "DECLARE GLOBAL TEMPORARY TABLE test_table6 as select societe, etat from test_table4 ON COMMIT PRESERVE ROWS WITH NORECOVERY", 0)
		AssertSqlExec(t, db, true, "DECLARE GLOBAL TEMPORARY TABLE session.sesstab1701270873090 (ID_COTATION DECIMAL (8,0), DATE_MAJ INGRESDATE) ON COMMIT PRESERVE ROWS WITH NORECOVERY", 0)
		AssertSqlExec(t, db, true, "INSERT INTO session.sesstab1701270873090 (ID_COTATION, DATE_MAJ) VALUES (5.6, date('now'))", 1)
		testQuery = "select DATE_MAJ FROM session.sesstab1701270873090"
		timeResults = AssertSqlRowCount[time.Time](t, db, testQuery, 1)
		_ = math.Abs(float64(timeResults[0].Second()) - float64(time.Now().Second()))

		AssertSqlExec(t, db, true, "update test_table4 h set h.etat = 'E' from session.test_table6 where h.societe = session.test_table6.societe", 0)
		AssertSqlQuery(t, db, "select date_part('day', date(' 2023-01-25'))", []int64{25})
		AssertSqlQuery(t, db, "select decimal('2.45656', 4, 2) + 5", []float64{7.45})
		AssertSqlQuery(t, db, "select 'toto' + 'tata'", []string{"tototata"})
		AssertSqlQuery(t, db, "select substr('toto' + lowercase('TATA'), 5, 2)", []string{"ta"})
		AssertSqlQuery(t, db, "select ifnull(null, 'xxx') + ifnull(null, 'toto')", []string{"xxxtoto"})
		AssertSqlQuery(t, db, "SELECT squeeze(' t ')", []string{"t"})
		AssertSqlQuery(t, db, "SELECT 1 OFFSET 0 FETCH FIRST 1 ROWS ONLY", []int{1})
		AssertSqlQuery(t, db, "select * from (SELECT 1 as T union SELECT 2 as T union SELECT 3 as T) a order by t desc OFFSET 0 FETCH FIRST 2 ROWS ONLY", []int{3, 2})
		AssertSqlQuery(t, db, "select int4('123')", []int{123})
		AssertSqlQuery(t, db, "select trim(char(date_part('yr','now')))+ right(trim('0'+char(date_part('mo','now'))),2)+ right(trim('0'+char(date_part('day','now'))),2)", []string{now.Format("20060102")})
		AssertSqlQuery(t, db, "select DATE_FORMAT(DATE('now'), '%Y%m%d')", []string{now.Format("20060102")})
		AssertSqlQuery(t, db, "select DATE_FORMAT(SYSDATE, '%Y')", []string{fmt.Sprintf("%d", now.Year())})
		AssertSqlQuery(t, db, "select TO_CHAR(SYSDATE, 'YYYYMMDD')", []string{now.Format("20060102")})
		AssertSqlQuery(t, db, "select ('x x ')::char(4)", []string{now.Format("x x ")})
		// not implemented AssertSqlQuery(t, db, "SELECT SHIFT('Company',4)", []string{"   Com"})
		// not implemented AssertSqlQuery(t, db, "SELECT SHIFT('Company',-4)", []string{"any    "})
		tm, err := time.Parse("2006-01-02", "2022-05-25")
		AssertNoError(t, err)

		timeResults = AssertSqlRowCount[time.Time](t, db, "select Ifnull(null, date('05/25/2022'))", 1)
		AssertEquals(t, testQuery, tm.Unix(), (*timeResults[0]).Unix())
		now = time.Now()
		timeResults = AssertSqlRowCount[time.Time](t, db, "select sysdate", 1)
		AssertEquals(t, testQuery, now.Unix(), (*timeResults[0]).Unix())
		AssertSqlQuery(t, db, "SELECT date_part('year', SYSDATE) + date_part('month', SYSDATE) + date_part('day', SYSDATE)", []int{now.Year() + int(now.Month()) + now.Day()})
		AssertSqlRowCount[string](t, db, "select dbmsinfo('SESSION_ID')", 1)
		AssertSqlRowCount[string](t, db, "select dbmsinfo('DUMMY')", 1)
		AssertSqlQuery(t, db, "select char('456', 2)", []string{"45"})
		AssertSqlQuery(t, db, "select char(456)", []string{"456"})
		AssertSqlQuery(t, db, "select char(456, 2)", []string{"45"})
		AssertSqlQuery(t, db, "select right(cast('123456' as char(10)), 2)", []string{"  "})
		AssertSqlQuery(t, db, "select left(cast('123456' as char(10)), 8)", []string{"123456  "})

		now = time.Now()
		timeResults = AssertSqlRowCount[time.Time](t, db, "select TIMESTAMPADD(HOUR, 1, SYSDATE)", 1)
		AssertEquals(t, testQuery, now.Add(time.Hour).Unix(), (*timeResults[0]).Unix())

		currentHour, err := strconv.Atoi(now.Format("15"))
		AssertNoError(t, err)
		AssertSqlQuery(t, db, "select TO_CHAR(TIMESTAMPADD(HOUR, 1, SYSDATE), 'HH24')", []string{fmt.Sprintf("%02d", (currentHour+1)%24)})
		AssertSqlExec(t, db, true, "modify dout_numdispo to btree unique on societe, numdoss, typedoss with location=(ii_database), fillfactor = 80, extend = 16, allocation = 4", 0)
		AssertSqlExec(t, db, true, "modify te_sapx_payeur to isam unique on societe, sap_payeur with fillfactor = 80, extend = 16, page_size = 8192", 0)
		AssertSqlExec(t, db, true, "DROP SEQUENCE IF EXISTS seq_tarif", 0)
		AssertSqlExec(t, db, true, "CREATE SEQUENCE seq_tarif INCREMENT BY 1 MINVALUE 1 MAXVALUE 100000 START 1", 0)
		AssertSqlQuery(t, db, "select seq_tarif.nextval", []int{1})
	}

}

func TestPerf(t *testing.T) {
	large := `--
--@getInstructionsClient
--
SELECT h.societe,
       h.agence,
       h.client,
       f.identite,
       f.nopc,
       f.chrono_as400 as chrono,
       trim(min(h.etat)) as etat,
       f.importance,
       f.siren,
       f.aug_validedu,
       f.aug_date_report,
       h.date1,
       trim(h.edition) as edition,
       trim(h.mail) as mail,
       trim(h.excel) as excel,
       f.raisoc,
       ag.raisoc as agence_libelle,
       trim(f.cli_groupe) as groupe,
       trim(gr.libelle) AS groupe_libelle,
       trim(com1.matric) as matric,
       trim(com1.nom) as nom,
       trim(com1.prenom) as prenom,
       trim(h.sans_argumentaire) as argumentaire,
       trim(h.circulaire) as circulaire,
       trim(c.circulaire_type) as circulaire_type,
       c.strategique,
       h.circulaire_date,
        -- comptage des no demande de foc != 0 pour "multi"
        CASE
            WHEN Min (h.no_demande_foc) > 0 and (min(h.no_demande_foc) <>  max(h.no_demande_foc)) then 'MULTI'
            WHEN Count(ALL
                CASE h.no_demande_foc
                        WHEN 0 THEN NULL
                        ELSE h.no_demande_foc
                END) > count(h.produit) THEN 'MULTI'
            ELSE Max(h.no_demande_foc)
        END AS nodemandefoc,
        -- comptage des id derogation != 0 pour "multi"
        CASE
            WHEN Min (h.id_derogation) > 0 and (min(h.id_derogation) <>  max(h.id_derogation)) then 'MULTI'
            WHEN Count(ALL
                CASE h.id_derogation
                        WHEN 0 THEN NULL
                        ELSE h.id_derogation
                END) > count(h.produit) THEN 'MULTI'
            ELSE Max(h.id_derogation)
        END AS idderogation,
       tpf.tauxcirculaire_fr,
       tpi.tauxcirculaire_inter,
       ifnull(cafacture_fr, 0) + ifnull(cafacture_inter, 0) + ifnull(cafacture_direct, 0) AS cafacture,
       tpf.cafacture_fr,
       tpf.tauxtarif_fr,
       tpi.cafacture_inter,
       tpi.tauxtarif_inter,
       tpd.cafacture_direct,
       tpd.tauxtarif_direct,
       per.periodicite_min,
       e.raisoc as nom_dossier,
       e.situation,
       con.decideur,
       CASE max(f.has_tarif_as400) WHEN 'O' THEN true ELSE false END AS has_tarif_as400,
       max(f.aug_taux_min_derog) AS tauxderogation,
       max(f.aug_taux_couleur) AS tauxcouleur,
       max(h.datecre) AS datecre,
       max(h.cutil) AS cutil,
       max(h.datemaj) AS datemaj,
       max(h.heuremaj) AS heuremaj
FROM jdev.tb_tarf_hausse h
INNER JOIN jdev.tb_tarf_hausse_fixe f
    ON f.societe = h.societe
    AND f.agence = h.agence
    AND f.client = h.client
    AND f.produit = h.produit
    AND f.hausse = h.hausse
INNER JOIN  jdev.tb_tarf_hausse_circulaire c
    ON f.societe = c.societe
    AND f.nopc = c.nopc
    AND f.identite = c.identite
    AND f.hausse = c.hausse
    AND f.regleaugmt = c.regleaugmt
LEFT JOIN  jdev.gcom_entreprise e
    ON f.societe = e.societe
    AND f.nopc = e.nopc
    AND f.identite = e.identite
LEFT JOIN jdev.gcom_nomenclature gr ON e.groupe = gr.code
    AND gr.nomenclature = 'GROUPE'
-- commercial
LEFT JOIN jdev.jdev_commerc com1
    ON f.matric = com1.matric
--DECIDEUR
LEFT JOIN(
select societe, identite, nopc, min(nom || ' ' || prenom) as decideur
    from  jdev.gcom_contact con where con.nomenclature = 'DECI'
    AND con.radie <> 'T'  group by societe, identite, nopc
) con
on con.societe = f.societe
and con.nopc = f.nopc
and con.identite = f.identite
-- CA
LEFT JOIN (
    SELECT f.societe,
        f.agence,
        f.client,
        sum(f.cafacture) AS cafacture_fr,
        CASE
            WHEN min(h.taux1) <>  max(h.taux1) then 'MULTI'
            ELSE avg(h.taux1)
        END as tauxtarif_fr,
        min(h.circulaire_taux) AS tauxcirculaire_fr
   FROM jdev.tb_tarf_hausse h
   INNER JOIN jdev.tb_tarf_hausse_fixe f
        ON f.societe = h.societe
        AND f.agence = h.agence
        AND f.client = h.client
        AND f.produit = h.produit
        AND f.hausse = h.hausse
   where f.produit < '60'
     --AND TRIM (f.tarif) <> ''
     --AND h.taux1 > 0
   GROUP BY f.societe,f.agence,f.client
)tpf
    ON h.societe = tpf.societe
    AND h.agence = tpf.agence
    AND h.client = tpf.client
LEFT JOIN (
    SELECT f.societe,
        f.agence,
        f.client,
        sum(f.cafacture) AS cafacture_inter,
        CASE
            WHEN min(h.taux1) <>  max(h.taux1) then 'MULTI'
            ELSE avg(h.taux1)
        END as tauxtarif_inter,
        min(h.circulaire_taux) AS tauxcirculaire_inter
   FROM jdev.tb_tarf_hausse h
   INNER JOIN jdev.tb_tarf_hausse_fixe f ON f.societe = h.societe
   AND f.agence = h.agence
   AND f.client = h.client
   AND f.produit = h.produit
   AND f.hausse = h.hausse
   WHERE f.produit >= '60' and f.produit <'80'
     --AND (TRIM (f.tarif) <> '' OR trim(f.has_tarif_as400) <> '')
   GROUP BY f.societe,f.agence,f.client
)tpi
    ON h.societe = tpi.societe
    AND h.agence = tpi.agence
    AND h.client = tpi.client
LEFT JOIN (
    SELECT f.societe,
          f.agence,
          f.client,
          sum(f.cafacture) AS cafacture_direct,
          min(h.taux1) AS tauxtarif_direct
   FROM jdev.tb_tarf_hausse h
   INNER JOIN jdev.tb_tarf_hausse_fixe f ON f.societe = h.societe
   AND f.agence = h.agence
   AND f.client = h.client
   AND f.produit = h.produit
   AND f.hausse = h.hausse
   WHERE f.produit >= '80'
   GROUP BY f.societe,f.agence,f.client
)tpd
    ON h.societe = tpd.societe
    AND h.agence = tpd.agence
    AND h.client = tpd.client
-- Périodicité
LEFT join (
    select societe, agence, client,
        CASE
            WHEN min (periodicite)  = '4' then 'M'
            WHEN min (periodicite)  = '3' then 'Q'
            WHEN min (periodicite)  = '2' then 'H'
            WHEN min (periodicite)  = '1' then 'J'
            ELSE '9'
         end as periodicite_min
    from (
        select f.societe, f.agence, f.client,
           CASE
            WHEN period = 'M' then '4'
            WHEN period = 'Q' then '3'
            WHEN period = 'H' then '2'
            WHEN period = 'J' then '1'
            ELSE '9'
         end as periodicite
        from jdev.tb_tarf_hausse_fixe f
        group by f.societe,
                f.agence,
                f.client,
                f.period
    ) t
 group by societe, agence, client
) per
    ON f.societe = per.societe
    AND f.agence = per.agence
    AND f.client = per.client
INNER JOIN jdev.gcom_portefeuille pf
    ON f.societe = pf.societe
    AND f.nopc = pf.nopc
    AND f.identite = pf.identite
INNER JOIN jdev.jdev_agence ag on ag.societe = f.societe and ag.agence = pf.agence
WHERE h.societe = :societe
    AND h.client = ifnull(:client, h.client)
    AND f.nopc = ifnull(:nopc,f.nopc)
    AND f.identite = ifnull(:identite,f.identite)
    AND h.cutil = ifnull(:utilisateur, h.cutil)
GROUP BY h.etat,
         h.societe,
         h.agence,
         h.client,
         f.nopc,
         f.identite,
         f.chrono_as400,
         f.importance,
         f.siren,
         f.aug_validedu,
         f.aug_date_report,
         h.date1,
         h.mail,
         h.edition,
         h.excel,
         h.sans_argumentaire,
         h.circulaire,
         c.circulaire_type,
         c.strategique,
         h.circulaire_date,
         tpf.tauxcirculaire_fr,
         tpi.tauxcirculaire_inter,
         f.raisoc,
         f.cli_groupe,
         gr.libelle,
         com1.matric,
         com1.nom,
         com1.prenom,
         tpi.cafacture_inter,
         tpi.tauxtarif_inter,
         tpf.cafacture_fr,
         tpf.tauxtarif_fr,
         tpd.cafacture_direct,
         tpd.tauxtarif_direct,
         per.periodicite_min,
         con.decideur,
         e.raisoc,
         e.situation,
         ag.raisoc
ORDER BY h.societe, h.etat, h.agence, h.client`

	var numThreads = 25

	ss := time.Now()
	for range numThreads * 100 {
		lexer := sqllexer.New(large, sqllexer.WithDBMS(sqllexer.DBMSOracle))
		for {
			token := lexer.Scan()
			if token.Type == sqllexer.EOF {
				break
			}
		}
	}
	t.Logf("Time for %d go-sqllexer : %d ms.\n", numThreads*100, time.Since(ss).Milliseconds())

	ss = time.Now()
	for range numThreads * 100 {
		r, err := ParseSql(large, sqllexer.DBMSOracle)
		AssertNoError(t, err)
		r.Sql()
	}
	t.Logf("Time for %d lexers : %d ms.\n", numThreads*100, time.Since(ss).Milliseconds())

	ss = time.Now()
	translator := IngresTranslator()

	for range numThreads * 100 {
		translator.Translate(large, true, false)
	}
	t.Logf("Time for %d rewrites : %d ms.\n", numThreads*100, time.Since(ss).Milliseconds())
}

func TestParseFile(t *testing.T) {
	result, err := ParseSql(`
	UPDATE jdev.tb_tarf_hausse h
	SET etat = :etat,
		taux1 = :taux,
		taux2 = :taux2,
		montant_cte = :montant_cte,
		date1 = :dateaugmentation,
		id_derogation = :idderogation,
		no_demande_foc = :nodemandefoc,
		cutil = :cutil,
		datemaj = :datemaj,
		heuremaj = :heuremaj
	WHERE h.societe = :societe
	  AND h.agence = :agence
	  AND h.client = :client;
	
	UPDATE jdev.tb_tarf_hausse
	SET etat = :etat,    
		cutil = :cutil,
		datemaj = :datemaj,
		heuremaj = :heuremaj
	WHERE societe = :societe
	  AND agence = :agence
	  AND client = :client
	  AND produit = :produit;
	
	UPDATE jdev.tb_tarf_hausse
	SET edition = :edition,
		mail = :mail,
		excel = :excel,
		cutil = :cutil,
		datemaj = :datemaj,
		heuremaj = :heuremaj
	WHERE societe = :societe
	  AND agence = :agence
	  AND client = :client;
	  
	  `, sqllexer.DBMSOracle)
	AssertNoError(t, err)
	AssertEquals(t, "file", 3, len(result.Split()))

	result, err = ParseSql("--comment", sqllexer.DBMSOracle)
	AssertNoError(t, err)
	AssertEquals(t, "file", 0, len(result.Split()))
}
