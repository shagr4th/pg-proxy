package ingres

import (
	"database/sql"
	"fmt"
	"math"
	"net"
	"os"
	"testing"
	"time"

	"schenker/pg-proxy/proxy"
	"schenker/pg-proxy/sqlutils"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	_ "github.com/jackc/pgx/v5/stdlib"
)

const loadProxyPort = 5440
const loadDatabasePort = 5441

func churnLoop(t *testing.T, name string, url string, iterations int) {
	t.Helper()
	durations := make([]time.Duration, 0, iterations)
	errors := 0

	for i := range iterations {
		start := time.Now()
		db, err := sql.Open("pgx", url)
		if err != nil {
			t.Errorf("iteration %d: open: %v", i, err)
			errors++
			continue
		}
		rows, err := db.Query("SELECT 'a' + 'b'")
		if err != nil {
			t.Errorf("iteration %d: query: %v", i, err)
			errors++
			db.Close()
			continue
		}
		var got string
		if rows.Next() {
			rows.Scan(&got)
		}
		rows.Close()
		db.Close()
		if got != "ab" {
			t.Errorf("iteration %d: expected 'ab', got %q", i, got)
			errors++
		}
		durations = append(durations, time.Since(start))
	}

	if len(durations) == 0 {
		t.Errorf("%s: no successful iterations", name)
		return
	}

	var total time.Duration
	minD := durations[0]
	maxD := durations[0]
	for _, d := range durations {
		total += d
		if d < minD {
			minD = d
		}
		if d > maxD {
			maxD = d
		}
	}
	avg := total / time.Duration(len(durations))

	var variance float64
	for _, d := range durations {
		diff := float64(d - avg)
		variance += diff * diff
	}
	stddev := time.Duration(math.Sqrt(variance / float64(len(durations))))

	t.Logf("%s churn (%d/%d ok): min=%v avg=%v max=%v stddev=%v total=%v errors=%d",
		name, len(durations), iterations, minD, avg, maxD, stddev, total, errors)
}

func TestLoad_ConnectionChurn(t *testing.T) {
	const iterations = 200

	pgPath := fmt.Sprintf("./pg-load-%s-%s", TestDatabaseName, TestUsername)
	err := os.RemoveAll(pgPath + "/data")
	sqlutils.AssertNoError(t, err)

	postgres := embeddedpostgres.NewDatabase(
		embeddedpostgres.DefaultConfig().
			Version(embeddedpostgres.V17).
			BinariesPath(pgPath + "/extracted").
			RuntimePath(pgPath + "/runtime").
			DataPath(pgPath + "/data").
			CachePath(pgPath).
			Encoding("UTF8").
			Logger(nil).
			Locale(TestDatabaseEncoding).
			Database(TestDatabaseName).
			Username(TestUsername).
			Password(TestPassword).
			Port(loadDatabasePort))
	sqlutils.AssertNoError(t, postgres.Start())
	defer postgres.Stop()

	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", loadProxyPort))
	sqlutils.AssertNoError(t, err)

	nativeURL := fmt.Sprintf("postgres://%s:%s@localhost:%d/%s?sslmode=disable",
		TestUsername, TestPassword, loadDatabasePort, TestDatabaseName)
	proxyURL := fmt.Sprintf("postgres://%s:%s@localhost:%d/%s@localhost:%d?sslmode=require",
		TestUsername, TestPassword, loadProxyPort, TestDatabaseName, loadDatabasePort)

	instance := &proxy.ProxyInstance{
		Translator:      IngresTranslator(withStrictFixedChar),
		Verbose:         0,
		CertificateFile: "dummy.crt",
		DefaultClientParameters: map[string]string{
			"datestyle": "iso,us",
		},
	}
	server, err := instance.NewServer()
	sqlutils.AssertNoError(t, err)
	go server.Serve(ln)

	// warm up
	db, err := sql.Open("pgx", proxyURL)
	sqlutils.AssertNoError(t, err)
	sqlutils.AssertSqlQuery(t, db, "SELECT 'a' + 'b'", []string{"ab"})
	db.Close()

	churnLoop(t, "native", nativeURL, iterations)
	churnLoop(t, "proxy", proxyURL, iterations)

	// verify no goroutines are left hanging after all connections closed
	done := make(chan struct{})
	go func() {
		server.Shutdown()
		close(done)
	}()
	select {
	case <-done:
		t.Log("server shutdown cleanly — no goroutine leaks detected")
	case <-time.After(3 * time.Second):
		t.Error("server.Shutdown() timed out — possible goroutine leak")
	}
}
