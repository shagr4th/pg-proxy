package proxy

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"
)

func ReplayFile(filename string, db *sql.DB) error {
	data, err := os.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("read %s: %w", filename, err)
	}
	var rec Recording
	if err := json.Unmarshal(data, &rec); err != nil {
		return fmt.Errorf("parse %s: %w", filename, err)
	}

	var errs []error
	for i, interaction := range rec.Interactions {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		var queryErr error
		switch interaction.Type {
		case "simple":
			rows, err := db.QueryContext(ctx, interaction.OriginalSQL)
			if err != nil {
				queryErr = err
			} else {
				rows.Close()
			}
		case "extended":
			args := make([]any, len(interaction.Parameters))
			for j, p := range interaction.Parameters {
				args[j] = p
			}
			rows, err := db.QueryContext(ctx, interaction.OriginalSQL, args...)
			if err != nil {
				queryErr = err
			} else {
				rows.Close()
			}
		}
		cancel()
		if queryErr != nil {
			errs = append(errs, fmt.Errorf("interaction %d (%s): %w", i, interaction.OriginalSQL, queryErr))
		}
	}

	if len(errs) > 0 {
		for _, e := range errs {
			log.Printf("REPLAY ERROR [%s] %v", filepath.Base(filename), e)
		}
		return fmt.Errorf("%d error(s) replaying %s", len(errs), filepath.Base(filename))
	}
	log.Printf("REPLAY OK    [%s] %d interaction(s)", filepath.Base(filename), len(rec.Interactions))
	return nil
}

func ReplayDir(dir string, proxyAddr string, user string, password string, database string) {
	matches, err := filepath.Glob(filepath.Join(dir, "record-*.json"))
	if err != nil || len(matches) == 0 {
		log.Printf("[Replay] no record files found in %s", dir)
		return
	}

	dsn := fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=require", user, password, proxyAddr, database)
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		log.Printf("[Replay] cannot open connection: %v", err)
		return
	}
	defer db.Close()

	ok, fail := 0, 0
	for _, f := range matches {
		if err := ReplayFile(f, db); err != nil {
			fail++
		} else {
			ok++
		}
	}
	log.Printf("[Replay] done: %d ok, %d failed (total %d files)", ok, fail, len(matches))
}
