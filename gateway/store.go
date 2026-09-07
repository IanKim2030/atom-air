package main

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite" // pure-Go driver: no cgo, so the service cross-compiles freely
)

// RawRow is one 1-second sensor sample on its way to local storage.
type RawRow struct {
	TS    int64
	DevID uint8
	Seq   uint16
	Temp  float64
	Hum   float64
	Light uint16
	Flags uint8
}

// MinutePoint is a downsampled minute. The JSON tags are the cloud's
// `minute_stats` payload shape.
type MinutePoint struct {
	DevID       uint8   `json:"dev_id"`
	TS          int64   `json:"ts"`
	TempAvg     float64 `json:"temp_avg"`
	TempMin     float64 `json:"temp_min"`
	TempMax     float64 `json:"temp_max"`
	HumAvg      float64 `json:"hum_avg"`
	LightAvg    float64 `json:"light_avg"`
	SampleCount int     `json:"sample_count"`
}

// Counts is the storage summary the heartbeat logs.
type Counts struct{ Raw, Minute, Pending int64 }

const localSchema = `
CREATE TABLE IF NOT EXISTS raw_readings (
    ts     INTEGER NOT NULL,          -- epoch seconds
    dev_id INTEGER NOT NULL,
    seq    INTEGER,
    temp   REAL, hum REAL, light INTEGER, flags INTEGER,
    PRIMARY KEY (dev_id, ts)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS idx_raw_ts ON raw_readings (ts);

CREATE TABLE IF NOT EXISTS minute_stats (
    ts           INTEGER NOT NULL,
    dev_id       INTEGER NOT NULL,
    temp_avg REAL, temp_min REAL, temp_max REAL,
    hum_avg  REAL, light_avg REAL,
    sample_count INTEGER,
    uploaded     INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (dev_id, ts)
);
CREATE INDEX IF NOT EXISTS idx_minute_outbox ON minute_stats (uploaded, ts);
`

// Store is the local SQLite database: a rolling year of 1-second raw readings
// plus the minute-statistics outbox that survives cloud outages.
type Store struct {
	db   *sql.DB
	Path string
}

// OpenStore opens (creating if needed) the local database in WAL mode.
func OpenStore(path string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}
	dsn := "file:" + path +
		"?_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=busy_timeout(15000)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	// One writer: SQLite serialises writes anyway, and this keeps the WAL
	// free of contention at our modest write rate (one batch per second).
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(localSchema); err != nil {
		db.Close()
		return nil, fmt.Errorf("create schema: %w", err)
	}
	return &Store{db: db, Path: path}, nil
}

func (s *Store) Close() error { return s.db.Close() }

// InsertRaw writes a batch of 1-second samples in a single transaction.
func (s *Store) InsertRaw(rows []RawRow) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	tx, err := s.db.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`INSERT OR REPLACE INTO raw_readings
		(ts, dev_id, seq, temp, hum, light, flags) VALUES (?,?,?,?,?,?,?)`)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	for _, r := range rows {
		if _, err := stmt.Exec(r.TS, r.DevID, r.Seq, r.Temp, r.Hum, r.Light, r.Flags); err != nil {
			return 0, err
		}
	}
	return len(rows), tx.Commit()
}

// UpsertMinute stores closed minute buckets, queued for upload.
func (s *Store) UpsertMinute(pts []MinutePoint) (int, error) {
	if len(pts) == 0 {
		return 0, nil
	}
	tx, err := s.db.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`INSERT OR REPLACE INTO minute_stats
		(ts, dev_id, temp_avg, temp_min, temp_max, hum_avg, light_avg, sample_count, uploaded)
		VALUES (?,?,?,?,?,?,?,?,0)`)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	for _, p := range pts {
		if _, err := stmt.Exec(p.TS, p.DevID, p.TempAvg, p.TempMin, p.TempMax,
			p.HumAvg, p.LightAvg, p.SampleCount); err != nil {
			return 0, err
		}
	}
	return len(pts), tx.Commit()
}

// Outbox returns minute rows the cloud has not acknowledged yet, oldest first.
func (s *Store) Outbox(limit int) ([]MinutePoint, error) {
	rows, err := s.db.Query(`SELECT ts, dev_id, temp_avg, temp_min, temp_max,
		hum_avg, light_avg, sample_count FROM minute_stats
		WHERE uploaded=0 ORDER BY ts LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []MinutePoint
	for rows.Next() {
		var p MinutePoint
		if err := rows.Scan(&p.TS, &p.DevID, &p.TempAvg, &p.TempMin, &p.TempMax,
			&p.HumAvg, &p.LightAvg, &p.SampleCount); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// MarkUploaded clears rows from the outbox once the cloud has them.
func (s *Store) MarkUploaded(pts []MinutePoint) error {
	if len(pts) == 0 {
		return nil
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`UPDATE minute_stats SET uploaded=1 WHERE dev_id=? AND ts=?`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, p := range pts {
		if _, err := stmt.Exec(p.DevID, p.TS); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// PurgeRaw enforces the retention policy: keep a rolling year of raw samples.
func (s *Store) PurgeRaw(days int) (int64, error) {
	cutoff := time.Now().Unix() - int64(days)*86400
	res, err := s.db.Exec(`DELETE FROM raw_readings WHERE ts < ?`, cutoff)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (s *Store) Counts() (Counts, error) {
	var c Counts
	err := s.db.QueryRow(`SELECT
		(SELECT COUNT(*) FROM raw_readings),
		(SELECT COUNT(*) FROM minute_stats),
		(SELECT COUNT(*) FROM minute_stats WHERE uploaded=0)`).Scan(&c.Raw, &c.Minute, &c.Pending)
	return c, err
}
