// Package eventoutbox is a durable, restart-surviving queue of outbound job
// lifecycle events POSTed to a controller.
//
// It exists because the obvious alternative — fire-and-forget POSTs — loses a
// terminal event whenever the controller is briefly unreachable, and a lost
// terminal event leaves the central row stuck "running" forever. Nothing
// retries it, because nothing recorded that it failed.
//
// Backed by a `pending_events` table on a caller-supplied *sql.DB — the package
// is driver-agnostic (the agent opens the DB with its own sqlite driver and may
// share it with other tables). Enqueue persists the event then fires an
// immediate best-effort push; a drain loop retries the backlog on an interval,
// oldest-first, bailing on the first failure (the controller is probably down)
// to retry next tick.
package eventoutbox

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

const (
	defaultDrainInterval = 30 * time.Second
	drainBatchLimit      = 100
	pushTimeout          = 5 * time.Second
)

// Outbox is the durable push queue. Construct with New, then Start the drain
// loop and call Enqueue on every event.
type Outbox struct {
	db       *sql.DB
	client   *http.Client
	urlFor   func(jobID string) string
	interval time.Duration

	wg       sync.WaitGroup
	stopOnce sync.Once
	stop     chan struct{}
}

// New creates the pending_events table on db (idempotent) and returns an Outbox.
// urlFor builds the POST URL from an event's job id (e.g.
// CONTROL_CENTER_URL + "/api/duplicacy/jobs/" + id + "/event"). client should
// already carry any auth (e.g. a bearer round-tripper).
func New(db *sql.DB, client *http.Client, urlFor func(string) string) (*Outbox, error) {
	if db == nil || client == nil || urlFor == nil {
		return nil, fmt.Errorf("eventoutbox: db, client and urlFor are required")
	}
	if _, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS pending_events (
			id          INTEGER PRIMARY KEY AUTOINCREMENT,
			job_id      TEXT NOT NULL,
			event       TEXT NOT NULL,
			payload     BLOB NOT NULL,
			created_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			attempts    INTEGER NOT NULL DEFAULT 0,
			last_error  TEXT
		);
		CREATE INDEX IF NOT EXISTS idx_pending_events_id ON pending_events(id);
	`); err != nil {
		return nil, fmt.Errorf("eventoutbox: create schema: %w", err)
	}
	return &Outbox{
		db:       db,
		client:   client,
		urlFor:   urlFor,
		interval: defaultDrainInterval,
		stop:     make(chan struct{}),
	}, nil
}

// Enqueue durably persists an event then fires an immediate best-effort push in
// a detached goroutine. On push failure the row stays for the drain loop.
func (o *Outbox) Enqueue(jobID, event string, payload []byte) {
	res, err := o.db.Exec(
		`INSERT INTO pending_events (job_id, event, payload) VALUES (?, ?, ?)`,
		jobID, event, payload,
	)
	if err != nil {
		slog.Error("eventoutbox: enqueue failed; event lost", "error", err, "job", jobID)
		return
	}
	rowID, _ := res.LastInsertId()
	go o.push(context.Background(), rowID, jobID, payload)
}

// push POSTs one row. On 2xx it deletes the row and returns true; otherwise it
// records the failure and returns false.
func (o *Outbox) push(ctx context.Context, rowID int64, jobID string, body []byte) bool {
	reqCtx, cancel := context.WithTimeout(ctx, pushTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, o.urlFor(jobID), bytes.NewReader(body))
	if err != nil {
		o.recordFailure(rowID, err.Error())
		return false
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := o.client.Do(req)
	if err != nil {
		o.recordFailure(rowID, err.Error())
		return false
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		if _, err := o.db.Exec(`DELETE FROM pending_events WHERE id = ?`, rowID); err != nil {
			slog.Warn("eventoutbox: delete pushed row failed", "error", err, "row", rowID)
		}
		return true
	}
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	o.recordFailure(rowID, fmt.Sprintf("HTTP %d: %s", resp.StatusCode, truncate(string(respBody), 200)))
	return false
}

func (o *Outbox) recordFailure(rowID int64, msg string) {
	if _, err := o.db.Exec(
		`UPDATE pending_events SET attempts = attempts + 1, last_error = ? WHERE id = ?`,
		msg, rowID,
	); err != nil {
		slog.Warn("eventoutbox: record failure failed", "error", err, "row", rowID)
	}
}

// Start runs the drain loop in a tracked goroutine until ctx is cancelled or
// Close is called.
func (o *Outbox) Start(ctx context.Context) {
	o.wg.Go(func() { o.drainLoop(ctx) })
}

func (o *Outbox) drainLoop(ctx context.Context) {
	t := time.NewTicker(o.interval)
	defer t.Stop()
	o.drainOnce(ctx) // flush any leftover from a previous run immediately
	for {
		select {
		case <-ctx.Done():
			return
		case <-o.stop:
			return
		case <-t.C:
			o.drainOnce(ctx)
		}
	}
}

func (o *Outbox) drainOnce(ctx context.Context) {
	rows, err := o.db.QueryContext(ctx,
		`SELECT id, job_id, payload FROM pending_events ORDER BY id ASC LIMIT ?`, drainBatchLimit)
	if err != nil {
		slog.Warn("eventoutbox: drain query failed", "error", err)
		return
	}
	type pending struct {
		id      int64
		jobID   string
		payload []byte
	}
	var batch []pending
	for rows.Next() {
		var p pending
		if err := rows.Scan(&p.id, &p.jobID, &p.payload); err != nil {
			slog.Warn("eventoutbox: scan failed", "error", err)
			continue
		}
		batch = append(batch, p)
	}
	rows.Close()
	if len(batch) == 0 {
		return
	}
	pushed := 0
	for _, p := range batch {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if !o.push(ctx, p.id, p.jobID, p.payload) {
			break // controller likely down — retry next tick
		}
		pushed++
	}
	if pushed > 0 {
		slog.Info("eventoutbox: drained", "pushed", pushed, "remaining", len(batch)-pushed)
	}
}

// Close stops the drain loop and waits for it to exit. Does not close the *sql.DB
// (the caller owns it).
func (o *Outbox) Close() {
	if o == nil {
		return
	}
	o.stopOnce.Do(func() { close(o.stop) })
	o.wg.Wait()
}

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}
