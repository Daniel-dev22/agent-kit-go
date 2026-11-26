package blobqueue

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Rejection is a per-item refusal in an Ack.
type Rejection struct {
	Seq       uint64 `json:"seq"`
	Code      string `json:"code"`
	Retryable bool   `json:"retryable"`
}

// Ack is the receiver's answer to one frame. Accepted and Duplicate are pointers so a 2xx whose body
// lacks them (wrong route, empty reply) is DETECTED, not read as zero: the queue holds the batch
// unless accepted + duplicate + len(rejected) == the number of items sent. Every item NOT listed in
// Rejected is then treated as delivered. Unmarshal the reply body straight into an Ack, or use NewAck.
type Ack struct {
	Accepted  *int        `json:"accepted"`
	Duplicate *int        `json:"duplicate"`
	Rejected  []Rejection `json:"rejected"`
}

// NewAck builds a well-formed Ack.
func NewAck(accepted, duplicate int, rejected ...Rejection) Ack {
	return Ack{Accepted: &accepted, Duplicate: &duplicate, Rejected: rejected}
}

// validate mirrors eventoutbox.BatchOutbox's ack check: the counts must be present, non-negative and
// account for every item, and each rejection must name a distinct item of this batch.
func (a Ack) validate(rows []qrow) error {
	if a.Accepted == nil || a.Duplicate == nil {
		return errors.New("blobqueue: 2xx without an accepted/duplicate ack")
	}
	if *a.Accepted < 0 || *a.Duplicate < 0 || *a.Accepted+*a.Duplicate+len(a.Rejected) != len(rows) {
		return fmt.Errorf("blobqueue: ack shortfall: accepted=%d duplicate=%d rejected=%d batch=%d", *a.Accepted, *a.Duplicate, len(a.Rejected), len(rows))
	}
	in := make(map[uint64]bool, len(rows))
	for _, r := range rows {
		in[r.Seq] = true
	}
	seen := make(map[uint64]bool, len(a.Rejected))
	for _, rj := range a.Rejected {
		if !in[rj.Seq] || seen[rj.Seq] {
			return fmt.Errorf("blobqueue: ack rejects seq %d not in batch or twice", rj.Seq)
		}
		seen[rj.Seq] = true
	}
	return nil
}

// Sender delivers one encoded frame. A non-nil error means nothing was acknowledged (transport
// failure, 5xx, or a request-wide refusal): the batch is held and retried with backoff. Classify
// refusals by the reply body's code, never by status alone.
type Sender func(ctx context.Context, frame []byte) (Ack, error)

type qrow struct {
	Item
	size      int64
	protected bool
}

// Run drains the queue oldest-first until ctx ends: it sends a batch, deletes what the receiver
// acknowledged, and backs off with jitter on failure. A retryable per-item rejection holds that row.
func (q *Queue) Run(ctx context.Context, send Sender) error {
	// A second concurrent Run would send the same head batch and its release() would clear the
	// in-flight mark under the first, letting shedding delete rows still in flight.
	if !q.running.CompareAndSwap(false, true) {
		return ErrRunning
	}
	defer q.running.Store(false)
	failures := 0
	failing := false
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		q.Maintain(ctx)
		n, err := q.drainOnce(ctx, send)
		var wait time.Duration
		switch {
		case err != nil:
			failures++
			q.lastErr.Store(err.Error())
			if !failing {
				failing = true
				q.log.Warn("blobqueue: delivery failing", "err", err)
			}
			wait = backoff(failures, q.cfg.BackoffBase, q.cfg.BackoffMax, q.cfg.Rand())
		case n == 0:
			failures = 0
			wait = q.cfg.DrainInterval
		default:
			failures = 0
			if failing {
				failing = false
				q.lastErr.Store("")
				q.log.Info("blobqueue: delivery recovered")
			}
			continue // more may be queued
		}
		if err == nil && failing {
			failing = false
			q.lastErr.Store("")
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(wait)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-q.poke:
			if err != nil {
				// A new row does not make a failing receiver healthier; honour the backoff.
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-timer.C:
				}
			}
		case <-timer.C:
		}
	}
}

// backoff is base*2^(n-1) capped at max, scaled by jitter in [0.5,1.0).
func backoff(n int, base, max time.Duration, r float64) time.Duration {
	d := base
	for i := 1; i < n && d < max; i++ {
		d *= 2
	}
	if d > max {
		d = max
	}
	return time.Duration(float64(d) * (0.5 + 0.5*r))
}

// drainOnce sends one batch. It returns the number of rows deleted (delivered or dead-lettered).
func (q *Queue) drainOnce(ctx context.Context, send Sender) (int, error) {
	rows, err := q.loadBatch(ctx)
	if err != nil || len(rows) == 0 {
		return 0, err
	}
	defer q.release()
	items := make([]Item, len(rows))
	for i := range rows {
		items[i] = rows[i].Item
	}
	frame, err := EncodeFrame(make([]byte, 0, EncodedSize(q.epoch, items)), q.epoch, items)
	if err != nil {
		return 0, err
	}
	ack, err := send(ctx, frame)
	if err != nil {
		return 0, err
	}
	if err := ack.validate(rows); err != nil {
		return 0, err // hold the whole batch: an unparseable 2xx proves nothing was stored
	}
	keep := map[uint64]struct{}{}
	for _, rj := range ack.Rejected {
		if rj.Retryable {
			keep[rj.Seq] = struct{}{}
		}
	}
	var del []qrow
	var dead []DeadLetter
	for _, r := range rows {
		if _, held := keep[r.Seq]; held {
			continue
		}
		del = append(del, r)
	}
	for _, rj := range ack.Rejected {
		if rj.Retryable {
			continue
		}
		for _, r := range rows {
			if r.Seq == rj.Seq {
				d := DeadLetter{Seq: r.Seq, Signal: r.Signal, Code: rj.Code}
				if r.protected {
					d.Body = r.Body // a refused loss marker is the only durable loss record: hand it to OnDead
				}
				dead = append(dead, d)
			}
		}
	}
	if err := q.deleteRows(ctx, del); err != nil {
		return 0, err
	}
	q.sent.Add(int64(len(del) - len(dead)))
	q.dead.Add(int64(len(dead)))
	if q.cfg.OnDead != nil {
		for _, d := range dead {
			q.cfg.OnDead(d)
		}
	}
	if len(keep) > 0 {
		return len(del), fmt.Errorf("blobqueue: %d items rejected as retryable", len(keep))
	}
	return len(del), nil
}

// loadBatch reads the oldest batch and marks it in flight, atomically with respect to shedding.
func (q *Queue) loadBatch(ctx context.Context) ([]qrow, error) {
	q.wmu.Lock()
	defer q.wmu.Unlock()
	rs, err := q.db.QueryContext(ctx, `SELECT seq,signal,ctype,cenc,body,size,protected FROM blobq_items ORDER BY seq LIMIT ?`, q.cfg.MaxBatchItems)
	if err != nil {
		return nil, fmt.Errorf("blobqueue: load: %w", err)
	}
	var out []qrow
	var markers []any
	total := 0
	for rs.Next() {
		var r qrow
		var seq, prot int64
		if err := rs.Scan(&seq, &r.Signal, &r.ContentType, &r.ContentEncoding, &r.Body, &r.size, &prot); err != nil {
			rs.Close()
			return nil, err
		}
		r.Seq = uint64(seq)
		if len(out) > 0 && total+len(r.Body) > q.cfg.MaxBatchBytes {
			break
		}
		total += len(r.Body)
		if prot == 1 {
			r.protected = true
			markers = append(markers, seq)
		}
		out = append(out, r)
	}
	err = rs.Err()
	rs.Close()
	if err != nil {
		return nil, err
	}
	if len(markers) > 0 {
		// A loss marker that goes out may already be stored by the receiver even if this send fails,
		// so it must never be rewritten (see flushLoss); later loss gets a new marker row.
		if _, err := q.db.ExecContext(ctx, `UPDATE blobq_items SET sent=1 WHERE protected=1 AND seq IN (?`+strings.Repeat(",?", len(markers)-1)+`)`, markers...); err != nil {
			return nil, fmt.Errorf("blobqueue: mark marker sent: %w", err)
		}
	}
	if len(out) > 0 {
		q.inflightMax = out[len(out)-1].Seq
	}
	return out, nil
}

func (q *Queue) release() {
	q.wmu.Lock()
	q.inflightMax = 0
	q.wmu.Unlock()
}

func (q *Queue) deleteRows(ctx context.Context, rows []qrow) error {
	if len(rows) == 0 {
		return nil
	}
	q.wmu.Lock()
	defer q.wmu.Unlock()
	tx, err := q.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("blobqueue: begin: %w", err)
	}
	defer tx.Rollback()
	var bytes int64
	for i := 0; i < len(rows); i += 500 {
		end := min(i+500, len(rows))
		if err := deleteChunk(ctx, tx, rows[i:end]); err != nil {
			return err
		}
	}
	for _, r := range rows {
		bytes += r.size
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("blobqueue: commit ack: %w", err)
	}
	q.rows -= int64(len(rows))
	q.bytes -= bytes
	return nil
}

func deleteChunk(ctx context.Context, tx *sql.Tx, rows []qrow) error {
	args := make([]any, len(rows))
	for i, r := range rows {
		args[i] = int64(r.Seq)
	}
	_, err := tx.ExecContext(ctx, `DELETE FROM blobq_items WHERE seq IN (?`+strings.Repeat(",?", len(rows)-1)+`)`, args...)
	if err != nil {
		return fmt.Errorf("blobqueue: delete acked: %w", err)
	}
	return nil
}

// Stats is a point-in-time view, shaped for a metrics exporter.
type Stats struct {
	Epoch       string
	Durability  string // "full" | "normal" | "weak" (read back from the caller's database at New)
	Rows        int64  // depth, including a loss marker
	Bytes       int64  // logical bytes queued
	OldestAge   time.Duration
	ShedAge     int64 // rows shed, by reason
	ShedRows    int64
	ShedBytes   int64
	ShedFloor   int64
	ShedByte    int64 // logical bytes shed, all reasons
	Refused     int64 // Enqueue calls refused at the free-disk floor
	StatfsErrs  int64 // sticky count of failed free-space reads; while it grows the floor is silently OFF
	ReclaimErrs int64 // sticky count of failed vacuum/checkpoint; shedding may be freeing no disk
	Sent        int64
	Dead        int64
	LastError   string
}

// Stats returns the current counters; the oldest-row age costs one indexed read.
func (q *Queue) Stats(ctx context.Context) Stats {
	q.wmu.Lock()
	rows, bytes := q.rows, q.bytes
	q.wmu.Unlock()
	s := Stats{
		Epoch: q.epoch, Durability: q.durability, Rows: rows, Bytes: bytes,
		ShedAge: q.shedAge.Load(), ShedRows: q.shedRows.Load(), ShedBytes: q.shedBytes.Load(),
		ShedFloor: q.shedFloor.Load(), ShedByte: q.shedByteTotal.Load(),
		Refused: q.refused.Load(), Sent: q.sent.Load(), Dead: q.dead.Load(), StatfsErrs: q.statfsErrs.Load(), ReclaimErrs: q.reclaimErrs.Load(),
	}
	s.LastError, _ = q.lastErr.Load().(string)
	var created int64
	err := q.db.QueryRowContext(ctx, `SELECT created_at FROM blobq_items ORDER BY seq LIMIT 1`).Scan(&created)
	if err == nil {
		s.OldestAge = q.cfg.Now().Sub(time.Unix(0, created))
	} else if !errors.Is(err, sql.ErrNoRows) {
		s.LastError = err.Error()
	}
	return s
}
