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

// Ack is the receiver's answer to one frame. Every item NOT listed in Rejected is treated as
// delivered (accepted, or a duplicate the receiver already had under the same (epoch, seq)).
type Ack struct {
	Accepted  int         `json:"accepted"`
	Duplicate int         `json:"duplicate"`
	Rejected  []Rejection `json:"rejected"`
}

// Sender delivers one encoded frame. A non-nil error means nothing was acknowledged (transport
// failure, 5xx, or a request-wide refusal): the batch is held and retried with backoff. Classify
// refusals by the reply body's code, never by status alone.
type Sender func(ctx context.Context, frame []byte) (Ack, error)

type qrow struct {
	Item
	size int64
}

// Run drains the queue oldest-first until ctx ends: it sends a batch, deletes what the receiver
// acknowledged, and backs off with jitter on failure. A retryable per-item rejection holds that row.
func (q *Queue) Run(ctx context.Context, send Sender) error {
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
				dead = append(dead, DeadLetter{Seq: r.Seq, Signal: r.Signal, Code: rj.Code})
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
	rs, err := q.db.QueryContext(ctx, `SELECT seq,signal,ctype,cenc,body,size FROM blobq_items ORDER BY seq LIMIT ?`, q.cfg.MaxBatchItems)
	if err != nil {
		return nil, fmt.Errorf("blobqueue: load: %w", err)
	}
	defer rs.Close()
	var out []qrow
	total := 0
	for rs.Next() {
		var r qrow
		var seq int64
		if err := rs.Scan(&seq, &r.Signal, &r.ContentType, &r.ContentEncoding, &r.Body, &r.size); err != nil {
			return nil, err
		}
		r.Seq = uint64(seq)
		if len(out) > 0 && total+len(r.Body) > q.cfg.MaxBatchBytes {
			break
		}
		total += len(r.Body)
		out = append(out, r)
	}
	if err := rs.Err(); err != nil {
		return nil, err
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
	Epoch      string
	Durability string // "full" | "normal" | "weak" (read back from the caller's database at New)
	Rows       int64  // depth, including a loss marker
	Bytes      int64  // logical bytes queued
	OldestAge  time.Duration
	ShedAge    int64 // rows shed, by reason
	ShedRows   int64
	ShedBytes  int64
	ShedFloor  int64
	ShedByte   int64 // logical bytes shed, all reasons
	Refused    int64 // Enqueue calls refused at the free-disk floor
	Sent       int64
	Dead       int64
	LastError  string
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
		Refused: q.refused.Load(), Sent: q.sent.Load(), Dead: q.dead.Load(),
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
