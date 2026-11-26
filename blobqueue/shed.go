package blobqueue

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	mrand "math/rand/v2"
	"syscall"
)

const (
	reasonAge = iota
	reasonRows
	reasonBytes
	reasonFloor
)

// LossMarker is the body of the single coalesced loss item (Signal == SignalLoss, JSON). It is
// protected from shedding, carries everything dropped since the receiver last saw a marker, and is
// delivered like any other item so a gap shows downstream instead of reading as "quiet".
type LossMarker struct {
	Rows     int64  `json:"rows"`      // rows shed
	Bytes    int64  `json:"bytes"`     // logical bytes shed
	OldestNs int64  `json:"oldest_ns"` // created_at of the oldest row shed
	NewestNs int64  `json:"newest_ns"` // created_at of the newest row shed
	FirstSeq uint64 `json:"first_seq"`
	LastSeq  uint64 `json:"last_seq"`
	Refused  int64  `json:"refused"` // Enqueue calls refused at the free-disk floor (never got a seq)
	Age      int64  `json:"age"`     // rows shed per reason
	RowCap   int64  `json:"row_cap"`
	ByteCap  int64  `json:"byte_cap"`
	Floor    int64  `json:"floor"`
}

func (m *LossMarker) merge(o *LossMarker) {
	m.Rows += o.Rows
	m.Bytes += o.Bytes
	m.Refused += o.Refused
	m.Age += o.Age
	m.RowCap += o.RowCap
	m.ByteCap += o.ByteCap
	m.Floor += o.Floor
	if o.OldestNs != 0 && (m.OldestNs == 0 || o.OldestNs < m.OldestNs) {
		m.OldestNs = o.OldestNs
	}
	if o.NewestNs > m.NewestNs {
		m.NewestNs = o.NewestNs
	}
	if o.FirstSeq != 0 && (m.FirstSeq == 0 || o.FirstSeq < m.FirstSeq) {
		m.FirstSeq = o.FirstSeq
	}
	if o.LastSeq > m.LastSeq {
		m.LastSeq = o.LastSeq
	}
}

type lossAcc struct{ LossMarker }

func (l *lossAcc) empty() bool { return l.Rows == 0 && l.Refused == 0 }

func (l *lossAcc) add(reason int, r shedRow) {
	m := LossMarker{Rows: 1, Bytes: r.size, OldestNs: r.created, NewestNs: r.created, FirstSeq: r.seq, LastSeq: r.seq}
	switch reason {
	case reasonAge:
		m.Age = 1
	case reasonRows:
		m.RowCap = 1
	case reasonBytes:
		m.ByteCap = 1
	case reasonFloor:
		m.Floor = 1
	}
	l.merge(&m)
}

// count publishes the committed loss to the queue's atomic counters.
func (l *lossAcc) count(q *Queue) {
	q.shedAge.Add(l.Age)
	q.shedRows.Add(l.RowCap)
	q.shedBytes.Add(l.ByteCap)
	q.shedFloor.Add(l.Floor)
	q.shedByteTotal.Add(l.Bytes)
}

type shedRow struct {
	seq     uint64
	size    int64
	created int64
}

type tally struct{ rows, bytes int64 }

// shedUntil deletes the oldest unprotected rows with lo < seq < below (lo is the drainer's in-flight
// high-water mark: rows it holds are never shed) while take(row, shedRows, shedBytes) is true, in the
// caller's transaction, and records each in loss. Counters move only after the caller commits.
func (q *Queue) shedUntil(ctx context.Context, tx *sql.Tx, below uint64, reason int, loss *lossAcc, take func(r shedRow, sr, sb int64) bool) (tally, error) {
	var t tally
	if below == 0 {
		below = math.MaxInt64
	}
	for {
		rs, err := tx.QueryContext(ctx, `SELECT seq,size,created_at FROM blobq_items WHERE protected=0 AND seq>? AND seq<? ORDER BY seq LIMIT 64`, int64(q.inflightMax), int64(below))
		if err != nil {
			return t, fmt.Errorf("blobqueue: shed scan: %w", err)
		}
		var batch []shedRow
		for rs.Next() {
			var r shedRow
			var s int64
			if err := rs.Scan(&s, &r.size, &r.created); err != nil {
				rs.Close()
				return t, err
			}
			r.seq = uint64(s)
			batch = append(batch, r)
		}
		if err := rs.Err(); err != nil {
			rs.Close()
			return t, err
		}
		rs.Close()
		var first, last uint64
		n := 0
		for _, r := range batch {
			if !take(r, t.rows, t.bytes) {
				break
			}
			if n == 0 {
				first = r.seq
			}
			last = r.seq
			t.rows++
			t.bytes += r.size
			loss.add(reason, r)
			n++
		}
		if n > 0 {
			// Only unprotected rows above the in-flight mark lie in [first,last] apart from protected
			// markers, which the predicate leaves alone, so the range delete is exact.
			if _, err := tx.ExecContext(ctx, `DELETE FROM blobq_items WHERE protected=0 AND seq BETWEEN ? AND ?`, int64(first), int64(last)); err != nil {
				return t, fmt.Errorf("blobqueue: shed delete: %w", err)
			}
		}
		if n < len(batch) || len(batch) < 64 {
			return t, nil
		}
	}
}

// flushLoss folds loss into the single not-in-flight marker row (updated in place) or inserts one,
// in the caller's transaction, and returns the row/byte delta it caused.
func (q *Queue) flushLoss(ctx context.Context, tx *sql.Tx, loss *lossAcc) (tally, error) {
	var t tally
	if loss.empty() {
		return t, nil
	}
	var seq, oldSize int64
	var body []byte
	// Only a marker never handed to the Sender is updated in place. Once a batch holding it has been
	// sent (even if the send failed, e.g. a timeout AFTER the receiver stored it) its (epoch, seq) may
	// be known to the receiver, which would discard a changed body as a duplicate; later loss goes in
	// a NEW marker row carrying only the delta, and the receiver sums markers.
	err := tx.QueryRowContext(ctx, `SELECT seq,body,size FROM blobq_items WHERE protected=1 AND sent=0 ORDER BY seq DESC LIMIT 1`).Scan(&seq, &body, &oldSize)
	merged := LossMarker{}
	switch {
	case err == nil:
		if err := json.Unmarshal(body, &merged); err != nil {
			merged = LossMarker{} // a corrupt marker must not block shedding; the counters stay authoritative
		}
	case errors.Is(err, sql.ErrNoRows):
		seq = 0
	default:
		return t, fmt.Errorf("blobqueue: read marker: %w", err)
	}
	merged.merge(&loss.LossMarker)
	nb, err := json.Marshal(&merged)
	if err != nil {
		return t, err
	}
	size := int64(len(nb) + len(SignalLoss) + len("application/json"))
	if seq != 0 {
		if _, err := tx.ExecContext(ctx, `UPDATE blobq_items SET body=?, size=? WHERE seq=?`, nb, size, seq); err != nil {
			return t, fmt.Errorf("blobqueue: update marker: %w", err)
		}
		t.bytes = size - oldSize
		return t, nil
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO blobq_items (signal,ctype,cenc,body,size,created_at,protected) VALUES (?,?,?,?,?,?,1)`,
		SignalLoss, "application/json", "", nb, size, q.cfg.Now().UnixNano()); err != nil {
		return t, fmt.Errorf("blobqueue: insert marker: %w", err)
	}
	t.rows, t.bytes = 1, size
	return t, nil
}

// floorBytes is the free space that must remain: the stricter of the percentage and absolute floors.
func (q *Queue) floorBytes(total uint64) uint64 {
	var f uint64
	if q.cfg.FloorBytes > 0 {
		f = uint64(q.cfg.FloorBytes)
	}
	if q.cfg.FloorPercent > 0 {
		if p := uint64(float64(total) * q.cfg.FloorPercent / 100); p > f {
			f = p
		}
	}
	return f
}

// ensureFloor keeps free space >= floor + size by shedding the oldest rows (reclaiming the file so
// the space actually returns). It returns ErrDiskFloor when nothing is left to shed: something else
// is filling the disk, and the queue refuses rather than make it worse.
func (q *Queue) ensureFloor(ctx context.Context, size int64) error {
	if q.path == "" || (q.cfg.FloorBytes < 0 && q.cfg.FloorPercent < 0) {
		return nil
	}
	for attempt := 0; attempt < 8; attempt++ {
		free, total, err := q.cfg.Statfs(q.path)
		if err != nil {
			// Unmeasurable: accept (the alternative is refusing all traffic on a transient statfs error)
			// and surface it in Stats.
			q.lastErr.Store("statfs: " + err.Error())
			q.statfsErrs.Add(1)
			return nil
		}
		need := q.floorBytes(total) + uint64(size)
		if free >= need {
			return nil
		}
		deficit := int64(need - free)
		tx, err := q.db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("blobqueue: begin: %w", err)
		}
		var loss lossAcc
		t, err := q.shedUntil(ctx, tx, 0, reasonFloor, &loss, func(_ shedRow, _, sb int64) bool { return sb < deficit+(1<<20) })
		if err != nil {
			tx.Rollback()
			return err
		}
		if t.rows == 0 {
			tx.Rollback()
			return fmt.Errorf("%w (free %d B, need %d B)", ErrDiskFloor, free, need)
		}
		m, err := q.flushLoss(ctx, tx, &loss)
		if err != nil {
			tx.Rollback()
			return err
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("blobqueue: commit shed: %w", err)
		}
		q.rows += m.rows - t.rows
		q.bytes += m.bytes - t.bytes
		loss.count(q)
		// Deleted pages sit on the freelist and in the WAL; without this the file never shrinks and a
		// floor breach would shed the entire queue for nothing.
		if _, err := q.db.ExecContext(ctx, `PRAGMA incremental_vacuum`); err != nil {
			q.lastErr.Store("vacuum: " + err.Error())
		}
		if _, err := q.db.ExecContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
			q.lastErr.Store("checkpoint: " + err.Error())
		}
	}
	return fmt.Errorf("%w (still below floor after shedding)", ErrDiskFloor)
}

// Maintain sheds rows older than MaxAge. Run calls it every pass; Enqueue calls it at most once a second.
func (q *Queue) Maintain(ctx context.Context) {
	q.wmu.Lock()
	defer q.wmu.Unlock()
	q.maintainLocked(ctx)
}

func (q *Queue) maintainLocked(ctx context.Context) {
	if q.cfg.MaxAge < 0 {
		return
	}
	cutoff := q.cfg.Now().Add(-q.cfg.MaxAge).UnixNano()
	tx, err := q.db.BeginTx(ctx, nil)
	if err != nil {
		q.lastErr.Store("maintain: " + err.Error())
		return
	}
	defer tx.Rollback()
	var loss lossAcc
	t, err := q.shedUntil(ctx, tx, 0, reasonAge, &loss, func(r shedRow, _, _ int64) bool { return r.created < cutoff })
	if err != nil || t.rows == 0 {
		if err != nil {
			q.lastErr.Store("maintain: " + err.Error())
		}
		return
	}
	m, err := q.flushLoss(ctx, tx, &loss)
	if err == nil {
		err = tx.Commit()
	}
	if err != nil {
		q.lastErr.Store("maintain: " + err.Error())
		return
	}
	q.rows += m.rows - t.rows
	q.bytes += m.bytes - t.bytes
	loss.count(q)
}

func statfs(path string) (free, total uint64, err error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return 0, 0, err
	}
	bs := uint64(st.Bsize)
	return uint64(st.Bavail) * bs, uint64(st.Blocks) * bs, nil
}

func randFloat() float64 { return mrand.Float64() }
