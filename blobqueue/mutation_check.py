#!/usr/bin/env python3
"""Mutation check for blobqueue's shed/bounds logic.

Flips one comparison at a time in the source, runs the bounds tests, and requires every mutant to be
KILLED (a test fails). A surviving mutant means a bound is unguarded. The source is always restored.

    python3 -I blobqueue/mutation_check.py [TMPDIR]
"""
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
TESTS = r"Cap|Age|Floor|Marker|Inflight|Ack|Statfs|Vacuum|Sticky|OneConnection|SecondRun|Reclaim|Epoch"

# (file, original, mutant, what it breaks)
MUTANTS = [
    ("shed.go", "if !take(r, t.rows, t.bytes) {", "if take(r, t.rows, t.bytes) {", "shed predicate inverted"),
    ("queue.go", "rows+dRows-sr > int64(q.cfg.MaxRows)", "rows+dRows-sr < int64(q.cfg.MaxRows)", "row cap comparison flipped"),
    ("queue.go", "bytes+dBytes-sb > q.cfg.MaxBytes", "bytes+dBytes-sb < q.cfg.MaxBytes", "byte cap comparison flipped"),
    ("queue.go", "bytes+dBytes > q.cfg.MaxBytes {", "bytes+dBytes < q.cfg.MaxBytes {", "byte-cap reason selection flipped"),
    ("shed.go", "return r.created < cutoff", "return r.created > cutoff", "age cutoff flipped"),
    ("shed.go", "if free >= need {", "if free <= need {", "free-disk floor comparison flipped"),
    ("shed.go", "if p := uint64(float64(total) * q.cfg.FloorPercent / 100); p > f {", "if p := uint64(float64(total) * q.cfg.FloorPercent / 100); p < f {", "stricter-floor-wins flipped"),
    ("shed.go", "if t.rows == 0 {\n\t\t\ttx.Rollback()", "if t.rows != 0 {\n\t\t\ttx.Rollback()", "refuse-when-nothing-to-shed flipped"),
    ("shed.go", "WHERE protected=0 AND seq>? AND seq<?", "WHERE protected=1 AND seq>? AND seq<?", "protected rows shed instead of data"),
    ("shed.go", "WHERE protected=0 AND seq>? AND seq<?", "WHERE protected=0 AND seq>=? AND seq<?", "in-flight high-water mark off by one"),
    ("shed.go", "return sb < deficit+(1<<20)", "return sb > deficit+(1<<20)", "floor shed target flipped"),
    ("queue.go", "if q.pendRefused > 0 {", "if q.pendRefused < 0 {", "refusals never folded into the marker"),
    ("shed.go", "WHERE protected=1 AND sent=0 ORDER BY seq DESC LIMIT 1", "WHERE protected=1 ORDER BY seq DESC LIMIT 1", "a marker already sent is rewritten in place"),
    ("shed.go", "PRAGMA incremental_vacuum", "PRAGMA user_version", "incremental_vacuum removed (file never shrinks)"),
    ("queue.go", "PRAGMA auto_vacuum=INCREMENTAL", "PRAGMA user_version=0", "auto_vacuum never enabled"),
    ("drain.go", "if a.Accepted == nil || a.Duplicate == nil {", "if a.Accepted == nil && a.Duplicate == nil {", "partial ack accepted"),
    ("drain.go", "+len(a.Rejected) != len(rows) {", "+len(a.Rejected) == len(rows) {", "ack count check inverted"),
    ("shed.go", "q.statfsErrs.Add(1)", "q.statfsErrs.Add(0)", "statfs failures not counted"),
    ("queue.go", "defer conn.Close()", "_ = conn", "pinned connection never released (hangs on a one-connection pool)"),
    ("drain.go", "if !q.running.CompareAndSwap(false, true) {", "if q.running.CompareAndSwap(false, true) {", "second Run not refused"),
    ("shed.go", "q.reclaimErrs.Add(1)\n\t\tq.lastErr.Store(\"vacuum: \" + err.Error())\n\t}", "q.lastErr.Store(\"vacuum: \" + err.Error())\n\t}", "vacuum failure not counted"),
]


def run(env):
    try:
        return subprocess.run(
            ["go", "-C", ROOT, "test", "-count=1", "-timeout=120s", "-run", TESTS, "./blobqueue/"],
            env=env, capture_output=True, text=True, timeout=300,
        )
    except subprocess.TimeoutExpired:
        # A mutant that makes the tests hang is detected (the suite never passes with it).
        return subprocess.CompletedProcess([], 1, "", "timeout")


def main():
    env = dict(os.environ)
    if len(sys.argv) > 1:
        env["TMPDIR"] = sys.argv[1]
    base = run(env)
    if base.returncode != 0:
        print("baseline is red; fix the tests first\n" + base.stdout[-2000:])
        return 2
    survived = []
    for fname, orig, mut, why in MUTANTS:
        path = os.path.join(HERE, fname)
        with open(path) as fh:
            src = fh.read()
        if src.count(orig) != 1:
            print(f"STALE   {why}: pattern found {src.count(orig)}x in {fname}")
            survived.append(why)
            continue
        try:
            with open(path, "w") as fh:
                fh.write(src.replace(orig, mut))
            res = run(env)
        finally:
            with open(path, "w") as fh:
                fh.write(src)
        killed = res.returncode != 0
        print(("KILLED  " if killed else "SURVIVED ") + why)
        if not killed:
            survived.append(why)
    print(f"{len(MUTANTS) - len(survived)}/{len(MUTANTS)} mutants killed")
    return 1 if survived else 0


if __name__ == "__main__":
    sys.exit(main())
