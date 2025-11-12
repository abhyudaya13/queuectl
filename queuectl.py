#!/usr/bin/env python3
# queuectl.py
# CLI background job queue with SQLite persistence, retries (exponential backoff), DLQ, multi-worker

import argparse, json, os, signal, sqlite3, subprocess, sys, threading, time
from datetime import datetime, timedelta
from uuid import uuid4

DB_PATH = os.environ.get("QUEUECTL_DB", "queue.db")
DEFAULT_MAX_RETRIES = 3
DEFAULT_BACKOFF_BASE = 2.0
HEARTBEAT_SECS = 3

def utcnow():
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

class QueueDB:
    def __init__(self, path=DB_PATH):
        self.path = path
        self.conn = sqlite3.connect(self.path, timeout=30, isolation_level=None, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.migrate()

    def migrate(self):
        cur = self.conn.cursor()
        cur.execute("""PRAGMA journal_mode=WAL;""")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            id TEXT PRIMARY KEY,
            command TEXT NOT NULL,
            state TEXT NOT NULL,                -- pending|processing|completed|failed|dead
            attempts INTEGER NOT NULL DEFAULT 0,
            max_retries INTEGER NOT NULL DEFAULT 3,
            backoff_base REAL NOT NULL DEFAULT 2.0,
            run_at TEXT NOT NULL,               -- ISO time when job becomes eligible
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            locked_by TEXT,
            locked_at TEXT,
            last_error TEXT,
            last_exit_code INTEGER,
            stdout TEXT,
            stderr TEXT
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS workers (
            id TEXT PRIMARY KEY,
            started_at TEXT NOT NULL,
            last_heartbeat TEXT NOT NULL,
            stopping INTEGER NOT NULL DEFAULT 0
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS config (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        """)
        # sensible defaults if not set
        cur.execute("INSERT OR IGNORE INTO config(key,value) VALUES('max_retries', ?)", (str(DEFAULT_MAX_RETRIES),))
        cur.execute("INSERT OR IGNORE INTO config(key,value) VALUES('backoff_base', ?)", (str(DEFAULT_BACKOFF_BASE),))
        self.conn.commit()

    # ------- job api -------
    def enqueue(self, job):
        now = utcnow()
        job = {
            "id": job.get("id") or str(uuid4()),
            "command": job["command"],
            "state": "pending",
            "attempts": 0,
            "max_retries": int(job.get("max_retries", self.get_config("max_retries", DEFAULT_MAX_RETRIES))),
            "backoff_base": float(job.get("backoff_base", self.get_config("backoff_base", DEFAULT_BACKOFF_BASE))),
            "run_at": job.get("run_at") or now,
            "created_at": now,
            "updated_at": now,
            "locked_by": None,
            "locked_at": None,
            "last_error": None,
            "last_exit_code": None,
            "stdout": None,
            "stderr": None,
        }
        with self.conn:
            self.conn.execute("""
              INSERT INTO jobs(id,command,state,attempts,max_retries,backoff_base,run_at,created_at,updated_at,
                               locked_by,locked_at,last_error,last_exit_code,stdout,stderr)
              VALUES(:id,:command,:state,:attempts,:max_retries,:backoff_base,:run_at,:created_at,:updated_at,
                     :locked_by,:locked_at,:last_error,:last_exit_code,:stdout,:stderr)""", job)
        return job["id"]

    def get_config(self, key, default=None):
        row = self.conn.execute("SELECT value FROM config WHERE key=?", (key,)).fetchone()
        return type(default)(row["value"]) if row else default

    def set_config(self, key, value):
        with self.conn:
            self.conn.execute("INSERT INTO config(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                              (key, str(value)))

    def claim_next_job(self, worker_id):
        # atomic claim using IMMEDIATE transaction
        now = utcnow()
        cur = self.conn.cursor()
        cur.execute("BEGIN IMMEDIATE;")
        row = cur.execute("""
            SELECT id FROM jobs
            WHERE state='pending' AND run_at <= ?
            ORDER BY created_at ASC
            LIMIT 1
        """, (now,)).fetchone()
        if not row:
            self.conn.execute("COMMIT;")
            return None
        job_id = row["id"]
        now_time = utcnow()
        cur.execute("""
            UPDATE jobs SET state='processing', locked_by=?, locked_at=?, updated_at=?
            WHERE id=? AND state='pending'
        """, (worker_id, now_time, now_time, job_id))
        self.conn.execute("COMMIT;")
        return self.get_job(job_id)

    def get_job(self, job_id):
        row = self.conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        return dict(row) if row else None

    def update_job_result(self, job_id, exit_code, stdout, stderr, error_text=None):
        now = utcnow()
        job = self.get_job(job_id)
        if not job:
            return
        if exit_code == 0:
            state = "completed"
            attempts = job["attempts"]
            next_run = job["run_at"]
            last_error = None
        else:
            attempts = job["attempts"] + 1
            if attempts > job["max_retries"]:
                state = "dead"
                next_run = job["run_at"]
            else:
                state = "pending"  # retry
                delay = job["backoff_base"] ** attempts  # exponential
                next_run = (datetime.utcnow() + timedelta(seconds=delay)).replace(microsecond=0).isoformat() + "Z"

            last_error = error_text or f"Exit code {exit_code}"

        with self.conn:
            self.conn.execute("""
              UPDATE jobs
              SET state=?, attempts=?, run_at=?, updated_at=?, last_exit_code=?, stdout=?, stderr=?, last_error=?, locked_by=NULL, locked_at=NULL
              WHERE id=?
            """, (state, attempts, next_run, now, exit_code, stdout, stderr, last_error, job_id))

    def list_by_state(self, state=None, limit=100):
        if state:
            q = "SELECT * FROM jobs WHERE state=? ORDER BY created_at DESC LIMIT ?"
            return [dict(r) for r in self.conn.execute(q, (state, limit)).fetchall()]
        else:
            q = "SELECT state, COUNT(*) as cnt FROM jobs GROUP BY state"
            return [dict(r) for r in self.conn.execute(q).fetchall()]

    # ------- worker api -------
    def register_worker(self, worker_id):
        now = utcnow()
        with self.conn:
            self.conn.execute("INSERT INTO workers(id, started_at, last_heartbeat, stopping) VALUES(?,?,?,0) "
                              "ON CONFLICT(id) DO UPDATE SET last_heartbeat=excluded.last_heartbeat, stopping=0",
                              (worker_id, now, now))

    def heartbeat(self, worker_id):
        with self.conn:
            self.conn.execute("UPDATE workers SET last_heartbeat=? WHERE id=?", (utcnow(), worker_id))

    def stop_worker(self, worker_id=None):
        with self.conn:
            if worker_id:
                self.conn.execute("UPDATE workers SET stopping=1 WHERE id=?", (worker_id,))
            else:
                self.conn.execute("UPDATE workers SET stopping=1")

    def is_stopping(self, worker_id):
        row = self.conn.execute("SELECT stopping FROM workers WHERE id=?", (worker_id,)).fetchone()
        return bool(row and row["stopping"])

    def status(self):
        counts = {r["state"]: r["cnt"] for r in self.list_by_state()}
        workers = [dict(r) for r in self.conn.execute("SELECT * FROM workers").fetchall()]
        return counts, workers

# -------- worker process loop ---------
stop_flag = threading.Event()

def _install_signals():
    def handler(signum, frame):
        stop_flag.set()
    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)

def run_worker_loop(db: QueueDB, worker_id: str):
    _install_signals()
    db.register_worker(worker_id)
    last_hb = 0
    current_job = None
    while not stop_flag.is_set() and not db.is_stopping(worker_id):
        if time.time() - last_hb > HEARTBEAT_SECS:
            db.heartbeat(worker_id)
            last_hb = time.time()

        job = db.claim_next_job(worker_id)
        if not job:
            time.sleep(0.5)
            continue

        current_job = job["id"]
        try:
            proc = subprocess.run(job["command"], shell=True, capture_output=True, text=True)
            exit_code = proc.returncode
            db.update_job_result(job["id"], exit_code, proc.stdout, proc.stderr)
        except Exception as e:
            db.update_job_result(job["id"], 1, "", "", error_text=str(e))
        finally:
            current_job = None

    # graceful: finish current job, then exit
    print(f"[worker {worker_id}] stopping gracefully.")

# -------- CLI --------
def cmd_enqueue(args):
    db = QueueDB()
    if args.json:
        payload = json.loads(args.json)
    else:
        payload = {"command": args.command}
        if args.id: payload["id"] = args.id
        if args.max_retries is not None: payload["max_retries"] = args.max_retries
        if args.backoff_base is not None: payload["backoff_base"] = args.backoff_base
        if args.run_at: payload["run_at"] = args.run_at
    job_id = db.enqueue(payload)
    print(job_id)

def worker_entry(wid):
    """Entry point for each worker process (Windows-safe)."""
    db = QueueDB()
    run_worker_loop(db, wid)

def cmd_worker_start(args):
    import multiprocessing as mp
    procs = []

    for i in range(args.count):
        wid = f"w-{uuid4().hex[:8]}"
        p = mp.Process(target=worker_entry, args=(wid,), daemon=False)
        p.start()
        procs.append(p)
        print(f"started worker {wid} (pid={p.pid})")

    try:
        for p in procs:
            p.join()
    except KeyboardInterrupt:
        db = QueueDB()
        db.stop_worker()
        print("Stopping workers...")


def cmd_worker_stop(args):
    db = QueueDB()
    db.stop_worker()
    print("Signaled workers to stop. They will finish current job and exit.")

def cmd_status(args):
    db = QueueDB()
    counts, workers = db.status()
    print("Job states:", counts)
    print("Workers:")
    for w in workers:
        print(f" - {w['id']} hb={w['last_heartbeat']} stopping={w['stopping']}")

def cmd_list(args):
    db = QueueDB()
    rows = db.list_by_state(args.state, limit=args.limit)
    if args.state:
        for r in rows:
            print(f"{r['id']}  {r['state']}  cmd='{r['command']}' attempts={r['attempts']} max={r['max_retries']} run_at={r['run_at']}")
    else:
        print(rows)

def cmd_dlq_list(args):
    db = QueueDB()
    rows = db.conn.execute("SELECT * FROM jobs WHERE state='dead' ORDER BY updated_at DESC LIMIT ?", (args.limit,)).fetchall()
    for r in rows:
        r = dict(r)
        print(f"{r['id']} dead after {r['attempts']} attempts | last_error={r['last_error']}")

def cmd_dlq_retry(args):
    db = QueueDB()
    job = db.get_job(args.job_id)
    if not job or job["state"] != "dead":
        print("Job not found in DLQ.")
        return
    with db.conn:
        db.conn.execute("""
            UPDATE jobs SET state='pending', attempts=0, run_at=?, updated_at=?, last_error=NULL, last_exit_code=NULL
            WHERE id=?
        """, (utcnow(), utcnow(), args.job_id))
    print(f"Retried {args.job_id} -> pending")

def cmd_config_set(args):
    db = QueueDB()
    db.set_config(args.key, args.value)
    print(f"config {args.key}={args.value}")

def main():
    p = argparse.ArgumentParser(prog="queuectl", description="Tiny job queue with workers, retries and DLQ.")
    sub = p.add_subparsers(dest="cmd")

    pe = sub.add_parser("enqueue", help="Add a new job")
    pe.add_argument("command", nargs="?", help="Command to run (e.g., \"sleep 2\")")
    pe.add_argument("--id", help="Job id")
    pe.add_argument("--max-retries", type=int)
    pe.add_argument("--backoff-base", type=float)
    pe.add_argument("--run-at", help="ISO time to run")
    pe.add_argument("--json", help="Raw JSON payload for full control")
    pe.set_defaults(func=cmd_enqueue)

    pw = sub.add_parser("worker", help="Manage workers")
    pw_sub = pw.add_subparsers(dest="wcmd")

    pws = pw_sub.add_parser("start", help="Start N workers")
    pws.add_argument("--count", type=int, default=1)
    pws.set_defaults(func=cmd_worker_start)

    pws2 = pw_sub.add_parser("stop", help="Gracefully stop all workers")
    pws2.set_defaults(func=cmd_worker_stop)

    ps = sub.add_parser("status", help="Cluster status")
    ps.set_defaults(func=cmd_status)

    pl = sub.add_parser("list", help="List jobs by state")
    pl.add_argument("--state", choices=["pending","processing","completed","failed","dead"])
    pl.add_argument("--limit", type=int, default=100)
    pl.set_defaults(func=cmd_list)

    pd = sub.add_parser("dlq", help="Dead Letter Queue ops")
    dlq_sub = pd.add_subparsers(dest="dcmd")

    pdl = dlq_sub.add_parser("list", help="List DLQ jobs")
    pdl.add_argument("--limit", type=int, default=100)
    pdl.set_defaults(func=cmd_dlq_list)

    pdr = dlq_sub.add_parser("retry", help="Retry a DLQ job")
    pdr.add_argument("job_id")
    pdr.set_defaults(func=cmd_dlq_retry)

    pc = sub.add_parser("config", help="Set global config")
    pc.add_argument("key", choices=["max_retries","backoff_base"])
    pc.add_argument("value")
    pc.set_defaults(func=cmd_config_set)

    args = p.parse_args()
    if not getattr(args, "cmd", None):
        p.print_help(); sys.exit(0)
    # delegate
    if hasattr(args, "func"):
        args.func(args)
    else:
        p.print_help()

if __name__ == "__main__":
    main()
