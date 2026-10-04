#!/usr/bin/env python3
"""
Continuous, referentially-consistent inserts into the SOURCE database.
Writes to customers, products, orders, order_items. All ids are UUIDv7.

    pip install -r requirements.txt
    python simulate.py [--rate 5] [--duration 0]

Connection comes from the standard libpq env vars (set in .env / docker compose):
    PGHOST  PGPORT  PGUSER  PGPASSWORD  PGDATABASE      (or one DSN in SOURCE_DSN)

A failed write (read-only source, killed session, DB down) only prints a WARN; the loop keeps
going and reconnects on its own. Ctrl+C to stop.
"""
import argparse
import os
import random
import signal
import threading
import time
import uuid
from datetime import datetime
from decimal import Decimal

import psycopg

# ---------------------------------------------------------------- ids
# Python 3.14 ships uuid.uuid7(). The fallback below (RFC 9562 layout) only exists so the script
# also runs on older Pythons; on 3.14 it is never used.
if hasattr(uuid, "uuid7"):
    new_id = uuid.uuid7
else:
    import secrets
    _last_ms = -1
    _ctr = 0

    def new_id() -> uuid.UUID:
        global _last_ms, _ctr
        ms = time.time_ns() // 1_000_000
        if ms <= _last_ms:
            ms = _last_ms
            _ctr += 1
            if _ctr > 0xFFF:
                ms, _ctr = ms + 1, 0
        else:
            _ctr = secrets.randbits(10)
        _last_ms = ms
        value = (ms << 80) | (0x7 << 76) | (_ctr << 64) | (0b10 << 62) | secrets.randbits(62)
        return uuid.UUID(int=value)


# ---------------------------------------------------------------- data
FIRST = ["Aarav", "Priya", "Rohan", "Sneha", "Vikram", "Ananya", "Kabir", "Meera", "Arjun", "Isha",
         "Neha", "Rahul", "Divya", "Karan", "Pooja"]
LAST = ["Patil", "Sharma", "Kulkarni", "Deshmukh", "Iyer", "Gupta", "Joshi", "Mehta", "Rao", "Nair"]
PRODUCTS = [("Laptop", 30000, 120000), ("Keyboard", 500, 6000), ("Mouse", 300, 4000),
            ("Monitor", 7000, 45000), ("Webcam", 1200, 9000), ("Headset", 800, 15000),
            ("USB-C Dock", 2000, 14000), ("SSD 1TB", 4000, 11000), ("Router", 1500, 12000),
            ("Office Chair", 5000, 30000), ("Desk Lamp", 400, 3500), ("Power Bank", 700, 5000)]
STATUSES = ["NEW", "PENDING", "PAID", "SHIPPED", "DELIVERED", "CANCELLED"]
STATUS_W = [15, 20, 35, 15, 10, 5]

known_customers: list[uuid.UUID] = []
known_products: list[tuple[uuid.UUID, Decimal]] = []   # (product_id, price) - order item price = product price


def ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(m): print(f"{ts()}  {m}", flush=True)
def warn(m): print(f"{ts()}  WARN  {m}", flush=True)


def load_existing(conn, first: bool) -> None:
    """Start from what is already in the DB (the seed rows + anything from earlier runs)."""
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM customers ORDER BY id DESC LIMIT 500")
        known_customers[:] = [r[0] for r in cur.fetchall()]
        cur.execute("SELECT product_id, price FROM products ORDER BY product_id DESC LIMIT 500")
        known_products[:] = [(r[0], r[1]) for r in cur.fetchall()]
    conn.commit()
    if first:
        log(f"loaded {len(known_customers)} customers and {len(known_products)} products from the database")


def one_transaction(conn) -> int:
    """One 'user action' in a single transaction. Returns number of rows written."""
    rows = 0
    new_customers: list[uuid.UUID] = []
    new_products: list[tuple[uuid.UUID, Decimal]] = []
    with conn.transaction():
        with conn.cursor() as cur:
            # customer: usually an existing one, sometimes a brand-new sign-up
            if not known_customers or random.random() < 0.25:
                cid = new_id()
                name = f"{random.choice(FIRST)} {random.choice(LAST)}"
                email = None if random.random() < 0.1 else \
                    f"{name.lower().replace(' ', '.')}.{cid.hex[-6:]}@example.com"
                cur.execute("INSERT INTO customers (id, name, email) VALUES (%s, %s, %s)", (cid, name, email))
                new_customers.append(cid)
                rows += 1
            else:
                cid = random.choice(known_customers)

            # occasionally the catalogue grows
            if not known_products or random.random() < 0.15:
                base, lo, hi = random.choice(PRODUCTS)
                pid = new_id()
                price = Decimal(random.randint(lo * 100, hi * 100)) / 100
                cur.execute("INSERT INTO products (product_id, name, price) VALUES (%s, %s, %s)",
                            (pid, f"{base} {random.choice(['Pro', 'Lite', 'Max', 'Plus', 'X'])}", price))
                new_products.append((pid, price))
                rows += 1

            # 1-4 DISTINCT products (composite PK order_id+product_id) from the catalogue
            catalogue = known_products + new_products
            picked = random.sample(catalogue, k=min(len(catalogue), random.randint(1, 4)))
            items = [(pid, random.randint(1, 3), price) for pid, price in picked]
            total = sum(q * p for _, q, p in items)           # orders.total == sum of its items

            oid = new_id()
            cur.execute("INSERT INTO orders (order_id, customer_id, total, status) VALUES (%s, %s, %s, %s)",
                        (oid, cid, total, random.choices(STATUSES, STATUS_W)[0]))
            rows += 1
            for pid, qty, price in items:
                cur.execute("INSERT INTO order_items (order_id, product_id, quantity, price) "
                            "VALUES (%s, %s, %s, %s)", (oid, pid, qty, price))
                rows += 1

    # only after COMMIT do the new ids become referenceable
    known_customers.extend(new_customers)
    known_products.extend(new_products)
    del known_customers[:-500]
    del known_products[:-500]
    return rows


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dsn", default=os.environ.get("SOURCE_DSN", ""),
                    help="optional full DSN; otherwise PGHOST/PGPORT/PGUSER/PGPASSWORD/PGDATABASE are used")
    ap.add_argument("--rate", type=float, default=float(os.environ.get("SIM_RATE", 5)),
                    help="transactions per second")
    ap.add_argument("--duration", type=float, default=float(os.environ.get("SIM_DURATION", 0)),
                    help="stop after N seconds (0 = run until Ctrl+C)")
    args = ap.parse_args()

    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())

    conn = None
    loaded = False
    first_load = True
    state = "unknown"
    stats = {"ok": 0, "failed": 0, "rows": 0}
    last_warn_at, last_warn_msg, suppressed = 0.0, "", 0
    last_stats = started = time.monotonic()
    interval = 1.0 / max(args.rate, 0.1)
    next_t = time.monotonic()

    target = args.dsn.split("@")[-1] if args.dsn else (
        f"{os.environ.get('PGHOST', 'localhost')}:{os.environ.get('PGPORT', '5432')}/"
        f"{os.environ.get('PGDATABASE', os.environ.get('PGUSER', 'postgres'))}")
    log(f"inserting ~{args.rate} txn/s into {target}  (Ctrl+C to stop)")
    while not stop.is_set() and not (args.duration and time.monotonic() - started >= args.duration):
        try:
            if conn is None or conn.closed:
                conn = psycopg.connect(args.dsn, connect_timeout=3,
                                       options="-c statement_timeout=10000")
                loaded = False
            if not loaded:
                load_existing(conn, first_load)
                loaded = True
                first_load = False
            stats["rows"] += one_transaction(conn)
            stats["ok"] += 1
            if state == "failing":
                log("writes are working again")
            state = "ok"
        except (psycopg.Error, OSError) as e:
            stats["failed"] += 1
            if isinstance(e, psycopg.errors.ReadOnlySqlTransaction):
                msg = "write rejected, source is read-only"
            else:
                msg = f"write failed: {type(e).__name__}: {str(e).strip().splitlines()[0] if str(e).strip() else ''}"
            now = time.monotonic()
            if msg != last_warn_msg or now - last_warn_at > 5:
                warn(msg + (f"  [+{suppressed} similar suppressed]" if suppressed else ""))
                last_warn_msg, last_warn_at, suppressed = msg, now, 0
            else:
                suppressed += 1
            state = "failing"
            # drop the session: a stale one stays read-only / broken even after the DB recovers
            try:
                if conn is not None:
                    conn.close()
            except Exception:
                pass
            conn = None

        if time.monotonic() - last_stats >= 10:
            log(f"ok={stats['ok']} failed={stats['failed']} rows_written={stats['rows']} state={state}")
            last_stats = time.monotonic()
        next_t += interval
        delay = next_t - time.monotonic()
        if delay > 0:
            stop.wait(delay)
        else:
            next_t = time.monotonic()   # fell behind; don't burst

    log(f"done. ok={stats['ok']} failed={stats['failed']} rows_written={stats['rows']}")
    if conn is not None and not conn.closed:
        conn.close()


if __name__ == "__main__":
    main()