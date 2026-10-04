#!/usr/bin/env bash
# End-to-end test. Run from anywhere:  ./scripts/test.sh
#
# Expects a freshly initialised stack (docker compose down -v && docker compose up -d --build)
# and the usage simulator NOT running: the test checks the exact seed row counts and compares
# whole-table fingerprints, so any other writer would make it fail.
set -u
cd "$(dirname "$0")/.."

src() { docker exec -i source-postgres psql -U postgres -d migration -v ON_ERROR_STOP=1 -At "$@"; }
tgt() { docker exec -i target-postgres psql -U postgres -d migration -v ON_ERROR_STOP=1 -At "$@"; }

# All entity ids are UUIDs. u N builds an id in the same shape as the seed ids in
# source-init/init.sql: u 1 -> 01900000-0000-7000-8000-000000000001
u() { printf '01900000-0000-7000-8000-%012d' "$1"; }
C1=$(u 1);  C2=$(u 2);  C3=$(u 3);  C4=$(u 4);  C50=$(u 50);  C51=$(u 51);  C60=$(u 60)
P101=$(u 101); P102=$(u 102); P103=$(u 103); P203=$(u 203); P300=$(u 300)
O1001=$(u 1001); O1002=$(u 1002); O1003=$(u 1003)

PASS=0; FAIL=0
check() {
  if [ "$2" == "$3" ]; then echo "PASS  $1"; PASS=$((PASS+1))
  else echo "FAIL  $1"; echo "      expected: $2"; echo "      actual:   $3"; FAIL=$((FAIL+1)); fi
}

# md5 fingerprint of every table's full row content, ordered by PK
fp() {
  local r=$1 out="" spec tbl pk
  for spec in "customers:id" "products:product_id" "orders:order_id" "order_items:order_id,product_id"; do
    tbl=${spec%%:*}; pk=${spec##*:}
    out+="$($r -c "SELECT md5(coalesce(string_agg(t::text, ',' ORDER BY $pk),'')) FROM $tbl t")"
  done
  echo "$out"
}

wait_sync() {  # up to 60s for target to equal source
  for _ in $(seq 1 60); do
    [ "$(fp src)" == "$(fp tgt)" ] && return 0
    sleep 1
  done
  return 1
}

sync_check() {
  if wait_sync; then check "$1" "in-sync" "in-sync"
  else check "$1" "in-sync" "DIVERGED (see: docker compose logs debezium)"; fi
}

echo "== 0. pre-flight =="
if docker ps --format '{{.Names}}' 2>/dev/null | grep -qx simulator; then
  echo "ABORT: the usage simulator is running. Stop it first:  docker compose stop simulator"
  exit 1
fi
SEED=$(src -c 'SELECT count(*) FROM customers')
if [ "$SEED" != "2" ]; then
  echo "ABORT: source has $SEED customers, expected the 2 seed rows (a previous run or the simulator left data)."
  echo "       Start from a clean stack:  docker compose down -v && docker compose up -d --build"
  exit 1
fi
echo "source holds only the seed rows"

echo "== waiting for Debezium health =="
for _ in $(seq 1 60); do curl -sf localhost:8080/q/health >/dev/null && break; sleep 2; done
curl -sf localhost:8080/q/health | head -c 300; echo

echo "== 1. initial snapshot =="
sync_check "snapshot: all 4 tables identical to source"
check "customers rows"   "2" "$(tgt -c 'SELECT count(*) FROM customers')"
check "products rows"    "2" "$(tgt -c 'SELECT count(*) FROM products')"
check "orders rows"      "2" "$(tgt -c 'SELECT count(*) FROM orders')"
check "order_items rows" "3" "$(tgt -c 'SELECT count(*) FROM order_items')"
check "id columns are uuid on the target" "uuid" "$(tgt -c "SELECT data_type FROM information_schema.columns WHERE table_schema='public' AND table_name='orders' AND column_name='order_id'")"
check "replication slot active" "t" "$(src -c "SELECT active FROM pg_replication_slots WHERE slot_name='dbz_migration_slot'")"

echo "== 2. streaming INSERT =="
src -c "INSERT INTO customers(id,name,email) VALUES ('$C3','Priya','priya@example.com');
        INSERT INTO products(product_id,name,price) VALUES ('$P103','Mouse',999.50);
        INSERT INTO orders(order_id,customer_id,total,status) VALUES ('$O1003','$C3',999.50,'NEW');
        INSERT INTO order_items(order_id,product_id,quantity,price) VALUES ('$O1003','$P103',1,999.50);"
sync_check "inserts replicated"

echo "== 3. streaming UPDATE (incl. composite PK table) =="
src -c "UPDATE customers SET email='ajinkya.new@example.com' WHERE id='$C1';
        UPDATE orders SET status='SHIPPED' WHERE order_id='$O1001';
        UPDATE order_items SET quantity=5 WHERE order_id='$O1001' AND product_id='$P102';"
sync_check "updates replicated"
check "composite-PK update applied" "5" "$(tgt -c "SELECT quantity FROM order_items WHERE order_id='$O1001' AND product_id='$P102'")"
check "no duplicate composite rows" "3" "$(tgt -c "SELECT count(*) FROM order_items WHERE order_id IN ('$O1001','$O1002')")"

echo "== 4. streaming DELETE (incl. composite PK table) =="
src -c "DELETE FROM order_items WHERE order_id='$O1001' AND product_id='$P101';
        DELETE FROM customers WHERE id='$C2';"
sync_check "deletes replicated"
check "composite-PK delete applied" "0" "$(tgt -c "SELECT count(*) FROM order_items WHERE order_id='$O1001' AND product_id='$P101'")"
check "customer 2 removed" "0" "$(tgt -c "SELECT count(*) FROM customers WHERE id='$C2'")"

echo "== 5. primary-key change (delete + insert) =="
src -c "UPDATE products SET product_id='$P203' WHERE product_id='$P103';"
sync_check "PK change replicated"
check "old PK gone" "0" "$(tgt -c "SELECT count(*) FROM products WHERE product_id='$P103'")"
check "new PK present" "1" "$(tgt -c "SELECT count(*) FROM products WHERE product_id='$P203'")"

echo "== 6. data fidelity (UUID, NUMERIC, unicode, quote, NULL, timestamp micros) =="
src -c "INSERT INTO products(product_id,name,price,created_at) VALUES ('$P300','Ünï ''quoted'' 日本',12345.67,'2026-01-01 12:34:56.789123');
        INSERT INTO customers(id,name,email) VALUES ('$C4','NullMail',NULL);"
sync_check "fidelity rows identical"
check "uuid exact" "$P300" "$(tgt -c "SELECT product_id FROM products WHERE product_id='$P300'")"
check "price exact" "12345.67" "$(tgt -c "SELECT price FROM products WHERE product_id='$P300'")"
check "timestamp micros exact" "2026-01-01 12:34:56.789123" "$(tgt -c "SELECT created_at FROM products WHERE product_id='$P300'")"
check "NULL email preserved" "t" "$(tgt -c "SELECT email IS NULL FROM customers WHERE id='$C4'")"

echo "== 7. downtime catch-up (slot retains WAL) =="
docker compose stop debezium >/dev/null 2>&1
src -c "INSERT INTO customers(id,name,email) VALUES ('$C50','Offline1','o1@example.com'),('$C51','Offline2','o2@example.com');
        DELETE FROM customers WHERE id='$C3';"
check "target unchanged while Debezium is down" "0" "$(tgt -c "SELECT count(*) FROM customers WHERE id IN ('$C50','$C51')")"
docker compose start debezium >/dev/null 2>&1
sync_check "caught up after restart"

echo "== 8. container recreation keeps offsets (no re-snapshot) =="
docker compose rm -sf debezium >/dev/null 2>&1
docker compose up -d debezium >/dev/null 2>&1
src -c "INSERT INTO customers(id,name,email) VALUES ('$C60','AfterRecreate','ar@example.com');"
sync_check "streaming resumes after recreate"
if docker compose logs debezium 2>&1 | grep -qiE "snapshot.*(completed|finished)|Snapshot ended"; then
  echo "INFO  snapshot messages present in logs - inspect manually to confirm it did not re-snapshot"
fi

echo
echo "RESULT: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]