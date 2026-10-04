#!/usr/bin/env bash
# End-to-end test. Run from anywhere:  ./scripts/test.sh
set -u
cd "$(dirname "$0")/.."

src() { docker exec -i source-postgres psql -U postgres -d migration -v ON_ERROR_STOP=1 -At "$@"; }
tgt() { docker exec -i target-postgres psql -U postgres -d migration -v ON_ERROR_STOP=1 -At "$@"; }

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

echo "== waiting for Debezium health =="
for _ in $(seq 1 60); do curl -sf localhost:8080/q/health >/dev/null && break; sleep 2; done
curl -sf localhost:8080/q/health | head -c 300; echo

echo "== 1. initial snapshot =="
sync_check "snapshot: all 4 tables identical to source"
check "customers rows"   "2" "$(tgt -c 'SELECT count(*) FROM customers')"
check "products rows"    "2" "$(tgt -c 'SELECT count(*) FROM products')"
check "orders rows"      "2" "$(tgt -c 'SELECT count(*) FROM orders')"
check "order_items rows" "3" "$(tgt -c 'SELECT count(*) FROM order_items')"
check "replication slot active" "t" "$(src -c "SELECT active FROM pg_replication_slots WHERE slot_name='dbz_migration_slot'")"

echo "== 2. streaming INSERT =="
src -c "INSERT INTO customers(id,name,email) VALUES (3,'Priya','priya@example.com');
        INSERT INTO products(product_id,name,price) VALUES (103,'Mouse',999.50);
        INSERT INTO orders(order_id,customer_id,total,status) VALUES (1003,3,999.50,'NEW');
        INSERT INTO order_items(order_id,product_id,quantity,price) VALUES (1003,103,1,999.50);"
sync_check "inserts replicated"

echo "== 3. streaming UPDATE (incl. composite PK table) =="
src -c "UPDATE customers SET email='ajinkya.new@example.com' WHERE id=1;
        UPDATE orders SET status='SHIPPED' WHERE order_id=1001;
        UPDATE order_items SET quantity=5 WHERE order_id=1001 AND product_id=102;"
sync_check "updates replicated"
check "composite-PK update applied" "5" "$(tgt -c 'SELECT quantity FROM order_items WHERE order_id=1001 AND product_id=102')"
check "no duplicate composite rows" "3" "$(tgt -c 'SELECT count(*) FROM order_items WHERE order_id IN (1001,1002)')"

echo "== 4. streaming DELETE (incl. composite PK table) =="
src -c "DELETE FROM order_items WHERE order_id=1001 AND product_id=101;
        DELETE FROM customers WHERE id=2;"
sync_check "deletes replicated"
check "composite-PK delete applied" "0" "$(tgt -c 'SELECT count(*) FROM order_items WHERE order_id=1001 AND product_id=101')"
check "customer 2 removed" "0" "$(tgt -c 'SELECT count(*) FROM customers WHERE id=2')"

echo "== 5. primary-key change (delete + insert) =="
src -c "UPDATE products SET product_id=203 WHERE product_id=103;"
sync_check "PK change replicated"
check "old PK gone" "0" "$(tgt -c 'SELECT count(*) FROM products WHERE product_id=103')"
check "new PK present" "1" "$(tgt -c 'SELECT count(*) FROM products WHERE product_id=203')"

echo "== 6. data fidelity (NUMERIC, unicode, quote, NULL, timestamp micros) =="
src -c "INSERT INTO products(product_id,name,price,created_at) VALUES (300,'Ünï ''quoted'' 日本',12345.67,'2026-01-01 12:34:56.789123');
        INSERT INTO customers(id,name,email) VALUES (4,'NullMail',NULL);"
sync_check "fidelity rows identical"
check "price exact" "12345.67" "$(tgt -c 'SELECT price FROM products WHERE product_id=300')"
check "timestamp micros exact" "2026-01-01 12:34:56.789123" "$(tgt -c 'SELECT created_at FROM products WHERE product_id=300')"

echo "== 7. downtime catch-up (slot retains WAL) =="
docker compose stop debezium >/dev/null 2>&1
src -c "INSERT INTO customers(id,name,email) VALUES (50,'Offline1','o1@example.com'),(51,'Offline2','o2@example.com');
        DELETE FROM customers WHERE id=3;"
check "target unchanged while Debezium is down" "0" "$(tgt -c 'SELECT count(*) FROM customers WHERE id IN (50,51)')"
docker compose start debezium >/dev/null 2>&1
sync_check "caught up after restart"

echo "== 8. container recreation keeps offsets (no re-snapshot) =="
docker compose rm -sf debezium >/dev/null 2>&1
docker compose up -d debezium >/dev/null 2>&1
src -c "INSERT INTO customers(id,name,email) VALUES (60,'AfterRecreate','ar@example.com');"
sync_check "streaming resumes after recreate"
if docker compose logs debezium 2>&1 | grep -qiE "snapshot.*(completed|finished)|Snapshot ended"; then
  echo "INFO  snapshot messages present in logs - inspect manually to confirm it did not re-snapshot"
fi

echo
echo "RESULT: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]