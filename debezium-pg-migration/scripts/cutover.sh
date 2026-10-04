#!/usr/bin/env bash
# Cutover rehearsal / runbook.
#   ./scripts/cutover.sh          asks for confirmation
#   ./scripts/cutover.sh --yes    no prompt (rehearsal)
set -euo pipefail
cd "$(dirname "$0")/.."

src() { docker exec -i source-postgres psql -U postgres -d migration -v ON_ERROR_STOP=1 -At "$@"; }
# Admin variant: overrides the read-only default we set on the source during the freeze.
src_admin() { docker exec -i -e PGOPTIONS="-c default_transaction_read_only=off" \
              source-postgres psql -U postgres -d migration -v ON_ERROR_STOP=1 -At "$@"; }
tgt() { docker exec -i target-postgres psql -U postgres -d migration -v ON_ERROR_STOP=1 -At "$@"; }

TABLES="customers:id products:product_id orders:order_id order_items:order_id,product_id events:event_id,created_at cutover_marker:id"

# count + md5 of full row content per table
fp() {
  local r=$1 out="" spec tbl pk
  for spec in $TABLES; do
    tbl=${spec%%:*}; pk=${spec##*:}
    out+="$tbl=$($r -c "SELECT count(*)||':'||md5(coalesce(string_agg(t::text, ',' ORDER BY $pk),'')) FROM $tbl t") "
  done
  echo "$out"
}

abort() {
  echo
  echo "ABORT: $*"
  echo "Source may be read-only. To roll back (nothing was written to target by the app):"
  echo "  docker exec -e PGOPTIONS='-c default_transaction_read_only=off' source-postgres \\"
  echo "    psql -U postgres -d migration -c 'ALTER DATABASE migration RESET default_transaction_read_only;'"
  exit 1
}

echo "== 0. pre-flight =="
[ "$(src -c "SELECT active FROM pg_replication_slots WHERE slot_name='dbz_migration_slot'")" == "t" ] \
  || abort "replication slot dbz_migration_slot is not active"
echo "replication lag (bytes): $(src -c "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) FROM pg_replication_slots WHERE slot_name='dbz_migration_slot'")"

echo "== 1. confirm application writes are stopped =="
if [ "${1:-}" != "--yes" ]; then
  read -r -p "Have ALL application writes to the SOURCE been stopped? (type yes): " ans
  [ "$ans" == "yes" ] || abort "not confirmed"
fi

echo "== 2. write sentinel row =="
SENT=$(date +%s)
src -c "INSERT INTO cutover_marker(id,note) VALUES ($SENT,'sentinel')"
echo "sentinel id: $SENT"

echo "== 3. freeze source (read-only for new sessions, drop existing sessions) =="
src_admin -c "ALTER DATABASE migration SET default_transaction_read_only = on"
echo "terminated sessions: $(src_admin -c "SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity WHERE datname='migration' AND backend_type='client backend' AND usename <> 'debezium' AND pid <> pg_backend_pid()")"

echo "== 4. wait for sentinel on target =="
ok=0
for _ in $(seq 1 120); do
  if [ "$(tgt -c "SELECT count(*) FROM cutover_marker WHERE id=$SENT")" == "1" ]; then ok=1; break; fi
  sleep 1
done
[ "$ok" == "1" ] || abort "sentinel did not arrive on target within 120s (check: docker compose logs debezium)"
echo "sentinel arrived: all earlier changes are on the target"

echo "== 5. verify counts + content hashes =="
A=$(fp src); B=$(tgt -c "SELECT 1" >/dev/null; fp tgt)
echo "source: $A"
echo "target: $B"
[ "$A" == "$B" ] || abort "source and target differ"
echo "IDENTICAL"

echo "== 6. sync sequences (not replicated by logical replication) =="
SEQ_SQL=$(src -c "SELECT format('SELECT setval(%L, %s, true);', quote_ident(schemaname)||'.'||quote_ident(sequencename), last_value) FROM pg_sequences WHERE last_value IS NOT NULL")
if [ -n "$SEQ_SQL" ]; then
  echo "$SEQ_SQL" | tgt >/dev/null
  echo "synced $(echo "$SEQ_SQL" | wc -l) sequence(s)"
else
  echo "no sequences with values found (nothing to sync)"
fi

echo "== 7. stop Debezium =="
docker compose stop debezium >/dev/null 2>&1

echo "== 8. drop replication slot (otherwise source retains WAL forever) =="
dropped=0
for _ in $(seq 1 15); do
  if src_admin -c "SELECT pg_drop_replication_slot('dbz_migration_slot')" >/dev/null 2>&1; then dropped=1; break; fi
  sleep 2
done
[ "$dropped" == "1" ] || abort "could not drop slot - drop it manually: SELECT pg_drop_replication_slot('dbz_migration_slot');"
echo "slots remaining: $(src -c 'SELECT count(*) FROM pg_replication_slots')"

echo "== 9. drop publication =="
src_admin -c "DROP PUBLICATION IF EXISTS dbz_publication"

echo "== 10. confirm target accepts writes (rolled back) =="
tgt -c "BEGIN; INSERT INTO cutover_marker(id,note) VALUES (-1,'write-test'); ROLLBACK;" >/dev/null
echo "target is writable"

cat <<'MSG'

CUTOVER STEPS COMPLETE.
Next (manual):
  1. Point the application at the TARGET connection string.
  2. Run smoke tests; watch application errors and DB logs.
  3. Keep the SOURCE read-only for several days as the rollback path.
     (Writes made on the target after this point are NOT replicated back.)
  4. Later: drop the cutover_marker table on the target if you don't want it.
MSG