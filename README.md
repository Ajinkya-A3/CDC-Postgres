# Debezium Server: PostgreSQL to PostgreSQL migration (no Kafka)

Replicates four tables from a source PostgreSQL database to a target PostgreSQL database using
**Debezium Server 3.7.0.Final** with the **PostgreSQL source connector** (`pgoutput`) and the
**JDBC sink**. There is no Kafka and no Kafka Connect. The target schema is created by you; Debezium
only moves rows (initial snapshot, then streaming inserts, updates and deletes).

All entity ids (`customers.id`, `products.product_id`, `orders.order_id`, ...) are **UUIDs**, and the
included usage simulator (section 9) generates **UUIDv7** values, so you can watch realistic, time-ordered
traffic replicate and rehearse a cutover under load.

The project is designed to be run and tested in **GitHub Codespaces**, which has a Docker networking
quirk that breaks container-to-container traffic. Section 4 explains it and how to fix it. **Run the
fix before starting the stack.**

## Contents

1. [How it works](#1-how-it-works)
2. [Repository layout](#2-repository-layout)
3. [Quick start (Codespaces)](#3-quick-start-codespaces)
4. [Codespaces networking problem](#4-codespaces-networking-problem)
5. [Files explained](#5-files-explained)
6. [The table-name placeholder problem](#6-the-table-name-placeholder-problem)
7. [Checking the data](#7-checking-the-data)
8. [Automated tests](#8-automated-tests)
9. [Usage simulator (continuous test traffic)](#9-usage-simulator-continuous-test-traffic)
10. [Cutover](#10-cutover)
11. [Troubleshooting](#11-troubleshooting)
12. [Verification status](#12-verification-status)
13. [Reset and cleanup](#13-reset-and-cleanup)
14. [References](#14-references)

---

## 1. How it works

```
 source-postgres                debezium (one container)                    target-postgres
 ---------------      ------------------------------------------------      ---------------
 tables               PostgreSQL connector                                   tables
 publication  ------> reads the logical replication slot (pgoutput)          customers
 slot (created by     builds a change event; topic = migration.public.X      products
 Debezium)            RegexRouter transform: topic -> X          ------>     orders
                      JDBC sink: upsert / delete into table named X          order_items
                      offsets saved to a file on a volume                    cutover_marker
```

* **No Kafka.** "Topic" is only a name label on each change event inside the Debezium process. The
  JDBC sink uses that label as the target table name. Nothing connects to a broker. Debezium reuses
  Kafka Connect libraries internally (you will see `connect-api` and `kafka-clients` on the classpath
  and a default `bootstrap.servers = localhost:9092` in a config dump), but it never uses them to
  talk to Kafka.
* **Snapshot, then stream.** On first start Debezium copies all existing rows, then streams changes
  from the exact point where the snapshot was taken.
* **Upsert.** The sink inserts a new row or updates it if the key exists, so replays and the snapshot
  are safe to repeat.
* **Offsets.** Progress is stored in `/debezium/data/offsets.dat` on a named volume. The replication
  slot lives on the source and holds WAL until Debezium confirms it.

## 2. Repository layout

```
.
├── LICENSE
├── README.md
└── debezium-pg-migration
    ├── docker-compose.yml          # source DB, target DB, Debezium Server, usage simulator
    ├── debezium
    │   ├── Dockerfile              # Debezium Server image + PostgreSQL JDBC driver
    │   └── application.properties  # all Debezium configuration
    ├── source-init
    │   └── init.sql                # source schema, seed data, replication user, publication
    ├── target-init
    │   └── init.sql                # same schema, empty
    ├── scripts
    │   ├── test.sh                 # end-to-end test (insert/update/delete/restart ...)
    │   └── cutover.sh              # cutover rehearsal / runbook
    └── usage-simultaion            # (folder name as it is in the repo)
        ├── Dockerfile              # python:3.14-slim image for the simulator
        ├── requirements.txt        # psycopg[binary]
        └── simulate.py             # continuous UUIDv7 inserts into the source
```

All commands below run from `debezium-pg-migration/` unless stated otherwise.

## 3. Quick start (Codespaces)

```bash
cd debezium-pg-migration
chmod +x scripts/*.sh

# 0. Codespaces only: allow traffic between containers (see section 4). Repeat after every restart.
sudo sysctl -w net.bridge.bridge-nf-call-iptables=0

# 1. Clean start (init.sql files only run when a database volume is empty)
docker compose down -v
docker compose up -d --build

# 2. Check that the containers can reach each other. Expect: OPEN
docker exec source-postgres bash -c 'timeout 3 bash -c "</dev/tcp/target-postgres/5432" && echo OPEN || echo CLOSED'

# 3. Watch Debezium start. Expect "Snapshot completed" then "Starting streaming".
docker compose logs -f debezium | grep -E "Snapshot|Finished exporting|ERROR|WARN|Engine has failed"
```

`docker ps` showing `Up` does **not** mean the CDC works. Debezium can stop its engine on an error
while the container keeps running, so always read the logs.

Then verify (section 7) and run `./scripts/test.sh` (section 8).

Optional: generate continuous test traffic against the source (section 9):

```bash
docker compose --profile sim up -d --build simulator
docker compose logs -f simulator
```

## 4. Codespaces networking problem

### Symptoms

* Debezium logs `SocketTimeoutException: Connect timed out`, followed by a long Hibernate error
  `Unable to determine Dialect without JDBC metadata`.
* `docker exec source-postgres pg_isready -h target-postgres -p 5432` prints `no response`.
* A raw TCP test between containers prints `CLOSED` or "Connection timed out".
* Everything else looks healthy: both databases pass their healthchecks, both containers are on the
  same network, DNS resolves `target-postgres` to the right IP, `listen_addresses` is `*`, and the
  target accepts connections on its own IP from inside its own container.

The Hibernate "Dialect" error is only a side effect. Hibernate needs a database connection to read
metadata, and the connection timed out.

### Cause

The Docker daemon in the Codespace uses the nftables backend, but the host also has an **older
iptables-legacy ruleset** with `-P FORWARD DROP` and accept rules only for the default `docker0`
bridge. With `net.bridge.bridge-nf-call-iptables = 1`, traffic inside a custom bridge network is
passed through that legacy ruleset and dropped. Traffic that leaves a container through the host
gateway still works, which is why published ports (5433 and 5434) are reachable.

This was diagnosed from observed output in this environment (a minimal two-container test on a
brand-new network also timed out). It is a Codespaces/host quirk, not a problem in this project, and a
normal Docker host would not show it.

### Fix

```bash
sudo sysctl -w net.bridge.bridge-nf-call-iptables=0
```

It takes effect immediately; no containers need to be recreated. **The setting is lost when the
Codespace restarts**, so run it again after every restart. Optionally save it as a script:

```bash
cat > scripts/fix-codespaces-network.sh <<'EOF'
#!/usr/bin/env bash
set -e
sudo sysctl -w net.bridge.bridge-nf-call-iptables=0
EOF
chmod +x scripts/fix-codespaces-network.sh
```

A `postStartCommand` in `.devcontainer/devcontainer.json` could automate it (not tested here).

### Diagnose it yourself

```bash
sysctl net.bridge.bridge-nf-call-iptables        # 1 = likely cause

# Isolate from this project: two plain containers on a fresh network.
docker network create nettest
docker run -d --rm --name n1 --network nettest postgres:16 sleep 300
docker run -d --rm --name n2 --network nettest postgres:16 sleep 300
docker exec n1 bash -c 'timeout 3 bash -c "</dev/tcp/n2/22"; echo exit=$?'
#   exit=124 -> packets dropped (the problem)    exit=1 -> network healthy (connection refused)
docker rm -f n1 n2; docker network rm nettest
```

### Fallbacks if the sysctl is not enough

1. **Explicit forward rule (untested).** Allow traffic inside the project's bridge in the legacy
   table. The bridge name is derived from the network ID and changes whenever the network is recreated:

   ```bash
   BR=br-$(docker network inspect cdc -f '{{.Id}}' | cut -c1-12)
   sudo iptables-legacy -I FORWARD -i "$BR" -o "$BR" -j ACCEPT
   ```

2. **Go through the host gateway and published ports.** This path was confirmed to work
   (`pg_isready -h 172.18.0.1 -p 5434` accepted connections from inside a container). In
   `debezium/application.properties`, change only these:

   ```properties
   debezium.source.database.hostname=172.18.0.1
   debezium.source.database.port=5433
   debezium.sink.jdbc.connection.url=jdbc:postgresql://172.18.0.1:5434/migration?currentSchema=public
   ```

   Confirm the gateway first with `docker network inspect cdc -f '{{range .IPAM.Config}}{{.Gateway}}{{end}}'`.
   You can also pin the subnet under `networks.cdc.ipam` in the Compose file so it never changes.

## 5. Files explained

### 5.1 `docker-compose.yml`

| Part | What it does |
|---|---|
| `postgres:16` for both databases | Two independent databases standing in for the old and new systems. |
| `command: postgres -c wal_level=logical ...` (source only) | `wal_level=logical` is what lets Postgres expose row-level changes for logical decoding. `max_wal_senders` and `max_replication_slots` are set to 10 (the PostgreSQL 16 defaults, kept explicit for clarity). The target needs none of this. |
| `./source-init/init.sql:/docker-entrypoint-initdb.d/init.sql:ro` | The postgres image runs scripts in this directory **only when the data directory is empty**. After editing an init file you must run `docker compose down -v`. |
| `ports: 5433:5432` and `5434:5432` | Lets you connect from the Codespace host. Containers talk to each other on 5432 by service name. |
| `healthcheck: pg_isready -h 127.0.0.1 ...` | The `-h 127.0.0.1` forces a TCP connection. During first start the image runs a temporary server that listens only on a Unix socket, which would pass a socket-based check too early. |
| `depends_on ... condition: service_healthy` | Debezium starts only after both databases are healthy. |
| `application.properties` mounted read-only at `/debezium/config/` | The location Debezium Server reads its configuration from. Edit the file, then restart the container. |
| `debezium-offsets:/debezium/data` | Named volume that holds the offsets file so progress survives container recreation. |
| `8080:8080` | Debezium Server's health endpoint: `curl localhost:8080/q/health`. |
| `networks: cdc` | One explicit bridge network (name `cdc`) shared by all services. |
| `simulator` service, `profiles: ["sim"]` | The usage simulator (section 9). The profile keeps it from starting with a plain `docker compose up`; start it with `--profile sim`. Its connection settings are plain `environment:` entries on the service. |

### 5.2 `source-init/init.sql`

Runs once, as the `postgres` superuser, against the `migration` database.

| Statement | Why it exists |
|---|---|
| `CREATE USER debezium WITH REPLICATION LOGIN PASSWORD ...` | The account Debezium connects as. The `REPLICATION` attribute allows it to open replication connections and create the replication slot. It is deliberately **not** a superuser. (The password is a demo value; use a secret in real use.) |
| `GRANT CONNECT ON DATABASE`, `GRANT USAGE ON SCHEMA public` | The minimum needed to reach the tables. |
| `CREATE TABLE customers, products, orders` | Ordinary tables with single-column **`UUID`** primary keys (`id`, `product_id`, `order_id`). `orders.customer_id` is also `UUID`. |
| `CREATE TABLE order_items` | Has a **composite** primary key `(order_id, product_id)` (both `UUID`) to prove multi-column keys work. |
| `CREATE TABLE cutover_marker` | A tiny table used by `scripts/cutover.sh` to write a "sentinel" row (section 10). Its `id` stays **`BIGINT`** on purpose: the script writes an epoch number there, not an entity id. |
| Seed `INSERT`s | These rows exist before the replication slot does. The initial snapshot copies them, so the target is not empty-handed when streaming starts. Their ids are fixed UUIDs in v7 format (for example `01900000-0000-7000-8000-000000000001`) so the foreign-key relationships between orders, customers and products are readable. |
| `GRANT SELECT ON ALL TABLES/SEQUENCES ...` | Debezium needs `SELECT` for the snapshot. These come **after** `CREATE TABLE` so they cover the tables. The sequence grant is unnecessary here but harmless. |
| `ALTER DEFAULT PRIVILEGES ...` | Grants `SELECT` on tables created in the future by the role that ran the script (`postgres`). |
| `CREATE PUBLICATION dbz_publication FOR ALL TABLES WITH (publish_via_partition_root = true)` | The publication says which changes are published to the logical replication stream. `FOR ALL TABLES` needs superuser, which is why it is created here and Debezium is told not to create it (`publication.autocreate.mode=disabled`). `publish_via_partition_root = true` (PostgreSQL 13+) publishes a partitioned table's changes under the **root table's name**. Without it, changes are published under the partition's name, which is not in `table.include.list`, so Debezium **silently drops them**. It does nothing for non-partitioned tables, so it is safe to keep. |

What is **not** in the file: the replication slot. Debezium creates `dbz_migration_slot` itself on
first start (visible in its log as `CREATE_REPLICATION_SLOT ... LOGICAL pgoutput`).

How the pieces fit:

```
wal_level=logical  -> Postgres can expose logical changes
REPLICATION role   -> Debezium may consume them
PUBLICATION        -> defines which tables' changes are published
SELECT grants      -> Debezium can read the initial data
replication slot   -> remembers Debezium's position in the WAL
```

### 5.3 `target-init/init.sql`

The same four tables plus `cutover_marker`, created empty with identical column names, types (UUID ids,
`BIGINT` for `cutover_marker`) and primary keys. **You own the target schema; Debezium only writes rows.** There are no foreign keys on
purpose, so the replication test stays focused. Add foreign keys, indexes and triggers separately
(ideally before cutover) once replication works.

### 5.4 `debezium/Dockerfile`

```dockerfile
FROM quay.io/debezium/server:3.7.0.Final
ADD --chmod=644 https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.13/postgresql-42.7.13.jar \
    /debezium/lib/postgresql-42.7.13.jar
```

* The image already contains the PostgreSQL **source** connector and the **JDBC sink**. The Debezium
  Server docs state it does not ship JDBC drivers for target databases, so the driver is added to `lib/`.
* `--chmod=644` is needed because `ADD <url>` otherwise creates a root-only file that the non-root
  Debezium user may not be able to read. The `# syntax=docker/dockerfile:1` first line enables this
  BuildKit feature.
* The startup log shows `lib/postgresql-42.7.13.jar` on the classpath once. If you ever see two
  different PostgreSQL driver jars, remove the `ADD` line.

### 5.5 `debezium/application.properties`

Debezium Server uses MicroProfile Config. Properties are grouped by prefix:

| Prefix | Meaning |
|---|---|
| `debezium.source.*` | Passed to the source connector with the prefix removed (`debezium.source.database.hostname` becomes `database.hostname`). |
| `debezium.sink.*` | Selects and configures the sink. `debezium.sink.jdbc.*` is passed to the JDBC sink connector. |
| `debezium.transforms*` | Kafka Connect single message transformations applied to every event before the sink. |
| `quarkus.*` | Settings for the Quarkus runtime Debezium Server runs on. |

**Source connection**

| Property | Value | Meaning |
|---|---|---|
| `debezium.source.connector.class` | `io.debezium.connector.postgresql.PostgresConnector` | Selects the PostgreSQL source connector. Each Debezium Server instance runs exactly one connector. |
| `database.hostname` / `database.port` | `source-postgres` / `5432` | The source, reached by service name on the Docker network. |
| `database.user` / `database.password` | `debezium` / `debezium` | The replication user from `source-init/init.sql`. |
| `database.dbname` | `migration` | The database to capture. |

**Logical decoding**

| Property | Value | Meaning |
|---|---|---|
| `plugin.name` | `pgoutput` | PostgreSQL's built-in logical decoding plugin (available since PostgreSQL 10), so nothing extra is installed. |
| `publication.name` | `dbz_publication` | The publication created in `init.sql`. |
| `publication.autocreate.mode` | `disabled` | Debezium must not create or alter publications (that needs superuser, which the `debezium` user lacks). |
| `slot.name` | `dbz_migration_slot` | Name of the replication slot Debezium creates on first start. The slot holds WAL until Debezium confirms it. **Drop it when you stop using it** (section 13). |
| `topic.prefix` | `migration` | Logical name of this source. Each event's "topic" label becomes `<prefix>.<schema>.<table>`, for example `migration.public.customers`. The transform below shortens it. |

**Capture scope and snapshot**

| Property | Value | Meaning |
|---|---|---|
| `schema.include.list` | `public` | Capture only the `public` schema. |
| `table.include.list` | `public.customers,public.products,public.orders,public.order_items,public.cutover_marker` | Exactly which tables to capture. Use **root** table names if any table is partitioned. Tables outside this list are ignored even though the publication is `FOR ALL TABLES`. |
| `snapshot.mode` | `initial` | On first start, copy all existing rows, then stream changes from the point the snapshot was taken (no gap). On later starts with stored offsets it resumes streaming instead. |

**Offset storage (replaces Kafka's offset topic)**

| Property | Value | Meaning |
|---|---|---|
| `offset.storage` | `org.apache.kafka.connect.storage.FileOffsetBackingStore` | Store progress in a local file. |
| `offset.storage.file.filename` | `/debezium/data/offsets.dat` | The file, on the `debezium-offsets` volume. The Server docs say the directory must already exist before startup. |
| `offset.flush.interval.ms` | `1000` | How often offsets are written to the file. |

**Topic-to-table-name transform (the fix for section 6)**

| Property | Value | Meaning |
|---|---|---|
| `debezium.transforms` | `route` | Declares one transformation named `route` (the name is arbitrary). |
| `debezium.transforms.route.type` | `org.apache.kafka.connect.transforms.RegexRouter` | A standard Kafka Connect transform that rewrites a record's topic using a regular expression. Debezium Server supports Kafka Connect transforms through `debezium.transforms`. |
| `debezium.transforms.route.regex` | `migration[.]public[.](.*)` | Matches topics starting with `migration.public.` and captures the rest (the table name). `[.]` is a literal dot, which avoids backslash escaping in a properties file. |
| `debezium.transforms.route.replacement` | `$1` | New topic = the captured table name, so `migration.public.order_items` becomes `order_items`. |

**Target JDBC sink**

| Property | Value | Meaning |
|---|---|---|
| `debezium.sink.type` | `jdbc` | Use the JDBC sink. |
| `debezium.sink.jdbc.connection.url` | `jdbc:postgresql://target-postgres:5432/migration?currentSchema=public` | The target database. `currentSchema=public` makes unqualified table names resolve to `public`. |
| `connection.username` / `connection.password` | `postgres` / `postgres` | Target credentials (demo values). |
| `insert.mode` | `upsert` | Insert a new row, or update it if the primary key exists. This makes replays and the snapshot idempotent. |
| `primary.key.mode` | `record_key` | Take the key columns from the change event's key. Each table's own primary key is used, including the composite one. |
| `primary.key.fields` | *(not set, on purpose)* | When omitted, **all** fields of the record key are used. Setting it to `id` would break the other three tables, which have different key columns. |
| `delete.enabled` | `true` | Turn delete events into `DELETE` statements. It requires `primary.key.mode=record_key`. |
| `schema.evolution` | `none` | The sink never creates or alters tables. If a table or column is missing, it fails loudly. This is what you want when you own the target schema. |
| `use.time.zone` | `UTC` | Time zone the sink uses when writing timestamp values, keeping `created_at` consistent. |

**Runtime**

| Property | Value | Meaning |
|---|---|---|
| `quarkus.log.console.json` | `false` | Human-readable console logs instead of JSON. |

**Deliberately absent:** `debezium.sink.jdbc.collection.name.format`. See the next section.

## 6. The table-name placeholder problem

By default the JDBC sink names the target table after the event's topic. To keep the original table
names (`customers`, not `migration_public_customers`), the sink's `collection.name.format` can use a
`${source.table}` placeholder. **In Debezium Server 3.7.0 this does not work reliably**, and this
cost several rounds of debugging:

| Setting in the file | What happened |
|---|---|
| `...collection.name.format=${source.table}` | Quarkus expands `${...}` itself and fails: no property named `source.table`. |
| `...=$${source.table}` (the documented escape) | Startup fails with `SRCFG00011: Could not expand value source.table in property quarkus.debezium.offset.storage.jdbc.collection.name.format`. |
| `...=$$$${source.table}` | Startup succeeds, but the sink writes to a table literally named **`$customers`** (`Could not find table: $customers`), and the engine stops. |
| `schema.evolution=validate-only` together with any `${source.*}` format | The JDBC docs say the connector fails during startup (it cannot validate table existence for dynamic names). |

**Why** (inferred from the logs, **not described in the official docs**): the Debezium Server docs
say a `$` prefix is escaped with a second `$`, and that works for a property read once. But the
startup log shows Debezium Server also copies every `debezium.sink.jdbc.*` property into
`offset.storage.jdbc.*` and `schema.history.internal.jdbc.*`, and those copies are expanded one more
time. The sink's own value needs two dollar signs, and the copies need four, so no single line
satisfies both. The two outcomes above match that.

**Fix:** keep every `${...}` out of the file and rename the topic with the `RegexRouter` transform
(section 5.5). The sink's default table name is the topic, so renaming the topic to `customers` gives
the table `customers`. The Debezium Server docs list `debezium.transforms` as the way to apply
Kafka Connect transformations.

Other options if you cannot use the transform: rename the target tables to
`migration_public_customers` and so on (the sink's default naming), or set one literal table name
(only works for a single table). A backslash escape (`\\${source.table}`) is not documented in the
Debezium Server or Quarkus pages checked here, so treat it as an experiment.

## 7. Checking the data

Set shortcuts first (re-run these in each new terminal):

```bash
S="docker exec -i source-postgres psql -U postgres -d migration -At"
T="docker exec -i target-postgres psql -U postgres -d migration -At"
```

**Tables present on each side** (the target must show `customers`, not `$customers`):

```bash
$S -c "\dt"
$T -c "\dt"
```

**Row counts side by side:**

```bash
for t in customers products orders order_items cutover_marker; do
  echo "$t  source=$($S -c "SELECT count(*) FROM $t")  target=$($T -c "SELECT count(*) FROM $t")"
done
```

**Checksums (hash of every row, so any differing value is detected):**

```bash
for spec in customers:id products:product_id orders:order_id order_items:order_id,product_id cutover_marker:id; do
  t=${spec%%:*}; pk=${spec##*:}
  q="SELECT md5(coalesce(string_agg(x::text, ',' ORDER BY $pk),'')) FROM $t x"
  a=$($S -c "$q"); b=$($T -c "$q")
  [ "$a" == "$b" ] && echo "OK    $t  $a" || echo "DIFF  $t  source=$a target=$b"
done
```

`OK` means identical. `DIFF` means something differs. An empty table is `OK` if both are empty, so
compare the counts too. If the target is still catching up, wait a few seconds and run it again.

**Show exactly which rows differ** (change the table and `ORDER BY` column):

```bash
$S -c "SELECT * FROM customers ORDER BY id" > /tmp/src.txt
$T -c "SELECT * FROM customers ORDER BY id" > /tmp/tgt.txt
diff /tmp/src.txt /tmp/tgt.txt && echo IDENTICAL
```

Lines starting with `<` exist only on the source, and `>` only on the target.

**Look at the data directly:**

```bash
$S -c "SELECT * FROM orders ORDER BY order_id"
$T -c "SELECT * FROM orders ORDER BY order_id"
```

**Live test: change the source and watch the target:**

```bash
ID='01900000-0000-7000-8000-000000000900'                      # ids are UUIDs
$S -c "INSERT INTO customers(id,name,email) VALUES ('$ID','LiveTest','live@example.com')"
sleep 3; $T -c "SELECT * FROM customers WHERE id='$ID'"        # row appears
$S -c "UPDATE customers SET email='changed@example.com' WHERE id='$ID'"
sleep 3; $T -c "SELECT email FROM customers WHERE id='$ID'"    # changed@example.com
$S -c "DELETE FROM customers WHERE id='$ID'"
sleep 3; $T -c "SELECT count(*) FROM customers WHERE id='$ID'" # 0
```

**Replication health:**

```bash
$S -c "SELECT slot_name, active FROM pg_replication_slots"     # dbz_migration_slot | t
$S -c "SELECT slot_name, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS lag FROM pg_replication_slots"
$S -c "SELECT pubname, puballtables, pubviaroot FROM pg_publication"   # pubviaroot = t
curl -s localhost:8080/q/health                                 # status UP
docker compose logs debezium | grep -E "ERROR|Engine has failed"   # should print nothing
```

## 8. Automated tests

```bash
./scripts/test.sh
```

> **Run it on a clean stack with the simulator stopped.** The test checks the exact seed row counts
> and compares whole-table fingerprints, so any other writer makes it fail. It checks this first
> (step 0) and aborts with a hint if the `simulator` container is running or the source holds more
> than the 2 seed customers. Start clean with `docker compose down -v && docker compose up -d --build`.
> The ids it uses are UUIDs in the same shape as the seed ids (`01900000-0000-7000-8000-0000000000NN`).

It compares an md5 fingerprint of every row of every table between source and target after each step
and ends with `RESULT: N passed, M failed`:

0. Pre-flight: simulator not running, source holds only the seed rows.
1. Initial snapshot (counts and content identical, id columns are `uuid` on the target).
2. Streaming inserts into all four tables.
3. Updates, including the composite-key table.
4. Deletes, including the composite-key table.
5. Primary-key change (delete plus insert).
6. Data fidelity: UUIDs, `NUMERIC(12,2)`, unicode, quotes, NULLs, microsecond timestamps.
7. Catch-up after Debezium is stopped and restarted (the slot retains WAL).
8. Container recreation keeps offsets (no re-snapshot).

## 9. Usage simulator (continuous test traffic)

`usage-simultaion/simulate.py` plays the part of application users. It inserts customers, products,
orders and order items into the **source** database all the time, so you can check that the pipeline
keeps up under load and see what a cutover looks like while the application is still writing.

### 9.1 What it writes

Each loop iteration is **one transaction** (one "user action"):

1. A customer: an existing one most of the time, a brand-new sign-up about 1 time in 4.
2. Sometimes (about 1 in 7) a new product is added to the catalogue.
3. An order for that customer with **1 to 4 distinct products** (the composite key
   `(order_id, product_id)` allows each product once per order). Each item's `price` is the product's
   price, and `orders.total` is exactly the sum of `quantity * price` of its items.
4. The order's `status` is one of `NEW, PENDING, PAID, SHIPPED, DELIVERED, CANCELLED`.

Rules it follows, because the schema has no foreign keys to enforce them:

* Orders only reference customers that exist, and items only reference orders and products that exist.
* New ids become referenceable only **after the transaction commits**, so a rolled-back transaction
  never leaves a dangling reference.
* At start-up it loads the newest existing customers and products from the database (including the
  seed rows), so it also works on a database that already has data.
* It only **inserts**. It does not update or delete.

All ids come from `uuid.uuid7()` (built into Python 3.14): a 48-bit millisecond timestamp followed by
random/counter bits, so ids sort in creation order. On an older Python the script falls back to an
equivalent RFC 9562 implementation; the Docker image uses 3.14 and never needs the fallback.

### 9.2 Running it

The `simulator` service is in the `sim` profile, so it does not start with the rest of the stack:

```bash
docker compose --profile sim up -d --build simulator   # start
docker compose logs -f simulator                       # watch
docker compose stop simulator                          # stop the traffic
```

To run it from the host instead of Docker (Python 3.14 recommended):

```bash
cd usage-simultaion
pip install -r requirements.txt
PGHOST=localhost PGPORT=5433 PGUSER=postgres PGPASSWORD=postgres PGDATABASE=migration \
  python simulate.py --rate 10 --duration 60
```

Typical log lines:

```
inserting ~5.0 txn/s into source-postgres:5432/migration  (Ctrl+C to stop)
loaded 2 customers and 2 products from the database
ok=300 failed=0 rows_written=1180 state=ok            # printed every 10 seconds
WARN  write rejected, source is read-only             # during a cutover freeze
writes are working again
```

### 9.3 Environment variables

Set on the `simulator` service in `docker-compose.yml`. The first five are the standard PostgreSQL
(libpq) variables, which `psycopg` reads directly.

| Variable | Default in compose | Meaning |
|---|---|---|
| `PGHOST` | `source-postgres` | Host to connect to. Inside the Compose network use the **service name**. `localhost` would be the simulator container itself. From the host use `localhost`. |
| `PGPORT` | `5432` | Port **inside** the Docker network. The published `5433` only exists on the host, so use `5433` only when running the script on the host. |
| `PGUSER` | `postgres` | Login role. It must be allowed to `INSERT`. **Do not use `debezium`:** that role only has `SELECT` and replication rights, so every write fails. |
| `PGPASSWORD` | `postgres` | Password for that role (demo value; use a secret in real use). |
| `PGDATABASE` | `migration` | Database to write into. |
| `SIM_RATE` | `5` | Transactions per second. One transaction is about 3 to 6 rows, so `5` is roughly 20 rows/s. If the database cannot keep up the script does not burst to catch up. |
| `SIM_DURATION` | `0` | Seconds to run before exiting. `0` runs until stopped. |
| `SOURCE_DSN` | *(not set)* | Optional full connection string, for example `postgresql://user:pass@host:5432/db`. When set, it is used instead of the `PG*` variables. |

Command-line flags override the matching variables: `--rate`, `--duration`, `--dsn`.

To keep generating traffic **after** a cutover, set `PGHOST: target-postgres` and recreate the service
(`docker compose --profile sim up -d simulator`).

### 9.4 What happens when a write fails

The simulator is meant to run through a cutover, so a failed write never stops it:

* It prints one `WARN` line, drops the database connection, and retries on the next tick with a fresh
  connection. A session opened while the database was read-only would otherwise **stay read-only**
  even after the database became writable again.
* Identical warnings are collapsed to one line every 5 seconds (`[+N similar suppressed]`).
* The cases covered are: source set read-only (`default_transaction_read_only = on`), sessions
  terminated by `pg_terminate_backend`, the database restarting or being down, and connect timeouts.
* When writes succeed again it prints `writes are working again`.

The container also has `restart: unless-stopped`, which only matters if the process itself dies.

### 9.5 Checking it end to end

Compare counts and content on both sides (stop the simulator first, or accept a short lag):

```bash
for spec in customers:id products:product_id orders:order_id order_items:order_id,product_id; do
  t=${spec%%:*}; pk=${spec##*:}
  q="SELECT count(*)||':'||md5(coalesce(string_agg(x::text, ',' ORDER BY $pk),'')) FROM $t x"
  a=$($S -c "$q"); b=$($T -c "$q")
  [ "$a" == "$b" ] && echo "OK    $t  $a" || echo "DIFF  $t  source=$a target=$b"
done
```

Check referential integrity of what the simulator wrote (all four counts should be `0`):

```bash
$S -c "SELECT count(*) FROM orders o LEFT JOIN customers c ON c.id=o.customer_id WHERE c.id IS NULL"
$S -c "SELECT count(*) FROM order_items i LEFT JOIN orders o USING(order_id) WHERE o.order_id IS NULL"
$S -c "SELECT count(*) FROM order_items i LEFT JOIN products p USING(product_id) WHERE p.product_id IS NULL"
$S -c "SELECT count(*) FROM orders o WHERE total <> (SELECT sum(quantity*price) FROM order_items i WHERE i.order_id=o.order_id)"
```

Watch replication lag while it runs (section 7, "Replication health").

### 9.6 Example run (Codespaces)

Output captured from a real run with the simulator writing to the source. `$S` and `$T` are the
shortcuts from section 7. The commands are the row-count loop and the checksum loop from section 7.

**1. Row counts while the simulator is still running.** The target is slightly behind, which is
normal: the source is queried first, then the target, and Debezium needs a moment to apply the newest
transactions.

```
$ for t in customers products orders order_items cutover_marker; do   echo "$t  source=$($S -c "SELECT count(*) FROM $t")  target=$($T -c "SELECT count(*) FROM $t")"; done
customers  source=797  target=797
products  source=477  target=477
orders  source=3007  target=3005
order_items  source=7517  target=7508
cutover_marker  source=0  target=0
```

Here `orders` is 2 rows behind and `order_items` 9 rows behind. A gap like this that closes on its own
is lag, not data loss.

**2. Checksums once the target had caught up.** Every table hashes identically, so every row
(including all UUID values, `NUMERIC` amounts and timestamps) is the same on both sides.
`d41d8cd98f00b204e9800998ecf8427e` is the md5 of an empty string, which is what you expect for the
empty `cutover_marker`.

```
$ for spec in customers:id products:product_id orders:order_id order_items:order_id,product_id cutover_marker:id; do   t=${spec%%:*}; pk=${spec##*:};   q="SELECT md5(coalesce(string_agg(x::text, ',' ORDER BY $pk),'')) FROM $t x";   a=$($S -c "$q"); b=$($T -c "$q");   [ "$a" == "$b" ] && echo "OK    $t  $a" || echo "DIFF  $t  source=$a target=$b"; done
OK    customers  e36e38246fd3bc2026ec48fdad7cff37
OK    products  9dfe6ef02c7e6c70b483014b5acb0314
OK    orders  4f27f300e2bf2d80a2e3ee9ed891bee4
OK    order_items  df02f0c6c08642f5e79d715f47f78030
OK    cutover_marker  d41d8cd98f00b204e9800998ecf8427e
```

**3. Row counts just after the simulator was stopped.** With no new writes, the counts are identical
on both sides (977 customers, 598 products, 3774 orders, 9434 order items).

```
$ for t in customers products orders order_items cutover_marker; do   echo "$t  source=$($S -c "SELECT count(*) FROM $t")  target=$($T -c "SELECT count(*) FROM $t")"; done
customers  source=977  target=977
products  source=598  target=598
orders  source=3774  target=3774
order_items  source=9434  target=9434
cutover_marker  source=0  target=0
```

How to read it: compare checksums only when the target has caught up. If a count differs while the
simulator is running, wait a few seconds (or stop the simulator) and run the check again. Only a
difference that **stays** after the target has caught up indicates a problem.

### 9.7 Cautions

* The source and target schemas must use the UUID columns from `init.sql`. A volume created with the
  old `BIGINT` schema fails with `invalid input syntax for type bigint` (section 11). Run
  `docker compose down -v` once.
* Rehearsing a cutover **with the simulator running** shows how the application side behaves:
  `cutover.sh` freezes the source and terminates sessions, and the simulator keeps running and logs
  `write rejected, source is read-only`. The cutover completes under load because the script freezes
  the source before it writes the sentinel (section 10). Stop the simulator before `./scripts/test.sh`,
  though: that test needs a quiet source.
* Do not point the simulator at the **target** while replication is running; that creates rows
  Debezium does not know about and the two databases drift apart.
* Rows written by the simulator are real rows. `docker compose down -v` removes them.

## 10. Cutover

Once the databases are in sync, cutover closes the small remaining gap. `scripts/cutover.sh` is a
rehearsal of the procedure. **It is destructive for the rehearsal** (it freezes the source and drops
the slot); run `docker compose down -v` and start again to repeat.

```bash
./scripts/cutover.sh --yes      # without --yes it asks you to confirm writes are stopped
```

What it does:

1. Pre-flight: slot is active, prints lag.
2. You confirm all application writes to the source are stopped (the one manual step).
3. Freezes the source (`default_transaction_read_only = on`) and terminates other sessions, so no
   application session can write any more.
4. Inserts a **sentinel row** into `cutover_marker` through an admin session (the freeze blocks normal
   ones). The change stream is ordered, so when that row appears on the target, every change committed
   before it has arrived. Because the freeze comes first, nothing can be committed after the sentinel.
5. Waits for the sentinel on the target.
6. Compares counts and content hashes of every table; aborts if they differ.
7. Syncs sequences (logical replication does **not** replicate sequence values).
8. Stops Debezium, drops the replication slot (an orphaned slot makes the source keep WAL until the
   disk fills), and drops the publication.
9. Confirms the target accepts writes (inside a rolled-back transaction).

`cutover_marker.id` is `BIGINT` (the script writes `date +%s`); all other ids are UUIDs, and the hash
comparison orders by them directly.

**With the usage simulator running (section 9)**, the freeze shows up as `WARN write rejected, source
is read-only` in `docker compose logs simulator`, and the cutover still completes: the freeze happens
before the sentinel, so the sentinel arriving on the target means every row the simulator managed to
commit is already there. This is a good rehearsal of cutting over under load. The script still asks
whether application writes are stopped; a real cutover should stop the application first so users see
a clean switch rather than write errors.

Then, manually: point the application at the target, run smoke tests, keep the source read-only for
several days as the rollback path. There is no reverse replication, so writes made on the target are
not copied back.

**Before a real cutover**, check what replication does not carry: DDL changes (freeze schema changes
for the whole migration, because `schema.evolution=none`), foreign keys, triggers (keep them disabled
while Debezium writes), extra indexes, roles and grants, views, functions, extensions. Run `ANALYZE`
on the target before sending traffic to it.

**Rollback before the app has written to the target:**

```bash
docker exec -e PGOPTIONS='-c default_transaction_read_only=off' source-postgres \
  psql -U postgres -d migration -c 'ALTER DATABASE migration RESET default_transaction_read_only;'
```

(The override is needed because the freeze also blocks the `ALTER` itself.)

## 11. Troubleshooting

| Symptom (in `docker compose logs debezium` or from a command) | Cause | Fix |
|---|---|---|
| `Connect timed out`, `Unable to determine Dialect without JDBC metadata`, `pg_isready ... no response` between containers | Codespaces bridge traffic dropped (section 4). The Hibernate error is only a side effect. | `sudo sysctl -w net.bridge.bridge-nf-call-iptables=0`, then restart Debezium. |
| `SRCFG00011: Could not expand value source.table` | `$${source.table}` in the properties (section 6). | Remove `collection.name.format`; use the `RegexRouter` transform. |
| `Table '$customers' ...` or `Could not find table: $customers` | `$$$${source.table}` in the properties (section 6). | Same as above. |
| Startup fails with `validate-only` and a `${source.*}` format | JDBC docs: validation is impossible with dynamic names. | Use `schema.evolution=none`. |
| `Engine has failed` or `Engine state ... STOPPING`, yet `docker ps` shows `Up` | The engine stopped but the container did not. | Read the logs; fix the first `ERROR`; reset (section 13). |
| Target tables empty after startup | The sink is failing before writing anything (the snapshot reads fine, the writes fail). | Find the first `WARN` or `ERROR` in the logs. |
| `Permission denied` on `/debezium/data/offsets.dat` | Offsets volume not writable by the container user. | Add `user: "0"` to the `debezium` service (test only), or fix ownership of the volume. |
| `No suitable driver` or `ClassNotFound org.postgresql.Driver` | Driver jar missing or unreadable. | Check `docker exec debezium ls -l /debezium/lib \| grep -i postgres`; keep `--chmod=644` in the Dockerfile. |
| Two different `postgresql-*.jar` files in `/debezium/lib` | The image already ships one. | Remove the `ADD` line from the Dockerfile and rebuild. |
| `NoClassDefFoundError` mentioning transform or SMT classes | A known issue reported against the 3.7.0 image (debezium/dbz#2740). Not yet confirmed in this project. | Change the Dockerfile to `FROM quay.io/debezium/server:3.6.3.Final` and rebuild. Not confirmed that 3.6.3 has the JDBC sink. |
| Errors about key or value schema, `SinkRecord`, or primary key | The Server's JDBC sink may need events that carry a schema. | Try `debezium.format.key=json` and `debezium.format.value=json`, and compare with the JDBC sink section of the Debezium Server docs. Not yet needed in this project. |
| Rows from partitioned tables never arrive | Publication missing `publish_via_partition_root = true`, or `table.include.list` names partitions instead of the root. | `ALTER PUBLICATION dbz_publication SET (publish_via_partition_root = true);` and restart Debezium. |
| Init script changes have no effect | Init scripts only run on an empty volume. | `docker compose down -v` then `up -d --build`. |
| Slot exists but offsets are gone (or the reverse), or a failed run left state behind | Source slot and offsets file are out of step. | Reset both (section 13). |
| Source disk grows | An unused replication slot retains WAL. | Drop the slot: `SELECT pg_drop_replication_slot('dbz_migration_slot');`. |
| Simulator logs `WARN write rejected, source is read-only` | Expected during a cutover freeze. Otherwise the database still has `default_transaction_read_only = on`. | Roll back with the command in section 10. The simulator recovers by itself. |
| Simulator logs `permission denied for table ...` | `PGUSER` is the `debezium` role, which cannot insert. | Use `postgres` (or another role with `INSERT`). |
| Simulator logs `invalid input syntax for type bigint` or `column "id" is of type bigint` | The volume was created with the old BIGINT schema. | `docker compose down -v`, then `up -d --build`. |
| Simulator logs `connection failed ... Name or service not known` | `PGHOST` is wrong for where it runs. | In Compose use `source-postgres`; on the host use `localhost` with `PGPORT=5433`. |
| `pip install` / image build fails for `psycopg-binary` | No wheel for your Python version. | Use `psycopg[binary]>=3.2.10` on Python 3.14, or build the image from a Python version that has a wheel. |
| Rows appear on the source but not on the target, and Debezium logs a UUID or type error | The sink did not accept the `uuid` column values. The normal case works (section 9.6), so this points to a column type mismatch on the target. | Send the first `ERROR` from `docker compose logs debezium`; check that the target columns are `UUID`. |

## 12. Verification status

Be aware of what has and has not been confirmed.

**Confirmed in a Codespace during development:**

* The source and target start healthy; the init scripts run.
* The Codespaces networking cause and the `sysctl` fix (a two-container test went from a timeout to
  `Connection refused` on a closed port, which is what a working network does).
* Debezium connects to the source, creates the slot, and completes the initial snapshot
  (2 customers, 2 products, 2 orders, 3 order items, 0 sentinel rows).
* The `${source.table}` failures in section 6, with the exact error messages shown there.
* The PostgreSQL JDBC driver loads (`postgresql-42.7.13.jar` is on the classpath).

**Confirmed in a Codespace (UUID schema, simulator and Debezium together):**

* With the simulator writing continuously to the source, Debezium replicated the data to the target.
  After the simulator stopped, row counts matched on both sides (977 customers, 598 products, 3774
  orders, 9434 order items) and the per-table md5 checksums were identical (section 9.6). This also
  shows Debezium's JDBC sink writes the `UUID` columns correctly.
* While the simulator was running, the target trailed the source by a few rows and then caught up.
* The sink writes into tables named after the source tables (`customers`, `orders`, ...), so the
  `RegexRouter` transform works on the 3.7.0 image.

**Confirmed outside Docker (the simulator and the UUID schema, on a local PostgreSQL 16):**

* `source-init/init.sql` loads cleanly with the UUID columns.
* `simulate.py` ran with integrity checks passing: no orders without a customer, no items without an
  order or product, no orders without items, and `orders.total` equal to the sum of its items.
* All generated ids are valid UUIDv7 and sort in creation order.
* With the database set read-only, its sessions terminated, and the server stopped and restarted, the
  simulator only logged warnings, never exited, and logged `writes are working again` afterwards.
* The settings come from the standard `PG*` variables.

**Not yet confirmed:**

* The `simulator` image building on `python:3.14-slim`, and `uuid.uuid7()` on 3.14 specifically (the
  local test ran on 3.13 with the built-in fallback, and the Codespace run in section 9.6 does not
  record how the simulator was started).
* `scripts/test.sh` and `scripts/cutover.sh` against the real Debezium stack. Both were updated for the
  UUID schema and run against two local PostgreSQL 16 servers with PostgreSQL's built-in logical
  replication standing in for Debezium (test.sh: 25 of 25 checks passed; cutover.sh completed with
  the simulator writing 50 to 200 transactions per second). Run them in Codespaces to confirm.
* The untested fallbacks in section 4 (the `iptables-legacy` rule and `postStartCommand`).

If a step fails, send the first `ERROR` or `WARN` line from `docker compose logs debezium`.

## 13. Reset and cleanup

**Reset only Debezium's state** (keeps both databases and their data). Use this after a failed run.
Stop the simulator first (`docker compose stop simulator`) so the source does not keep changing:

```bash
docker compose stop debezium
docker exec -i source-postgres psql -U postgres -d migration -At \
  -c "SELECT pg_drop_replication_slot('dbz_migration_slot')"
docker compose rm -sf debezium
docker volume rm debezium-pg-migration_debezium-offsets   # find the name with: docker volume ls | grep offsets
docker compose up -d debezium
```

**Everything, including data and volumes** (also required after any change to the `init.sql` files, for
example the move from `BIGINT` to `UUID` ids):

```bash
docker compose --profile sim down -v
```

**If you stop Debezium for good but keep the source database**, always drop the slot, or the source
retains WAL indefinitely:

```sql
SELECT pg_drop_replication_slot('dbz_migration_slot');
```

## 14. References

* Debezium JDBC sink: <https://debezium.io/documentation/reference/stable/connectors/jdbc.html>
* Debezium Server (configuration, escaping, transformations, JDBC sink): <https://debezium.io/documentation/reference/stable/operations/debezium-server.html>
* Debezium PostgreSQL connector: <https://debezium.io/documentation/reference/stable/connectors/postgresql.html>
* Debezium topic routing: <https://debezium.io/documentation/reference/stable/transformations/topic-routing.html>
* Quarkus configuration reference (property expressions): <https://quarkus.io/guides/config-reference>