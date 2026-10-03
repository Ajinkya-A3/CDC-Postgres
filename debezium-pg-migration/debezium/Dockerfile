
FROM quay.io/debezium/server:3.7.0.Final

# JDBC sink implementation is already inside Debezium Server.
# Add the PostgreSQL JDBC driver for the target database.
# --chmod=644: ADD <url> otherwise creates a root-only file the non-root
# Debezium user may not be able to read.
ADD --chmod=644 \
  https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.13/postgresql-42.7.13.jar \
  /debezium/lib/postgresql-42.7.13.jar