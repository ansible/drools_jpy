#!/bin/bash
# PostgreSQL entrypoint init script — enables SSL and configures
# client-certificate authentication for the eda_user role.
#
# Mounted into /docker-entrypoint-initdb.d/ by docker-compose-ssl.yaml.
set -e

# Copy certs into PGDATA so PostgreSQL can read them
cp /tmp/ssl/server.key "$PGDATA/server.key"
cp /tmp/ssl/server.crt "$PGDATA/server.crt"
cp /tmp/ssl/ca.crt     "$PGDATA/ca.crt"

chmod 600 "$PGDATA/server.key"
chown postgres:postgres "$PGDATA/server.key" "$PGDATA/server.crt" "$PGDATA/ca.crt"

# Enable SSL in postgresql.conf
cat >> "$PGDATA/postgresql.conf" <<EOF
ssl = on
ssl_cert_file = 'server.crt'
ssl_key_file  = 'server.key'
ssl_ca_file   = 'ca.crt'
EOF

# pg_hba.conf:
#   - local (unix socket): trust  (for init scripts / healthcheck)
#   - hostssl eda_user:    cert   (client certificate auth over SSL)
#   - host    any user:    scram  (password fallback for non-SSL)
cat > "$PGDATA/pg_hba.conf" <<'EOF'
local   all   all                     trust
hostssl all   eda_user   0.0.0.0/0    cert
hostssl all   eda_user   ::/0         cert
host    all   all        0.0.0.0/0    scram-sha-256
host    all   all        ::/0         scram-sha-256
EOF

# No pg_ctl reload needed — the docker entrypoint restarts PostgreSQL
# after all init scripts finish, picking up these config changes.
