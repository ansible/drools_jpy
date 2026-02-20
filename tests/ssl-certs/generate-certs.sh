#!/bin/bash
# Generate SSL certificates for PostgreSQL mTLS testing.
#
# Produces:
#   ca.crt / ca.key          — self-signed CA
#   server.crt / server.key  — server cert (CN=localhost, SAN=localhost+127.0.0.1)
#   client.crt / client.key  — client cert (CN=eda_user, encrypted PKCS#8 PEM)
#
# Client key passphrase: testpassphrase

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PASSPHRASE="testpassphrase"

echo "Generating SSL certificates in $SCRIPT_DIR ..."

# --- CA ---
openssl req -new -x509 -days 365 -nodes \
    -keyout "$SCRIPT_DIR/ca.key" \
    -out "$SCRIPT_DIR/ca.crt" \
    -subj "/CN=Test CA"

# --- Server (CN=localhost, SAN=DNS:localhost,IP:127.0.0.1) ---
openssl req -new -nodes \
    -keyout "$SCRIPT_DIR/server.key" \
    -out "$SCRIPT_DIR/server.csr" \
    -subj "/CN=localhost"

openssl x509 -req -days 365 \
    -in "$SCRIPT_DIR/server.csr" \
    -CA "$SCRIPT_DIR/ca.crt" -CAkey "$SCRIPT_DIR/ca.key" -CAcreateserial \
    -out "$SCRIPT_DIR/server.crt" \
    -extfile <(printf "subjectAltName=DNS:localhost,IP:127.0.0.1")

# --- Client (CN=eda_user — must match the PostgreSQL username) ---
openssl req -new -nodes \
    -keyout "$SCRIPT_DIR/client-raw.key" \
    -out "$SCRIPT_DIR/client.csr" \
    -subj "/CN=eda_user"

openssl x509 -req -days 365 \
    -in "$SCRIPT_DIR/client.csr" \
    -CA "$SCRIPT_DIR/ca.crt" -CAkey "$SCRIPT_DIR/ca.key" -CAcreateserial \
    -out "$SCRIPT_DIR/client.crt"

# Convert client key to encrypted PKCS#8 PEM (BEGIN ENCRYPTED PRIVATE KEY)
openssl pkcs8 -topk8 \
    -in "$SCRIPT_DIR/client-raw.key" \
    -out "$SCRIPT_DIR/client.key" \
    -v2 aes-256-cbc \
    -passout "pass:$PASSPHRASE"

# --- Clean up temporary files ---
rm -f "$SCRIPT_DIR/server.csr" "$SCRIPT_DIR/client.csr" \
      "$SCRIPT_DIR/client-raw.key" "$SCRIPT_DIR/ca.srl"

# NOTE: Do NOT chmod 600 server.key here — the container needs to read it
# via bind mount. The pg-ssl-init.sh script copies it into PGDATA and sets
# 600 there.

echo "Done."
echo "  CA cert:     $SCRIPT_DIR/ca.crt"
echo "  Server cert: $SCRIPT_DIR/server.crt"
echo "  Server key:  $SCRIPT_DIR/server.key"
echo "  Client cert: $SCRIPT_DIR/client.crt"
echo "  Client key:  $SCRIPT_DIR/client.key  (passphrase: $PASSPHRASE)"
