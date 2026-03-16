# HA Tests Setup

This document describes how to set up and run High Availability (HA) tests for drools_jpy.

## Prerequisites

1. **PostgreSQL database** running and accessible
2. **Database schema** created (automatically handled by the Drools jar on first initialization)
3. **Python 3.9+** with virtual environment activated

## Environment Variables

Set these environment variables before running HA tests. But if using docker-compose.yaml, it should work with default values.

```bash
export POSTGRES_HOST=localhost
export POSTGRES_PORT=5432
export POSTGRES_DB=eda_ha_db
export POSTGRES_USER=eda_user
export POSTGRES_PASSWORD=eda_password
```

## Setting Up PostgreSQL

### Option 1: Docker Compose (Recommended)

The repository includes a `docker-compose.yaml` file with PostgreSQL configured. Simply run:

```bash
docker-compose up -d
```

The default configuration uses:
- **Database**: eda_ha_db
- **User**: eda_user
- **Password**: eda_password
- **Port**: 5432

Stop PostgreSQL:
```bash
docker-compose down
```

Stop and remove data:
```bash
docker-compose down -v
```

## Running HA Tests

### Activate Virtual Environment

```bash
source venv/bin/activate
```

### Run All HA Tests with H2 database (default)

The test runs with H2 file-backed database by default. No external database needed:

```bash
python3.9 -m pytest tests/test_ha.py -v
```

DB files are created under `target` directory. They should be automatically cleaned up after each test.

### Run All HA Tests with PostgreSQL database

To run tests with PostgreSQL instead of H2, start the database and set the environment variable:

```bash
docker-compose up -d
DROOLS_HA_DB_TYPE=postgres python3.9 -m pytest tests/test_ha.py -v
```

### Run Specific Test

```bash
# Test HA initialization and leader lifecycle
python3.9 -m pytest tests/test_ha.py::test_ha_initialization_and_leader_lifecycle -v

# Test ActionInfo operations
python3.9 -m pytest tests/test_ha.py::test_action_info_lifecycle -v

# Test HA statistics
python3.9 -m pytest tests/test_ha.py::test_ha_stats -v

# Test multiple actions
python3.9 -m pytest tests/test_ha.py::test_multiple_action_infos -v

# Test failover scenario
python3.9 -m pytest tests/test_ha.py::test_ha_failover_scenario -v
```

### Run with Debug Logging

To see both Python and Drools jar debug logs:

```bash
python3.9 -m pytest tests/test_ha.py -s --log-cli-level=DEBUG
```

## Test Coverage

The test suite includes:

1. **test_ha_initialization_and_leader_lifecycle**
   - Tests the basic HA workflow: initialize HA → create ruleset → enable leader → assert event
   - Captures the Matches object and extracts matching_uuid directly from `matches.matching_uuid`
   - Tests ActionInfo operations using the real matching_uuid

2. **test_action_info_lifecycle**
   - Asserts an event to generate a real matching event
   - Extracts matching_uuid directly from the Matches object
   - Tests CRUD operations for ActionInfo (add, check exists, get, delete)
   - Asserts that matching_uuid is not None in HA mode

3. **test_ha_stats**
   - Tests getting HA statistics before and after enabling leader mode

4. **test_multiple_action_infos**
   - Asserts an event to get a real matching_uuid from the Matches object
   - Tests managing multiple actions (3 actions) for the same matching event

5. **test_ha_failover_scenario** 
   - Tests failover scenario where one leader goes down and another takes over
   - Consumes pending matching event info after failover via async channel

## HA SSL Tests (PostgreSQL with SSL/mTLS)

The `test_ha_ssl.py` tests verify HA functionality over an SSL-encrypted PostgreSQL connection with mutual TLS (client certificate authentication).

### Setup

#### 1. Generate SSL certificates

```bash
tests/ssl-certs/generate-certs.sh
```

This creates the following files under `tests/ssl-certs/`:

| File | Description |
|---|---|
| `ca.crt` / `ca.key` | Self-signed CA |
| `server.crt` / `server.key` | Server certificate (CN=localhost, SAN=localhost+127.0.0.1) |
| `client.crt` / `client.key` | Client certificate (CN=eda_user, encrypted PKCS#8 PEM) |

The client key passphrase is `testpassphrase`. The client certificate CN (`eda_user`) must match the PostgreSQL username for `cert` authentication.

#### 2. Start SSL PostgreSQL (port 5433)

```bash
docker-compose -f docker-compose-ssl.yaml up -d
```

This runs on port **5433** to avoid conflict with the non-SSL instance on 5432. The container is configured with:
- SSL enabled with server certificates
- `pg_hba.conf`: `hostssl` with `cert` auth for `eda_user` (requires client certificate)

> **Note:** The init script only runs on first startup. If you need to re-initialize, tear down with `-v` first (see below).

#### 3. Run SSL tests

```bash
pytest tests/test_ha_ssl.py -v
```

No environment variables needed — defaults point to `tests/ssl-certs/` and port 5433.

#### Tear down

```bash
docker-compose -f docker-compose-ssl.yaml down -v
```

The `-v` flag removes the data volume so the init script re-runs on next `up`.

### SSL Environment Variables

All have sensible defaults for the docker-compose-ssl setup. Override if connecting to a different SSL PostgreSQL instance:

| Variable | Default |
|---|---|
| `POSTGRES_HOST` | `localhost` |
| `POSTGRES_PORT` | `5433` |
| `POSTGRES_DB` | `eda_ha_db` |
| `POSTGRES_USER` | `eda_user` |
| `POSTGRES_PASSWORD` | `eda_password` |
| `POSTGRES_SSLMODE` | `verify-full` |
| `POSTGRES_SSLROOTCERT` | `tests/ssl-certs/ca.crt` |
| `POSTGRES_SSLCERT` | `tests/ssl-certs/client.crt` |
| `POSTGRES_SSLKEY` | `tests/ssl-certs/client.key` |
| `POSTGRES_SSLPASSWORD` | `testpassphrase` |

### Backward Compatibility

The `_dispatch` method in `Ruleset` handles both:
- **New HA format**: With "name", "events", and "matching_uuid" at root level
- **Legacy format**: Direct rule name to data mapping (for non-HA mode)
