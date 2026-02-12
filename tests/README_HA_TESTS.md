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

### Run All HA Tests with H2 database

```bash
python3.9 -m pytest tests/test_ha.py -v
```

### Run All HA Tests with PostgreSQL database

The test runs with PostgreSQL by default. If you use the docker-compose setup, just run:

```bash
python3.9 -m pytest tests/test_ha.py -v
```

### Run All HA Tests with H2 database

To run tests with H2 database instead of PostgreSQL, set the following environment variable:

```bash
DROOLS_HA_DB_TYPE=h2 DROOLS_HA_H2_FILE=./eda_ha python3.9 -m pytest tests/test_ha.py -v
````

After the tests, you may delete the `./eda_ha.mv.db` file if desired.

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

### Backward Compatibility

The `_dispatch` method in `Ruleset` handles both:
- **New HA format**: With "name", "events", and "matching_uuid" at root level
- **Legacy format**: Direct rule name to data mapping (for non-HA mode)
