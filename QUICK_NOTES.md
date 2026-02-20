See tests/README_HA_TESTS.md for more details.

## How to run with PostgreSQL (default)

1. Build drools_jpy (`source venv/bin/activate`, `python3.9 -m pip install .`)
2. In another terminal, `docker-compose up -d`
3. Wait 5 seconds for PostgreSQL to initialize.
4`python3.9 -m pytest tests/test_ha.py -s --log-cli-level=DEBUG` -> All tests should pass.
5`docker-compose down -v` to stop and remove data.

## How to run with H2 (file-backed)

1. Build drools_jpy (`source venv/bin/activate`, `python3.9 -m pip install .`)
2. `rm ./eda_ha.mv.db; DROOLS_HA_DB_TYPE=h2 DROOLS_HA_H2_FILE=./eda_ha python3.9 -m pytest tests/test_ha.py -s --log-cli-level=DEBUG`
3. You may delete the db file afterward if desired. `rm ./eda_ha.mv.db`

## How to run with PostgreSQL SSL (mTLS)

1. Build drools_jpy (`source venv/bin/activate`, `python3.9 -m pip install .`)
2. `tests/ssl-certs/generate-certs.sh` (one-time cert generation)
3. `docker-compose -f docker-compose-ssl.yaml up -d` (port 5433)
4. Wait 10 seconds for PostgreSQL to initialize with SSL.
5. `python3.9 -m pytest tests/test_ha_ssl.py -s --log-cli-level=DEBUG`
6. `docker-compose -f docker-compose-ssl.yaml down -v`