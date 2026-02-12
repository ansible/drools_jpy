See tests/README_HA_TESTS.md for more details.

## How to run with PostgreSQL (default)

1. Build drools_jpy (`source venv/bin/activate`, `python3.9 -m pip install .`)
2. In another terminal, `docker-compose up -d`
3. `python3.9 -m pytest tests/test_ha.py -s --log-cli-level=DEBUG` -> All tests should pass.
4. `docker-compose down -v` to stop and remove data.

## How to run with H2 (file-backed)

1. Build drools_jpy (`source venv/bin/activate`, `python3.9 -m pip install .`)
2. `DROOLS_HA_DB_TYPE=h2 DROOLS_HA_H2_FILE=./eda_ha python3.9 -m pytest tests/test_ha.py -s --log-cli-level=DEBUG`
3. You may delete the `./eda_ha.mv.db` file afterward if desired.