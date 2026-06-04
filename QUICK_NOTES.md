See tests/README_HA_TESTS.md for more details.

## How to run with H2 (file-backed, default)

1. Build drools_jpy (`source venv/bin/activate`, `python3.9 -m pip install .`)
2. `python3.9 -m pytest tests/test_ha.py -s --log-cli-level=DEBUG` -> All tests should pass.
3. DB files are created under `target` directory. They should be automatically cleaned up after each test.

## How to run with PostgreSQL

1. Build drools_jpy (`source venv/bin/activate`, `python3.9 -m pip install .`)
2. In another terminal, `docker-compose up -d`
3. Wait 5 seconds for PostgreSQL to initialize.
4. `DROOLS_HA_DB_TYPE=postgres POSTGRES_PASSWORD=eda_password python3.9 -m pytest tests/test_ha.py -s --log-cli-level=DEBUG` -> All tests should pass.
5. `docker-compose down -v` to stop and remove data.

To access the PostgreSQL database directly (e.g., for debugging), you can connect with:
```bash
docker exec -it eda-ha-postgres psql -U eda_user -d eda_ha_db
```

## How to run with PostgreSQL SSL (mTLS)

1. Build drools_jpy (`source venv/bin/activate`, `python3.9 -m pip install .`)
2. `tests/ssl-certs/generate-certs.sh` (one-time cert generation)
3. `docker-compose -f docker-compose-ssl.yaml up -d` (port 5433)
4. Wait 10 seconds for PostgreSQL to initialize with SSL.
5. `python3.9 -m pytest tests/test_ha_ssl.py -s --log-cli-level=DEBUG`
6. `docker-compose -f docker-compose-ssl.yaml down -v`