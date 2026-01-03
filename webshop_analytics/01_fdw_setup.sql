-- FDW SETUP
CREATE EXTENSION IF NOT EXISTS postgres_fdw;

CREATE SERVER webshop_server
    FOREIGN DATA WRAPPER postgres_fdw
    OPTIONS (
        host 'app-postgres',
        port '5432',
        dbname 'web_shop'
    );