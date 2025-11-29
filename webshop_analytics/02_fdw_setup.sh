#!/bin/bash
set -e

# A variável de ambiente WEB_SHOP_DB_PASSWORD é lida do ambiente do contêiner.

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER MAPPING FOR postgres
        SERVER webshop_server
        OPTIONS (
            user 'postgres',
            password '${WEB_SHOP_DB_PASSWORD}'
        );
EOSQL