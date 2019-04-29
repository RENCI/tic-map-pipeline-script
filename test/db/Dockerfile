FROM postgres:11.2

COPY schema.sql schema.sql
COPY init-user-db.sh docker-entrypoint-initdb.d/init-user-db.sh
