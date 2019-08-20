# tic-map-pipeline-script

### How to build ###

```
git submodule init --update
```

```
docker build
```



### How to run test ###

download data dictionary in json format and store it under `test/redcap/metadata.json`

```
cd test
docker-compose up --build -V
```
## environment

`POSTGRES_DATABASE_NAME` postgres database name

`POSTGRES_USER` postgres user

`POSTGRES_PASSWORD` postgres password

`POSTGRES_PORT` postgres port    

`POSTGRES_HOST` postgres host

`REDCAP_APPLICATION_TOKEN` redcap application token

`REDCAP_URL_BASE` redcap url base

`POSTGRES_DUMP_PATH` postgres dump path

`AUXILIARY_PATH` path to auxiliary files to be joined with source data

`RELOAD_SCHEDULE` set to `1` to daily reload

`SCHEDULE_RUN_TIME` schedule run time of reload format `HH:MM`

`RELOAD_DATABASE` set to `1` to reload database on start up

`SERVER` set to `1` to run a REST API

`CREATE_TABLES` set to `1` to create tables in database from `data/tables.sql`

`INSERT_DATA` set to `1` to insert data in database from `data/tables`

`REDIS_QUEUE_HOST` redis host for task queue

`REDIS_QUEUE_PORT` redis port for task queue

`REDIS_QUEUE_DB` redis database for task queue

`REDIS_LOCK_HOST` redis host for distributed locking

`REDIS_LOCK_PORT` redis port for distributed locking

`REDIS_LOCK_DB` redis database for distributed locking

`REDIS_LOCK_EXPIRE` expire time for distributed locking in seconds

`REDIS_LOCK_TIMEOUT` timeout for distributed locking in seconds


## api

list all backups
```
GET /backup
```

create a new backup
```
POST /backup
```

delete a backup
```
DELETE /backup/<backup>
```

restore from a backup
```
POST /restore/<backup>
```

sync with source
```
POST /sync
```

insert data into table in csv
```
POST /table/<tablename>
```
csv should have matching header as the table 

get data from table in json
```
GET /table/<tablename>
```

list all tasks
```
GET /task
```

list a task
```
GET /task/<id>
```

delete task
```
DELETE /task/<id>
```

get table
```
GET /table/<table>
```

overwrite table
```
PUT /table/<table>
```
with file `data` in csv with header or json, and `json` for additional columns, and `content-type` for content type of the data

append table
```
POST /table/<table>
```
with file `data` in csv with header or json, and `json` for additional columns, and `content-type` for content type of the data
