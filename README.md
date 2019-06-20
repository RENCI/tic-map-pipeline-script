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
## api

list all backups
```
GET /backup
```

create a new backup
```
POST /backup
```

restore from a backup
```
POST /restore/<backup>
```

sync with source
```
POST /sync
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
