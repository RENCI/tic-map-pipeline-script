# tic-map-pipeline-script

### How to build ###

```
git submodule init --update
```

```
docker build
```

### How to run test ###

download data dictionary in json format and store it under `test/redcap`
create a one record data in json format and store it under `test/redcap`

```
cd test
docker-compose up --build -V
```
