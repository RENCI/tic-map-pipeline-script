# tic-map-pipeline-script

### How to build ###

build `TIC preprocessing-assembly-0.1.0.jar` from https://github.com/RENCI/map-pipeline rename it to `TIC preprocessing-assembly.jar`

download `HEAL data mapping_finalv6.csv` from https://github.com/RENCI/HEAL-data-mapping rename it to `HEAL data mapping.csv`

```docker build```

### How to run test ###

download data dictionary in json format and store it under `test/redcap`
create a one record data in json format and store it under `test/redcap`

```
cd test
docker-compose up --build
```
