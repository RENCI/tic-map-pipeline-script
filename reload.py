import csv
import datetime
import filecmp
import functools
import json
import logging
import os
import shutil
import socket
import stat
import subprocess
import sys
import tempfile
import time
from multiprocessing import Process
from pathlib import Path
from stat import S_ISREG, ST_MODE, ST_MTIME

import redis
import requests
import schedule
import sherlock
from flask import Flask, request
from psycopg2 import connect
from rq import Connection, Queue, Worker
from sherlock import Lock
from tx.functional.either import Left, Right

import utils

sherlock.configure(
    backend=sherlock.backends.REDIS,
    client=redis.StrictRedis(
        host=os.environ["REDIS_LOCK_HOST"],
        port=int(os.environ["REDIS_LOCK_PORT"]),
        db=int(os.environ["REDIS_LOCK_DB"]),
    ),
    expire=int(os.environ["REDIS_LOCK_EXPIRE"]),
    timeout=int(os.environ["REDIS_LOCK_TIMEOUT"]),
)

G_LOCK = "g_lock"


def redisQueue():
    return redis.StrictRedis(
        host=os.environ["REDIS_QUEUE_HOST"],
        port=int(os.environ["REDIS_QUEUE_PORT"]),
        db=int(os.environ["REDIS_QUEUE_DB"]),
    )


q = Queue(connection=redisQueue())

logger = utils.getLogger(__name__)


def waitForDatabaseToStart(host, port):
    logger.info("waiting for database to start host=" + host + " port=" + str(port))
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    while True:
        try:
            s.connect((host, port))
            s.close()
            break
        except socket.error as ex:
            logger.info("waiting for database to start host=" + host + " port=" + str(port))
            time.sleep(1)
    logger.info("database started host=" + host + " port=" + str(port))


def waitForRedisToStart(host, port):
    logger.info("waiting for redis to start host=" + host + " port=" + str(port))
    s = redis.StrictRedis(host, port)
    while True:
        try:
            s.ping()
            break
        except socket.error as ex:
            logger.info("waiting for redis to start host=" + host + " port=" + str(port))
            time.sleep(1)
    logger.info("redis started host=" + host + " port=" + str(port))


def pgpass(ctx):
    home = str(Path.home())
    with open(home + "/.pgpass", "w+") as f:
        f.write(ctx["dbhost"] + ":" + ctx["dbport"] + ":" + ctx["dbname"] + ":" + ctx["dbuser"] + ":" + ctx["dbpass"])
    os.chmod(home + "/.pgpass", stat.S_IREAD | stat.S_IWRITE)


def backUpDatabase(ctx, ts):
    with Lock(G_LOCK):
        return _backUpDatabase(ctx, ts)


def _backUpDatabase(ctx, ts):
    pgpass(ctx)
    pgdumpfile = ctx["backupDir"] + "/" + ts
    cp = subprocess.run(
        [
            "pg_dump",
            "-O",
            "-d",
            ctx["dbname"],
            "-U",
            ctx["dbuser"],
            "-h",
            ctx["dbhost"],
            "-p",
            ctx["dbport"],
            "-f",
            pgdumpfile,
        ]
    )
    if cp.returncode != 0:
        logger.error("backup encountered an error: " + str(cp.returncode))
        return False
    else:
        return True


def deleteBackup(ctx, ts):
    with Lock(G_LOCK):
        return _deleteBackup(ctx, ts)


def _deleteBackup(ctx, ts):
    try:
        dirpath = ctx["backupDir"]
        os.remove(dirpath + "/" + ts)
        return True
    except Exception as e:
        logger.error("delete backup error: " + str(e))
        return False


def clearDatabase(ctx):
    conn = connect(
        user=ctx["dbuser"],
        password=ctx["dbpass"],
        host=ctx["dbhost"],
        dbname=ctx["dbname"],
    )
    conn.autocommit = True
    cursor = conn.cursor()

    try:
        cursor.execute(
            "SELECT table_schema,table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_schema,table_name"
        )
        rows = cursor.fetchall()
        for row in rows:
            logger.info("dropping table: " + row[1])
            cursor.execute('select count(*) from "' + row[1] + '"')
            logger.info(str(cursor.fetchone()[0]) + " rows")
            query = 'drop table "' + row[1] + '" cascade'
            logger.info(query)
            cursor.execute(query)
            logger.info("table dropped")
        logger.info("closing cursor")
        cursor.close()
        logger.info("closing connection")
        conn.close()
        logger.info("database cleared")
        return True
    except Exception as e:
        logger.error("clear database encountered an error: " + str(e))
        return False


def restoreDatabase(ctx, ts):
    with Lock(G_LOCK):
        return _restoreDatabase(ctx, ts)


def _restoreDatabase(ctx, ts):
    if not clearDatabase(ctx):
        return False
    pgpass(ctx)
    pgdumpfile = ctx["backupDir"] + "/" + ts
    logger.info("restoring database")
    cp = subprocess.run(
        [
            "psql",
            "-d",
            ctx["dbname"],
            "-U",
            ctx["dbuser"],
            "-h",
            ctx["dbhost"],
            "-p",
            ctx["dbport"],
            "-f",
            pgdumpfile,
        ]
    )
    logger.info("database restored")
    if cp.returncode != 0:
        logger.error("restore encountered an error: " + str(cp.returncode))
        return False
    else:
        return True


def dataDictionaryBackUpDirectory(ctx):
    return ctx["backupDir"] + "/redcap_data_dictionary"


def backUpDataDictionary(ctx):
    data_dictionary_backup_dir = dataDictionaryBackUpDirectory(ctx)
    data_dictionary_backup_path = data_dictionary_backup_dir + "/redcap_data_dictionary_export.json"
    do_backup = False
    if os.path.isfile(ctx["dataDictionaryInputFilePath"]):
        if not os.path.isfile(data_dictionary_backup_path):
            do_backup = True
        elif not filecmp.cmp(ctx["dataDictionaryInputFilePath"], data_dictionary_backup_path):
            logger.info(data_dictionary_backup_path + " is a file")
            mtime = os.path.getmtime(data_dictionary_backup_path)
            shutil.copy(data_dictionary_backup_path, data_dictionary_backup_path + str(mtime))
            do_backup = True
        if do_backup:
            if not os.path.exists(data_dictionary_backup_dir):
                os.makedirs(data_dictionary_backup_dir)
            shutil.copy(ctx["dataDictionaryInputFilePath"], data_dictionary_backup_path)
    return True


def download(ctx, headers, data, output):
    if os.path.isfile(output):
        os.remove(output)
    logger.info("downloading " + output)
    with open(output, "wb+") as f:
        r = requests.post(ctx["redcapURLBase"], data=data, headers=headers, stream=True)
        for chunk in r.iter_content(chunk_size=8192):
            if chunk:  # filter out keep-alive new chunks
                f.write(chunk)


def downloadRedcapData(ctx, token, output):
    if os.path.isfile(output):
        os.remove(output)
    logger.info("downloading Redcap Data")
    r = utils.RedcapExport(token, ctx["redcapURLBase"])
    proposal_ids = r.get_proposal_ids()
    proposals = r.get_proposals(r.chunk_proposals(proposal_ids))
    r.write_to_file(proposals, output)


def createTables(ctx):
    with Lock(G_LOCK):
        _createTables(ctx)


def _createTables(ctx):
    logger.info("create tables start")
    subprocess.run(
        [
            "stack",
            "exec",
            "map-pipeline-schema-exe",
            "/mapping.json",
            "/data/tables.sql",
        ],
        cwd="/map-pipeline-schema",
    )
    conn = connect(
        user=ctx["dbuser"],
        password=ctx["dbpass"],
        host=ctx["dbhost"],
        dbname=ctx["dbname"],
    )
    conn.autocommit = True
    cursor = conn.cursor()
    with open("data/tables.sql", encoding="utf-8") as f:
        for line in f:
            logger.info("executing " + line)
            cursor.execute(line)
    cursor.close()
    conn.close()
    logger.info("create tables end")
    return True


def getTables(ctx):
    return list(filter(lambda x: not x.startswith("."), os.listdir("data/tables")))


def _deleteTables(ctx):
    logger.info("delete tables start")
    conn = connect(
        user=ctx["dbuser"],
        password=ctx["dbpass"],
        host=ctx["dbhost"],
        dbname=ctx["dbname"],
    )
    conn.autocommit = True
    cursor = conn.cursor()
    tables = getTables(ctx)
    for f in tables:
        logger.info("deleting from table " + f)
        cursor.execute('DELETE FROM "' + f + '"')
    cursor.close()
    conn.close()
    logger.info("delete tables end")
    return True


def insertData(ctx):
    with Lock(G_LOCK):
        _insertData(ctx)


def _insertData(ctx):
    logger.info("insert data start")
    tables = getTables(ctx)
    for f in tables:
        if not _insertDataIntoTable(ctx, f, "data/tables/" + f, {}):
            return False
    logger.info("insert data end")
    return True


def runFile(func, f, kvp):
    if len(kvp) == 0:
        add_headers = add_data = []
    else:
        add_headers, add_data = map(list, zip(*kvp.items()))
    outf = tempfile.NamedTemporaryFile("w+", newline="", encoding="utf-8", delete=False)
    try:
        with outf:
            writer = csv.writer(outf)
            with open(f, newline="", encoding="utf-8") as inf:
                reader = csv.reader(inf)
                headers = next(reader)
                print(add_headers, add_data)
                writer.writerow(headers + add_headers)
                for row in reader:
                    writer.writerow(row + add_data)

        cp = func(outf.name)
    finally:
        os.unlink(outf.name)
    return cp


def insertDataIntoTable(ctx, table, f, kvp):
    with Lock(G_LOCK):
        return _insertDataIntoTable(ctx, table, f, kvp)


def _insertDataIntoTable(ctx, table, f, kvp):
    logger.info("inserting into table " + table)
    checkId(table)
    cp = runFile(
        lambda fn: subprocess.run(
            [
                "csvsql",
                "--db",
                "postgresql://" + ctx["dbuser"] + ":" + ctx["dbpass"] + "@" + ctx["dbhost"] + "/" + ctx["dbname"],
                "--insert",
                "--no-create",
                "-d",
                ",",
                "-e",
                "utf8",
                "--no-inference",
                "--tables",
                table,
                fn,
            ]
        ),
        f,
        kvp,
    )
    if cp.returncode != 0:
        logger.error("error inserting data into table " + table + " " + f + " " + str(cp.returncode))
        return False
    return True


def updateDataIntoTable(ctx, table, f, kvp):
    with Lock(G_LOCK):
        return _updateDataIntoTable(ctx, table, f, kvp)


def _updateDataIntoTable(ctx, table, f, kvp):
    logger.info("inserting into table " + table)
    checkId(table)

    conn = connect(
        user=ctx["dbuser"],
        password=ctx["dbpass"],
        host=ctx["dbhost"],
        dbname=ctx["dbname"],
    )
    cursor = conn.cursor()
    cursor.execute('delete from "{0}"'.format(table))
    cursor.close()
    conn.commit()
    conn.close()

    return _insertDataIntoTable(ctx, table, f, kvp)


def checkId(i):
    if '"' in i:
        raise RuntimeError("invalid name {0}".format(i))


def getColumnDataType(ctx, table, column):
    conn = connect(
        user=ctx["dbuser"],
        password=ctx["dbpass"],
        host=ctx["dbhost"],
        dbname=ctx["dbname"],
    )
    cursor = conn.cursor()
    cursor.execute(
        """
select data_type
from information_schema.columns
where table_schema NOT IN ('information_schema', 'pg_catalog') and table_name=%s and column_name=%s
order by table_schema, table_name
    """,
        (table, column),
    )
    rows = cursor.fetchall()
    dt = rows[0][0]
    cursor.close()
    conn.close()
    return dt


def validateTable(ctx, tablename, tfname, kvp):
    with open(tfname, "r", newline="", encoding="utf-8") as tfi:
        reader = csv.reader(tfi)
        header = next(reader)
        n = len(header)
        i = 0
        for row in reader:
            n2 = len(row)
            if n2 != n:
                return Left(f"row {i} number of items, expected {n}, encountered {n2}")
            i += 1
        seen = set()
        dups = []
        for x in header:
            if x in seen:
                dups.append(x)
            else:
                seen.add(x)
        if len(dups) > 0:
            return Left(f"duplicate header(s) in upload {dups}")
        header2 = list(kvp.keys())
        i2 = [a for a in header if a in header2]
        if len(i2) > 0:
            return Left(f"duplicate header(s) in input {i2}")
        conn = connect(
            user=ctx["dbuser"],
            password=ctx["dbpass"],
            host=ctx["dbhost"],
            dbname=ctx["dbname"],
        )
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                select column_name, data_type
                from information_schema.columns
                where table_schema NOT IN ('information_schema', 'pg_catalog') and table_name=%s
                order by table_schema, table_name
                """,
                (tablename,),
            )
            rows = cursor.fetchall()
            header3 = list(map(lambda r: r[0], rows))
            d2 = [a for a in header + header2 if a not in header3]
            if len(d2) > 0:
                return Left(f"undefined header(s) in input {d2} available {header3}")
            return Right(())
        finally:
            cursor.close()
            conn.close()


def updateDataIntoTableColumn(ctx, table, column, f, kvp):
    with Lock(G_LOCK):
        return _updateDataIntoTableColumn(ctx, table, column, f, kvp)


def _updateDataIntoTableColumn(ctx, table, column, f, kvp):
    logger.info("inserting into table " + table + " with column " + column)
    checkId(table)
    checkId(column)
    dt = getColumnDataType(ctx, table, column)

    updated = set()

    if len(kvp) == 0:
        add_headers = add_data = []
    else:
        add_headers, add_data = map(list, zip(*kvp.items()))

    conn = connect(
        user=ctx["dbuser"],
        password=ctx["dbpass"],
        host=ctx["dbhost"],
        dbname=ctx["dbname"],
    )
    cursor = conn.cursor()
    with open(f, newline="", encoding="utf-8") as inf:
        reader = csv.reader(inf)
        headers = next(reader)
        headers2 = headers + add_headers
        for header in headers2:
            checkId(header)
        index = headers2.index(column)
        if dt == "integer":
            fn = int
        elif dt == "bigint":
            fn = int
        elif dt == "text":
            fn = lambda x: x
        elif dt == "character varying":
            fn = lambda x: x
        else:
            raise RuntimeError("unsupported data type {0}".format(dt))
        for row in reader:
            row2 = row + add_data
            val = fn(row2[index])
            if val not in updated:
                cursor.execute('delete from "{0}" where "{1}" = %s'.format(table, column), (val,))
                updated.add(val)

    cursor.close()
    conn.commit()
    conn.close()
    return _insertDataIntoTable(ctx, table, f, kvp)


def readDataFromTable(ctx, table):
    logger.info("reading from table " + table)
    checkId(table)
    conn = connect(
        user=ctx["dbuser"],
        password=ctx["dbpass"],
        host=ctx["dbhost"],
        dbname=ctx["dbname"],
    )
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM "' + table + '"')
    colnames = [c.name for c in cursor.description]
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return [{colname: str(cell) for colname, cell in zip(colnames, row)} for row in rows]


def syncDatabase(ctx):
    with Lock(G_LOCK):
        return _syncDatabase(ctx)


def _syncDatabase(ctx):
    logger.info("synchronizing database start")
    if not _deleteTables(ctx):
        return False
    if not _insertData(ctx):
        return False
    logger.info("synchronizing database end")
    return True


def etl(ctx):
    if os.path.isdir("data/tables"):
        for f in os.listdir("data/tables"):
            os.remove("data/tables/" + f)
    # logger.info("THIS IS THE FILE THAT IS GOING TO BE READ", ctx["dataInputFilePath"])
    cp = subprocess.run(
        [
            "spark-submit",
            "--driver-memory",
            ctx["sparkDriverMemory"],
            "--executor-memory",
            ctx["sparkExecutorMemory"],
            "--master",
            "local[*]",
            "--class",
            "tic.Transform2",
            ctx["assemblyPath"],
            "--mapping_input_file",
            ctx["mappingInputFilePath"],
            "--data_input_file",
            ctx["dataInputFilePath"],
            "--data_dictionary_input_file",
            ctx["dataDictionaryInputFilePath"],
            "--auxiliary_dir",
            ctx["auxiliaryDir"],
            "--filter_dir",
            ctx["filterDir"],
            "--block_dir",
            ctx["blockDir"],
            "--output_dir",
            ctx["outputDirPath"],
        ]
    )
    if cp.returncode != 0:
        logger.error("pipeline encountered an error: " + str(cp.returncode))
        return False
    else:
        return True


def downloadData(ctx):
    data = {
        "token": ctx["redcapApplicationToken"],
        "content": "record",
        "format": "json",
        "type": "flat",
        # "records[0]" : "1",
        # fields[0]=
        "rawOrLabel": "raw",
        "rawOrLabelHeaders": "raw",
        "exportCheckboxLabel": "false",
        "exportSurveyFields": "false",
        "exportDataAccessGroups": "false",
        "returnFormat": "json",
    }
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json",
    }
    downloadRedcapData(ctx, ctx["redcapApplicationToken"], ctx["dataInputFilePath"])


def downloadDataDictionary(ctx):
    data = {
        "token": ctx["redcapApplicationToken"],
        "content": "metadata",
        "format": "json",
        "returnFormat": "json",
    }
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json",
    }
    download(ctx, headers, data, ctx["dataDictionaryInputFilePath"])


def clearTasks():
    q.empty()


def context():
    return {
        "home": str(Path.home()),
        "redcapApplicationToken": os.environ["REDCAP_APPLICATION_TOKEN"],
        "dbuser": os.environ["POSTGRES_USER"],
        "dbpass": os.environ["POSTGRES_PASSWORD"],
        "dbhost": os.environ["POSTGRES_HOST"],
        "dbport": os.environ["POSTGRES_PORT"],
        "dbname": os.environ["POSTGRES_DATABASE_NAME"],
        "reloaddb": os.environ["RELOAD_DATABASE"] == "1",
        "backupDir": os.environ["POSTGRES_DUMP_PATH"],
        "redcapURLBase": os.environ["REDCAP_URL_BASE"],
        "assemblyPath": "TIC preprocessing-assembly.jar",
        "mappingInputFilePath": "mapping.json",
        "downloadRedcapData": os.getenv("DOWNLOAD_REDCAP_DATA") if os.getenv("DOWNLOAD_REDCAP_DATA") is not None else True,
        "dataInputFilePath": os.getenv("DATA_INPUT_FILE_PATH") or "redcap_export.json",
        "dataDictionaryInputFilePath": os.getenv("DATA_DICTIONARY_INPUT_FILE_PATH") or "redcap_data_dictionary_export.json",
        "auxiliaryDir": os.environ["AUXILIARY_PATH"],
        "filterDir": os.environ["FILTER_PATH"],
        "blockDir": os.environ["BLOCK_PATH"],
        "outputDirPath": "data",
        "redisQueueHost": os.environ["REDIS_QUEUE_HOST"],
        "redisQueuePort": os.environ["REDIS_QUEUE_PORT"],
        "redisQueueDatabase": os.environ["REDIS_QUEUE_DB"],
        "redisLockHost": os.environ["REDIS_LOCK_HOST"],
        "redisLockPort": os.environ["REDIS_LOCK_PORT"],
        "redisLockDatabase": os.environ["REDIS_LOCK_DB"],
        "redisLockExpire": os.environ["REDIS_LOCK_EXPIRE"],
        "redisLockTimeout": os.environ["REDIS_LOCK_TIMEOUT"],
        "sparkDriverMemory": os.environ["SPARK_DRIVER_MEMORY"],
        "sparkExecutorMemory": os.environ["SPARK_EXECUTOR_MEMORY"],
    }


def startWorker():
    conn = redisQueue()
    worker = Worker(Queue(connection=conn), connection=conn)
    worker.work()


def runPipeline(ctx):
    with Lock(G_LOCK):
        return _runPipeline(ctx)


def _runPipeline(ctx):
    if ctx["reloaddb"]:
        logger.info(f"ctx['downloadRedcapData'] {ctx['downloadRedcapData']}")
        if int(ctx["downloadRedcapData"]):
            logger.info(f"Downloading data. Using {ctx['dataInputFilePath']}")
            downloadData(ctx)
            downloadDataDictionary(ctx)
    if not backUpDataDictionary(ctx):
        return False

    if not etl(ctx):
        return False

    ts = str(datetime.datetime.now())
    if not _backUpDatabase(ctx, ts):
        return False

    return _syncDatabase(ctx)


def entrypoint(
    ctx,
    create_tables=None,
    insert_data=None,
    reload=None,
    one_off=None,
    schedule_run_time=None,
):
    waitForDatabaseToStart(ctx["dbhost"], int(ctx["dbport"]))
    waitForRedisToStart(ctx["redisQueueHost"], int(ctx["redisQueuePort"]))
    waitForRedisToStart(ctx["redisLockHost"], int(ctx["redisLockPort"]))
    logger.info("create_tables=" + str(create_tables))
    logger.info("insert_data=" + str(insert_data))
    logger.info("one_off=" + str(one_off))
    logger.info("reload=" + str(reload))
    with Lock(G_LOCK):
        if create_tables:
            try:
                _createTables(ctx)
            except Exception as e:
                logger.error("pipeline encountered an error when creating tables" + str(e))

        if insert_data:
            try:
                _insertData(ctx)
            except Exception as e:
                logger.error("pipeline encountered an error when inserting data" + str(e))

        if one_off:
            try:
                _runPipeline(ctx)
            except Exception as e:
                logger.error("pipeline encountered an error during one off run" + str(e))

    if reload:
        schedule.every().day.at(schedule_run_time).do(lambda: runPipeline(ctx))
        while True:
            schedule.run_pending()
            time.sleep(1000)
