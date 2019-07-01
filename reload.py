import os
import subprocess
import sys
from psycopg2 import connect
import schedule
import time
import requests
import stat
import datetime
from pathlib import Path
import filecmp
import shutil
import json
from multiprocessing import Process
import logging
from pathlib import Path
from stat import S_ISREG, ST_MTIME, ST_MODE
from flask import Flask, request
import sherlock
from sherlock import Lock
import redis
from rq import Queue, Worker, Connection
import socket


sherlock.configure(backend=sherlock.backends.REDIS, client=redis.StrictRedis(host=os.environ["REDIS_LOCK_HOST"], port=int(os.environ["REDIS_LOCK_PORT"]), db=int(os.environ["REDIS_LOCK_DB"])), expire=int(os.environ["REDIS_LOCK_EXPIRE"]), timeout=int(os.environ["REDIS_LOCK_TIMEOUT"]))

G_LOCK="g_lock"


def redisQueue():
    return redis.StrictRedis(host=os.environ["REDIS_QUEUE_HOST"], port=int(os.environ["REDIS_QUEUE_PORT"]), db=int(os.environ["REDIS_QUEUE_DB"]))


q = Queue(connection=redisQueue())

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def waitForDatabaseToStart(ctx):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    while True:
        try:
            s.connect((ctx["dbhost"], ctx["dbport"]))
            s.close()
            break
        except socket.error as ex:
            logger.info("waiting for database to start")
            time.sleep(1)

            
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
    cp = subprocess.run(["pg_dump", "-O", "-d", ctx["dbname"], "-U", ctx["dbuser"], "-h", ctx["dbhost"], "-p", ctx["dbport"], "-f", pgdumpfile])
    if cp.returncode != 0:
        logger.error("backup encountered an error: " + str(cp.returncode))
        return False
    else:
        return True

            
def clearDatabase(ctx):
    conn = connect(user=ctx["dbuser"], password=ctx["dbpass"], host=ctx["dbhost"], dbname=ctx["dbname"])
    conn.autocommit = True
    cursor = conn.cursor()
            
    try:
        cursor.execute("SELECT table_schema,table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_schema,table_name")
        rows = cursor.fetchall()
        for row in rows:
            logger.info ("dropping table: " + row[1])
            cursor.execute("drop table \"" + row[1] + "\" cascade")
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
    cp = subprocess.run(["psql", "-d", ctx["dbname"], "-U", ctx["dbuser"], "-h", ctx["dbhost"], "-p", ctx["dbport"], "-f", pgdumpfile])
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
    if not os.path.isfile(data_dictionary_backup_path):
        do_backup = True
    elif not filecmp.cmp(ctx["dataDictionaryInputFilePath"], data_dictionary_backup_path):
        logger.info(data_dictionary_backup_path + " is a file")
        mtime = os.path.getmtime(data_dictionary_backup_path)
        shutil.copy(data_dictionary_backup_path, data_dictionary_backup_path+str(mtime))
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

                
def createTables(ctx):
    conn = connect(user=ctx["dbuser"], password=ctx["dbpass"], host=ctx["dbhost"], dbname=ctx["dbname"])
    cursor = conn.cursor()
    with open("data/tables.sql") as f:
        for line in f:
            logger.info("executing " + line)
            cursor.execute(line)
    cursor.close()
    conn.commit()
    conn.close()
    return True
    

def getTables(ctx):
    return list(filter(lambda x : not x.startswith("."), os.listdir("data/tables")))

def deleteTables(ctx):
    conn = connect(user=ctx["dbuser"], password=ctx["dbpass"], host=ctx["dbhost"], dbname=ctx["dbname"])
    cursor = conn.cursor()
    tables = getTables(ctx)
    for f in tables:
        logger.info("deleting from table " + f)
        cursor.execute("DELETE FROM \"" + f + "\"")
    cursor.close()
    conn.commit()
    conn.close()
    return True


def insertData(ctx):
    tables = getTables(ctx)
    for f in tables:
        logger.info("inserting into table " + f)
        cp = subprocess.run(["csvsql", "--db", "postgresql://"+ctx["dbuser"]+":" + ctx["dbpass"] + "@" + ctx["dbhost"] +"/" + ctx["dbname"], "--insert", "--no-create", "-d", ",", "-e", "utf8", "--no-inference", "data/tables/" + f])
        if cp.returncode != 0:
            logger.error("error syncing database " + str(cp.returncode))
            return False
    return True


def syncDatabase(ctx):
    if not deleteTables(ctx):
        return False
    if not insertData(ctx):
        return False
    return True


def etl(ctx):
    if os.path.isdir("data/tables"):
        for f in os.listdir("data/tables"):
            os.remove("data/tables/" + f)
    cp = subprocess.run(["spark-submit", "--driver-memory", "2g", "--executor-memory", "2g", "--master", "local[*]", "--class", "tic.Transform2", ctx["assemblyPath"],
                         "--mapping_input_file", ctx["mappingInputFilePath"], "--data_input_file", ctx["dataInputFilePath"],
                         "--data_dictionary_input_file", ctx["dataDictionaryInputFilePath"], "--output_dir", ctx["outputDirPath"]])
    if cp.returncode != 0:
        logger.error("pipeline encountered an error: " + str(cp.returncode))
        return False
    else:
        return True

    
def downloadData(ctx):
    data = {
        "token" : ctx["redcapApplicationToken"],
        "content" : "record",
        "format" : "json",
        "type" : "flat",
        # "records[0]" : "1",
        # fields[0]=
        "rawOrLabel" : "raw",
        "rawOrLabelHeaders" : "raw",
        "exportCheckboxLabel" : "false",
        "exportSurveyFields" : "false",
        "exportDataAccessGroups" : "false",
        "returnFormat" : "json"
    }
    headers = {
        "Content-Type" : "application/x-www-form-urlencoded",
        "Accept" : "application/json"
    }
    download(ctx, headers, data, ctx["dataInputFilePath"])


def downloadDataDictionary(ctx):
    data = {
        "token" : ctx["redcapApplicationToken"],
        "content" : "metadata",
        "format" : "json",
        "returnFormat" : "json"
    }
    headers = {
        "Content-Type" : "application/x-www-form-urlencoded",
        "Accept" : "application/json"
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
        "mappingInputFilePath": "HEAL data mapping.csv",
        "dataInputFilePath": "redcap_export.json",
        "dataDictionaryInputFilePath": "redcap_data_dictionary_export.json",
        "outputDirPath": "data",
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
            downloadData(ctx)
            downloadDataDictionary(ctx)
        if not backUpDataDictionary(ctx):
            return False

        if not etl(ctx):
            return False

        ts = str(datetime.datetime.now())
        if not _backUpDatabase(ctx, ts):
            return False
        
        return syncDatabase(ctx)


def entrypoint(ctx, create_tables=None, insert_data=None, reload=None, one_off=None, schedule_run_time=None):
    logger.info("entrypoint create_tables="+str(create_tables)+" insert_data="+str(insert_data)+" reload="+str(reload)+" one_off="+str(one_off)+" schedule_run_time="+str(schedule_run_time))

    waitForDatabaseToStart(ctx)

    with Lock(G_LOCK):
        if create_tables:
            try:
                createTables(ctx)
            except Exception as e:
                logger.error("pipeline encountered an error when creating tables" + str(e))

        if insert_data:
            try:
                insertData(ctx)
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


