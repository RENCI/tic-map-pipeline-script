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
from multiprocessing import Process, RLock
import logging
from pathlib import Path

logging.basicConfig(level=logging.DEBUG)

def pgpass(ctx):
    home = str(Path.home())
    with open(home + "/.pgpass", "w+") as f:
        f.write(ctx["dbhost"] + ":" + ctx["dbport"] + ":" + ctx["dbname"] + ":" + ctx["dbuser"] + ":" + ctx["dbpass"])
    os.chmod(home + "/.pgpass", stat.S_IREAD | stat.S_IWRITE)

def backUpDatabase(ctx, lock, ts):
    with lock:
        pgpass(ctx)
        pgdumpfile = ctx["backupDir"] + "/" + ts
        cp = subprocess.run(["pg_dump", "-O", "-d", ctx["dbname"], "-U", ctx["dbuser"], "-h", ctx["dbhost"], "-p", ctx["dbport"], "-f", pgdumpfile])
        if cp.returncode != 0:
            sys.stderr.write("backup encountered an error: " + str(cp.returncode))
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
            print ("dropping table: ", row[1])
            cursor.execute("drop table \"" + row[1] + "\" cascade")
            print("table dropped")
        print("closing cursor")
        cursor.close()
        print("closing connection")
        conn.close()
        print("database cleared")
        return True
    except Exception as e:
        sys.stderr.write("clear database encountered an error: " + str(e))
        return False
        

def restoreDatabase(ctx, lock, ts):
    with lock:
        if not clearDatabase(ctx):
            return False
        pgpass(ctx)
        pgdumpfile = ctx["backupDir"] + "/" + ts
        print("running psql")
        cp, out, err = subprocess.run(["psql", "-d", ctx["dbname"], "-U", ctx["dbuser"], "-h", ctx["dbhost"], "-p", ctx["dbport"], "-f", pgdumpfile], capture_output = True)
        print("psql done ["+out+"]["+err+"]")
        if cp.returncode != 0:
            sys.stderr.write("restore encountered an error: " + str(cp.returncode))
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
        print(data_dictionary_backup_path, "is a file")
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
    print("downloading", output)
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
            print("executing", line)
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
        print("deleting from table", f)
        cursor.execute("DELETE FROM \"" + f + "\"")
    cursor.close()
    conn.commit()
    conn.close()
    return True


def insertData(ctx):
    tables = getTables(ctx)
    for f in tables:
        print("inserting into table", f)
        cp = subprocess.run(["csvsql", "--db", "postgresql://"+ctx["dbuser"]+":" + ctx["dbpass"] + "@" + ctx["dbhost"] +"/" + ctx["dbname"], "--insert", "--no-create", "-d", ",", "-e", "utf8", "--no-inference", "data/tables/" + f])
        if cp.returncode != 0:
            print("error syncing database", cp.returncode)
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
        sys.stderr.write("pipeline encountered an error: " + str(cp.returncode))
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


def runPipeline(ctx, lock):
    with lock:
        if ctx["reloaddb"]:
            downloadData(ctx)
            downloadDataDictionary(ctx)
        if not backUpDataDictionary(ctx):
            return False

        if not etl(ctx):
            return False

        ts = str(datetime.datetime.now())
        if not backUpDatabase(ctx, lock, ts):
            return False
        
        return syncDatabase(ctx)


def entrypoint(ctx, lock, create_tables=None, insert_data=None, reload=None, one_off=None, schedule_run_time=None):
    with lock:
        if create_tables:
            createTables(ctx)

        if insert_data:
            insertData(ctx)

        if one_off:
            runPipeline(ctx, lock)
            
    if reload:
        schedule.every().day.at(schedule_run_time).do(lambda: runPipeline(ctx, lock))
        while True:
            schedule.run_pending()
            time.sleep(1000)


from flask import Flask

if __name__ == "__main__":
    ctx = context()
    s = os.environ["RELOAD_SCHEDULE"] == "1"
    o = os.environ["RELOAD_ONE_OFF"] == "1"
    cdb = os.environ["CREATE_TABLES"] == "1"
    idb = os.environ["INSERT_DATA"] == "1"
    scheduleRunTime = os.environ["SCHEDULE_RUN_TIME"]
    runServer = os.environ["SERVER"] == "1"
    lock = RLock()
    p = Process(target = entrypoint, args=[ctx, lock], kwargs={
        "create_tables": cdb,
        "insert_data": idb,
        "reload": s,
        "one_off": o,
        "schedule_run_time": scheduleRunTime
    })
    p.start()
    if runServer:
        app = Flask(__name__)

        @app.route("/backup")
        def backup():
            ctx = context()
            ts = str(datetime.datetime.now())
            pBackup = Process(target = backUpDatabase, args=[ctx, lock, ts])
            pBackup.start()
            return json.dumps("")
    
        @app.route("/restore/<string:ts>")
        def restore(ts):
            ctx = context()
            pRestore = Process(target = restoreDatabase, args=[ctx, lock, ts])
            pRestore.start()
            return json.dumps("")
    
        @app.route("/sync")
        def sync():
            ctx = context()
            pSync = Process(target = entrypoint, args=[ctx, lock], kwargs={
                "one_off": True
            })
            pSync.start()
            return json.dumps("")
            
        app.run(host="0.0.0.0")
    p.join()
        
    
       

