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
from pathlib import Path
from stat import S_ISREG, ST_MTIME, ST_MODE
from flask import Flask, request
import redis
from rq import Queue
import tempfile
import logging
import csv
import reload

q = Queue(connection=redis.StrictRedis(host=os.environ["REDIS_QUEUE_HOST"], port=int(os.environ["REDIS_QUEUE_PORT"]), db=int(os.environ["REDIS_QUEUE_DB"])))

TASK_TIME=3600

logger = logging.getLogger(__name__)

def handleTableFunc(handler, ctx, tablename, tfname, kvp):
    try:
        handler(ctx, tablename, tfname, kvp)
    finally:
        os.unlink(tfname)

def server(ctx):
    app = Flask(__name__)

    @app.route("/backup", methods=['GET', 'POST'])
    def backup():
        if request.method == 'GET':
            return getBackup(ctx)
        else:
            return postBackup(ctx)
        
    def getBackup(ctx):
        dirpath = ctx["backupDir"]
        entries = ((os.path.join(dirpath, fn), fn) for fn in os.listdir(dirpath))
        entries = ((os.stat(path), fn) for path, fn in entries)
        entries = ((stat[ST_MTIME], fn) for stat, fn in entries if S_ISREG(stat[ST_MODE]))
        entries = (fn for _, fn in sorted(entries, reverse=True))
        entries = list(entries)
                          
        return json.dumps(entries)
    
    def postBackup(ctx):
        ts = str(datetime.datetime.now())
        pBackup = q.enqueue(reload.backUpDatabase, args=[ctx, ts], job_timeout=TASK_TIME)
        return json.dumps(pBackup.id)

    @app.route("/backup/<string:ts>", methods=["DELETE"])
    def deleteBackup(ts):
        pDeleteBackup = q.enqueue(reload.deleteBackup, args=[ctx, ts], job_timeout=TASK_TIME)        
        return json.dumps(pDeleteBackup.id)
    
    @app.route("/restore/<string:ts>", methods=['POST'])
    def restore(ts):
        pRestore = q.enqueue(reload.restoreDatabase, args=[ctx, ts], job_timeout=TASK_TIME)
        return json.dumps(pRestore.id)
    
    @app.route("/sync", methods=['POST'])
    def sync():
        pSync = q.enqueue(reload.entrypoint, args=[ctx], kwargs={
            "one_off": True
        }, job_timeout=TASK_TIME)
        return json.dumps(pSync.id)


    def handleTable(handler, ctx, tablename):
        tf = tempfile.NamedTemporaryFile(delete=False)
        try:
            tf.close()
            tfname = tf.name
            f = request.files["data"]
            if request.form["content-type"] == "application/json":
                j = json.load(f)
                with open(tfname, "w", newline="") as tfi:
                    writer = csv.writer(tfi)
                    if len(j) == 0:
                        writer.writerow([])
                    else:
                        keys = list(j[0].keys())
                        writer.writerow(keys)
                    for rowdict in j:
                        writer.writerow([rowdict[key] for key in keys])
            elif request.form["content-type"] == "text/csv":
                f.save(tfname)
            else:
                logger.error("unsupported type")
                raise RuntimeError("unsupported type")
            kvp = json.loads(request.form["json"])
        except Exception as e:
            os.unlink(tfname)
            logger.error("exception " + str(e))
            raise
                
        pTable = q.enqueue(handleTableFunc, args=[handler, ctx, tablename, tfname, kvp], job_timeout=TASK_TIME)
        return json.dumps(pTable.id)            

    @app.route("/table/<string:tablename>", methods=["GET", "POST", "PUT"])
    def table(tablename):
        if request.method == "GET":
            logger.info("get table")
            return json.dumps(reload.readDataFromTable(ctx, tablename))
        elif request.method == "PUT":
            logger.info("put table")
            return handleTable(reload.updateDataIntoTable, ctx, tablename)
        else:
            logger.info("post table")
            return handleTable(reload.insertDataIntoTable, ctx, tablename)
            
    @app.route("/task", methods=["GET"])
    def task():
        return json.dumps(q.job_ids)
            
    @app.route("/task/<string:taskid>", methods=["GET", "DELETE"])
    def taskId(taskid):
        if request.method == "GET":
            return getTaskId(taskid)
        else:
            return deleteTaskId(taskid)

    def getTaskId(taskid):
        job = q.fetch_job(taskid)
        return json.dumps({
            "status": job.get_status(),
            "name": job.func_name,
            "created_at": str(job.created_at),
            "enqueued_at": str(job.enqueued_at),
            "started_at": str(job.started_at),
            "ended_at": str(job.ended_at),
            "description": job.description
        })
    
    def deleteTaskId(taskid):
        job = q.fetch_job(taskid)
        job.cancel()
        return json.dumps(taskid)

    app.run(host="0.0.0.0")
