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
import reload

q = Queue(connection=redis.StrictRedis(host=os.environ["REDIS_QUEUE_HOST"], port=int(os.environ["REDIS_QUEUE_PORT"]), db=int(os.environ["REDIS_QUEUE_DB"])))

TASK_TIME=3600

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
