import reload
import server
import filecmp
from sqlalchemy import create_engine, text
import os
import os.path
import shutil
from multiprocessing import Process
import datetime
import requests
import time
from rq import Worker
from psycopg2 import connect
import pytest
import json
import csv

def countrows(src):
    with open(src, newline="") as inf:
        reader = csv.reader(inf)
        headers = next(reader)
        return sum(1 for row in reader)
    
def bag_equal(a, b):
    t = list(b)
    for elem in a:
        if elem in t:
            t.remove(elem)
        else:
            return False
    return True


def contains(a,b):
    return all(item in a.items() for item in b.items())


def bag_contains(a, b):
    t = list(b)
    for e in a:
        found = None
        for f in t:
            if contains(e, f):
                found = f
                break
        if found:
            t.remove(found)
        else:
            return False
    return True


@pytest.fixture(scope="session", autouse=True)
def pause():
    yield
    if os.environ.get("PAUSE") == "1":
        input("Press Enter to continue...")

        
@pytest.fixture(scope='function', autouse=True)
def test_log(request):
    print("Test '{}' STARTED".format(request.node.nodeid)) # Here logging is used, you can use whatever you want to use for logs
    def fin():
        print("Test '{}' COMPLETED".format(request.node.nodeid))
    request.addfinalizer(fin)

    
def test_downloadData():
    os.chdir("/")
    ctx = reload.context()
    reload.downloadData(ctx)
    assert filecmp.cmp(ctx["dataInputFilePath"], "redcap/record.json")
    os.remove(ctx["dataInputFilePath"])

    
def test_downloadDataDictionary():
    os.chdir("/")
    ctx = reload.context()
    reload.downloadDataDictionary(ctx)
    assert filecmp.cmp(ctx["dataDictionaryInputFilePath"], "redcap/metadata.json")
    os.remove(ctx["dataDictionaryInputFilePath"])


def test_clear_database():
    os.chdir("/")
    ctx = reload.context()
    reload.clearDatabase(ctx)
    engine = create_engine("postgresql+psycopg2://" + ctx["dbuser"] + ":" + ctx["dbpass"] + "@" + ctx["dbhost"] + ":" + ctx["dbport"] + "/" + ctx["dbname"])
    conn = engine.connect()
            
    rs = conn.execute("SELECT table_schema,table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_schema,table_name").fetchall()
    assert len(rs) == 0
    conn.close()
    reload.createTables(ctx)

    
def test_etl():
    os.chdir("/")
    ctx = reload.context()
    shutil.copy("redcap/record.json", ctx["dataInputFilePath"])
    shutil.copy("redcap/metadata.json", ctx["dataDictionaryInputFilePath"])
    assert reload.etl(ctx)
    assert os.path.isfile("/data/tables/Proposal")
    with open("/data/tables/Proposal") as f:
        assert sum(1 for _ in f) == 2
    os.remove(ctx["dataInputFilePath"])
    os.remove(ctx["dataDictionaryInputFilePath"])
    shutil.rmtree("/data/tables")


def test_sync(cleanup = True):
    os.chdir("/")
    ctx = reload.context()
    conn = connect(user=ctx["dbuser"], password=ctx["dbpass"], host=ctx["dbhost"], port=ctx["dbport"], dbname=ctx["dbname"])
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute('''SELECT COUNT(*) FROM "Proposal"''')
    rs = cur.fetchall()
    assert len(rs) == 1
    for row in rs:
        assert row[0] == 0

    shutil.copytree("/etlout", "/data/tables")
    print("sync database")
    assert reload.syncDatabase(ctx)
    cur.execute('''SELECT COUNT(*) FROM "Proposal"''')
    rs = cur.fetchall()
    assert len(rs) == 1
    for row in rs:
        assert row[0] == 1
    print("database synced")
    shutil.rmtree("/data/tables")
    conn.close()
    if cleanup:
        reload.clearDatabase(ctx)
        reload.createTables(ctx)


def test_back_up_data_dictionary():
    os.chdir("/")
    ctx = reload.context()
    shutil.copy("redcap/metadata.json", ctx["dataDictionaryInputFilePath"])
    assert reload.backUpDataDictionary(ctx)
    directory = reload.dataDictionaryBackUpDirectory(ctx)
    os.remove(ctx["dataDictionaryInputFilePath"])
    shutil.rmtree(directory)


def test_back_up_data_dictionary_makedirs_exists():
    os.chdir("/")
    ctx = reload.context()
    directory = reload.dataDictionaryBackUpDirectory(ctx)
    os.makedirs(directory)
    shutil.copy("redcap/metadata.json", ctx["dataDictionaryInputFilePath"])
    assert reload.backUpDataDictionary(ctx)
    os.remove(ctx["dataDictionaryInputFilePath"])
    shutil.rmtree(directory)


def test_back_up_database(cleanup=True):
    print("test_back_up_database")
    test_sync(False)
    os.chdir("/")
    ctx = reload.context()
    ts = str(datetime.datetime.now())
    assert reload._backUpDatabase(ctx, ts)
    assert(ts in os.listdir(ctx["backupDir"]))
    if cleanup:
        os.remove(ctx["backupDir"] + "/" + ts)
        reload.clearDatabase(ctx)
        reload.createTables(ctx)
    else:
        return ts


def test_delete_back_up_database():
    print("test_back_up_database")
    test_sync(False)
    os.chdir("/")
    ctx = reload.context()
    ts = str(datetime.datetime.now())
    assert reload._backUpDatabase(ctx, ts)
    assert reload._deleteBackup(ctx, ts)
    assert(ts not in os.listdir(ctx["backupDir"]))

    reload.clearDatabase(ctx)
    reload.createTables(ctx)


def test_restore_database():
    print("test_restore_database")
    ts = test_back_up_database(False)
    os.chdir("/")
    ctx = reload.context()
    reload.clearDatabase(ctx)
    reload.createTables(ctx)
    assert reload._restoreDatabase(ctx, ts)
    os.remove(ctx["backupDir"] + "/" + ts)
    reload.clearDatabase(ctx)
    reload.createTables(ctx)


def test_back_up_database_with_lock(cleanup=True):
    print("test_back_up_database")
    test_sync(False)
    os.chdir("/")
    ctx = reload.context()
    ts = str(datetime.datetime.now())
    assert reload.backUpDatabase(ctx, ts)
    assert(ts in os.listdir(ctx["backupDir"]))
    if cleanup:
        os.remove(ctx["backupDir"] + "/" + ts)
        reload.clearDatabase(ctx)
        reload.createTables(ctx)
    else:
        return ts


def test_restore_database_with_lock():
    print("test_restore_database")
    ts = test_back_up_database(False)
    os.chdir("/")
    ctx = reload.context()
    reload.clearDatabase(ctx)
    reload.createTables(ctx)
    assert reload.restoreDatabase(ctx, ts)
    os.remove(ctx["backupDir"] + "/" + ts)
    reload.clearDatabase(ctx)
    reload.createTables(ctx)


def test_sync_endpoint():
    os.chdir("/")
    ctx = reload.context()
    p = Process(target = server.server, args=[ctx], kwargs={})
    p.start()
    time.sleep(10)
    try:
        resp = requests.post("http://localhost:5000/sync")
        assert resp.status_code == 200
        print(resp.json())
        assert isinstance(resp.json(), str)
    finally:
        p.terminate()
        reload.clearTasks()

    
def test_back_up_endpoint():
    os.chdir("/")
    ctx = reload.context()
    p = Process(target = server.server, args=[ctx], kwargs={})
    p.start()
    time.sleep(10)
    try:
        resp = requests.get("http://localhost:5000/backup")
        assert resp.status_code == 200
        print(resp.json())
        assert isinstance(resp.json(), list)
    finally:
        p.terminate()
        reload.clearTasks()


def test_task():
    os.chdir("/")
    ctx = reload.context()
    p = Process(target = server.server, args=[ctx], kwargs={})
    p.start()
    time.sleep(10)
    try:
        resp0 = requests.get("http://localhost:5000/task")
        assert len(resp0.json()) == 0
        resp = requests.post("http://localhost:5000/backup")
        resp2 = requests.get("http://localhost:5000/task")
        assert len(resp2.json()) == 1
        assert resp.json() in resp2.json()
    finally:
        p.terminate()
        reload.clearTasks()


def test_get_task():
    os.chdir("/")
    ctx = reload.context()
    p = Process(target = server.server, args=[ctx], kwargs={})
    p.start()
    time.sleep(10)
    try:
        resp = requests.post("http://localhost:5000/backup")
        resp2 = requests.get("http://localhost:5000/task/" + resp.json())
        assert "name" in resp2.json()
        assert "created_at" in resp2.json()
        assert "ended_at" in resp2.json()
        assert "started_at" in resp2.json()
        assert "enqueued_at" in resp2.json()
        assert "description" in resp2.json()
        assert "status" in resp2.json()

    finally:
        p.terminate()
        reload.clearTasks()


def test_delete_task():
    os.chdir("/")
    ctx = reload.context()
    p = Process(target = server.server, args=[ctx], kwargs={})
    p.start()
    time.sleep(10)
    try:
        resp0 = requests.get("http://localhost:5000/task")
        assert len(resp0.json()) == 0
        resp = requests.post("http://localhost:5000/sync")
        resp1 = requests.post("http://localhost:5000/sync")
        resp2 = requests.get("http://localhost:5000/task")
        assert len(resp2.json()) == 2
        assert resp.json() in resp2.json()
        assert resp1.json() in resp2.json()
        requests.delete("http://localhost:5000/task/" + resp1.json())
        resp3 = requests.get("http://localhost:5000/task")
        assert len(resp3.json()) == 1
        assert resp.json() in resp3.json()
        assert resp1.json() not in resp3.json()
    finally:
        p.terminate()
        reload.clearTasks()


def wait_for_task_to_finish(taskid):
    os.chdir("/")
    ctx = reload.context()
    resp = requests.get("http://localhost:5000/task/" + taskid)
    print(resp.json())
    while resp.json()["status"] in ["queued", "started"]:
        time.sleep(1)
        resp = requests.get("http://localhost:5000/task/" + taskid)
        print(resp.json())


def test_start_worker():
    os.chdir("/")
    ctx = reload.context()
    p = Process(target = reload.startWorker)
    workers = Worker.all(connection=reload.redisQueue())
    assert len(list(workers)) == 0
    p.start()
    time.sleep(10)
    workers = Worker.all(connection=reload.redisQueue())
    assert len(list(workers)) == 1
    p.terminate()


def do_test_auxiliary(aux1, exp):
    os.chdir("/")
    aux0 = os.environ.get("AUXILIARY_PATH")
    os.environ["AUXILIARY_PATH"] = aux1
    ctx = reload.context()
    shutil.copy("redcap/record.json", ctx["dataInputFilePath"])
    shutil.copy("redcap/metadata.json", ctx["dataDictionaryInputFilePath"])
    assert reload.etl(ctx)
    with open("/data/tables/ProposalFunding") as f:
        i = f.readline().split(",").index("totalBudgetInt")
        assert f.readline().split(",")[i] == exp
    os.remove(ctx["dataInputFilePath"])
    os.remove(ctx["dataDictionaryInputFilePath"])
    shutil.rmtree("/data/tables")
    if aux0 is None:
        del os.environ["AUXILIARY_PATH"]
    else:
        os.environ["AUXILIARY_PATH"] = aux0


def test_auxiliary1():
    do_test_auxiliary("auxiliary1", "123")


def test_auxiliary2():
    do_test_auxiliary("auxiliary2", '""')


def test_auxiliary3():
    do_test_auxiliary("auxiliary3", '""')


def test_post_table():
    os.chdir("/")
    ctx = reload.context()
    pServer = Process(target = server.server, args=[ctx], kwargs={})
    pServer.start()
    time.sleep(10)
    pWorker = Process(target = reload.startWorker)
    pWorker.start()
    src = "/etlout/Proposal"
    time.sleep(10)
    try:
        print("get proposal")
        resp = requests.get("http://localhost:5000/table/Proposal")
        assert(len(resp.json()) == 0)
        print("post proposal")
        resp = requests.post("http://localhost:5000/table/Proposal", files={
            "json": (None, json.dumps({}), "application/json"),
            "data": (src, open(src, "rb"), "application/octet-stream")
        })
        assert resp.status_code == 200
        taskid = resp.json()
        assert isinstance(taskid, str)
        wait_for_task_to_finish(taskid)
        print("get proposal")
        resp = requests.get("http://localhost:5000/table/Proposal")
        respjson = resp.json()
        assert(len(respjson) == 1)
        print("post proposal")
        resp = requests.post("http://localhost:5000/table/Proposal", files={
            "json": (None, json.dumps({}), "application/json"),
            "data": (src, open(src, "rb"), "application/octet-stream")
        })
        assert resp.status_code == 200
        taskid = resp.json()
        assert isinstance(taskid, str)
        wait_for_task_to_finish(taskid)
        print("get proposal")
        resp = requests.get("http://localhost:5000/table/Proposal")
        respjson = resp.json()
        assert(len(respjson) == 2)
    finally:
        pWorker.terminate() 
        pServer.terminate()
        reload.clearTasks()
        reload.clearDatabase(ctx)
        reload.createTables(ctx)
    

def test_put_table():
    os.chdir("/")
    ctx = reload.context()
    pServer = Process(target = server.server, args=[ctx], kwargs={})
    pServer.start()
    time.sleep(10)
    pWorker = Process(target = reload.startWorker)
    pWorker.start()
    src = "/etlout/Proposal"
    time.sleep(10)
    try:
        print("get proposal")
        resp = requests.get("http://localhost:5000/table/Proposal")
        assert(len(resp.json()) == 0)
        print("put proposal")
        resp = requests.put("http://localhost:5000/table/Proposal", files={
            "json": (None, json.dumps({}), "application/json"),
            "data": (src, open(src, "rb"), "application/octet-stream")
        })
        assert resp.status_code == 200
        taskid = resp.json()
        assert isinstance(taskid, str)
        wait_for_task_to_finish(taskid)
        print("get proposal")
        resp = requests.get("http://localhost:5000/table/Proposal")
        respjson = resp.json()
        assert(len(respjson) == 1)
        print("put proposal")
        resp = requests.put("http://localhost:5000/table/Proposal", files={
            "json": (None, json.dumps({}), "application/json"),
            "data": (src, open(src, "rb"), "application/octet-stream")
        })
        assert resp.status_code == 200
        taskid = resp.json()
        assert isinstance(taskid, str)
        wait_for_task_to_finish(taskid)
        print("get proposal")
        resp = requests.get("http://localhost:5000/table/Proposal")
        respjson = resp.json()
        assert(len(respjson) == 1)
    finally:
        pWorker.terminate() 
        pServer.terminate()
        reload.clearTasks()
        reload.clearDatabase(ctx)
        reload.createTables(ctx)


def test_post_table_kvp():
    do_test_post_table_kvp("/add/ssd.csv")


def test_post_table_kvp2():
    do_test_post_table_kvp("/add/ssd2.csv")


def do_test_post_table_kvp(src):
    os.chdir("/")
    ctx = reload.context()
    pServer = Process(target = server.server, args=[ctx], kwargs={})
    pServer.start()
    time.sleep(10)
    pWorker = Process(target = reload.startWorker)
    pWorker.start()
    time.sleep(10)
    n = countrows(src)
    try:
        print("get siteInformation")
        resp = requests.get("http://localhost:5000/table/SiteInformation")
        assert(resp.json() == [])
        print("post siteInformation")
        resp = requests.post("http://localhost:5000/table/SiteInformation", files={
            "json": (None, json.dumps({"ProposalID": "0"}), "application/json"),
            "data": (src, open(src, "rb"), "application/octet-stream")
        })
        assert resp.status_code == 200
        taskid = resp.json()
        assert isinstance(taskid, str)
        wait_for_task_to_finish(taskid)
        print("get siteInformation")
        resp = requests.get("http://localhost:5000/table/SiteInformation")
        respjson = resp.json()
        assert(bag_contains(respjson, [
            {
                "ProposalID": "0",
                "siteNumber": str(i)
            } for i in range(1, n+1)
        ]))
        print("post proposal")
        resp = requests.post("http://localhost:5000/table/SiteInformation", files={
            "json": (None, json.dumps({"ProposalID": "1"}), "application/json"),
            "data": (src, open(src, "rb"), "application/octet-stream")
        })
        assert resp.status_code == 200
        taskid = resp.json()
        assert isinstance(taskid, str)
        wait_for_task_to_finish(taskid)
        print("get siteInformation")
        resp = requests.get("http://localhost:5000/table/SiteInformation")
        respjson = resp.json()
        assert(bag_contains(respjson, [
            {
                "ProposalID": str(i),
                "siteNumber": str(j)
            } for i in [0, 1] for j in range(1, n+1)
        ]))
    finally:
        pWorker.terminate() 
        pServer.terminate()
        reload.clearTasks()
        reload.clearDatabase(ctx)
        reload.createTables(ctx)


def test_put_table_kvp():
    do_test_put_table_kvp("/add/ssd.csv")
                             

def test_put_table_kvp2():
    do_test_put_table_kvp("/add/ssd2.csv")
                             

def do_test_put_table_kvp(src):
    os.chdir("/")
    ctx = reload.context()
    pServer = Process(target = server.server, args=[ctx], kwargs={})
    pServer.start()
    time.sleep(10)
    pWorker = Process(target = reload.startWorker)
    pWorker.start()
    time.sleep(10)
    n = countrows(src)
    try:
        print("get siteInformation")
        resp = requests.get("http://localhost:5000/table/SiteInformation")
        assert(resp.json() == [])
        print("put siteInformation")
        resp = requests.put("http://localhost:5000/table/SiteInformation", files={
            "json": (None, json.dumps({"ProposalID": "0"}), "application/json"),
            "data": (src, open(src, "rb"), "application/octet-stream")
        })
        assert resp.status_code == 200
        taskid = resp.json()
        assert isinstance(taskid, str)
        wait_for_task_to_finish(taskid)
        print("get siteInformation")
        resp = requests.get("http://localhost:5000/table/SiteInformation")
        respjson = resp.json()
        assert(bag_contains(respjson, [
            {
                "ProposalID": "0",
                "siteNumber": str(i)
            } for i in range(1, n+1)
        ]))
        print("put siteInformation")
        resp = requests.put("http://localhost:5000/table/SiteInformation", files={
            "json": (None, json.dumps({"ProposalID": "0"}), "application/json"),
            "data": (src, open(src, "rb"), "application/octet-stream")
        })
        assert resp.status_code == 200
        taskid = resp.json()
        assert isinstance(taskid, str)
        wait_for_task_to_finish(taskid)
        print("get siteInformation")
        resp = requests.get("http://localhost:5000/table/SiteInformation")
        respjson = resp.json()
        assert(bag_contains(respjson, [
            {
                "ProposalID": "0",
                "siteNumber": str(i)
            } for i in range(1, n+1)
        ]))
    finally:
        pWorker.terminate() 
        pServer.terminate()
        reload.clearTasks()
        reload.clearDatabase(ctx)
        reload.createTables(ctx)

        
def test_insert_table():
    do_test_insert_table("/add/ssd.csv", {})

    
def test_insert_table2():
    do_test_insert_table("/add/ssd2.csv", {})

    
def test_insert_table_kvp():
    do_test_insert_table("/add/ssd.csv", {"ProposalID":"0"})

    
def test_insert_table_kvp2():
    do_test_insert_table("/add/ssd2.csv", {"ProposalID":"0"})

    
def do_test_insert_table(src, kvp):
    os.chdir("/")
    ctx = reload.context()
    n = countrows(src)
    try:
        reload.insertDataIntoTable(ctx, "SiteInformation", src, kvp)
        rows = reload.readDataFromTable(ctx, "SiteInformation")
        assert(bag_contains(rows, [
            {
                "siteNumber": str(i),
                **kvp
            } for i in range (1, n+1)
        ]))
        reload.insertDataIntoTable(ctx, "SiteInformation", src, kvp)
        rows = reload.readDataFromTable(ctx, "SiteInformation")
        assert(bag_contains(rows, [
            {
                "siteNumber": str(i),
                **kvp
            } for i in range (1, n+1)
        ] * 2))
    finally:
        reload.clearDatabase(ctx)
        reload.createTables(ctx)

