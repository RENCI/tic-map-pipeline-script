import reload
import server
import filecmp
from sqlalchemy import create_engine
import os
import os.path
import shutil
from multiprocessing import Process
import datetime
import requests
import time


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
    engine = create_engine("postgresql+psycopg2://" + ctx["dbuser"] + ":" + ctx["dbpass"] + "@" + ctx["dbhost"] + ":" + ctx["dbport"] + "/" + ctx["dbname"])
    conn = engine.connect()
    rs = conn.execute('''SELECT COUNT(*) FROM "Proposal"''').fetchall()
    assert len(rs) == 1
    for row in rs:
        assert row[0] == 0

    shutil.copytree("/etlout", "/data/tables")
    print("sync database")
    assert reload.syncDatabase(ctx)
    rs = conn.execute('''SELECT COUNT(*) FROM "Proposal"''').fetchall()
    assert len(rs) == 1
    for row in rs:
        assert row[0] == 1
    print("database synced")
    shutil.rmtree("/data/tables")
    conn.close()
    if cleanup:
        reload.clearDatabase(ctx)
        reload.createTables(ctx)


def test_back_data_dictionary():
    os.chdir("/")
    ctx = reload.context()
    shutil.copy("redcap/metadata.json", ctx["dataDictionaryInputFilePath"])
    assert reload.backUpDataDictionary(ctx)
    directory = reload.dataDictionaryBackUpDirectory(ctx)
    os.remove(ctx["dataDictionaryInputFilePath"])
    shutil.rmtree(directory)


def test_back_data_dictionary_makedirs_exists():
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
    if cleanup:
        os.remove(ctx["backupDir"] + "/" + ts)
        reload.clearDatabase(ctx)
        reload.createTables(ctx)
    else:
        return ts


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


def test_delete_task(cleanup=True):
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




