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
import yaml
from contextlib import contextmanager
import re

WAIT_PERIOD = 3

def countrows(src, mime):
    if mime == "text/csv":
        with open(src, newline="", encoding="utf-8") as inf:
            reader = csv.reader(inf)
            headers = next(reader)
            return sum(1 for row in reader)
    elif mime == "application/json":
        with open(src, encoding="utf-8") as inf:
            return len(json.load(inf))
    
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
def init_db():
    os.chdir("/")
    ctx = reload.context()
    reload.createTables(ctx)
    yield

@pytest.fixture(scope="session", autouse=True)
def pause():
    try:
        yield
    finally:
        if os.environ.get("PAUSE") == "1":
            input("Press Enter to continue...")


        
@pytest.fixture(scope='function', autouse=True)
def test_log(request):
    print("Test '{}' STARTED".format(request.node.nodeid)) # Here logging is used, you can use whatever you want to use for logs
    try:
        yield
    finally:
        print("Test '{}' COMPLETED".format(request.node.nodeid))

    
def test_downloadData():
    ctx = reload.context()
    reload.downloadData(ctx)
    assert filecmp.cmp(ctx["dataInputFilePath"], "redcap/record.json")
    os.remove(ctx["dataInputFilePath"])

    
def test_downloadDataDictionary():
    
    ctx = reload.context()
    reload.downloadDataDictionary(ctx)
    assert filecmp.cmp(ctx["dataDictionaryInputFilePath"], "redcap/metadata.json")
    os.remove(ctx["dataDictionaryInputFilePath"])


def test_clear_database():
    
    ctx = reload.context()
    reload.clearDatabase(ctx)
    engine = create_engine("postgresql+psycopg2://" + ctx["dbuser"] + ":" + ctx["dbpass"] + "@" + ctx["dbhost"] + ":" + ctx["dbport"] + "/" + ctx["dbname"])
    conn = engine.connect()
            
    rs = conn.execute("SELECT table_schema,table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_schema,table_name").fetchall()
    assert len(rs) == 0
    conn.close()
    reload.createTables(ctx)



@contextmanager
def copy_file(fromp, top):
    shutil.copy(fromp, top)
    try:
        yield
    finally:
        os.remove(top)


@contextmanager
def copytree(fromp, top):
    shutil.copytree(fromp, top)
    try:
        yield
    finally:
        shutil.rmtree(top)


@contextmanager
def datatables(nextvalue):
    try:
        ret = nextvalue()
        yield ret
    finally:
        shutil.rmtree("/data/tables")

    
@contextmanager
def connection(ctx, autocommit=False):
    conn = connect(user=ctx["dbuser"], password=ctx["dbpass"], host=ctx["dbhost"], port=ctx["dbport"], dbname=ctx["dbname"])
    conn.autocommit = autocommit
    try:
        yield conn
    finally:
        conn.close()        


@contextmanager
def database(ctx, cleanup=True):
    try:
        yield
    finally:
        reload.clearDatabase(ctx)
        reload.createTables(ctx)

        
def test_etl():
    
    ctx = reload.context()
    with copy_file("redcap/record.json", ctx["dataInputFilePath"]):
        with copy_file("redcap/metadata.json", ctx["dataDictionaryInputFilePath"]):
            with datatables(lambda: reload.etl(ctx)) as ret:
                assert ret
                assert os.path.isfile("/data/tables/Proposal")
                with open("/data/tables/Proposal") as f:
                    assert sum(1 for _ in f) == 2


def test_sync(cleanup = True):
    
    ctx = reload.context()
    with database(ctx, cleanup=cleanup):
        with connection(ctx, autocommit=True) as conn:
            cur = conn.cursor()
            cur.execute('''SELECT COUNT(*) FROM "Proposal"''')
            rs = cur.fetchall()
            assert len(rs) == 1
            for row in rs:
                assert row[0] == 0
            
            with copytree("/etlout", "/data/tables"):
                print("sync database")
                assert reload.syncDatabase(ctx)
                cur.execute('''SELECT COUNT(*) FROM "Proposal"''')
                rs = cur.fetchall()
                assert len(rs) == 1
                for row in rs:
                    assert row[0] == 1
                    print("database synced")


def test_entrypoint():
    
    ctx = reload.context()
    with database(ctx):
        with connection(ctx, autocommit=True) as conn:
            cur = conn.cursor()
            cur.execute('''SELECT COUNT(*) FROM "Proposal"''')
            rs = cur.fetchall()
            assert len(rs) == 1
            for row in rs:
                assert row[0] == 0

            ctx["reloaddb"]=False
            with copy_file("redcap/record.json", ctx["dataInputFilePath"]):
                with copy_file("redcap/metadata.json", ctx["dataDictionaryInputFilePath"]):
                    with datatables(lambda: reload.entrypoint(ctx, one_off=True)):
                        cur.execute('''SELECT COUNT(*) FROM "Proposal"''')
                        rs = cur.fetchall()
                        assert len(rs) == 1
                        for row in rs:
                            assert row[0] == 1


def test_back_up_data_dictionary():
    
    ctx = reload.context()
    with copy_file("redcap/metadata.json", ctx["dataDictionaryInputFilePath"]):
        assert reload.backUpDataDictionary(ctx)
        directory = reload.dataDictionaryBackUpDirectory(ctx)
        shutil.rmtree(directory)


def test_back_up_data_dictionary_not_exists():
    
    ctx = reload.context()
    assert reload.backUpDataDictionary(ctx)
    directory = reload.dataDictionaryBackUpDirectory(ctx)
    assert not os.path.exists(ctx["dataDictionaryInputFilePath"])
    assert not os.path.exists(directory)


def test_back_up_data_dictionary_makedirs_exists():
    
    ctx = reload.context()
    directory = reload.dataDictionaryBackUpDirectory(ctx)
    os.makedirs(directory)
    with copy_file("redcap/metadata.json", ctx["dataDictionaryInputFilePath"]):
        assert reload.backUpDataDictionary(ctx)
        shutil.rmtree(directory)


def test_back_up_database(cleanup=True):
    print("test_back_up_database")
    ctx = reload.context()
    with database(ctx, cleanup=cleanup):
        test_sync(False)
    
        ts = str(datetime.datetime.now())
        assert reload._backUpDatabase(ctx, ts)
        assert(ts in os.listdir(ctx["backupDir"]))
        if cleanup:
            os.remove(ctx["backupDir"] + "/" + ts)
        else:
            return ts


def test_delete_back_up_database():
    print("test_back_up_database")
    test_sync(False)
    
    ctx = reload.context()
    with database(ctx, cleanup=True):
        ts = str(datetime.datetime.now())
        assert reload._backUpDatabase(ctx, ts)
        assert reload._deleteBackup(ctx, ts)
        assert ts not in os.listdir(ctx["backupDir"])


def test_restore_database():
    print("test_restore_database")

    ctx = reload.context()
    with database(ctx, cleanup=True):
        ts = test_back_up_database(False)
    
    with database(ctx, cleanup=True):
        assert reload._restoreDatabase(ctx, ts)
        os.remove(ctx["backupDir"] + "/" + ts)


def test_back_up_database_with_lock(cleanup=True):
    print("test_back_up_database")
    test_sync(False)
    
    ctx = reload.context()
    with database(ctx, cleanup=cleanup):
        ts = str(datetime.datetime.now())
        assert reload.backUpDatabase(ctx, ts)
        assert(ts in os.listdir(ctx["backupDir"]))
        if cleanup:
            os.remove(ctx["backupDir"] + "/" + ts)
        else:
            return ts


def test_restore_database_with_lock():
    print("test_restore_database")

    ctx = reload.context()
    with database(ctx, cleanup=True):
        ts = test_back_up_database(False)
    
    with database(ctx, cleanup=True):
        assert reload.restoreDatabase(ctx, ts)
        os.remove(ctx["backupDir"] + "/" + ts)


def test_sync_endpoint():
    
    ctx = reload.context()
    p = Process(target = server.server, args=[ctx], kwargs={})
    p.start()
    time.sleep(WAIT_PERIOD)
    try:
        resp = requests.post("http://localhost:5000/sync")
        assert resp.status_code == 200
        print(resp.json())
        assert isinstance(resp.json(), str)
    finally:
        p.terminate()
        reload.clearTasks()

    
def test_back_up_endpoint():
    
    ctx = reload.context()
    p = Process(target = server.server, args=[ctx], kwargs={})
    p.start()
    time.sleep(WAIT_PERIOD)
    try:
        resp = requests.get("http://localhost:5000/backup")
        assert resp.status_code == 200
        print(resp.json())
        assert isinstance(resp.json(), list)
    finally:
        p.terminate()
        reload.clearTasks()


def test_task():
    
    ctx = reload.context()
    p = Process(target = server.server, args=[ctx], kwargs={})
    p.start()
    time.sleep(WAIT_PERIOD)
    try:
        resp0 = requests.get("http://localhost:5000/task")
        assert len(resp0.json()["queued"]) == 0
        resp = requests.post("http://localhost:5000/backup")
        resp2 = requests.get("http://localhost:5000/task")
        assert "queued" in resp2.json()
        assert len(resp2.json()["queued"]) == 1
        for status in ["started", "finished", "failed", "deferred"]:
            assert status in resp2.json()
            for category in ["job_ids", "expired_job_ids"]:
                assert category in resp2.json()[status]
                assert len(resp2.json()[status][category]) == 0
    finally:
        p.terminate()
        reload.clearTasks()


def test_get_task():
    
    ctx = reload.context()
    p = Process(target = server.server, args=[ctx], kwargs={})
    p.start()
    time.sleep(WAIT_PERIOD)
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
        assert "result" in resp2.json()

    finally:
        p.terminate()
        reload.clearTasks()


def test_get_all_tasks():
    
    ctx = reload.context()
    reload.clearTasks()
    reload.clearDatabase(ctx)
    reload.createTables(ctx)
    pServer = Process(target = server.server, args=[ctx], kwargs={})
    pServer.start()
    time.sleep(WAIT_PERIOD)
    pWorker = Process(target = reload.startWorker)
    pWorker.start()
    time.sleep(WAIT_PERIOD)
    try:
        resp0 = requests.get("http://localhost:5000/task")
        assert len(resp0.json()["queued"]) == 0
        resp1 = requests.post("http://localhost:5000/sync")
        task_id = resp1.json()
        wait_for_task_to_start(task_id)
        resp2 = requests.get("http://localhost:5000/task")
        assert resp2.json() == {
            "queued": [],
            "started": {
                "job_ids": [task_id],
                "expired_job_ids": []
            },
            "finished": {
                "job_ids": [],
                "expired_job_ids": []
            },
            "failed": {
                "job_ids": [],
                "expired_job_ids": []
            },
            "deferred": {
                "job_ids": [],
                "expired_job_ids": []
            }
        }
    finally:
        pWorker.terminate() 
        pServer.terminate()
        reload.clearTasks()
        reload.clearDatabase(ctx)
        reload.createTables(ctx)


def test_delete_task():
    
    ctx = reload.context()
    p = Process(target = server.server, args=[ctx], kwargs={})
    p.start()
    time.sleep(WAIT_PERIOD)
    try:
        resp0 = requests.get("http://localhost:5000/task")
        assert len(resp0.json()["queued"]) == 0
        resp = requests.post("http://localhost:5000/sync")
        resp1 = requests.post("http://localhost:5000/sync")
        resp2 = requests.get("http://localhost:5000/task")
        assert len(resp2.json()["queued"]) == 2
        assert resp.json() in resp2.json()["queued"]
        assert resp1.json() in resp2.json()["queued"]
        requests.delete("http://localhost:5000/task/" + resp1.json())
        resp3 = requests.get("http://localhost:5000/task")
        assert len(resp3.json()["queued"]) == 1
        assert resp.json() in resp3.json()["queued"]
        assert resp1.json() not in resp3.json()["queued"]
    finally:
        p.terminate()
        reload.clearTasks()


def wait_for_task_to_finish(taskid):
    
    ctx = reload.context()
    resp = requests.get("http://localhost:5000/task/" + taskid)
    print(resp.json())
    while resp.json()["status"] in ["queued", "started"]:
        time.sleep(1)
        resp = requests.get("http://localhost:5000/task/" + taskid)
        print(resp.json())


def wait_for_task_to_start(taskid):
    
    ctx = reload.context()
    resp = requests.get("http://localhost:5000/task/" + taskid)
    print(resp.json())
    while resp.json()["status"] in ["queued"]:
        time.sleep(1)
        resp = requests.get("http://localhost:5000/task/" + taskid)
        print(resp.json())


def test_start_worker():
    
    ctx = reload.context()
    p = Process(target = reload.startWorker)
    workers = Worker.all(connection=reload.redisQueue())
    assert len(list(workers)) == 0
    p.start()
    time.sleep(WAIT_PERIOD)
    workers = Worker.all(connection=reload.redisQueue())
    assert len(list(workers)) == 1
    p.terminate()


def do_test_auxiliary(aux1, exp):
    
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


def do_test_filter(aux1, exp):
    
    aux0 = os.environ.get("FILTER_PATH")
    os.environ["FILTER_PATH"] = aux1
    ctx = reload.context()
    shutil.copy("redcap/record.json", ctx["dataInputFilePath"])
    shutil.copy("redcap/metadata.json", ctx["dataDictionaryInputFilePath"])
    assert reload.etl(ctx)
    with open("/data/tables/Proposal", newline="") as f:
        reader = csv.reader(f)
        headers = next(reader)
        i = sum(1 for row in reader)
        assert i == exp
    os.remove(ctx["dataInputFilePath"])
    os.remove(ctx["dataDictionaryInputFilePath"])
    shutil.rmtree("/data/tables")
    if aux0 is None:
        del os.environ["FILTER_PATH"]
    else:
        os.environ["FILTER_PATH"] = aux0


def test_filter1():
    do_test_filter("filter1", 1)


def test_filter2():
    do_test_filter("filter2", 0)


def test_filter3():
    do_test_filter("filter3", 1)


def test_post_table():
    do_test_post_table(requests.post, requests.post, "/etlout/Proposal", "text/csv", "Proposal", {}, {}, [
            {
                "ProposalID": "0",
            }
        ], [
            {
                "ProposalID": "0",
            }
        ] * 2)

def test_put_table():
    do_test_post_table(requests.put, requests.put, "/etlout/Proposal", "text/csv", "Proposal", {}, {}, [
            {
                "ProposalID": "0",
            }
        ], [
            {
                "ProposalID": "0",
            }
        ])

def test_post_table_kvp():
    do_test_post_table_kvp_content_SiteInformation("/add/ssd.csv", "text/csv")

def test_post_table_kvp2():
    do_test_post_table_kvp_content_SiteInformation("/add/ssd2.csv", "text/csv")
    
def test_put_table_kvp():
    do_test_put_table_kvp_content_SiteInformation("/add/ssd.csv", "text/csv")
                             
def test_put_table_kvp2():
    do_test_put_table_kvp_content_SiteInformation("/add/ssd2.csv", "text/csv")

def test_post_table_kvp_json():
    do_test_post_table_kvp_content_SiteInformation("/add/ssd.json", "application/json")

def test_post_table_kvp2_json():
    do_test_post_table_kvp_content_SiteInformation("/add/ssd2.json", "application/json")
    
def test_put_table_kvp_json():
    do_test_put_table_kvp_content_SiteInformation("/add/ssd.json", "application/json")
                             
def test_put_table_kvp2_json():
    do_test_put_table_kvp_content_SiteInformation("/add/ssd2.json", "application/json")

def do_test_post_table_kvp_content_SiteInformation(src, mime):
    n = countrows(src, mime)
    do_test_post_table_kvp_SiteInformation(requests.post, src, mime, [
            {
                "ProposalID": "0",
                "siteNumber": str(i)
            } for i in range(1, n+1)
        ], [
            {
                "ProposalID": str(i),
                "siteNumber": str(j)
            } for i in [0, 1] for j in range(1, n+1)
        ])

def do_test_put_table_kvp_content_SiteInformation(src, mime):
    n = countrows(src, mime)
    do_test_post_table_kvp_SiteInformation(requests.put, src, mime, [
            {
                "ProposalID": "0",
                "siteNumber": str(i)
            } for i in range(1, n+1)
        ], [
            {
                "ProposalID": "1",
                "siteNumber": str(i)
            } for i in range(1, n+1)
        ])
    

def do_test_post_table_kvp_SiteInformation(verb, src, mime, content1, content2):
    do_test_post_table(verb, verb, src, mime, "SiteInformation", {"ProposalID": "0"}, {"ProposalID": "1"}, content1, content2)


def format_prep_req(req):
    """
    At this point it is completely built and ready
    to be fired; it is "prepared".

    However pay attention at the formatting used in 
    this function because it is programmed to be pretty 
    printed and may differ from the actual request.
    """
    return '{0}\n{1}\n{2}\n\n{3}'.format(
        '-----------START-----------',
        req.method + ' ' + req.url,
        '\n'.join('{0}: {1}'.format(k, v) for k, v in req.headers.items()),
        req.body.decode("utf-8"),
    )


def do_post_table(verb1, tablename, kvp1, src, cnttype):
    if os.environ.get("PRINT_REQUEST") == 1:
        if verb1 == requests.post:
            verb = "POST"
        elif verb1 == requests.put:
            verb = "PUT"
        else:
            raise RuntimeException("unsupported method")
        r = requests.Request(verb, "http://localhost:5000/table/" + tablename, files={
            "json": (None, json.dumps(kvp1), "application/json"),
            "data": (src, open(src, "rb"), "application/octet-stream"),
            "content-type": (None, cnttype, "text/plain")
        })

        prer = r.prepare()
        print(format_prep_req(prer))
        s = requests.Session()
        return s.send(prer)
    else:
        return verb1("http://localhost:5000/table/" + tablename, files={
            "json": (None, json.dumps(kvp1), "application/json"),
            "data": (src, open(src, "rb"), "application/octet-stream"),
            "content-type": (None, cnttype, "text/plain")
        })


def do_post_table_column(verb1, tablename, column, kvp1, src, cnttype):
    if os.environ.get("PRINT_REQUEST") == 1:
        if verb1 == requests.post:
            verb = "POST"
        elif verb1 == requests.put:
            verb = "PUT"
        else:
            raise RuntimeException("unsupported method")
        r = requests.Request(verb, "http://localhost:5000/table/" + tablename + "/column/" + column, files={
            "json": (None, json.dumps(kvp1), "application/json"),
            "data": (src, open(src, "rb"), "application/octet-stream"),
            "content-type": (None, cnttype, "text/plain")
        })

        prer = r.prepare()
        print(format_prep_req(prer))
        s = requests.Session()
        return s.send(prer)
    else:
        return verb1("http://localhost:5000/table/" + tablename + "/column/" + column, files={
            "json": (None, json.dumps(kvp1), "application/json"),
            "data": (src, open(src, "rb"), "application/octet-stream"),
            "content-type": (None, cnttype, "text/plain")
        })


def do_test_post_table(verb1, verb2, src, cnttype, tablename, kvp1, kvp2, content1, content2):
    
    ctx = reload.context()
    pServer = Process(target = server.server, args=[ctx], kwargs={})
    pServer.start()
    time.sleep(WAIT_PERIOD)
    pWorker = Process(target = reload.startWorker)
    pWorker.start()
    time.sleep(WAIT_PERIOD)
    try:
        print("get " + tablename)
        resp = requests.get("http://localhost:5000/table/" + tablename)
        assert(resp.json() == [])
        print("post " + tablename)
        resp = do_post_table(verb1, tablename, kvp1, src, cnttype)
        print(resp.text)
        assert resp.status_code == 200
        taskid = resp.json()
        assert isinstance(taskid, str)
        wait_for_task_to_finish(taskid)
        print("get " + tablename)
        resp = requests.get("http://localhost:5000/table/" + tablename)
        respjson = resp.json()
        assert(bag_contains(respjson, content1))
        print("post " + tablename)
        resp = do_post_table(verb2, tablename, kvp2, src, cnttype)
        assert resp.status_code == 200
        taskid = resp.json()
        assert isinstance(taskid, str)
        wait_for_task_to_finish(taskid)
        print("get " + tablename)
        resp = requests.get("http://localhost:5000/table/" + tablename)
        respjson = resp.json()
        assert(bag_contains(respjson, content2))
    finally:
        pWorker.terminate() 
        pServer.terminate()
        reload.clearTasks()
        reload.clearDatabase(ctx)
        reload.createTables(ctx)


def test_put_error_duplicate_column_upload():
    do_test_post_error(requests.put, "/add/ssd_error_duplicate_column_upload.csv", "text/csv", "SiteInformation", {}, 405, "duplicate header\\(s\\) in upload \\['siteNumber'\\]")


def test_put_error_duplicate_column_input():
    do_test_post_error(requests.put, "/add/ssd.csv", "text/csv", "SiteInformation", {"siteNumber": None}, 405, "duplicate header\\(s\\) in input \\['siteNumber'\\]")
    

def test_put_error_undefined_column_upload():
    do_test_post_error(requests.put, "/add/ssd_error_undefined_column_upload.csv", "text/csv", "SiteInformation", {}, 405, "undefined header\\(s\\) in input \\['header'\\] available \\[.*\\]")


def test_put_error_undefined_column_input():
    do_test_post_error(requests.put, "/add/ssd.csv", "text/csv", "SiteInformation", {"header": None}, 405, "undefined header\\(s\\) in input \\['header'\\] available \\[.*\\]")
    

def test_put_error_number_of_items0():
    do_test_post_error(requests.put, "/add/ssd_error_wrong_number_of_items0.csv", "text/csv", "SiteInformation", {"header": None}, 405, "row 0 number of items, expected 1, encountered 0")
    

def test_put_error_number_of_items2():
    do_test_post_error(requests.put, "/add/ssd_error_wrong_number_of_items2.csv", "text/csv", "SiteInformation", {"header": None}, 405, "row 0 number of items, expected 1, encountered 2")
    

def test_post_error_duplicate_column_upload():
    do_test_post_error(requests.post, "/add/ssd_error_duplicate_column_upload.csv", "text/csv", "SiteInformation", {}, 405, "duplicate header\\(s\\) in upload \\['siteNumber'\\]")


def test_post_error_duplicate_column_input():
    do_test_post_error(requests.post, "/add/ssd.csv", "text/csv", "SiteInformation", {"siteNumber": None}, 405, "duplicate header\\(s\\) in input \\['siteNumber'\\]")
    

def test_post_error_undefined_column_upload():
    do_test_post_error(requests.post, "/add/ssd_error_undefined_column_upload.csv", "text/csv", "SiteInformation", {}, 405, "undefined header\\(s\\) in input \\['header'\\] available \\[.*\\]")


def test_post_error_undefined_column_input():
    do_test_post_error(requests.post, "/add/ssd.csv", "text/csv", "SiteInformation", {"header": None}, 405, "undefined header\\(s\\) in input \\['header'\\] available \\[.*\\]")
    

def test_post_error_number_of_items0():
    do_test_post_error(requests.post, "/add/ssd_error_wrong_number_of_items0.csv", "text/csv", "SiteInformation", {"header": None}, 405, "row 0 number of items, expected 1, encountered 0")
    

def test_post_error_number_of_items2():
    do_test_post_error(requests.post, "/add/ssd_error_wrong_number_of_items2.csv", "text/csv", "SiteInformation", {"header": None}, 405, "row 0 number of items, expected 1, encountered 2")
    

def do_test_post_error(verb1, src, cnttype, tablename, kvp1, status_code, resp_text):
    
    ctx = reload.context()
    pServer = Process(target = server.server, args=[ctx], kwargs={})
    pServer.start()
    time.sleep(WAIT_PERIOD)
    pWorker = Process(target = reload.startWorker)
    pWorker.start()
    time.sleep(WAIT_PERIOD)
    try:
        resp = do_post_table(verb1, tablename, kvp1, src, cnttype)
        assert resp.status_code == status_code
        taskid = resp.text
        assert re.match(resp_text, taskid)
    finally:
        pWorker.terminate() 
        pServer.terminate()
        reload.clearTasks()
        reload.clearDatabase(ctx)
        reload.createTables(ctx)


def test_post_table_column():
    ctx = reload.context()
    fn = "/tmp/ssd1.csv"
    fn2 = "/tmp/ssd2.csv"
    csv1 = [
        [i, i] for i in range(10)
    ]
    csv2 = [
        [i, i+1] for i in range(1, 11)
    ]
    n = len(csv1)
    n2 = len(csv2)
    write_csv(fn, ["ProposalID", "siteNumber"], csv1)
    write_csv(fn2, ["ProposalID", "siteNumber"], csv2)
    tablename = "SiteInformation"
    column = "ProposalID"
    kvp1 = kvp2 = {}
    cnttype = "text/csv"
    verb1 = verb2 = requests.post
    content1 = [
        {
            "siteNumber": str(row[1]),
            "ProposalID": str(row[0])
        } for row in csv1
    ]
    content2 = [
        {
            "siteNumber": str(row[1]),
            "ProposalID": str(row[0])
        } for row in csv1 if row[0] not in list(map(lambda x: x[0], csv2))
    ] + [
        {
            "siteNumber": str(row[1]),
            "ProposalID": str(row[0])
        } for row in csv2
    ]

    pServer = Process(target = server.server, args=[ctx], kwargs={})
    pServer.start()
    time.sleep(WAIT_PERIOD)
    pWorker = Process(target = reload.startWorker)
    pWorker.start()
    time.sleep(WAIT_PERIOD)

    try:
        resp = do_post_table_column(verb1, tablename, column, kvp1, fn, cnttype)
        assert resp.status_code == 200
        taskid = resp.json()
        assert isinstance(taskid, str)
        wait_for_task_to_finish(taskid)
        print("get " + tablename)
        resp = requests.get("http://localhost:5000/table/" + tablename)
        respjson = resp.json()
        assert(bag_contains(respjson, content1))
        print("post " + tablename)
        resp = do_post_table_column(verb2, tablename, column, kvp2, fn2, cnttype)
        assert resp.status_code == 200
        taskid = resp.json()
        assert isinstance(taskid, str)
        wait_for_task_to_finish(taskid)
        print("get " + tablename)
        resp = requests.get("http://localhost:5000/table/" + tablename)
        respjson = resp.json()
        assert(bag_contains(respjson, content2))
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

    
def test_insert_table_non_ascii():
    do_test_insert_table("/add/ssd_non_ascii.csv", {})

    
def do_test_insert_table(src, kvp):
    
    ctx = reload.context()
    n = countrows(src, "text/csv")
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


def do_test_table(table_name, columns):
    ctx = reload.context()
    conn = connect(user=ctx["dbuser"], password=ctx["dbpass"], host=ctx["dbhost"], port=ctx["dbport"], dbname=ctx["dbname"])
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute('''SELECT * FROM "{0}"'''.format(table_name))
    rs = cur.fetchall()
    colnames = [desc[0] for desc in cur.description]
    for column in columns:
        assert column in colnames


def write_csv(fn, headers, rows):
    with open(fn, "w+") as outf:
        writer = csv.writer(outf)
        writer.writerow(headers)
        for row in rows:
            writer.writerow(row)

    
def test_update_table_column():
    ctx = reload.context()
    fn = "/tmp/ssd1.csv"
    fn2 = "/tmp/ssd2.csv"
    csv1 = [
        [i, i] for i in range(10)
    ]
    csv2 = [
        [i, i+1] for i in range(1, 11)
    ]
    n = len(csv1)
    n2 = len(csv2)
    write_csv(fn, ["ProposalID", "siteNumber"], csv1)
    write_csv(fn2, ["ProposalID", "siteNumber"], csv2)

    try:
        reload._updateDataIntoTableColumn(ctx, "SiteInformation", "ProposalID", fn, {})
        rows = reload.readDataFromTable(ctx, "SiteInformation")
        assert(bag_contains(rows, [
            {
                "siteNumber": str(row[1]),
                "ProposalID": str(row[0])
            } for row in csv1
        ]))
        reload._updateDataIntoTableColumn(ctx, "SiteInformation", "ProposalID", fn2, {})
        rows = reload.readDataFromTable(ctx, "SiteInformation")
        assert(bag_contains(rows, [
            {
                "siteNumber": str(row[1]),
                "ProposalID": str(row[0])
            } for row in csv1 if row[0] not in list(map(lambda x: x[0], csv2))
        ] + [
            {
                "siteNumber": str(row[1]),
                "ProposalID": str(row[0])
            } for row in csv2
        ]))
    finally:
        reload.clearDatabase(ctx)
        reload.createTables(ctx)
        os.unlink(fn)
        os.unlink(fn2)


def test_get_column_data_type_twice():
    ctx = reload.context()

    dt = reload.getColumnDataType(ctx, "SiteInformation", "ProposalID")
    assert dt == "bigint"

    dt = reload.getColumnDataType(ctx, "SiteInformation", "ProposalID")
    assert dt == "bigint"


def test_get_column_data_type_twice2():
    ctx = reload.context()
    fn = "/tmp/ssd1.csv"
    csv1 = [
        [i, i] for i in range(10)
    ]
    n = len(csv1)
    write_csv(fn, ["ProposalID", "siteNumber"], csv1)

    try:
        dt = reload.getColumnDataType(ctx, "SiteInformation", "ProposalID")
        assert dt == "bigint"

        reload._updateDataIntoTable(ctx, "SiteInformation", fn, {})
        
        dt = reload.getColumnDataType(ctx, "SiteInformation", "ProposalID")
        assert dt == "bigint"
    finally:
        reload.clearDatabase(ctx)
        reload.createTables(ctx)
        os.unlink(fn)


tables_yaml='''
- table: Sites
  columns:
  - siteId
  - siteName

- table: CTSAs
  columns:
  - ctsaId
  - ctsaName
  
- table: StudyProfile
  columns: 
  - ProposalID
  - network
  - tic
  - ric
  - type
  - linkedStudies
  - design
  - isRandomized
  - randomizationUnit
  - randomizationFeature
  - ascertainment
  - observations
  - isPilot
  - phase
  - isRegistry
  - ehrDataTransfer
  - isConsentRequired
  - isEfic
  - irbType
  - regulatoryClassification
  - clinicalTrialsGovId
  - isDsmbDmcRequired
  - initialParticipatingSiteCount
  - enrollmentGoal
  - initialProjectedEnrollmentDuration
  - leadPIs
  - awardeeSiteAcronym
  - primaryFundingType
  - isFundedPrimarilyByInfrastructure
  - fundingSource
  - fundingAwardDate
  - isPreviouslyFunded
  
- table: StudySites
  columns:
  - ProposalID
  - principalInvestigator
  - siteNumber
  - siteId
  - ctsaId
  - siteName
  - dateRegPacketSent
  - dateContractSent
  - dateIrbSubmission
  - dateIrbApproval
  - dateContractExecution
  - lpfv
  - dateSiteActivated
  - fpfv
  - patientsConsentedCount
  - patientsEnrolledCount
  - patientsWithdrawnCount
  - patientsExpectedCount
  - queriesCount
  - protocolDeviationsCount
'''

def test_tables():
    tabs = yaml.load(tables_yaml)
    for tab in tabs:
        do_test_table(tab["table"], tab["columns"])
    
