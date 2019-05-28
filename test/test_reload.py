import reload
import filecmp
from sqlalchemy import create_engine
import os
import os.path
import shutil
from multiprocessing import Process, RLock
import datetime


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
    reload.createTables(ctx)

    
def test_etl():
    os.chdir("/")
    ctx = reload.context()
    shutil.copy("redcap/record.json", ctx["dataInputFilePath"])
    shutil.copy("redcap/metadata.json", ctx["dataDictionaryFilePath"])
    assert reload.etl(ctx)
    assert os.path.isfile("/data/tables/Proposal")
    with open("/data/tables/Proposal") as f:
        assert sum(1 for _ in f) == 2
    os.remove(ctx["dataInputFilePath"])
    os.remove(ctx["dataDictionaryFilePath"])
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
    if cleanup:
        reload.clearDatabase(ctx)
        reload.createTables(ctx)


def test_back_data_dictionary():
    os.chdir("/")
    ctx = reload.context()
    shutil.copy("redcap/metadata.json", ctx["dataDictionaryFilePath"])
    assert reload.backUpDataDictionary(ctx)
    directory = reload.dataDictionaryBackUpDirectory(ctx)
    os.remove(ctx["dataDictionaryFilePath"])
    shutil.rmtree(directory)


def test_back_data_dictionary_makedirs_exists():
    os.chdir("/")
    ctx = reload.context()
    directory = reload.dataDictionaryBackUpDirectory(ctx)
    os.makedirs(directory)
    shutil.copy("redcap/metadata.json", ctx["dataDictionaryFilePath"])
    assert reload.backUpDataDictionary(ctx)
    os.remove(ctx["dataDictionaryFilePath"])
    shutil.rmtree(directory)


def test_back_up_database():
    print("test_back_up_database")
    test_sync(False)
    os.chdir("/")
    ctx = reload.context()
    lock = RLock()
    ts = str(datetime.datetime.now())
    assert reload.backUpDatabase(ctx, lock, ts)
    return ts


def test_restore_database():
    print("test_restore_database")
    ts = test_back_up_database()
    assert reload.restoreDatabase(ctx, lock, ts)




