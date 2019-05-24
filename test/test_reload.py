import reload
import filecmp
from sqlalchemy import create_engine
import os
import os.path
import shutil
from multiprocessing import Process, RLock


def test_downloadData():
    os.chdir("/")
    ctx = reload.context()
    reload.downloadData(ctx)
    assert filecmp.cmp(ctx["dataInputFilePath"], "redcap/record.json")

    
def test_downloadDataDictionary():
    os.chdir("/")
    ctx = reload.context()
    reload.downloadDataDictionary(ctx)
    assert filecmp.cmp(ctx["dataDictionaryInputFilePath"], "redcap/metadata.json")


def test_etl():
    os.chdir("/")
    ctx = reload.context()
    reload.downloadData(ctx)
    reload.downloadDataDictionary(ctx)
    assert reload.etl(ctx)
    assert os.path.isfile("/data/tables/Proposal")
    with open("/data/tables/Proposal") as f:
        assert sum(1 for _ in f) == 2


def test_sync():
    os.chdir("/")
    ctx = reload.context()
    reload.clearDatabase(ctx)
    engine = create_engine("postgresql+psycopg2://" + ctx["dbuser"] + ":" + ctx["dbpass"] + "@" + ctx["dbhost"] + ":" + ctx["dbport"] + "/" + ctx["dbname"])
    conn = engine.connect()
    rs = conn.execute('''SELECT COUNT(*) FROM "Proposal"''').fetchall()
    assert len(rs) == 1
    for row in rs:
        assert row[0] == 0
    
    reload.downloadData(ctx)
    reload.downloadDataDictionary(ctx)
    reload.etl(ctx)
    assert reload.syncDatabase(ctx)
    rs = conn.execute('''SELECT COUNT(*) FROM "Proposal"''').fetchall()
    assert len(rs) == 1
    for row in rs:
        assert row[0] == 1


def test_back_data_dictionary():
    os.chdir("/")
    ctx = reload.context()
    reload.downloadDataDictionary(ctx)
    assert reload.backUpDataDictionary(ctx)
    directory = reload.dataDictionaryBackUpDirectory(ctx)
    shutil.rmtree(directory)


def test_back_data_dictionary_makedirs_exists():
    os.chdir("/")
    ctx = reload.context()
    directory = reload.dataDictionaryBackUpDirectory(ctx)
    os.makedirs(directory)
    reload.downloadDataDictionary(ctx)
    assert reload.backUpDataDictionary(ctx)
    shutil.rmtree(directory)


def test_back_up_database():
    test_sync()
    os.chdir("/")
    ctx = reload.context()
    directory = ctx["backupDir"]
    os.makedirs(directory)
    lock = RLock()
    assert reload.backUpDatabase(ctx, lock)
    shutil.rmtree(directory)


def test_restore_database():
    test_sync()
    os.chdir("/")
    ctx = reload.context()
    directory = ctx["backupDir"]
    os.makedirs(directory)
    lock = RLock()
    reload.backUpDatabase(ctx, lock)
    assert reload.restoreDatabase(ctx, lock)
    shutil.rmtree(directory)


