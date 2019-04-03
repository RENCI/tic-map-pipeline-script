import os
import subprocess
import sys
from psycopg2 import connect
import schedule
import time

redcapApplicationToken = os.environ["REDCAP_APPLICATION_TOKEN"]
dbuser = os.environ["POSTGRES_USER"]
dbpass = os.environ["POSTGRES_PASSWORD"]
dbhost = os.environ["POSTGRES_HOST"]
dbname = os.environ["POSTGRES_DATABASE_NAME"]
reloaddb = os.environ["RELOAD_DATABASE"] == "1"

assemblyPath = "TIC preprocessing-assembly-1.0.jar"
mappingInputFilePath = "HEAL data mapping.csv"
dataInputFilePath = "redcap_export.json"
dataDictionaryInputFilePath = "redcap_data_dictionary_export.json"
outputDirPath = "data"



def runPipeline():
    reloaddb2 = True
    if reloaddb:
        cp = subprocess.run(["spark-submit", "--driver-memory", "2g", "--executor-memory", "2g", "--master", "local[*]", "--class", "tic.Transform2", assemblyPath,
                   "--mapping_input_file", mappingInputFilePath, "--data_input_file", dataInputFilePath,
                   "--data_dictionary_input_file", dataDictionaryInputFilePath, "--output_dir", outputDirPath, "--redcap_application_token", redcapApplicationToken])

        if cp.returncode != 0:
            sys.stderr.write("encountered an error: " + str(cp.returncode))
            reloaddb2 = False

    if reloaddb2:
        conn = connect(user=dbuser, password=dbpass, host=dbhost, dbname=dbname)
        cursor = conn.cursor()
        tables = list(filter(lambda x : not x.startswith("."), os.listdir("data/tables")))
        for f in tables:
            cursor.execute("DELETE FROM \"" + f + "\"")
        cursor.close()
        conn.commit()
        conn.close()
        cp = subprocess.run(["csvsql", "--db", "postgres://"+dbuser+":" + dbpass + "@" + dbhost +"/" + dbname, "--insert", "--no-create", "-p", "\\", "-e", "utf8"] + ["data/tables/" + x for x in tables])


# schedule.every().day.at("00:00").do(runPipeline)
schedule.every(1).minute.do(runPipeline)

while True:
    print("starting job")
    schedule.run_pending()
    time.sleep(1)