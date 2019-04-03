import os
import subprocess
import sys
from psycopg2 import connect
import schedule
import time
import requests

redcapApplicationToken = os.environ["REDCAP_APPLICATION_TOKEN"]
dbuser = os.environ["POSTGRES_USER"]
dbpass = os.environ["POSTGRES_PASSWORD"]
dbhost = os.environ["POSTGRES_HOST"]
dbname = os.environ["POSTGRES_DATABASE_NAME"]
reloaddb = os.environ["RELOAD_DATABASE"] == "1"
s = os.environ["RELOAD_SCHEDULE"] == "1"

assemblyPath = "TIC preprocessing-assembly-1.0.jar"
mappingInputFilePath = "HEAL data mapping.csv"
dataInputFilePath = "redcap_export.json"
dataDictionaryInputFilePath = "redcap_data_dictionary_export.json"
outputDirPath = "data"


import logging

logging.basicConfig(level=logging.DEBUG)

def runPipeline():
    reloaddb2 = True
    if reloaddb:
        if os.path.isfile(dataInputFilePath):
            os.remove(dataInputFilePath)
        if os.path.isfile(dataDictionaryInputFilePath):
            os.remove(dataDictionaryInputFilePath)
        print("downloading", dataInputFilePath)
        with open(dataInputFilePath, "wb+") as f:
            params = {
                "token" : redcapApplicationToken,
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
     

            r = requests.post("https://redcap.vanderbilt.edu/api/", data=params, headers=headers, stream=True)
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:  # filter out keep-alive new chunks
                    f.write(chunk)

        print("downloading", dataDictionaryInputFilePath)
        with open(dataDictionaryInputFilePath, "wb+") as f:
            params = {
                "token" : redcapApplicationToken,
                "content" : "metadata",
                "format" : "json",
                "returnFormat" : "json"
                }
            headers = {
                "Content-Type" : "application/x-www-form-urlencoded",
                "Accept" : "application/json"
                }
     

            r = requests.post("https://redcap.vanderbilt.edu/api/", data=params, headers=headers, stream=True)
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:  # filter out keep-alive new chunks
                    f.write(chunk)

        cp = subprocess.run(["spark-submit", "--driver-memory", "2g", "--executor-memory", "2g", "--master", "local[*]", "--class", "tic.Transform2", assemblyPath,
                   "--mapping_input_file", mappingInputFilePath, "--data_input_file", dataInputFilePath,
                   "--data_dictionary_input_file", dataDictionaryInputFilePath, "--output_dir", outputDirPath])

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


if s:
    schedule.every().day.at("00:00").do(runPipeline)
    while True:
        schedule.run_pending()
        time.sleep(1000)
else:
    runPipeline()

