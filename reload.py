import pause
import datetime
import os
import subprocess
import sys
import pandas as pd

redcapApplicationToken = os.environ["REDCAP_APPLICATION_TOKEN"]
assemblyPath = "TIC preprocessing-assembly-1.0.jar"
mappingInputFilePath = "HEAL data mapping.csv"
dataInputFilePath = "redcap_export.json"
dataDictionaryInputFilePath = "redcap_data_dictionary_export.json"
outputDirPath = "data"

def runPipeline():
    cp = subprocess.run(["spark-submit", "--driver-memory", "2g", "--executor-memory", "2g", "--master", "local[*]", "--class", "tic.Transform2", assemblyPath,
                   "--mapping_input_file", mappingInputFilePath, "--data_input_file", dataInputFilePath,
                   "--data_dictionary_input_file", dataDictionaryInputFilePath, "--output_dir", outputDirPath, "--redcap_application_token", redcapApplicationToken])

    if cp.returncode != 0:
        sys.stderr.write("encountered an error: " + str(cp.returncode))


tomorrow = None
tomorrow2 = None

while True:
    while tomorrow2 == tomorrow:
        print("checking time tomorrow = ", tomorrow, "tomorrow2 = ", tomorrow2)
        tomorrow2 = pd.Timestamp(datetime.date.today() + datetime.timedelta(days=1))

    print("waiting tomorrow = ", tomorrow, "tomorrow2 = ", tomorrow2)
    tomorrow = tomorrow2
    # pause.until(tomorrow)
    print("starting job")
    runPipeline()
    break
