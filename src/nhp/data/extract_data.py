"""Move data to NHP storage for use in the model.

You will need to manaually generate a SAS token for both the data and the inputs
data containers, and store these in databricks secrets.

The SAS tokens should have create and write permissions, and should have as
short an expiry time as possible (e.g. 1 day).

Once you have generated the tokens, run

    databricks secrets put-secret nhp MLCSU_DATA_SAS

and

    databricks secrets put-secret nhp MLCSU_INPUTS_DATA_SAS
"""

import os

from azure.storage.blob import BlobServiceClient
from pyspark.dbutils import DBUtils
from tqdm.auto import tqdm

from nhp.data.get_spark import get_spark


def _move_files(path: str, container, extract_version: str):
    all_files = list()
    for root, dirs, files in os.walk(path):
        for file in files:
            src = os.path.join(root, file)
            dst = os.path.relpath(src, path)

            all_files.append((src, dst))
    for src, dst in tqdm(all_files):
        with open(src, "rb") as data:
            container.upload_blob(
                name=f"{extract_version}/{dst}", data=data, overwrite=True
            )


def move_inputs_data(dbutils: DBUtils, path: str):
    sas = dbutils.secrets.get("nhp", "MLCSU_INPUTS_DATA_SAS")
    bsc = BlobServiceClient(sas)
    container = bsc.get_container_client("inputs-data")
    _move_files(f"{path}/inputs_data/dev", container, "dev")


def move_data(dbutils: DBUtils, path: str):
    sas = dbutils.secrets.get("nhp", "MLCSU_DATA_SAS")
    bsc = BlobServiceClient(sas)
    container = bsc.get_container_client("data")
    _move_files(f"{path}/model_data/dev", container, "dev")


def main():
    spark = get_spark()
    dbutils = DBUtils(spark)

    path = "/Volumes/udal_lake_mart/newhospitalprogramme/files"

    try:
        move_inputs_data(dbutils, path)
        move_data(dbutils, path)
    except Exception as e:
        print(
            "Error moving files, have you followed the instructions for generating the SAS token?"
        )
        raise e
