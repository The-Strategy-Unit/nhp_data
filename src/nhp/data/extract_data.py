"""Move data to NHP storage for use in the model.

You will need to manually generate a SAS token for both the data and the inputs
data containers, and store these in databricks secrets.

The SAS tokens should have create, write, and delete permissions, and should
have as short an expiry time as possible (e.g. 1 day).

Once you have generated the tokens, run

    databricks secrets put-secret nhp MLCSU_DATA_SAS

and

    databricks secrets put-secret nhp MLCSU_INPUTS_DATA_SAS
"""

import os

from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import ContainerClient
from azure.storage.filedatalake import DataLakeServiceClient
from pyspark.dbutils import DBUtils
from tqdm.auto import tqdm

from nhp.data.get_spark import get_spark


def _move_files(path: str, connection_url: str, extract_version: str):
    container = ContainerClient.from_container_url(connection_url)
    dlfs = DataLakeServiceClient(
        connection_url.replace("blob", "dfs")
    ).get_file_system_client(".")

    # remove existing files, this may fail if the directory doesn't exist, but we can ignore that
    try:
        dlfs.delete_directory(extract_version)
    except ResourceNotFoundError:
        pass

    # find files to upload
    all_files = list()
    for root, dirs, files in os.walk(path):
        for file in files:
            src = os.path.join(root, file)
            dst = os.path.relpath(src, path)

            all_files.append((src, dst))
    # upload files
    for src, dst in tqdm(all_files):
        with open(src, "rb") as data:
            container.upload_blob(
                name=f"{extract_version}/{dst}", data=data, overwrite=True
            )


def move_inputs_data(dbutils: DBUtils, path: str):
    sas = dbutils.secrets.get("nhp", "MLCSU_INPUTS_DATA_SAS")
    _move_files(f"{path}/inputs_data/dev", sas, "dev")


def move_data(dbutils: DBUtils, path: str):
    sas = dbutils.secrets.get("nhp", "MLCSU_DATA_SAS")
    _move_files(f"{path}/model_data/dev", sas, "dev")


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
