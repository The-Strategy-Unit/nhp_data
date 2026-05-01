"""Move data to NHP storage for use in the model.

You will need to manually generate a SAS token for both the data and the inputs
data containers, and store these in databricks secrets.

See the script update-sas-tokens.ps1.
"""

import os

from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import ContainerClient
from azure.storage.filedatalake import DataLakeServiceClient
from pyspark.dbutils import DBUtils
from tqdm.auto import tqdm

from nhp.data.get_spark import get_spark


def _move_files(
    path: str, account: str, container_name: str, sas: str, extract_version: str
):
    container = ContainerClient(
        f"https://{account}.blob.core.windows.net/", container_name, credential=sas
    )
    dlfs = DataLakeServiceClient(
        f"https://{account}.dfs.core.windows.net/", credential=sas
    ).get_file_system_client(container_name)

    # remove existing files, this may fail if the directory doesn't exist, but we can ignore that
    try:
        dlfs.delete_directory(extract_version)
    except ResourceNotFoundError:
        pass

    # find files to upload
    all_files = list()
    for root, _dirs, files in os.walk(path):
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
    account = dbutils.secrets.get("nhp", "MLCSU_INPUTS_DATA_ACCOUNT")
    sas = dbutils.secrets.get("nhp", "MLCSU_INPUTS_DATA_SAS")
    _move_files(f"{path}/inputs_data/dev", account, "inputs-data", sas, "dev")


def move_data(dbutils: DBUtils, path: str):
    account = dbutils.secrets.get("nhp", "MLCSU_DATA_ACCOUNT")
    sas = dbutils.secrets.get("nhp", "MLCSU_DATA_SAS")
    _move_files(f"{path}/model_data/dev", account, "data", sas, "dev")


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
