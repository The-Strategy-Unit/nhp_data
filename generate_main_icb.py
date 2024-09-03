# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Generate main ICB
# MAGIC
# MAGIC This script finds the "main ICB" for each provider

# COMMAND ----------

import os

from databricks.sdk.runtime import dbutils
from pyspark.sql import Window
from pyspark.sql import functions as F

from datasets.apc import hes_apc

# COMMAND ----------

SAVE_PATH = "/Volumes/su_data/nhp/reference_data/provider_main_icb.csv"

if os.path.exists(SAVE_PATH):
    dbutils.notebook.exit("data already exists: skipping")


# COMMAND ----------

w = Window.partitionBy("provider").orderBy(F.desc("count"))

main_icbs = (
    hes_apc.filter(F.col("icb").isNotNull())  # pylint: disable=undefined-variable
    .groupBy("provider", "icb")
    .count()
    .withColumn("row", F.row_number().over(w))
    .filter(F.col("row") == 1)
    .drop("row", "count")
    .toPandas()
)


# COMMAND ----------

main_icbs.to_csv(SAVE_PATH)
