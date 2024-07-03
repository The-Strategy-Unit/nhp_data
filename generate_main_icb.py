# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Generate main ICB
# MAGIC
# MAGIC This script finds the "main ICB" for each provider

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run ./get_hes_apc

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

main_icbs.to_csv("/Volumes/su_data/nhp/reference_data/provider_main_icb.csv")
