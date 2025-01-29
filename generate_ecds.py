# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Generate nhp ecds (aae)
# MAGIC

# COMMAND ----------

from databricks.connect import DatabricksSession
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *  # pylint: disable-all

spark = DatabricksSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## ECDS source data

# COMMAND ----------

df_aae = (
    spark.read.table("su_data.nhp.aae_ungrouped")
    .filter(F.col("fyear") < 201920)
    .drop("aekey")
    .withColumn("acuity", F.lit(None).cast("string"))
)

df_ecds = (
    spark.read.table("su_data.nhp.ecds_ungrouped")
    .filter(F.col("fyear") >= 201920)
    .drop("ec_ident")
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Extract data


# COMMAND ----------

hes_ecds_processed = (
    DataFrame.unionByName(df_aae, df_ecds)
    .groupBy(
        F.col("fyear"),
        F.col("provider"),
        F.col("age"),
        F.col("sex"),
        F.col("sitetret"),
        F.col("aedepttype"),
        F.col("attendance_category"),
        F.col("acuity"),
        F.col("tretspef"),
        F.col("group"),
        F.col("type"),
        F.col("hsagrp"),
        F.col("is_main_icb"),
        F.col("is_adult"),
        F.col("is_ambulance"),
        F.col("is_frequent_attender"),
        F.col("is_low_cost_referred_or_discharged"),
        F.col("is_left_before_treatment"),
        F.col("is_discharged_no_treatment"),
    )
    .agg(F.sum("arrival").alias("arrivals"))
    .repartition("fyear", "provider")
)

# COMMAND ----------

(
    hes_ecds_processed.withColumn("index", F.expr("uuid()"))
    .write.partitionBy("fyear", "provider")
    .mode("overwrite")
    .saveAsTable("su_data.nhp.ecds")
)
