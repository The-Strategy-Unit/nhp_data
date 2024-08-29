# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Generate nhp inpatients
# MAGIC

# COMMAND ----------

from databricks.connect import DatabricksSession
from pyspark.sql import functions as F
from pyspark.sql.types import *  # pylint: disable-all
from delta.tables import DeltaTable

spark = DatabricksSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %run ./get_hes_apc

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Has Procedure

# COMMAND ----------

procedures = (
    spark.read.table("hes.silver.apc_procedures")
    .filter(F.col("procedure_order") == 1)
    .filter(F.col("procedure_code").rlike("^[^U-Z-]"))
    .select(F.col("epikey"))
)
# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Get Provider Main ICB

# COMMAND ----------

main_icbs = spark.read.csv(
    "/Volumes/su_data/nhp/reference_data/provider_main_icb.csv", header=True
).select("provider", F.col("icb").alias("main_icb"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Create Inpatients Data

# COMMAND ----------

hes_apc_processed = (
    hes_apc.withColumn(
        "age",
        F.when(
            (F.col("admiage") == 999) | F.col("admiage").isNull(),
            F.when(F.col("startage") > 7000, 0).otherwise(F.col("startage")),
        ).otherwise(F.col("admiage")),
    )
    .withColumn("age", F.when(F.col("age") > 90, 90).otherwise(F.col("age")))
    .withColumn(
        "hsagrp",
        F.when(F.col("classpat").isin(["3", "4"]), "reg")
        .when(F.col("admimeth").isin(["82", "83"]), "birth")
        .when(F.col("mainspef") == "420", "paeds")
        .when(
            (F.col("admimeth").startswith("3") | F.col("mainspef").isin(["501", "560"]))
            & (F.col("age") < 56),
            "maternity",
        )
        .when(F.col("admimeth").startswith("2"), "emerg")
        .when(F.col("admimeth") == "81", "transfer")
        .when(
            (F.col("admimeth").isin(["11", "12", "13"])) & (F.col("classpat") == "1"),
            "ordelec",
        )
        .when(
            (F.col("admimeth").isin(["11", "12", "13"])) & (F.col("classpat") == "2"),
            "daycase",
        )
        .otherwise(None),
    )
    .withColumn("is_wla", F.col("admimeth") == "11")
    .withColumn(
        "group",
        F.when(F.col("admimeth").startswith("1"), "elective")
        .when(F.col("admimeth").startswith("3"), "maternity")
        .otherwise("non-elective"),
    )
    .filter(F.col("speldur").isNotNull())
    .filter(F.col("hsagrp").isNotNull())
    # add has_procedure column
    .join(
        procedures
        .filter(F.col("procedure_order") == 1)
        .filter(~F.col("procedure_code").rlike("^O(1[1-46]|28|3[01346]|4[2-8]|5[23]|)"))
        .filter(~F.col("procedure_code").rlike("^X[6-9]"))
        .filter(~F.col("procedure_code").rlike("^[UYZ]"))
        .select(F.col("epikey"), F.lit(True).alias("has_procedure")),
        "epikey",
        "left"
    )
    # add is_main_icb column
    .join(
        main_icbs
        .select(
            F.col("provider"),
            F.col("main_icb").alias("icb"),
            F.lit(True).alias("is_main_icb")
        ),
        ["provider", "icb"],
        "left"
    )
    .na.fill(False, ["has_procedure", "is_main_icb"])
    .select(
        F.col("epikey"),
        F.col("fyear"),
        F.col("person_id_deid").alias("person_id"),
        F.col("admiage"),
        F.col("age"),
        F.col("sex"),
        # F.col("imd04_decile"),
        F.col("classpat"),
        F.col("mainspef"),
        F.col("tretspef"),
        F.col("hsagrp"),
        F.col("group"),
        F.col("admidate"),
        F.col("disdate"),
        F.col("speldur"),
        F.col("epitype"),
        F.col("admimeth"),
        F.col("dismeth"),
        F.col("provider"),
        F.col("sitetret"),
        F.col("lsoa11"),
        F.col("resladst_ons"),
        F.col("sushrg"),
        F.col("operstat"),
        F.col("icb"),
        F.col("is_wla"),
        F.col("is_main_icb"),
        F.col("has_procedure"),
    )
    .repartition("fyear", "provider")
)

# COMMAND ----------

target = (
    DeltaTable.createIfNotExists(spark)
    .tableName("su_data.nhp.apc")
    .addColumns(hes_apc_processed.schema)
    .execute()
)

(
    target.alias("t")
    .merge(
        hes_apc_processed.alias("s"),
        "t.epikey = s.epikey"
    )
    .whenMatchedUpdateAll(condition=" or ".join([f"t.{i} != s.{i}" for i in hes_apc_processed.columns]))
    .whenNotMatchedInsertAll()
    .whenNotMatchedBySourceDelete()
    .execute()
)
