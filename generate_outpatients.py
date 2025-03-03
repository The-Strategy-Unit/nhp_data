# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Generate nhp outpatients
# MAGIC

# COMMAND ----------

from itertools import chain

from databricks.connect import DatabricksSession
from pyspark.sql import functions as F
from pyspark.sql.types import *  # pylint: disable-all

from nhp_datasets.icbs import icb_mapping, main_icbs
from nhp_datasets.providers import get_provider_successors_mapping, providers

spark = DatabricksSession.builder.getOrCreate()
provider_successors_mapping = get_provider_successors_mapping()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Has Procedure

# COMMAND ----------

df = spark.read.table("hes.silver.opa")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Calculate provider column

# COMMAND ----------

df = df.withColumn(
    "provider",
    F.when(F.col("sitetret") == "RW602", "R0A")
    .when(F.col("sitetret") == "RM318", "R0A")
    .otherwise(provider_successors_mapping[F.col("procode3")]),
).filter(F.col("provider").isin(providers))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Calculate icb column

# COMMAND ----------

df = df.withColumn("icb", icb_mapping[F.col("ccg_residence")])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Create OPA Data
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Extract data
# MAGIC

# COMMAND ----------

hes_opa_ungrouped = (
    df.filter(F.col("sex").isin(["1", "2"]))
    .filter(F.col("atentype").isin(["1", "2", "21", "22"]))
    .join(main_icbs, "provider", "left")
    .withColumn(
        "age",
        F.when(F.col("apptage") >= 7000, 0)
        .when(F.col("apptage") > 90, 90)
        .otherwise(F.col("apptage")),
    )
    .filter(F.col("age") <= 120)
    .withColumn(
        "is_main_icb", F.when(F.col("icb") == F.col("main_icb"), True).otherwise(False)
    )
    .drop("main_icb")
    .withColumn("is_surgical_specialty", F.col("tretspef").rlike("^1(?!=(80|9[012]))"))
    .withColumn("is_adult", (F.col("apptage") >= 18) & (F.col("apptage") <= 7000))
    .withColumn(
        "is_gp_ref", (F.col("refsourc") == "03") & F.col("firstatt").isin(["1", "3"])
    )
    .withColumn(
        "is_cons_cons_ref",
        (F.col("refsourc") == "05")
        & F.col("firstatt").isin(["1", "3"])
        & F.col("sushrg").rlike("^WF0[12]B$"),
    )
    .withColumn("is_first", F.col("atentype").isin(["1", "21"]))
    .withColumn("is_tele_appointment", F.col("atentype").isin(["21", "22"]).cast("int"))
    .withColumn(
        "has_procedures",
        ~F.col("sushrg").rlike("^(WF|U)") & (F.col("is_tele_appointment") != 1),
    )
    .withColumn("attendance", 1 - F.col("is_tele_appointment"))
    .withColumn("tele_attendance", F.col("is_tele_appointment"))
    .select(
        F.col("attendkey"),
        F.col("fyear"),
        F.col("procode3"),
        F.col("provider"),
        F.col("age"),
        F.col("sex").cast("int"),
        F.col("imd_decile"),
        F.col("imd_quintile"),
        F.col("tretspef"),
        F.col("sitetret"),
        F.col("has_procedures"),
        F.col("sushrg"),
        F.col("icb"),
        F.col("is_main_icb"),
        F.col("is_surgical_specialty"),
        F.col("is_adult"),
        F.col("is_gp_ref"),
        F.col("is_cons_cons_ref"),
        F.col("is_first"),
        F.col("attendance"),
        F.col("tele_attendance"),
    )
    .withColumn(
        "type",
        F.when(
            F.col("tretspef").isin(["424", "501", "505", "560"]), "maternity"
        ).otherwise(
            F.concat(
                F.when(F.col("is_adult"), "adult").otherwise("child"),
                F.lit("_"),
                F.when(F.col("is_surgical_specialty"), "surgical").otherwise(
                    "non-surgical"
                ),
            )
        ),
    )
    .withColumn(
        "group",
        F.when(F.col("has_procedures"), "procedure")
        .when(F.col("is_first"), "first")
        .otherwise("followup"),
    )
    .withColumn(
        "hsagrp", F.concat(F.lit("op_"), F.col("type"), F.lit("_"), F.col("group"))
    )
)

# COMMAND ----------

(
    hes_opa_ungrouped.write.partitionBy("fyear", "provider")
    .mode("overwrite")
    .saveAsTable("su_data.nhp.opa_ungrouped")
)

# COMMAND ----------

hes_opa_processed = (
    spark.read.table("su_data.nhp.opa_ungrouped")
    .groupBy(
        F.col("fyear"),
        F.col("provider"),
        F.col("age"),
        F.col("sex"),
        F.col("imd_quintile"),
        F.col("tretspef"),
        F.col("sitetret"),
        F.col("type"),
        F.col("group"),
        F.col("hsagrp"),
        F.col("has_procedures"),
        F.col("sushrg").substr(1, 4).alias("sushrg_trimmed"),
        F.col("is_main_icb"),
        F.col("is_surgical_specialty"),
        F.col("is_adult"),
        F.col("is_gp_ref"),
        F.col("is_cons_cons_ref"),
        F.col("is_first"),
    )
    .agg(
        F.sum("attendance").alias("attendances"),
        F.sum("tele_attendance").alias("tele_attendances"),
    )
    .withColumn("index", F.expr("uuid()"))
    .repartition("fyear", "provider")
)

# COMMAND ----------

(
    hes_opa_processed.write.partitionBy("fyear", "provider")
    .mode("overwrite")
    .saveAsTable("su_data.nhp.opa")
)
