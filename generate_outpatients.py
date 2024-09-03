# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Generate nhp inpatients
# MAGIC

# COMMAND ----------

from itertools import chain

from databricks.connect import DatabricksSession
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import *  # pylint: disable-all

from datasets.icbs import icb_mapping, main_icbs
from datasets.providers import provider_successors_mapping, providers

spark = DatabricksSession.builder.getOrCreate()

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


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Extract data


# COMMAND ----------

hes_opa_processed = (
    df.filter(F.col("sex").isin(["1", "2"]))
    .filter(F.col("atentype").isin(["1", "2", "3", "21", "22"]))
    .join(main_icbs, "provider", "left")
    .withColumn(
        "age",
        F.when(F.col("apptage") >= 7000, 0)
        .when(F.col("apptage") > 90, 90)
        .otherwise(F.col("apptage")),
    )
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
    .withColumn("is_face_to_face_appointment", 1 - F.col("is_tele_appointment"))
    .withColumn(
        "has_procedures",
        ~F.col("sushrg").rlike("^(WF|U)") & (F.col("is_face_to_face_appointment") == 1),
    )
    .groupBy(
        F.col("fyear"),
        F.col("provider"),
        F.col("age"),
        F.col("sex"),
        F.col("tretspef"),
        F.col("sitetret"),
        F.col("has_procedures"),
        F.col("is_main_icb"),
        F.col("is_surgical_specialty"),
        F.col("is_adult"),
        F.col("is_gp_ref"),
        F.col("is_cons_cons_ref"),
        F.col("is_first"),
    )
    .agg(
        F.sum("is_face_to_face_appointment").alias("attendances"),
        F.sum("is_tele_appointment").alias("tele_attendances"),
    )
    .withColumn(
        "type",
        F.concat(
            F.when(F.col("is_adult"), "adult").otherwise("child"),
            F.lit("-"),
            F.when(F.col("is_surgical_specialty"), "surgical").otherwise(
                "non-surgical"
            ),
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
    .repartition("fyear", "provider")
)


# COMMAND ----------

target = (
    DeltaTable.createIfNotExists(spark)
    .tableName("su_data.nhp.opa")
    .addColumns(hes_opa_processed.schema)
    .execute()
)

(
    target.alias("t")
    .merge(
        hes_opa_processed.alias("s"),
        " and ".join(
            f"t.{i} != s.{i}"
            for i in hes_opa_processed.columns
            if not i.endswith("attendances")
        ),
    )
    .whenMatchedUpdateAll(
        condition=" or ".join(
            f"s.{i} != t.{i}" for i in ["attendances", "tele_attendances"]
        )
    )
    .whenNotMatchedInsertAll()
    .whenNotMatchedBySourceDelete()
    .execute()
)
