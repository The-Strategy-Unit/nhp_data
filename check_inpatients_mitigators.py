# Databricks notebook source
import pyspark.sql.functions as F
import pyspark.sql.types as T

# COMMAND ----------

nhp_apc = spark.read.table("su_data.nhp.apc").filter(F.col("fyear") == 202223)
diagnoses = spark.read.table("hes.silver.apc_diagnoses")
procedures = spark.read.table("hes.silver.apc_procedures")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Load the counts of mitigators that exist in Sql currently

# COMMAND ----------

df_sql = (
    spark.read
    .option("header", True)
    .csv("/Volumes/su_data/nhp/reference_data/sql_mitigators_counts.csv")
)

# COMMAND ----------

# MAGIC %md
# MAGIC Load the counts of mitigators in the new process

# COMMAND ----------

df_db = (
    spark.read
    .table("su_data.nhp.apc_mitigators")
    .join(nhp_apc.filter(F.col("person_id").isNotNull()), "epikey")
    .groupBy("provider", "strategy")
    .agg(F.sum("sample_rate").alias("count_db"))
    .orderBy("provider", "strategy")
)

# COMMAND ----------

# MAGIC %md
# MAGIC Join the two together and find rows which exceed 5% difference

# COMMAND ----------

df = (
    df_sql
    .join(df_db, ["provider", "strategy"])
    .withColumn("diff", (F.col("count_db") / F.col("count_sql") - 1))
    .withColumn("check", F.abs(F.col("diff")) > 0.05)
)

(
    df
    .filter(F.col("check"))
    .filter(~F.col("strategy").rlike("^(evidence_based|medicines_related|ambulatory_emergency)"))
    .display()
)

# COMMAND ----------

# MAGIC %md
# MAGIC The same as above, but summarised to the mitigator level (ignoring different providers).
# MAGIC
# MAGIC Note the following we expect differences on:
# MAGIC
# MAGIC * ambulatory emergency care: changed the way we extract to use a more stable and workable code list. likely issues in the patterns used before
# MAGIC * evidence based interventions: updated code lists
# MAGIC * medicines related admissions: bug in Sql (explicit codes were not excluded from implicit properly)

# COMMAND ----------

df_s = (
    df.groupBy("strategy")
    .agg(
        F.sum("count_sql").alias("count_sql"),
        F.sum("count_db").alias("count_db"),
    )
    .withColumn("diff", (F.col("count_db") / F.col("count_sql") - 1))
    .withColumn("check", F.abs(F.col("diff")) > 0.05)
)

df_s.filter(F.col("check")).orderBy("strategy").display()
