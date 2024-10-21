# Databricks notebook source
import pyspark.sql.functions as F
import pyspark.sql.types as T

# COMMAND ----------

nhp_apc = (
    spark.read.table("su_data.nhp.apc")
    .filter(F.col("fyear") == 201920)
    .filter(F.col("person_id").isNotNull())
)
diagnoses = spark.read.table("hes.silver.apc_diagnoses")
procedures = spark.read.table("hes.silver.apc_procedures")

# COMMAND ----------

# Using old file
df_sql = spark.read.option("header", True).csv(
    "/Volumes/su_data/nhp/reference_data/sql_mitigators_counts.csv"
)

# COMMAND ----------

# MAGIC %md
# MAGIC Load the counts of mitigators in the new process

# COMMAND ----------

df_db = (
    spark.read.table("su_data.nhp.apc_mitigators")
    .join(nhp_apc, "epikey")
    .groupBy("provider", "strategy")
    .agg(F.sum("sample_rate").alias("count_db"))
    .orderBy("provider", "strategy")
)

# COMMAND ----------

# MAGIC %md
# MAGIC Join the two together and find rows which exceed 5% difference

# COMMAND ----------

df = df_sql.join(df_db, ["provider", "strategy"]).withColumn(
    "diff", (F.col("count_db") / F.col("count_sql") - 1)
)


# COMMAND ----------

df_s = (
    df.groupBy("strategy")
    .agg(
        F.sum("count_sql").alias("count_sql"),
        F.sum("count_db").alias("count_db"),
    )
    .withColumn("diff", (F.col("count_db") / F.col("count_sql") - 1))
    .withColumn("check", F.abs(F.col("diff")) > 0.05)
    .filter(F.col("check"))
    .persist()
)

df_s.orderBy("strategy").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ignore any of the mitigators which overall do not match above, check by provider

# COMMAND ----------

(
    df.join(df_s, ["strategy"], how="anti")
    .withColumn("check", F.abs(F.col("diff")) > 0.05)
    .filter(F.col("check"))
    .filter(~F.col("strategy").rlike("^(evidence_based|ambulatory_emergency)"))
    .display()
)

# COMMAND ----------

(spark.read.table("su_data.nhp.opa").display())

# COMMAND ----------

(spark.read.table("su_data.nhp.opa").filter(F.col("has_procedures").isNull()).count())
