# Databricks notebook source
import pyspark.sql.functions as F
from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.getOrCreate()


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

# MAGIC %md
# MAGIC We ignore the following:
# MAGIC
# MAGIC - `evidence_based_interventions`: changed some definitions
# MAGIC - `ambulatory_emergency`: fixed bugs in old code
# MAGIC - `pre-op_los`: added more procedures to the exclude list (to match has_procedure
# MAGIC    definition)
# MAGIC - `alcohol_partially_attributable`: fixed bugs in reference table
# MAGIC - `excess_beddays`: fixed bug in logic when loading reference table (handling NULL value
# MAGIC    '-')
# MAGIC - `readmissions_within_28_days`: old logic was excluding well-baby admissions from the prior
# MAGIC   admissions, undercounting readmissions

# COMMAND ----------

mitigators_to_remove = [
    "evidence_based_interventions",
    "ambulatory_emergency_care",
    "pre-op_los",
    "alcohol_partially_attributable",
    "excess_beddays",
    "readmissions_within_28_days",
]

# COMMAND ----------

df_s = (
    df.groupBy("strategy")
    .agg(
        F.sum("count_sql").alias("count_sql"),
        F.sum("count_db").alias("count_db"),
    )
    .withColumn("diff", (F.col("count_db") / F.col("count_sql") - 1))
    .withColumn("check", F.abs(F.col("diff")) > 0.05)
    .persist()
)

(
    df_s.filter(~F.col("strategy").rlike(f"^({'|'.join(mitigators_to_remove)})_"))
    .orderBy(F.desc(F.abs("diff")))
    .display()
)

# COMMAND ----------

# MAGIC %md
# MAGIC by provider, only those with significant differences to check

# COMMAND ----------

(
    df.withColumn("check", F.abs(F.col("diff")) > 0.025)
    .filter(F.col("check"))
    .filter(~F.col("strategy").rlike(f"^({'|'.join(mitigators_to_remove)})_"))
    .orderBy("strategy")
    .display()
)
