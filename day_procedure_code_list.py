# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from scipy.stats import binomtest
import pandas as pd

# COMMAND ----------

# MAGIC %md
# MAGIC Notes:
# MAGIC * should we change the code list to match the `has_procedure` code list?

# COMMAND ----------

import json

with open("/Volumes/su_data/nhp/reference_data/day_procedures.json") as f:
  old_codes = json.load(f)

# COMMAND ----------

# MAGIC %md
# MAGIC # Day Surgery Analysis
# MAGIC
# MAGIC The following report is an analysis of all procedures carried out in the year 2019/20 recorded in HES in either the Outpatients or Inpatients datasets.
# MAGIC
# MAGIC For inpatients we use spell level data, elective admissions, and only include patient classes `1` and `2`.
# MAGIC
# MAGIC For outpatients we include any attendances where the HRG did not start with `"WF"` or `"U"`.

# COMMAND ----------

providers = (
  spark.read.table("su_data.reference.provider_successors")
  .select(F.col("old_code").alias("procode3"))
)

# COMMAND ----------

apc = (
  spark.read.table("su_data.nhp.apc")
  .filter(F.col("fyear") == 201920)
  .filter(F.col("classpat").isin("1", "2"))
  .filter(F.col("admimeth").startswith("1"))
)

apc_procedures = (
  spark.read.table("hes.silver.apc_procedures")
  .filter(F.col("fyear") == 201920)
  .filter(F.col("procedure_order") == 1)
  # .filter(F.col("procedure_code").rlike("^[A-T][0-9]{3}$"))
  .filter(~F.col("procedure_code").rlike("^O(1[1-46]|28|3[01346]|4[2-8]|5[23])"))
  .filter(~F.col("procedure_code").rlike("^X[6-9]"))
  .filter(~F.col("procedure_code").rlike("^[UYZ]"))
)

opa = (
  spark.read.table("hes.silver.opa")
  .join(providers, on=["procode3"], how="semi")
  .filter(F.col("fyear") == 201920)
  .filter(~F.col("sushrg").startswith("WF"))
  .filter(~F.col("sushrg").startswith("U"))
  .filter(F.col("sex").isin(["1", "2"]))
  .filter(F.col("atentype").isin(["1", "2"]))
)

opa_procedures = (
  spark.read.table("hes.silver.opa_procedures")
  .filter(F.col("fyear") == 201920)
  .filter(F.col("procedure_order") == 1)
  # .filter(F.col("procedure_code").rlike("^[A-T][0-9]{3}$"))
  .filter(~F.col("procedure_code").rlike("^O(1[1-46]|28|3[01346]|4[2-8]|5[23])"))
  .filter(~F.col("procedure_code").rlike("^X[6-9]"))
  .filter(~F.col("procedure_code").rlike("^[UYZ]"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Extraction
# MAGIC
# MAGIC ### Outpatients

# COMMAND ----------

df_op = (
  opa_procedures
  .join(opa, on=["attendkey"], how="semi")
  .groupBy("procedure_code")
  .agg(F.count("attendkey").alias("op"))
  .persist()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inpatients

# COMMAND ----------

df_ip = (
  apc_procedures
  .join(apc, on=["epikey"], how="inner")
  .withColumn("type", F.when(F.col("classpat") == "1", F.lit("ip")).otherwise("dc"))
  .groupBy("procedure_code")
  .pivot("type")
  .count()
  .persist()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Combined data

# COMMAND ----------

df = (
  df_ip.join(df_op, "procedure_code", how="outer")
  .fillna(0)
  .withColumn("total", F.col("dc") + F.col("ip") + F.col("op"))
  .toPandas()
)

df["total"].sum()

# COMMAND ----------

# MAGIC %md
# MAGIC Exclude any procedures which have not been performed at least 100 times in the year

# COMMAND ----------

df = df[df["total"] >= 100]

df["total"].sum()

# COMMAND ----------

# MAGIC %md
# MAGIC Use a confidence level of 0.001

# COMMAND ----------

p_value = 0.001

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calculating what group each procedure is in
# MAGIC
# MAGIC First we find procedures which are performed in an outpatient or daycase setting more that 50% of the time. We then do the same, but 5% of the time, ignoring procedures that are in a "usually" category

# COMMAND ----------

df2 = (
  df
  .melt(id_vars=["procedure_code", "total"], value_vars=["op", "dc"], var_name="type")
  .assign(pu = lambda x: x.apply(lambda y: binomtest(y.value, y.total, p=0.5, alternative="greater").pvalue, axis=1))
  .assign(po = lambda x: x.apply(lambda y: binomtest(y.value, y.total, p=0.05, alternative="greater").pvalue, axis=1))
)

usually_dc = set(df2.loc[(df2.pu < p_value) & (df2.type == "dc"), "procedure_code"])
usually_op = set(df2.loc[(df2.pu < p_value) & (df2.type == "op"), "procedure_code"])

occasionally_dc = set(df2.loc[(df2.po < p_value) & (df2.type == "dc"), "procedure_code"]) - usually_dc - usually_op
occasionally_op = set(df2.loc[(df2.po < p_value) & (df2.type == "op"), "procedure_code"]) - usually_dc - usually_op

# COMMAND ----------

pd.concat(
  [
    pd.Series(
      {
        "usually_dc": len(usually_dc),
        "usually_op": len(usually_op),
        "occasionally_dc": len(occasionally_dc),
        "occasionally_op": len(occasionally_op)
      },
      name="new"
    ),
    pd.Series(
      {
        k: len(v) for k, v in old_codes.items()
      },
      name="old"
    )
  ],
  axis=1
)

# COMMAND ----------

len(usually_op - set(old_codes["usually_op"])), len(set(old_codes["usually_op"]) - usually_op)

# COMMAND ----------

len(usually_dc - set(old_codes["usually_dc"])), len(set(old_codes["usually_dc"]) - usually_dc)

# COMMAND ----------

len(occasionally_op - set(old_codes["occasionally_op"])), len(set(old_codes["occasionally_op"]) - occasionally_op)

# COMMAND ----------

len(occasionally_dc - set(old_codes["occasionally_dc"])), len(set(old_codes["occasionally_dc"]) - occasionally_dc)
