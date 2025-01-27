# Databricks notebook source
# DBTITLE 1,generic imports
from databricks.connect import DatabricksSession
from pyspark import SparkContext

# DBTITLE 1,import inputs_data functions
from inputs_data.age_sex import generate_age_sex
from inputs_data.diagnoses import generate_diagnoses
from inputs_data.expat_repat import (
    generate_expat,
    generate_repat_local,
    generate_repat_nonlocal,
)
from inputs_data.procedures import generate_procedures
from inputs_data.rates import generate_rates
from inputs_data.wli import generate_wli

# COMMAND ----------

# DBTITLE 1,set up spark context
spark: SparkContext = DatabricksSession.builder.getOrCreate()
spark.catalog.setCurrentCatalog("su_data")
spark.catalog.setCurrentDatabase("nhp")

PATH = "/Volumes/su_data/nhp/old_nhp_inputs/dev/"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate parquet files

# COMMAND ----------

# DBTITLE 1,generate age/sex
generate_age_sex(spark, PATH)

# COMMAND ----------

# DBTITLE 1,generate rates
generate_rates(spark, PATH)

# COMMAND ----------

# DBTITLE 1,generate diagnoses
generate_diagnoses(spark, PATH)

# COMMAND ----------

# DBTITLE 1,generate procedures
generate_procedures(spark, PATH)

# COMMAND ----------

# DBTITLE 1,generate expat
generate_expat(spark, PATH)

# COMMAND ----------

# DBTITLE 1,generate repat (local)
generate_repat_local(spark, PATH)

# COMMAND ----------

# DBTITLE 1,generate repat (non-local)
generate_repat_nonlocal(spark, PATH)

# COMMAND ----------

# DBTITLE 1,generate WLI
generate_wli(spark, PATH)
