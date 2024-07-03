# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Generate nhp apc view
# MAGIC
# MAGIC This script generates the hes_apc object

# COMMAND ----------

import json
from itertools import chain

from databricks.connect import DatabricksSession
from pyspark.sql import functions as F
from pyspark.sql.types import *  # pylint: disable-all

spark = DatabricksSession.builder.getOrCreate()

# COMMAND ----------

ccg_to_icb = spark.read.table("su_data.reference.ccg_to_icb").collect()

ccg_to_icb = {row["ccg"]: row["icb22cdh"] for row in ccg_to_icb}
icb_mapping = F.create_map([F.lit(x) for x in chain(*ccg_to_icb.items())])


# COMMAND ----------

provider_successors = spark.read.table(
    "su_data.reference.provider_successors"
).collect()

provider_successors = {row["old_code"]: row["new_code"] for row in provider_successors}
provider_successors_mapping = F.create_map(
    [F.lit(x) for x in chain(*provider_successors.items())]
)

# COMMAND ----------

with open(
    f"/Volumes/su_data/nhp/reference_data/providers.json", "r", encoding="UTF-8"
) as f:
    providers = json.load(f)

# COMMAND ----------

hes_apc = (
    spark.read.table("hes.silver.apc")
    .filter(F.col("last_episode_in_spell") == True)
    # remove well babies
    .filter(F.col("well_baby_ind") == "N")
    .filter(F.col("sushrg") != "PB03Z")
    .filter(~((F.col("tretspef") == "424") & (F.col("epitype") == "3")))
    # ---
    .filter(F.col("sex").isin(["1", "2"]))
    .withColumn(
        "provider",
        F.when(F.col("sitetret") == "RW602", "R0A")
        .when(F.col("sitetret") == "RM318", "R0A")
        .otherwise(provider_successors_mapping[F.col("procode3")]),
    )
    .withColumn("icb", icb_mapping[F.col("ccg_residence")])
    .filter(F.col("provider").isin(providers))
)
