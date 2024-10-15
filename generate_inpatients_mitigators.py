# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Generate inpatient mitigators
# MAGIC

# COMMAND ----------

# MAGIC %pip install tqdm

# COMMAND ----------

import importlib
import os

from databricks.connect import DatabricksSession
from tqdm.auto import tqdm

import mitigators

spark = DatabricksSession.builder.getOrCreate()

# COMMAND ----------

path = ["mitigators", "ip"]

for i in ["activity_avoidance", "efficiency"]:
    for j in sorted(os.listdir("/".join(path + [i]))):
        if j == "__init__.py":
            continue
        module = ".".join(path + [i, j])[:-3]
        try:
            importlib.import_module(module)
        except:  # pylint: disable=bare-except
            print(f"Error: {module}")

# COMMAND ----------

all_mitigators = [
    v2
    for v1 in mitigators.__registered_mitigators.values()  # pylint: disable=protected-access
    for v2 in v1.values()
]

errors = {}

for m in (pbar := tqdm(all_mitigators)):
    pbar.set_description(f"Processing {m}:")

    try:
        m.save()
    except Exception as e:
        errors[str(m)] = e

errors

# COMMAND ----------

(
    spark.read.table("su_data.nhp.apc_mitigators")
    .groupBy("type", "strategy")
    .count()
    .orderBy("type", "strategy")
    .display()
)
