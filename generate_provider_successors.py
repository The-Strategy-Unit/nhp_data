# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Generate provider successors
# MAGIC
# MAGIC This script takes a list of providers and queries the
# MAGIC [ODS API](https://digital.nhs.uk/developer/api-catalogue/organisation-data-service-ord)
# MAGIC to get the predecessors of that organisation, then reverses the list to get a mapping from
# MAGIC an organisation code to the current organisation .

# COMMAND ----------

import json
from functools import cache, reduce
from typing import Iterator

import pyspark.sql.functions as F
import requests
from databricks.connect import DatabricksSession
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, Window

from nhp_datasets.providers import providers

spark = DatabricksSession.builder.getOrCreate()
sc = spark.sparkContext

# COMMAND ----------


@cache
def get_predecessors(org_code: str) -> Iterator[str]:
    """_summary_

    Get a generator of an organisations predecessors

    Queries the ODS API to get a list containing the organisation and all of it's predecessors.
    If a predecessor itself has predecessors, these are also included in the results.

    :param org_code: The ODS organisation code
    :type org_code: str
    :yield: A list containing ODS organisation codes of the organisation and it's predecessors
    :rtype: Iterator[str]
    """
    r = requests.get(
        f"https://uat.directory.spineservices.nhs.uk/ORD/2-0-0/organisations/{org_code}",
        timeout=10,
    ).json()["Organisation"]

    # only include NHS Trusts
    if not any(map(lambda x: x["id"] == "RO197", r["Roles"]["Role"])):
        return

    # return the current organisation code
    yield org_code

    # iterate over the successor records
    # handle the case of not having any successor records
    if "Succs" not in r:
        return

    # extract just the predecessor records
    predecessor_records = filter(
        lambda x: x["Type"] == "Predecessor", r["Succs"]["Succ"]
    )
    # recursively call get_predecessors on the predecessors
    for i in predecessor_records:
        predecessor = i["Target"]["OrgId"]["extension"]
        # handle the edge case of Peninne / MFT / NCA
        if org_code == "R0A" and predecessor == "RW6":
            continue
        yield from get_predecessors(predecessor)


# COMMAND ----------

provider_successors = spark.read.json(
    sc.parallelize(
        [{"old_code": q, "new_code": p} for p in providers for q in get_predecessors(p)]
    )
).select("old_code", "new_code")

# show the results
provider_successors.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## handle edge cases

# COMMAND ----------

# Remove RAV, which closed in 1993, but was succeeded by RJ1 and RJ2
provider_successors = provider_successors.filter(F.col("old_code") != "RAV")

# COMMAND ----------

# there is at least one case of A<-B, and A<-C<-B, resulting in two records for A<-B. Remove these
provider_successors = provider_successors.distinct()

# COMMAND ----------

# MAGIC %md
# MAGIC ### find cases where there are multiple records for old_code -> new_code
# MAGIC
# MAGIC there can be two cases where this occurs:
# MAGIC
# MAGIC 1. an organisation is listed in our `providers.json` file, but it has now been succeeded.
# MAGIC    this needs to be removed from `providers.json`
# MAGIC 2. an organisation has been succeeded by two organisations. this case needs to be manually
# MAGIC    handled (e.g. Penine Acute Trust -> Manchester Univesity NHS FT and Northern Care
# MAGIC    Alliance NHS FT)
# MAGIC
# MAGIC we handle the first case, but the second case will raise an assertion error

# COMMAND ----------

to_remove = {
    r["old_code"]
    for r in (
        provider_successors.filter(F.col("old_code").isin(providers))
        .filter(F.col("new_code") != F.col("old_code"))
        .collect()
    )
}

# COMMAND ----------

with open(
    "/Volumes/su_data/nhp/reference_data/providers.json", "w", encoding="UTF-8"
) as f:
    json.dump(list(set(providers) - to_remove), f)

# COMMAND ----------

provider_successors = provider_successors.filter(~F.col("new_code").isin(to_remove))

# COMMAND ----------

w = Window.partitionBy("old_code")

multiple_successors = provider_successors.withColumn(
    "n", F.count("new_code").over(w)
).filter(F.col("n") > 1)

multiple_successors.display()

# COMMAND ----------

# Check there are no cases remaining of one organisation being succeeded twice
assert multiple_successors.count() == 0

# COMMAND ----------

# MAGIC %md
# MAGIC ## save table
# MAGIC
# MAGIC use an upsert to update the table, first show the updates/inserts/deletions that will occur
# MAGIC

# COMMAND ----------

source = spark.read.table("su_data.reference.provider_successors").alias("target")

tbls = [
    # updates
    (
        provider_successors.alias("new")
        .join(source, F.col("new.old_code") == F.col("target.old_code"))
        .filter(F.col("new.new_code") != F.col("target.new_code"))
        .select(
            F.lit("update").alias("type"),
            F.col("new.old_code"),
            F.col("target.new_code").alias("before"),
            F.col("new.new_code").alias("after"),
        )
    ),
    # inserts
    (
        provider_successors.alias("new")
        .join(source, F.col("new.old_code") == F.col("target.old_code"), "anti")
        .select(
            F.lit("insert").alias("type"),
            F.col("new.old_code"),
            F.lit(None).cast("string").alias("before"),
            F.col("new.new_code").alias("after"),
        )
    ),
    # deletes
    (
        source.join(
            provider_successors.alias("new"),
            F.col("target.old_code") == F.col("new.old_code"),
            "anti",
        ).select(
            F.lit("delete").alias("type"),
            F.col("target.old_code"),
            F.col("target.new_code").alias("before"),
            F.lit(None).cast("string").alias("after"),
        )
    ),
]

reduce(DataFrame.unionByName, tbls).display()

# COMMAND ----------

target = (
    DeltaTable.createIfNotExists(spark)
    .tableName("su_data.reference.provider_successors")
    .addColumns(provider_successors.schema)
    .execute()
)

(
    target.alias("t")
    .merge(provider_successors.alias("s"), "t.old_code = s.old_code")
    .whenMatchedUpdateAll(condition="s.new_code != t.new_code")
    .whenNotMatchedInsertAll()
    .whenNotMatchedBySourceDelete()
    .execute()
)
