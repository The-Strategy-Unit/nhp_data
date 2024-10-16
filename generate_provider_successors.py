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

from typing import Iterator

import pyspark.sql.functions as F
import requests
from databricks.connect import DatabricksSession
from delta.tables import DeltaTable

from nhp_datasets.providers import providers

spark = DatabricksSession.builder.getOrCreate()
sc = spark.sparkContext

# COMMAND ----------


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
    if r["Roles"]["Role"][0]["id"] != "RO197":
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
)

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

# Check there are no cases remaining of one organisation being succeeded twice
assert (
    provider_successors.groupBy("old_code").count().filter(F.col("count") > 1).count()
    == 0
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## save table
# MAGIC
# MAGIC use an upsert to update the table


# COMMAND ----------

target = (
    DeltaTable.createIfNotExists(spark)
    .tableName("su_data.reference.provider_successors_dt")
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
