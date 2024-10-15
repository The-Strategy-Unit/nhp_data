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
    yield org_code
    r = (
        requests.get(
            f"https://uat.directory.spineservices.nhs.uk/ORD/2-0-0/organisations/{org_code}",
            timeout=10,
        )
        .json()["Organisation"]
        .get("Succs", {"Succ": []})
    )

    for s in r["Succ"]:
        if s["Type"] == "Predecessor":
            yield from get_predecessors(s["Target"]["OrgId"]["extension"])


# COMMAND ----------

provider_successors = spark.read.json(
    sc.parallelize(
        [{"old_code": q, "new_code": p} for p in providers for q in get_predecessors(p)]
    )
)
provider_successors.display()

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
