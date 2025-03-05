"""Generate inpatient mitigators"""

import importlib
import os

from databricks.connect import DatabricksSession
from pyspark.context import SparkContext
from tqdm.auto import tqdm

import raw_data.mitigators as mitigators


def generate_inpatients_mitigators(spark: SparkContext) -> None:
    """Generrate Inpatients Mitigators"""
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
        except Exception as e:  # pylint: disable=broad-exception-caught
            errors[str(m)] = e

    print(errors)

    (
        spark.read.table("nhp.raw_data.apc_mitigators")
        .groupBy("type", "strategy")
        .count()
        .orderBy("type", "strategy")
        .display()
    )


if __name__ == "__main__":
    spark = DatabricksSession.builder.getOrCreate()
    generate_inpatients_mitigators(spark)
