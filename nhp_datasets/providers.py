import json
from itertools import chain

from databricks.connect import DatabricksSession
from pyspark.sql import functions as F
from pyspark.sql.types import *  # pylint: disable-all

spark = DatabricksSession.builder.getOrCreate()


def get_provider_successors_mapping():
    provider_successors = spark.read.table(
        "su_data.reference.provider_successors"
    ).collect()

    provider_successors = {
        row["old_code"]: row["new_code"] for row in provider_successors
    }
    provider_successors_mapping = F.create_map(
        [F.lit(x) for x in chain(*provider_successors.items())]
    )

    return provider_successors_mapping


with open(
    f"/Volumes/su_data/nhp/reference_data/providers.json", "r", encoding="UTF-8"
) as f:
    providers = json.load(f)
