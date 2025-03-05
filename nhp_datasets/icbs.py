from itertools import chain

from databricks.connect import DatabricksSession
from pyspark.sql import functions as F
from pyspark.sql.types import *  # pylint: disable-all

spark = DatabricksSession.builder.getOrCreate()

ccg_to_icb = spark.read.table("strategyunit.reference.ccg_to_icb").collect()

ccg_to_icb = {row["ccg"]: row["icb22cdh"] for row in ccg_to_icb}
icb_mapping = F.create_map([F.lit(x) for x in chain(*ccg_to_icb.items())])

main_icbs = spark.read.csv(
    "/Volumes/nhp/reference/files/provider_main_icb.csv", header=True
).select("provider", F.col("icb").alias("main_icb"))
