from databricks.connect import DatabricksSession
from pyspark.sql import functions as F
from pyspark.sql.types import *  # pylint: disable-all

from nhp_datasets.icbs import icb_mapping
from nhp_datasets.providers import get_provider_successors_mapping, providers

spark = DatabricksSession.builder.getOrCreate()
provider_successors_mapping = get_provider_successors_mapping()

hes_apc = (
    spark.read.table("hes.silver.apc")
    .filter(F.col("last_episode_in_spell") == True)
    # remove well babies
    .filter(F.col("well_baby_ind") == "N")
    .filter((F.col("sushrg") != "PB03Z") | F.col("sushrg").isNull())
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

apc_primary_procedures = (
    spark.read.table("hes.silver.apc_procedures")
    .filter(F.col("procedure_order") == 1)
    .filter(~F.col("procedure_code").rlike("^O(1[1-46]|28|3[01346]|4[2-8]|5[23])"))
    .filter(~F.col("procedure_code").rlike("^X[6-9]"))
    .filter(~F.col("procedure_code").rlike("^[UYZ]"))
)
