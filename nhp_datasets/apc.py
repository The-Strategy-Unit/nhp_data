from databricks.connect import DatabricksSession
from pyspark.sql import functions as F
from pyspark.sql.types import *  # pylint: disable-all

import nhp_datasets.providers  # pylint: disable=unused-import
from nhp_datasets.icbs import icb_mapping

spark = DatabricksSession.builder.getOrCreate()

hes_apc = (
    spark.read.table("hes.silver.apc")
    .add_provider_column(spark)
    .filter(F.col("last_episode_in_spell") == True)
    # remove well babies
    .filter(F.col("well_baby_ind") == "N")
    .filter((F.col("sushrg") != "PB03Z") | F.col("sushrg").isNull())
    .filter(~((F.col("tretspef") == "424") & (F.col("epitype") == "3")))
    # ---
    .filter(F.col("sex").isin(["1", "2"]))
    .withColumn("icb", icb_mapping[F.col("ccg_residence")])
)

apc_primary_procedures = (
    spark.read.table("hes.silver.apc_procedures")
    .filter(F.col("procedure_order") == 1)
    .filter(~F.col("procedure_code").rlike("^O(1[1-46]|28|3[01346]|4[2-8]|5[23])"))
    .filter(~F.col("procedure_code").rlike("^X[6-9]"))
    .filter(~F.col("procedure_code").rlike("^[UYZ]"))
)
