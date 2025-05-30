from databricks.connect import DatabricksSession
from pyspark.sql import functions as F
from pyspark.sql.types import *  # pylint: disable-all

from nhp_datasets.icbs import icb_mapping
from nhp_datasets.providers import read_data_with_provider

spark = DatabricksSession.builder.getOrCreate()

hes_apc = (
    read_data_with_provider(spark, "hes.silver.apc")
    .filter(F.col("last_episode_in_spell"))
    # remove well babies
    .filter(F.col("well_baby_ind") == "N")
    .filter((F.col("sushrg") != "PB03Z") | F.col("sushrg").isNull())
    .filter(~((F.col("tretspef") == "424") & (F.col("epitype") == "3")))
    # ---
    .filter(F.col("sex").isin(["1", "2"]))
    .withColumn(
        "icb",
        # use the ccg of residence if available, otherwise use the ccg of responsibility
        F.coalesce(
            icb_mapping[F.col("ccg_residence")],
            icb_mapping[F.col("ccg_responsibility")],
        ),
    )
)

apc_primary_procedures = (
    spark.read.table("hes.silver.apc_procedures")
    .filter(F.col("procedure_order") == 1)
    .filter(~F.col("procedure_code").rlike("^O(1[1-46]|28|3[01346]|4[2-8]|5[23])"))
    .filter(~F.col("procedure_code").rlike("^X[6-9]"))
    .filter(~F.col("procedure_code").rlike("^[UYZ]"))
)
