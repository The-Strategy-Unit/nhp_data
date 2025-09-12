from databricks.connect import DatabricksSession
from pyspark.sql import functions as F
from pyspark.sql.types import *  # noqa: F403

from nhp.data.nhp_datasets.icbs import icb_mapping
from nhp.data.nhp_datasets.local_authorities import local_authority_successors
from nhp.data.nhp_datasets.providers import read_data_with_provider

spark = DatabricksSession.builder.getOrCreate()

hes_apc = (
    local_authority_successors(
        spark, read_data_with_provider(spark, "hes.silver.apc"), "resladst_ons"
    )
    .filter(F.col("last_episode_in_spell"))
    .withColumn(
        "age",
        F.when(
            (F.col("admiage") == 999) | F.col("admiage").isNull(),
            F.when(F.col("startage") > 7000, 0).otherwise(F.col("startage")),
        ).otherwise(F.col("admiage")),
    )
    .withColumn("age", F.when(F.col("age") > 90, 90).otherwise(F.col("age")))
    # remove well babies
    .filter(F.col("well_baby_ind") == "N")
    .filter((F.col("sushrg") != "PB03Z") | F.col("sushrg").isNull())
    .filter(~((F.col("tretspef") == "424") & (F.col("epitype") == "3")))
    # ---
    .filter(F.col("sex").isin(["1", "2"]))
    .filter(F.col("age").between(0, 90))
    .filter(F.col("speldur").isNotNull())
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
