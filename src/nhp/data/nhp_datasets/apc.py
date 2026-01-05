from pyspark.sql import functions as F
from pyspark.sql.types import *  # noqa: F403

from nhp.data.get_spark import get_spark
from nhp.data.nhp_datasets.icbs import icb_mapping
from nhp.data.nhp_datasets.local_authorities import local_authority_successors
from nhp.data.nhp_datasets.providers import read_data_with_provider
from nhp.data.table_names import table_names

spark = get_spark()

hes_apc = (
    local_authority_successors(
        spark, read_data_with_provider(spark, table_names.hes_apc), "resladst_ons"
    )
    .filter(F.col("last_episode_in_spell"))
    .withColumn(
        "age",
        F.when(
            (F.col("admiage") == 999) | F.col("admiage").isNull(),  # ty: ignore[missing-argument]
            F.when(F.col("startage") > 7000, 0).otherwise(F.col("startage")),
        ).otherwise(F.col("admiage")),
    )
    .withColumn("age", F.when(F.col("age") > 90, 90).otherwise(F.col("age")))
    # remove well babies
    .filter(F.col("well_baby_ind") == "N")
    .filter((F.col("sushrg") != "PB03Z") | F.col("sushrg").isNull())  # ty: ignore[missing-argument]
    .filter(~((F.col("tretspef") == "424") & (F.col("epitype") == "3")))
    # filter out excessive los
    # - likely to be DQ issues
    # - unless mental health
    # - or, there are a large number of episodes in the spell
    .filter(
        ~(
            (F.col("speldur") > 2 * 365)
            & ~(F.col("tretspef").startswith("7"))  # ty: ignore[missing-argument, invalid-argument-type]
            & ~(F.col("mainspef").startswith("7"))  # ty: ignore[missing-argument, invalid-argument-type]
            & (F.col("epiorder") < 10)
        )
    )
    # ---
    .filter(F.col("sex").isin(["1", "2"]))
    .filter(F.col("age").between(0, 90))
    .filter(F.isnotnull("speldur"))
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
    spark.read.table(table_names.hes_apc_procedures)
    .filter(F.col("procedure_order") == 1)
    .filter(~F.col("procedure_code").rlike("^O(1[1-46]|28|3[01346]|4[2-8]|5[23])"))
    .filter(~F.col("procedure_code").rlike("^X[6-9]"))
    .filter(~F.col("procedure_code").rlike("^[UYZ]"))
)
