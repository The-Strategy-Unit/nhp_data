import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from nhp.data.functional_areas.ip_procedures import create_ip_procedure_groupings
from nhp.data.table_names import table_names


def get_ip_functional_areas_procedures(
    apc: DataFrame, spark: SparkSession
) -> DataFrame:
    episode_primary_procedure = (
        spark.read.table(table_names.hes_apc_procedures)
        .filter(F.col("procedure_order") == 1)
        .select(
            "epikey",
            "fyear",
            "procode3",
            F.col("procedure_code").alias("primary_procedure"),
        )
    )

    episodes = (
        spark.read.table(table_names.hes_apc)
        .filter(F.col("FCE") == 1)
        .join(episode_primary_procedure, ["epikey", "fyear", "procode3"], "left")
        .withColumn("has_procedure", ~F.col("primary_procedure").isNull())
        .drop("fyear", "speldur")
    )

    theatre_times = spark.read.table(table_names.reference_theatre_times)

    df = create_ip_procedure_groupings(
        apc.select(
            "rn",
            "susspellid",
            "dataset",
            # apply last episode values to the rest of the episodes
            "fyear",
            "group",
            "age",
        )
        .join(episodes, "susspellid")
        .join(
            theatre_times.select(
                "primary_procedure", F.col("mean").alias("theatre_time")
            ),
            "primary_procedure",
            "left",
        )
        .join(spark.read.table(table_names.reference_tretspef_type), "tretspef", "left")
    )

    return df.groupBy("fyear", "dataset", "rn", "sitetret", "functional_area").count()
