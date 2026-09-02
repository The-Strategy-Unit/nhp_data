import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession, Window

from nhp.data.functional_areas.ip_ward_groups import create_ip_ward_groupings
from nhp.data.table_names import table_names


def get_ip_functional_area_wards(apc: DataFrame, spark: SparkSession) -> DataFrame:
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

    df = create_ip_ward_groupings(
        apc.select(
            "rn",
            "susspellid",
            "dataset",
            # apply last episode values to the rest of the episodes
            "speldur",
            "fyear",
            "group",
            "age",
        )
        .join(episodes, "susspellid")
        .withColumn("is_zero_length_episode", (F.col("epidur") == 0).cast("int"))
        .join(spark.read.table(table_names.reference_tretspef_type), "tretspef", "left")
    )

    w = Window.partitionBy("rn")
    return (
        df.groupBy("fyear", "dataset", "rn", "sitetret", "ward_grouping")
        .agg(
            F.sum("epidur").alias("group_los"),
            F.count("epikey").alias("episodes"),
            F.sum("is_zero_length_episode").alias("zero_length_episodes"),
        )
        .withColumn("los_total", F.sum("group_los").over(w))
        .withColumn(
            "group_pcnt",
            F.when(F.col("los_total") == 0, 0).otherwise(
                F.col("group_los") / F.col("los_total")
            ),
        )
    )
