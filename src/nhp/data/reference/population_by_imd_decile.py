"""Create Population by IMD Decile"""

import sys

from databricks.connect import DatabricksSession
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from nhp.data.nhp_datasets.apc import hes_apc
from nhp.data.table_names import table_names


def create_population_by_imd_decile(
    spark: SparkSession, base_year: int = 201920
) -> None:
    """Create Population by IMD Decile

    :param spark: The spark context to use
    :type spark: SparkSession
    :param base_year: which year of activity to be based on, defaults to 201920
    :type base_year: int, optional
    """

    df = hes_apc

    # create age group lookups
    age_groups = spark.createDataFrame(
        [("0-16", i) for i in range(0, 16 + 1)]
        + [("17-40", i) for i in range(17, 40 + 1)]
        + [("41-65", i) for i in range(41, 65 + 1)]
        + [("66-75", i) for i in range(66, 75 + 1)]
        + [("76+", i) for i in range(76, 90 + 1)],
        ["age_group", "age"],
    )

    # cheat to get lsoa->msoa lookup
    lsoa_to_msoa = (
        spark.read.table(table_names.hes_apc)
        .select("lsoa11", "msoa11")
        .filter(F.col("lsoa11").startswith("E"))
        .filter(F.col("lsoa11") != "E99999999")
        .distinct()
        .persist()
    )

    pop = (
        spark.read.csv(table_names.reference_pop_by_lsoa, header=True)
        .withColumn("age", F.col("age").cast("int"))
        .withColumn("imd19", F.col("imd19").cast("int"))
        .withColumnRenamed("lsoa11cd", "lsoa11")
        .join(age_groups, "age")
        .groupBy("lsoa11", "imd19", "sex", "age_group")
        .agg(F.sum("pop").alias("pop"))
        .join(lsoa_to_msoa, "lsoa11")
    )

    df_counts = (
        df.filter(F.col("fyear") == base_year)
        .filter(F.isnotnull("age"))
        .filter(F.col("sex").isin(["1", "2"]))
        .filter(F.isnotnull("lsoa11"))
        .filter(F.col("admimeth").startswith("1"))
        .join(age_groups, "age")
        .groupBy("lsoa11", "icb", "provider", "age_group", "sex")
        .agg(F.countDistinct("person_id_deid").alias("n"))
        .persist()
    )

    w_msoa = Window.partitionBy("msoa11", "age_group", "sex")

    df_pop_msoa = (
        df_counts.join(lsoa_to_msoa, "lsoa11")
        .groupBy("icb", "provider", "msoa11", "age_group", "sex")
        # count the activity (n), then calculate the proportion of the activity in the msoa (p)
        .agg(F.sum("n").alias("n"))
        .withColumn("p", F.col("n") / F.sum("n").over(w_msoa))
        .join(pop, ["msoa11", "age_group", "sex"])
        .groupBy("icb", "provider", "imd19")
        .agg(F.sum(F.col("pop") * F.col("p")).alias("pop"))
    )

    df_pop_msoa.orderBy("icb", "provider", "imd19").write.option(
        "mergeSchema", "true"
    ).mode("overwrite").saveAsTable(table_names.reference_population_by_imd_decile)


def main() -> None:
    """main method"""
    spark = DatabricksSession.builder.getOrCreate()
    base_year = int(sys.argv[1])
    create_population_by_imd_decile(spark, base_year=base_year)
