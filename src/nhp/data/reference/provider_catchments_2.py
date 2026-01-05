# NOTE: this file is currently named provider_catchments_2.py
# the intention is to replace provider_catchments.py with this file
# however, that table is currently used by the demographics module pipeline, so needs to be
# checked before replacing

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from nhp.data.get_spark import get_spark
from nhp.data.reference.lsoa_lookups import get_lsoa11_to_lad23_lookup
from nhp.data.reference.population_by_lsoa21 import get_pop_by_lad23
from nhp.data.table_names import table_names


def create_provider_lad23_splits(spark: SparkSession) -> DataFrame:
    lsoa11_to_lad23 = get_lsoa11_to_lad23_lookup(spark)

    df = (
        spark.read.table(table_names.default_apc)
        .filter(F.col("lsoa11").startswith("E"))  # ty: ignore[missing-argument, invalid-argument-type]
        .filter(F.col("lsoa11") != "E99999999")
        .join(lsoa11_to_lad23, F.col("lsoa11") == F.col("lsoa11cd"))
        # get unique patients; more closely related to OHIDs logic
        # (https://tinyurl.com/ohid-nhs-acute-catchments)
        .select("fyear", "provider", "lad23cd", "age", "sex", "person_id")
        .distinct()
        .groupBy("fyear", "provider", "lad23cd", "age", "sex")
        .agg(F.count("person_id").alias("count"))
    )

    default_providers_for_lad = (
        df.groupBy("fyear", "lad23cd", "provider")
        .agg(F.sum("count").alias("count"))
        .withColumn(
            "rn",
            F.row_number().over(
                Window.partitionBy("fyear", "lad23cd").orderBy(F.desc("count"))
            ),
        )
        .filter(F.col("rn") == 1)
        .drop("count", "rn")
    )

    missing_counts = (
        df.select("fyear", "age", "sex")
        .distinct()
        .join(df.select("fyear", "lad23cd").distinct(), "fyear")
        .join(df, ["fyear", "age", "sex", "lad23cd"], "anti")
        .join(default_providers_for_lad, ["fyear", "lad23cd"])
        .withColumn("count", F.lit(1))
    )

    return (
        df.unionByName(missing_counts)
        .withColumn(
            "lad23_pcnt",
            F.col("count")
            / F.sum("count").over(Window.partitionBy("fyear", "lad23cd", "age", "sex")),
        )
        .drop("count")
    )


def get_provider_lad23_splits(spark: SparkSession) -> DataFrame:
    table = table_names.reference_provider_to_lad23_splits
    if not spark.catalog.tableExists(table):
        df = create_provider_lad23_splits(spark)
        df.write.mode("overwrite").saveAsTable(table)

    return spark.read.table(table)


def create_pop_by_provider(spark: SparkSession) -> DataFrame:
    provider_to_lad23_splits = get_provider_lad23_splits(spark)
    pop_by_lad23 = get_pop_by_lad23(spark)

    return (
        provider_to_lad23_splits.join(pop_by_lad23, ["fyear", "lad23cd", "age", "sex"])
        .groupBy("fyear", "provider", "age", "sex")
        .agg(F.sum(F.col("population") * F.col("lad23_pcnt")).alias("population"))
        .orderBy("fyear", "provider", "age", "sex")
    )


def get_pop_by_provider(spark: SparkSession) -> DataFrame:
    table = table_names.reference_pop_by_provider
    if not spark.catalog.tableExists(table):
        df = create_pop_by_provider(spark)
        df.write.mode("overwrite").saveAsTable(table)

    return spark.read.table(table)


def main():
    spark = get_spark()

    get_pop_by_provider(spark)
