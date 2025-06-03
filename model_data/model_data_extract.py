"""Extract data for model"""

# dbutils.widgets.text("version", "dev")
# dbutils.widgets.text("fyear", "201920")


import sys

import pyspark.sql.functions as F
from databricks.connect import DatabricksSession
from pyspark.sql import DataFrame, SparkSession, Window
from model_data.helpers import add_tretspef_column


def extract_opa_data(spark: SparkSession, save_path: str, fyear: int) -> None:
    """Extract OPA data

    :param spark: the spark context to use
    :type spark: SparkSession
    :param save_path: where to save the parquet files
    :type save_path: str
    :param fyear: what year to extract
    :type fyear: int
    """

    opa = (
        spark.read.table("opa")
        .filter(F.col("fyear") == fyear)
        .withColumnRenamed("provider", "dataset")
        .withColumn("fyear", F.floor(F.col("fyear") / 100))
        .withColumn("tretspef_raw", F.col("tretspef"))
        .withColumn("is_wla", F.lit(True))
    )

    inequalities = (
        spark.read.parquet("/Volumes/nhp/inputs_data/files/dev/inequalities.parquet")
        .filter(F.col("fyear") == fyear)
        .select("provider", "sushrg_trimmed")
        .withColumnRenamed("provider", "dataset")
        .distinct()
    )

    # We don't want to keep sushrg_trimmed and imd_quintile if not in inequalities parquet file
    opa_collapse = (
        opa.join(inequalities, how="anti", on=["dataset", "sushrg_trimmed"])
        .withColumn("sushrg_trimmed", F.lit(None))
        .withColumn("imd_quintile", F.lit(None))
        .groupBy(opa.drop("index", "attendances", "tele_attendances").columns)
        .agg(
            F.sum("attendances").alias("attendances"),
            F.sum("tele_attendances").alias("tele_attendances"),
            F.min("index").alias("index"),
        )
    )

    opa_dont_collapse = opa.join(
        inequalities, how="semi", on=["dataset", "sushrg_trimmed"]
    )

    opa = DataFrame.unionByName(opa_collapse, opa_dont_collapse)

    opa = add_tretspef_column(opa)

    (
        opa.repartition(1)
        .write.mode("overwrite")
        .partitionBy(["fyear", "dataset"])
        .parquet(f"{save_path}/op")
    )


def extract_ecds_data(spark: SparkSession, save_path: str, fyear: int) -> None:
    """Extract ECDS data

    :param spark: the spark context to use
    :type spark: SparkSession
    :param save_path: where to save the parquet files
    :type save_path: str
    :param fyear: what year to extract
    :type fyear: int
    """

    ecds = (
        spark.read.table("ecds")
        .filter(F.col("fyear") == fyear)
        .withColumnRenamed("provider", "dataset")
        .withColumn("fyear", F.floor(F.col("fyear") / 100))
    )

    (
        ecds.repartition(1)
        .write.mode("overwrite")
        .partitionBy(["fyear", "dataset"])
        .parquet(f"{save_path}/aae")
    )


def _create_population_projections(
    spark: SparkSession, df: DataFrame, fyear: int
) -> DataFrame:
    providers = (
        spark.read.table("strategyunit.reference.ods_trusts")
        .filter(F.col("org_type").startswith("ACUTE"))
        .select(F.col("org_to").alias("provider"))
        .distinct()
    )

    catchments = (
        spark.read.table("nhp.population_projections.provider_catchments")
        .filter(F.col("fyear") == fyear)
        .drop("fyear")
        .join(providers, "provider", how="semi")
    )

    # currently fixed to use the 2018 projection year: awaiting new data from ONS to be published
    return (
        df.filter(F.col("projection_year") == 2018)
        .join(catchments, "area_code")
        .withColumnRenamed("projection", "variant")
        .withColumnRenamed("provider", "dataset")
        .groupBy("dataset", "variant", "age", "sex")
        .pivot("year")
        .agg(F.sum(F.col("value") * F.col("pcnt")))
        .orderBy("dataset", "variant", "age", "sex")
    )


# pylint: disable=invalid-name
def create_custom_birth_factors_R0A66(
    spark: SparkSession, birth_factors: DataFrame
) -> DataFrame:
    """Create custom birth factors file for R0A66, using principal projection

    :param spark: the spark context to use
    :type spark: SparkSession
    """
    custom_R0A = birth_factors.filter(
        (F.col("dataset") == "R0A") & (F.col("variant") == "principal_proj")
    ).withColumn("variant", F.lit("custom_projection_R0A66"))

    return custom_R0A


# pylint: disable=invalid-name
def create_custom_birth_factors_RD8(
    spark: SparkSession, birth_factors: DataFrame
) -> DataFrame:
    """Create custom birth factors file for RD8, using principal projection

    :param spark: the spark context to use
    :type spark: SparkSession
    """
    custom_RD8 = birth_factors.filter(
        (F.col("dataset") == "RD8") & (F.col("variant") == "principal_proj")
    ).withColumn("variant", F.lit("custom_projection_RD8"))

    return custom_RD8


def extract_birth_factors_data(spark: SparkSession, save_path: str, fyear: int) -> None:
    """Extract Birth Factors data

    :param spark: the spark context to use
    :type spark: SparkSession
    :param save_path: where to save the parquet files
    :type save_path: str
    :param fyear: what year to extract
    :type fyear: int
    """
    births = spark.read.table("nhp.population_projections.births").withColumn(
        "sex", F.lit(2)
    )

    birth_factors = _create_population_projections(spark, births, 201819)

    custom_R0A = create_custom_birth_factors_R0A66(spark, birth_factors)
    custom_RD8 = create_custom_birth_factors_RD8(spark, birth_factors)

    (
        # using a fixed year of 2018/19 to match prior logic
        birth_factors.unionByName(custom_R0A)
        .unionByName(custom_RD8)
        .repartition(1)
        .write.mode("overwrite")
        .partitionBy("dataset")
        .parquet(f"{save_path}/birth_factors/fyear={fyear // 100}")
    )


# pylint: disable=invalid-name
def create_custom_demographic_factors_RD8(spark: SparkSession) -> None:
    """Create custom demographic factors file for RD8 using agreed methodology

    :param spark: the spark context to use
    :type spark: SparkSession
    """
    # Load demographics - principal projection only
    custom_file = (
        spark.read.csv(
            "/Volumes/nhp/population_projections/files/RD8_population_projection V2.csv",
            header=True,
            inferSchema=True,
        )
        .withColumnRenamed("Sex", "sex")
        .withColumnRenamed("Age", "age")
        .drop("Type")
        .withColumn("dataset", F.lit("RD8"))
        .withColumn("variant", F.lit("custom_projection_RD8"))
    )
    return custom_file


# pylint: disable=invalid-name
def create_custom_demographic_factors_R0A66(spark: SparkSession) -> None:
    """Create custom demographic factors file for R0A66 using agreed methodology

    :param spark: the spark context to use
    :type spark: SparkSession
    """
    # Load demographics - principal projection only
    demographics = (
        spark.read.table("nhp.population_projections.demographics")
        .filter(F.col("projection") == "principal_proj")
        .filter(F.col("projection_year") == 2018)
        .filter(F.col("area_code") != "E08000003")
        .drop("projection", "projection_year")
    )
    # Load custom file
    # TODO: this should be moved into population_projections scope
    years = [str(y) for y in range(2018, 2044)]
    stack_str = ", ".join(f"'{y}', `{y}`" for y in years)
    custom_file = (
        spark.read.csv(
            "/Volumes/nhp/population_projections/files/ManchesterCityCouncil_custom_E08000003.csv",
            header=True,
            inferSchema=True,
        )
        .withColumnRenamed("Sex", "sex")
        .withColumnRenamed("Age", "age")
        .withColumn(
            "sex", F.when(F.col("sex") == "male", 1).when(F.col("sex") == "female", 2)
        )
        .withColumn("area_code", F.lit("E08000003"))
        .selectExpr(
            "area_code",
            "sex",
            "age",
            f"stack({len(years)}, {stack_str}) as (year, value)",
        )
        .orderBy("age")
    )
    demographics = demographics.unionByName(custom_file)
    # Work out catchment with patched demographics
    total_window = Window.partitionBy("provider")
    df = (
        spark.read.table("nhp.raw_data.apc")
        .filter(F.col("sitetret") == "R0A66")
        .filter(F.col("fyear") == 202324)
        .filter(F.col("resladst_ons").rlike("^E0[6-9]"))
        .groupBy("provider", "resladst_ons")
        .count()
        .withColumn("pcnt", F.col("count") / F.sum("count").over(total_window))
        .filter(F.col("pcnt") > 0.05)
        .withColumn("pcnt", F.col("count") / F.sum("count").over(total_window))
        .withColumnRenamed("resladst_ons", "area_code")
        .withColumnRenamed("provider", "dataset")
        .join(demographics, "area_code")
        .withColumn("variant", F.lit("custom_projection_R0A66"))
        .withColumn("value", F.col("value") * F.col("pcnt"))
        .groupBy("dataset", "age", "sex", "variant")
        .pivot("year")
        .agg(F.sum("value"))
        .orderBy("dataset", "age", "sex", "variant")
    )
    return df


def extract_demographic_factors_data(
    spark: SparkSession, save_path: str, fyear: int
) -> None:
    """Extract Birth Factors data

    :param spark: the spark context to use
    :type spark: SparkSession
    :param save_path: where to save the parquet files
    :type save_path: str
    :param fyear: what year to extract
    :type fyear: int
    """

    demographics = spark.read.table("nhp.population_projections.demographics")

    custom_R0A = create_custom_demographic_factors_R0A66(spark)
    custom_RD8 = create_custom_demographic_factors_RD8(spark)

    (
        # using a fixed year of 2018/19 to match prior logic
        _create_population_projections(spark, demographics, 201819)
        .unionByName(custom_R0A)
        .unionByName(custom_RD8)
        .repartition(1)
        .write.mode("overwrite")
        .partitionBy("dataset")
        .parquet(f"{save_path}/demographic_factors/fyear={fyear // 100}")
    )


def main(save_path: str, fyear: int) -> None:
    """Main method

    :param save_path: where to save the parquet files
    :type save_path: str
    :param fyear: what year to extract
    :type fyear: int
    """

    spark: SparkSession = DatabricksSession.builder.getOrCreate()

    spark.catalog.setCurrentCatalog("nhp")
    spark.catalog.setCurrentDatabase("default")

    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    extract_opa_data(spark, save_path, fyear)
    extract_ecds_data(spark, save_path, fyear)
    extract_birth_factors_data(spark, save_path, fyear)
    extract_demographic_factors_data(spark, save_path, fyear)


def __init__(*args):
    path = args[0]
    fyear = int(args[1])
    main(path, fyear)


if __name__ == "__main__":
    __init__(*sys.argv[1:])
