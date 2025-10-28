"""Generate ECDS Data"""

from databricks.connect import DatabricksSession
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *  # noqa: F403

from nhp.data.table_names import table_names


def get_ecds_data(spark: SparkSession) -> DataFrame:
    """Get ECDS Data"""
    return (
        spark.read.table(table_names.raw_data_ecds)
        .groupBy(
            F.col("fyear"),
            F.col("provider"),
            F.col("age"),
            F.col("age_group"),
            F.col("sex"),
            F.col("sitetret"),
            F.col("aedepttype"),
            F.col("attendance_category"),
            F.col("acuity"),
            F.col("tretspef"),
            F.col("tretspef_grouped"),
            F.col("group"),
            F.col("pod"),
            F.col("type"),
            F.col("hsagrp"),
            F.col("ndggrp"),
            F.col("icb"),
            F.col("is_main_icb"),
            F.col("is_adult"),
            F.col("is_ambulance"),
            F.col("is_frequent_attender"),
            F.col("is_low_cost_referred_or_discharged"),
            F.col("is_left_before_treatment"),
            F.col("is_discharged_no_treatment"),
        )
        .agg(F.sum("arrival").alias("arrivals"))
        .repartition("fyear", "provider")
    )


def generate_ecds_data(spark: SparkSession, ecds: DataFrame) -> None:
    """Generate ECDS Data"""
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    (
        ecds.withColumn("index", F.expr("uuid()"))
        .write.partitionBy("fyear", "provider")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable(table_names.aggregated_data_ecds)
    )


def main() -> None:
    """main method"""
    spark = DatabricksSession.builder.getOrCreate()
    ecds = get_ecds_data(spark)
    generate_ecds_data(spark, ecds)
