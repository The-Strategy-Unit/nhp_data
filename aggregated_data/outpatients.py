"""Generate Outpatients Data"""

from databricks.connect import DatabricksSession
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *  # noqa: F403


def get_outpatients_data(spark: SparkSession) -> DataFrame:
    """Get Outpatients Data"""
    return (
        spark.read.table("nhp.raw_data.opa")
        .groupBy(
            F.col("fyear"),
            F.col("provider"),
            F.col("age"),
            F.col("age_group"),
            F.col("sex"),
            F.col("imd_quintile"),
            F.col("tretspef"),
            F.col("tretspef_grouped"),
            F.col("sitetret"),
            F.col("type"),
            F.col("group"),
            F.col("pod"),
            F.col("hsagrp"),
            F.col("ndggrp"),
            F.col("has_procedures"),
            F.col("sushrg").substr(1, 4).alias("sushrg_trimmed"),
            F.col("icb"),
            F.col("is_main_icb"),
            F.col("is_surgical_specialty"),
            F.col("is_adult"),
            F.col("is_gp_ref"),
            F.col("is_cons_cons_ref"),
            F.col("is_first"),
        )
        .agg(
            F.sum("attendance").alias("attendances"),
            F.sum("tele_attendance").alias("tele_attendances"),
        )
        .withColumn("index", F.expr("uuid()"))
        .repartition("fyear", "provider")
    )


def generate_outpatients_data(spark: SparkSession, opa: DataFrame) -> None:
    """Generate Outpatients Data"""
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    (
        opa.write.partitionBy("fyear", "provider")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable("nhp.aggregated_data.opa")
    )


def main() -> None:
    """main method"""
    spark = DatabricksSession.builder.getOrCreate()
    opa = get_outpatients_data(spark)
    generate_outpatients_data(spark, opa)


if __name__ == "__main__":
    main()
