"""Generate ECDS Data"""

from databricks.connect import DatabricksSession
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *  # pylint: disable-all


def generate_ecds_data(spark: SparkContext) -> None:
    """Generate ECDS Data"""
    hes_ecds_processed = (
        spark.read.table("nhp.raw_data.ecds")
        .groupBy(
            F.col("fyear"),
            F.col("provider"),
            F.col("age"),
            F.col("sex"),
            F.col("sitetret"),
            F.col("aedepttype"),
            F.col("attendance_category"),
            F.col("acuity"),
            F.col("tretspef"),
            F.col("group"),
            F.col("type"),
            F.col("hsagrp"),
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

    (
        hes_ecds_processed.withColumn("index", F.expr("uuid()"))
        .write.partitionBy("fyear", "provider")
        .mode("overwrite")
        .saveAsTable("nhp.aggregated_data.ecds")
    )


if __name__ == "__main__":
    spark = DatabricksSession.builder.getOrCreate()
    generate_ecds_data(spark)
