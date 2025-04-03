"""Generate outpatients mitigators data"""

from functools import reduce

from databricks.connect import DatabricksSession
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *  # pylint: disable-all


def get_outpatients_mitigators(spark: SparkContext) -> None:
    df = spark.read.table("nhp.raw_data.opa")

    op_strategies = [
        # Follow-up reduction
        df.filter(~F.col("has_procedures"))
        .filter(~F.col("is_first"))
        .withColumn("strategy", F.concat(F.lit("followup_reduction_"), F.col("type")))
        .select("fyear", "provider", "attendkey", "strategy"),
        # Consultant to Consultant reduction
        df.filter(F.col("is_cons_cons_ref"))
        .withColumn(
            "strategy",
            F.concat(F.lit("consultant_to_consultant_reduction_"), F.col("type")),
        )
        .select("fyear", "provider", "attendkey", "strategy"),
        # GP Referred First Attendance reduction
        df.filter(F.col("is_gp_ref"))
        .filter(F.col("is_first"))
        .withColumn(
            "strategy",
            F.concat(F.lit("gp_referred_first_attendance_reduction_"), F.col("type")),
        )
        .select("fyear", "provider", "attendkey", "strategy"),
        # Convert to tele
        df.filter(~F.col("has_procedures"))
        .filter(F.col("tele_attendance") == 0)
        .withColumn("strategy", F.concat(F.lit("convert_to_tele_"), F.col("type")))
        .select("fyear", "provider", "attendkey", "strategy"),
    ]

    return reduce(DataFrame.unionByName, op_strategies)


def generate_outpatients_mitigators(spark: SparkContext) -> None:
    """Generate Outpatients Mitigators Data"""
    hes_opa_mitigators = get_outpatients_mitigators(spark)

    (
        hes_opa_mitigators.orderBy("fyear", "provider", "strategy", "attendkey")
        .write.partitionBy("fyear", "provider")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable("nhp.raw_data.opa_mitigators")
    )


if __name__ == "__main__":
    spark = DatabricksSession.builder.getOrCreate()
    generate_outpatients_mitigators(spark)
