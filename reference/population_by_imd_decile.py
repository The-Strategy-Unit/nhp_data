"""Create Population by IMD Decile"""

from databricks.connect import DatabricksSession
from pyspark.context import SparkContext
from pyspark.sql import Window
from pyspark.sql import functions as F


def create_population_by_imd_decile(
    spark: SparkContext, base_year: int = 201920, min_pcnt: int = 0
) -> None:
    """Create Population by IMD Decile

    :param spark: The spark context to use
    :type spark: SparkContext
    :param base_year: which year of activity to be based on, defaults to 201920
    :type base_year: int, optional
    :param min_pcnt: what minimum percentage, defaults to 0
    :type min_pcnt: int, optional
    """
    lsoa_pop_imd = spark.read.csv(
        "/Volumes/strategyunit/reference/files/lsoa_pop_imd.csv",
        header=True,
        schema="lsoa11cd string, la11cd string, pop int, imd19_decile int",
    )

    total_window = Window.partitionBy("provider")

    df = (
        spark.read.table("su_data.nhp.apc")
        .filter(F.col("fyear") == base_year)
        .filter(F.col("group") == "elective")
        .filter(F.col("resladst_ons").rlike("^E0[6-9]"))
        .groupBy("provider", "resladst_ons")
        .count()
        .withColumn("pcnt", F.col("count") / F.sum("count").over(total_window))
        .filter(F.col("pcnt") > min_pcnt)
        .withColumn("pcnt", F.col("count") / F.sum("count").over(total_window))
        .withColumnRenamed("resladst_ons", "la11cd")
        .join(lsoa_pop_imd, "la11cd")
        .withColumn("pop", F.col("pop") * F.col("pcnt"))
        .groupBy("provider", "imd19_decile")
        .agg(F.sum("pop").alias("pop"))
        .orderBy("provider", "imd19_decile")
    )

    df.write.mode("overwrite").saveAsTable("nhp.reference.population_by_imd_decile")


def main() -> None:
    """main method"""
    spark = DatabricksSession.builder.getOrCreate()
    create_population_by_imd_decile(spark)


if __name__ == "__main__":
    main()
