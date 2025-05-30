"""Generate Provider Main ICB"""

from pyspark.sql import Window
from pyspark.sql import functions as F

from nhp_datasets.apc import hes_apc


def generate_provider_main_icb() -> None:
    """Generate Provider Main ICB"""

    w = Window.partitionBy("provider").orderBy(F.desc("count"))

    main_icbs = (
        hes_apc.filter(F.col("icb").isNotNull())  # pylint: disable=undefined-variable
        .groupBy("provider", "icb")
        .count()
        .withColumn("row", F.row_number().over(w))
        .filter(F.col("row") == 1)
        .drop("row", "count")
        .withColumnRenamed("icb", "main_icb")
    )

    main_icbs.write.mode("overwrite").saveAsTable("nhp.reference.provider_main_icb")


if __name__ == "__main__":
    generate_provider_main_icb()
