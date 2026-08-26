from functools import reduce

import pyspark.sql.functions as F

from nhp.data.table_names import table_names


def get_hes_apc(spark):
    df = spark.read.table(table_names.source_apc).filter(F.col("fyear").isNotNull())
    # all columns to lower case
    df = df.select([F.col(c).alias(c.lower()) for c in df.columns])
    # remove the _derived suffix
    df = reduce(
        lambda x, col: x.withColumnRenamed(col, col[:-8]),
        [i for i in df.columns if i.lower().endswith("derived")],
        df,
    )
    # convert fyear
    df = df.withColumn("fyear", F.col("fyear").cast("int") + 200000)
    # rename patient identifier
    df = df.withColumnRenamed("der_pseudo_nhs_number", "person_id_deid")

    return df
