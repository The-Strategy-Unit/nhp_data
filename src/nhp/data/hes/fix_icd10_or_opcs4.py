from pyspark.sql import functions as F
from functools import reduce

def fix_icd10_or_opcs4(df, col):
    v = F.col(col)
    return df.withColumn(
        col,
        F
            .when(v.rlike("^[A-Z][0-9][0-9]X"), F.substring(col, 1, 4))
            .when(v.rlike("^[A-Z][0-9][0-9][0-9][0-9]"), F.substring(col, 1, 5))
            .otherwise(F.substring(col, 1, 4))
    )