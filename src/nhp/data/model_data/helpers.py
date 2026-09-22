"""Helper methods/tables"""

from collections.abc import Callable
from functools import wraps

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession


def check_extract_for_nulls(
    df: DataFrame, exclude_cols: set[str] | None = None
) -> None:
    """Check a dataframe for any null values and raise an error if any are found.

    Args:
        df (DataFrame): the dataframe to check
        exclude_cols (set[str] | None): columns to exclude from the null check
    """
    if exclude_cols is None:
        exclude_cols = set()

    cols = sorted(set(df.columns) - exclude_cols)

    if not cols:
        return

    melt_str = ", ".join([f"'{c}', `{c}`" for c in cols])

    stack_expr = F.expr(f"stack({len(cols)}, {melt_str}) as (column, null_count)")

    null_check = (
        df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in cols])
        .select(stack_expr)
        .filter(F.col("null_count") > 0)
        .collect()
    )

    if null_check:
        raise ValueError(
            "Nulls/NaNs found in the following columns ["
            + ", ".join([i["column"] for i in null_check])
            + "]"
        )


def extract(
    extract_name: str,
    check_for_nulls: bool = True,
    exclude_cols: set[str] | None = None,
) -> Callable[
    [Callable[..., DataFrame]],
    Callable[..., None],
]:
    def decorator(func: Callable[..., DataFrame]):
        @wraps(func)
        def wrapper(
            save_path: str, fyear: int, spark: SparkSession, *args, **kwargs
        ) -> None:
            """Extract a dataframe to parquet, optionally checking for nulls.

            Args:
                save_path (str): the path to save the parquet files
                fyear (int): the fiscal year
                spark (SparkSession): the Spark session
            """
            df = func(save_path, fyear, spark, *args, **kwargs).persist()
            try:
                if check_for_nulls:
                    check_extract_for_nulls(df, exclude_cols)

                print(f"Rows to extract: {df.count():,}")

                (
                    df.repartition(1)
                    .write.mode("overwrite")
                    .partitionBy(["fyear", "dataset"])
                    .parquet(f"{save_path}/{extract_name}")
                )
            finally:
                df.unpersist()

        return wrapper

    return decorator
