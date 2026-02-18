from pyspark.sql import SparkSession
from table_names import table_names


def create(spark: SparkSession) -> None:
    spark.sql(
        f"""
    CREATE OR REPLACE VIEW {table_names.default_ecds}
    AS
    SELECT *
    FROM   {table_names.aggregated_data_ecds} a
    WHERE
      EXISTS (
        SELECT 1
        FROM   {table_names.reference_ods_trusts}
        WHERE  a.provider = org_to
        AND    org_type LIKE 'ACUTE%'
      )
    """
    )


def main() -> None:
    """Main method to create the apc view"""
    spark = SparkSession.builder.getOrCreate()
    create(spark)
