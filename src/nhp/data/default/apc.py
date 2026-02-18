from pyspark.sql import SparkSession

from nhp.data.table_names import table_names


def create(spark: SparkSession) -> None:
    spark.sql(
        f"""
    CREATE OR REPLACE VIEW {table_names.default_apc}
    AS
    SELECT *
    FROM   {table_names.raw_data_apc} a
    WHERE
      EXISTS (
        SELECT 1
        FROM   {table_names.reference_ods_trusts}
        WHERE  a.provider = org_to
        AND    org_type LIKE 'ACUTE%'
      )
    """
    )

    spark.sql(
        f"""
    CREATE OR REPLACE VIEW {table_names.default_apc_mitigators}
    AS
    SELECT *
    FROM   {table_names.raw_data_apc_mitigators}
    """
    )


def main() -> None:
    """Main method to create the apc view"""
    spark = SparkSession.builder.getOrCreate()
    create(spark)
