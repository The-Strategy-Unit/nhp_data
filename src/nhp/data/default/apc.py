from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

from nhp.data.get_spark import get_spark
from nhp.data.table_names import table_names


def create(spark: SparkSession, object_owner_group: str) -> None:
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

    spark.sql(
        f"""
    ALTER VIEW {table_names.default_apc}
    OWNER TO `{object_owner_group}`
    """
    )

    spark.sql(
        f"""
    ALTER VIEW {table_names.default_apc_mitigators}
    OWNER TO `{object_owner_group}`
    """
    )


def main() -> None:
    """Main method to create the apc view"""
    spark = get_spark()
    dbutils = DBUtils(spark)

    object_owner_group = dbutils.secrets.get(scope="nhp", key="object_owner_group")
    create(spark, object_owner_group)
