from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

from nhp.data.get_spark import get_spark
from nhp.data.table_names import table_names


def create(spark: SparkSession, object_owner_group: str) -> None:
    spark.sql(
        f"""
    CREATE OR REPLACE VIEW {table_names.default_opa}
    AS
    SELECT *
    FROM   {table_names.aggregated_data_opa} a
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
    ALTER VIEW {table_names.default_opa}
    OWNER TO `{object_owner_group}`
    """
    )


def main() -> None:
    """Main method to create the opa view"""
    spark = get_spark()
    dbutils = DBUtils(spark)

    object_owner_group = dbutils.secrets.get(scope="nhp", key="object_owner_group")
    create(spark, object_owner_group)
