from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

from nhp.data.get_spark import get_spark
from nhp.data.table_names import table_names


def create(spark: SparkSession, object_owner_group: str) -> None:
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

    spark.sql(
        f"""
    ALTER VIEW {table_names.default_ecds}
    OWNER TO `{object_owner_group}`
    """
    )


def main() -> None:
    """Main method to create the ecds view"""
    spark = get_spark()
    dbutils = DBUtils(spark)

    object_owner_group = dbutils.secrets.get(scope="nhp", key="object_owner_group")
    create(spark, object_owner_group)
