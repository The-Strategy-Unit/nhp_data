from pyspark.dbutils import DBUtils

from nhp.data.default._default_view import create_default_view
from nhp.data.get_spark import get_spark
from nhp.data.table_names import table_names


def main() -> None:
    """Main method to create the opa view"""
    spark = get_spark()
    dbutils = DBUtils(spark)

    object_owner_group = dbutils.secrets.get(scope="nhp", key="object_owner_group")
    create_default_view(
        spark,
        table_names.aggregated_data_opa,
        table_names.default_opa,
        object_owner_group,
    )
