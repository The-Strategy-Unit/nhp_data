from pyspark.dbutils import DBUtils

from nhp.data.default._default_view import create_default_view, get_object_owner_group
from nhp.data.get_spark import get_spark
from nhp.data.table_names import table_names


def main() -> None:
    """Main method to create the apc view"""
    spark = get_spark()
    dbutils = DBUtils(spark)

    object_owner_group = get_object_owner_group(dbutils)

    create_default_view(
        spark, table_names.raw_data_apc, table_names.default_apc, object_owner_group
    )
    create_default_view(
        spark,
        table_names.raw_data_apc_mitigators,
        table_names.default_apc_mitigators,
        object_owner_group,
    )
