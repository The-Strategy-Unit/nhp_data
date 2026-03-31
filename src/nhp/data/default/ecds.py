from pyspark.dbutils import DBUtils

from nhp.data.default._default_view import create_default_view, get_object_owner_group
from nhp.data.get_spark import get_spark
from nhp.data.table_names import table_names


def main() -> None:
    """Main method to create the ecds view"""
    spark = get_spark()
    dbutils = DBUtils(spark)

    object_owner_group = get_object_owner_group(dbutils)
    create_default_view(
        spark,
        table_names.aggregated_data_ecds,
        table_names.default_ecds,
        object_owner_group,
    )
