import re
from typing import Any

from pyspark.sql import SparkSession

from nhp.data.table_names import table_names


def get_object_owner_group(dbutils: Any) -> str:
    try:
        return dbutils.secrets.get(scope="nhp", key="object_owner_group")
    except Exception as exc:
        raise RuntimeError(
            "Failed to retrieve Databricks secret 'object_owner_group' from scope 'nhp'. "
            "Ensure that the secret scope 'nhp' exists, that the key 'object_owner_group' "
            "is provisioned, and that this job has permission to access it."
        ) from exc


def safe_ident(name: str) -> str:
    IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9_\.]+$")
    if not IDENTIFIER_RE.match(name):
        raise ValueError(f"Invalid identifier: {name}")
    return f"`{name}`"


def create_default_view(
    spark: SparkSession, source_table: str, view_name: str, object_owner_group: str
) -> None:
    source_table = safe_ident(source_table)
    view_name = safe_ident(view_name)
    object_owner_group = object_owner_group.replace("`", "``")

    spark.sql(
        f"""
    CREATE OR REPLACE VIEW {view_name}
    AS
    SELECT *
    FROM   {source_table} a
    WHERE
      EXISTS (
        SELECT 1
        FROM   {table_names.reference_ods_trusts}
        WHERE  a.provider = org_to
        AND    org_type LIKE 'ACUTE%'
      )
    """
    )

    spark.sql(f"ALTER VIEW {view_name} OWNER TO `{object_owner_group}`")
