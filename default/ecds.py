from pyspark.sql import SparkSession


def create(spark: SparkSession) -> None:
    spark.sql(
        """
    CREATE OR REPLACE VIEW nhp.default.ecds
    AS
    SELECT *
    FROM   nhp.aggregated_data.ecds a
    WHERE
      EXISTS (
        SELECT 1
        FROM   strategyunit.reference.ods_trusts
        WHERE  a.provider = org_to
        AND    org_type LIKE 'ACUTE%'
      )
    """
    )


def main() -> None:
    """Main method to create the apc view"""
    spark = SparkSession.builder.getOrCreate()
    create(spark)


if __name__ == "__main__":
    main()
