# Databricks notebook source
from databricks.connect import DatabricksSession
from pyspark.sql import Window
from pyspark.sql import functions as F

spark = DatabricksSession.builder.getOrCreate()

# COMMAND ----------

path = "/Volumes/nhp/population_projections/files"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Convert source demographics files to parquet

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Demographics

# COMMAND ----------


def create_demographic_parquet(old_projection_name, new_projection_name):
    years = [str(y) for y in range(2018, 2044)]
    stack_str = ", ".join(f"'{y}', `{y}`" for y in years)
    for sex_int, sex_string in [(1, "males"), (2, "females")]:
        (
            spark.read.csv(
                f"{path}/{old_projection_name}/2018 SNPP Population {sex_string}.csv",
                header=True,
            )
            .filter(F.col("AGE_GROUP") != "All ages")
            .filter(F.col("AREA_CODE").rlike("^E0[6-9]"))
            .withColumn(
                "age",
                F.when(F.col("AGE_GROUP") == "90 and over", 90).otherwise(
                    F.col("AGE_GROUP").astype("int")
                ),
            )
            .selectExpr(
                "AREA_CODE as area_code",
                "age",
                f"stack({len(years)}, {stack_str}) as (year, value)",
            )
            .orderBy("age")
            .repartition(1)
            .write.mode("overwrite")
            .partitionBy("area_code")
            .parquet(
                f"{path}/demographic_data/projection={new_projection_name}/sex={sex_int}"
            )
        )


# COMMAND ----------

create_demographic_parquet("snpp_2018b_principal_proj", "principal_proj")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Births

# COMMAND ----------


def create_birth_parquet():
    years = [str(y) for y in range(2019, 2044)]
    stack_str = ", ".join(f"'{y}', `{y}`" for y in years)
    (
        spark.read.csv(
            f"{path}/snpp_2018b_principal_proj_births/2018 SNPP Births persons.csv",
            header=True,
        )
        .filter(F.col("AGE_GROUP") != "All ages")
        .filter(F.col("AREA_CODE").rlike("^E0[6-9]"))
        .withColumn(
            "age",
            F.when(F.col("AGE_GROUP") == "90 and over", 90).otherwise(
                F.col("AGE_GROUP").astype("int")
            ),
        )
        .selectExpr(
            "AREA_CODE as area_code",
            "age",
            f"stack({len(years)}, {stack_str}) as (year, value)",
        )
        .orderBy("age")
        .repartition(1)
        .write.mode("overwrite")
        .partitionBy("area_code")
        .parquet(f"{path}/birth_data/projection=principal_proj")
    )


# COMMAND ----------

create_birth_parquet()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Create demographics for model
# MAGIC
# MAGIC These steps should not exist in this file in the long term, they should go into the actual
# MAGIC data extraction scripts. For now however, adding in some examples of how to use the data.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Create national demographics file

(
    spark.read.parquet(f"{path}/demographic_data")
    .filter(F.col("area_code").rlike("^E0[6-9]"))
    .groupBy("projection", "sex", "age")
    .pivot("year")
    .agg(F.sum("value").alias("value"))
    .orderBy("projection", "sex", "age")
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Create provider demographics file

# COMMAND ----------

demographics = spark.read.parquet(f"{path}/demographic_data")

total_window = Window.partitionBy("provider")
BASE_YEAR = 201920
MIN_PCNT = 0.05

df = (
    spark.read.table("nhp.raw_data.apc")
    .filter(F.col("fyear") == BASE_YEAR)
    .filter(F.col("resladst_ons").rlike("^E0[6-9]"))
    .groupBy("provider", "resladst_ons")
    .count()
    .withColumn("pcnt", F.col("count") / F.sum("count").over(total_window))
    .filter(F.col("pcnt") > MIN_PCNT)
    .withColumn("pcnt", F.col("count") / F.sum("count").over(total_window))
    .withColumnRenamed("resladst_ons", "area_code")
    .join(demographics, "area_code")
    .withColumn("value", F.col("value") * F.col("pcnt"))
    .groupBy("provider", "age", "sex", "projection")
    .pivot("year")
    .agg(F.sum("value"))
    .orderBy("provider", "age", "sex", "projection")
)

# COMMAND ----------

df

# COMMAND ----------
