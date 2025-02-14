# Databricks notebook source
# MAGIC %pip install xlrd

# COMMAND ----------

import re

import pandas as pd
from databricks.connect import DatabricksSession
from pyspark.sql import Window
from pyspark.sql import functions as F
from tqdm.auto import tqdm

spark = DatabricksSession.builder.getOrCreate()
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# COMMAND ----------

path = "/Volumes/su_data/nhp/population-projections"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Convert source demographics files to parquet

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Demographics

# COMMAND ----------


def create_demographic_parquet(projection_name: str) -> None:
    """Create Demographic Parquet Files

    Convert the csv files into partitioned parquet files

    :param projection_name: The name of the projection
    :type projection_name: str
    """
    prefix = "" if projection_name == "principal_proj" else "var_proj_"
    file_name = f"{prefix}{projection_name}"
    years = [str(y) for y in range(2018, 2044)]
    stack_str = ", ".join(f"'{y}', `{y}`" for y in years)
    for sex_int, sex_string in [(1, "males"), (2, "females")]:
        (
            spark.read.csv(
                f"{path}/snpp_2018b_{file_name}/2018 SNPP Population {sex_string}.csv",
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
                f"{path}/demographic_data/projection={projection_name}/sex={sex_int}"
            )
        )


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Births

# COMMAND ----------


def create_birth_parquet(projection_name: str) -> None:
    """Create Birth Parquet Files

    Convert the csv files into partitioned parquet files

    :param projection_name: The name of the projection
    :type projection_name: str
    """
    prefix = "" if projection_name == "principal_proj" else "var_proj_"
    file_name = f"{prefix}{projection_name}"
    years = [str(y) for y in range(2019, 2044)]
    stack_str = ", ".join(f"'{y}', `{y}`" for y in years)
    (
        spark.read.csv(
            f"{path}/snpp_2018b_{file_name}_births/2018 SNPP Births persons.csv",
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
        .parquet(f"{path}/birth_data/projection={projection_name}")
    )


# COMMAND ----------

projections = [
    "principal_proj",
    "10_year_migration",
    "alt_internal_migration",
    "high_intl_migration",
    "low_intl_migration",
]

for i in projections:
    create_demographic_parquet(i)
    create_birth_parquet(i)


# COMMAND ----------

# MAGIC %md
# MAGIC ## NPP

# COMMAND ----------

principal_proj = spark.read.parquet(f"{path}/demographic_data").filter(
    F.col("projection") == "principal_proj"
)

# COMMAND ----------

principal_proj = (
    spark.read.parquet(f"{path}/demographic_data")
    .filter(F.col("projection") == "principal_proj")
    .persist()
)


def create_npp_variant(file: str, projection_name: str) -> None:
    """Create NPP Parquet Files

    Create the national projections at subnational level

    :param file: The name of the file to load
    :type file: str
    :param projection_name: The name of the projection
    :type projection_name: str
    """
    df = pd.read_excel(
        f"{path}/npp_2018b/en_{file}_opendata2018.xls", sheet_name="Population"
    )
    df = df.rename(columns={"Age": "age", "Sex": "sex"})
    df["age"] = [
        min(90, int(re.sub("\s*(\\d+).*", "\\1", i))) for i in df["age"].to_list()
    ]
    df = df.groupby(["sex", "age"], as_index=False).sum()
    df = df.melt(id_vars=["age", "sex"], var_name="year", value_name="pop")
    df["year"] = df["year"].astype("int")
    df.set_index(["year", "sex", "age"], inplace=True)

    df = df.loc[(slice(2018, 2043), slice(None), slice(None))].reset_index()

    df = spark.createDataFrame(df)

    w = Window.partitionBy("year", "sex", "age")
    (
        principal_proj.join(df, ["year", "sex", "age"])
        .withColumn("value", F.col("value") * F.col("pop") / F.sum("value").over(w))
        .drop("pop")
        .orderBy("age")
        .repartition(1)
        .write.mode("overwrite")
        .partitionBy("sex", "area_code")
        .parquet(f"{path}/demographic_data/projection={projection_name}")
    )


# COMMAND ----------

variants = [
    ("hpp", "high_fertility"),
    ("lpp", "low_fertility"),
    ("php", "high_life_expectancy"),
    ("plp", "low_life_expectancy"),
    ("pph", "high_intl_migration"),
    ("ppl", "low_intl_migration"),
    ("hhh", "high_population"),
    ("lll", "low_population"),
    ("lhl", "old_age_structure"),
    ("hlh", "young_age_structure"),
    ("ppz", "zero_net_migration"),
    ("pnp", "no_mortality_improvement"),
    ("cnp", "const_fert_no_mort_imp"),
    ("cpp", "const_fertility"),
    ("rpp", "replacement_fertility"),
    ("ppr", "half_eu_migration"),
    ("ppq", "zero_eu_migration"),
]

for v in tqdm(variants):
    create_npp_variant(*v)

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
# MAGIC
# MAGIC ```
# MAGIC (
# MAGIC     spark.read.parquet(f"{path}/demographic_data")
# MAGIC     .filter(F.col("area_code").rlike("^E0[6-9]"))
# MAGIC     .groupBy("projection", "sex", "age")
# MAGIC     .pivot("year")
# MAGIC     .agg(F.sum("value").alias("value"))
# MAGIC     .orderBy("projection", "sex", "age")
# MAGIC )
# MAGIC ```

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
    spark.read.table("su_data.nhp.apc")
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
