import logging
import os
import sys
import uuid
from datetime import datetime
from random import Random
from typing import Callable

import numpy as np
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from nhp.data.get_spark import get_spark
from nhp.data.table_names import table_names

logger = logging.getLogger(__name__)


def generate_data(name: str):
    def decorator(func: Callable[["SynthData", DataFrame], pd.DataFrame]):
        def wrapper(self):
            logger.info(f"Generating synthetic data for {name}")
            df = self.read_dev_file(name)
            result = func(self, df)
            self.save_synth_file(name, result)
            logger.info(f"Synthetic data for {name} saved")

        return wrapper

    return decorator


class SynthData:
    # how many rows should we target?
    IP_N = 100_000
    AAE_N = IP_N * 1.2
    OP_N = IP_N * 10

    def __init__(self, fyear: int, path: str, seed: int, spark: SparkSession):
        self._fyear = fyear
        self._dev_path = f"{path}/dev"
        self._synth_path = f"{path}/synth"
        self._seed = seed
        self._rng = Random(seed)

        self._spark = spark

    # helper methods

    def read_dev_file(self, file: str) -> DataFrame:
        return self.read_file(file, self._dev_path)

    def read_synth_file(self, file: str) -> DataFrame:
        return self.read_file(file, self._synth_path)

    def read_file(self, file: str, path: str) -> DataFrame:
        return (
            self._spark.read.parquet(f"{path}/{file}")
            .filter(F.col("fyear") == self._fyear)
            .drop("fyear")
        )

    def save_synth_file(self, file: str, df: pd.DataFrame) -> None:
        p = f"{self._synth_path}/{file}/fyear={self._fyear}/dataset=synthetic"
        os.makedirs(p, exist_ok=True)
        df.attrs = {
            k: v
            for k, v in df.attrs.items()
            if k not in ["metrics", "observed_metrics"]
        }
        df.to_parquet(f"{p}/0.parquet", index=False)

    def generate(self) -> None:
        self._ip()
        self._inequalities()
        self._aae()
        self._op()
        self._birth_factors()
        self._demographic_factors()
        self._hsa_activity_tables()

    @property
    def hrgs(self) -> list:
        if not hasattr(self, "_hrgs"):
            ip_df = (
                self.read_dev_file("ip")
                .groupBy("sushrg_trimmed")
                .count()
                .orderBy(F.desc("count"))
                .collect()
            )
            self._hrgs = [row["sushrg_trimmed"] for row in ip_df]
        return self._hrgs

    def _hrg_remapping(self, col: pd.Series) -> pd.Series:
        hrgs = self.hrgs
        if not hrgs:
            raise ValueError("HRGs list is empty. Cannot remap.")
        return col.replace(hrgs, [f"HRG{i + 1}" for i, _ in enumerate(hrgs)])

    # synth methods

    def _ip(self) -> None:
        # get the raw inpatients data for the whole year, all providers
        df_raw = (
            self.read_dev_file("ip")
            .filter(F.col("fyear") == self._fyear)
            .drop(
                "procode3",
                "person_id",
                "admiage",
                "admidate",
                "disdate",
                "imd_decile",
                "ethnos",
                "resgor_ons",
                "lsoa11",
                "lad23cd",
                "primary_diagnosis",
                "primary_procedure",
                "sushrg",
                "dismeth",
                "operstat",
                "epitype",
                "dataset",
                "sitetret",
            )
            .withColumn("dataset", F.lit("synthetic"))
            .withColumn("icb", F.when(F.col("is_main_icb"), "A").otherwise("B"))
            .withColumn("mainspef", F.col("tretspef"))
            .filter(~F.col("tretspef_grouped").startswith("Other"))
            .orderBy("rn")
            .persist()
        )

        grouping_cols = ["age_group", "sex", "classpat", "tretspef", "hsagrp", "group"]

        # count how many times each combination of the grouping columns appears, and filter out those that appear less
        # than 10 times
        df_counts = (
            df_raw.groupBy(grouping_cols).count().filter(F.col("count") >= 10).persist()
        )

        # now filter the raw data to only include the rows that appear 10 times in the counts
        df_raw = df_raw.join(df_counts, grouping_cols, "semi").persist()

        # calculate the mean los for each subgroup
        mean_los = df_raw.groupBy(grouping_cols).agg(
            F.mean("speldur").alias("mean_los")
        )

        # sample the rows
        ip_R = 100_000 / df_raw.count()
        df = df_raw.sample(False, ip_R, 123).persist()

        # generate the avoidance strategies
        df_ip_aa = (
            self.read_dev_file("ip_activity_avoidance_strategies")
            .filter(F.col("fyear") == self._fyear)
            .drop("dataset")
        )

        df_ip_aa_summary = (
            df_raw.join(df_ip_aa, ["rn", "fyear"])
            .groupBy(grouping_cols + ["strategy"])
            .agg(
                F.count("sample_rate").alias("n"),
                F.mode("sample_rate").alias("mode"),
                F.min("sample_rate").alias("min"),
                F.max("sample_rate").alias("max"),
            )
            .join(df_counts, grouping_cols)
            .withColumn("r", F.col("n") / F.col("count"))
            .drop("n", "count")
            .orderBy(F.desc("r"))
        )

        df_ip_aa = (
            df_ip_aa_summary.join(df[["rn"] + grouping_cols], grouping_cols)
            .drop(*grouping_cols)
            .toPandas()
        )

        df_ip_aa = df_ip_aa[
            np.random.binomial(1, df_ip_aa["r"].to_numpy()).astype(bool)
        ]

        df_ip_aa["sample_rate"] = df_ip_aa["mode"]
        df_ip_aa_fractions = df_ip_aa["min"] < df_ip_aa["max"]
        df_ip_aa.loc[df_ip_aa_fractions, "sample_rate"] = np.random.triangular(
            df_ip_aa.loc[df_ip_aa_fractions, "min"],
            df_ip_aa.loc[df_ip_aa_fractions, "mode"],
            df_ip_aa.loc[df_ip_aa_fractions, "max"],
        )

        df_ip_aa = df_ip_aa[["rn", "strategy", "sample_rate"]]

        # generate the efficiencies strategies
        df_ip_ef = (
            self.read_dev_file("ip_efficiencies_strategies")
            .filter(F.col("fyear") == self._fyear)
            .drop("dataset")
        )

        df_ip_ef_summary = (
            df_raw.join(df_ip_ef, ["rn", "fyear"])
            .groupBy(grouping_cols + ["strategy"])
            .agg(
                F.count("sample_rate").alias("n"),
            )
            .join(df_counts, grouping_cols)
            .withColumn("r", F.col("n") / F.col("count"))
            .drop("n", "count")
            .orderBy(F.desc("r"))
        )

        df_ip_ef = (
            df_ip_ef_summary.join(df[["rn"] + grouping_cols], grouping_cols)
            .drop(*grouping_cols)
            .toPandas()
        )

        df_ip_ef = df_ip_ef[
            np.random.binomial(1, df_ip_ef["r"].to_numpy()).astype(bool)
        ]

        df_ip_ef["sample_rate"] = 1

        df_ip_ef = df_ip_ef[["rn", "strategy", "sample_rate"]]

        # now generate the sample ip data
        df_p = df.join(mean_los, grouping_cols).toPandas()

        df_p["speldur"] = np.random.poisson(df_p["mean_los"])
        df_p.loc[df_p["pod"] == "ip_elective_daycase", "speldur"] = 0

        df_p["disdate"] = pd.Series(
            [datetime(self._fyear, 4, 1)] * len(df_p)
        ) + pd.to_timedelta(np.random.choice(range(0, 365), len(df_p)), unit="D")
        df_p["admidate"] = df_p["disdate"] - pd.to_timedelta(df_p["speldur"], unit="d")
        # convert the date columns to just date, otherwise pyspark can't read
        df_p["disdate"] = df_p["disdate"].dt.date
        df_p["admidate"] = df_p["admidate"].dt.date

        df_p = df_p.drop(columns=["mean_los"])

        new_rn = list(range(len(df_p)))
        self._rng.shuffle(new_rn)
        new_rn = dict(zip(df_p["rn"], new_rn))

        df_p["rn"] = df_p["rn"].map(new_rn)

        df_p = df_p.assign(sitetret=np.random.choice(["a", "b", "c"], len(df_p)))

        df_p["sushrg_trimmed"] = self._hrg_remapping(df_p["sushrg_trimmed"])

        # remap the rn column in the strategies tables
        df_ip_aa["rn"] = df_ip_aa["rn"].map(new_rn)
        df_ip_ef["rn"] = df_ip_ef["rn"].map(new_rn)

        # save the dataframes
        self.save_synth_file("ip", df_p)
        self.save_synth_file("ip_activity_avoidance_strategies", df_ip_aa)
        self.save_synth_file("ip_efficiencies_strategies", df_ip_ef)

    @generate_data("inequalities")
    def _inequalities(self, df: DataFrame) -> pd.DataFrame:
        inequalities = df.drop("dataset").toPandas()
        inequalities["sushrg_trimmed"] = self._hrg_remapping(
            inequalities["sushrg_trimmed"]
        )
        return inequalities.drop_duplicates(
            subset=["sushrg_trimmed", "icb", "imd_quintile"]
        )

    @generate_data("aae")
    def _aae(self, df: DataFrame) -> pd.DataFrame:
        rng = np.random.default_rng(self._seed)

        df = (
            df.drop("index", "dataset")
            .withColumn("sitetret", F.lit("a"))
            .withColumn("icb", F.when(F.col("is_main_icb"), "A").otherwise("B"))
        )

        df = (
            df.groupBy(df.drop("arrivals").columns)
            .agg(F.sum("arrivals").alias("arrivals"))
            .filter(F.col("arrivals") >= 10)
        )

        aae_R = self.AAE_N / df.agg(F.sum("arrivals")).collect()[0][0]
        assert 0 < aae_R < 1, f"A&E Scaling factor must be between 0 and 1, got {aae_R}"

        aae = (
            df.toPandas()
            .assign(arrivals=lambda r: rng.poisson(r["arrivals"] * aae_R))
            .query("arrivals > 0")
        )

        aae["rn"] = [str(uuid.uuid4()) for _ in aae.index]

        return aae

    @generate_data("op")
    def _op(self, df: DataFrame) -> pd.DataFrame:
        rng = np.random.default_rng(self._seed)

        df = (
            df.drop("index", "dataset")
            .withColumn("sitetret", F.lit("a"))
            .withColumn("icb", F.when(F.col("is_main_icb"), "A").otherwise("B"))
        )

        df = (
            df.groupBy(df.drop("attendances", "tele_attendances").columns)
            .agg(
                F.sum("attendances").alias("attendances"),
                F.sum("tele_attendances").alias("tele_attendances"),
            )
            .filter(F.col("attendances") >= 10)
            .persist()
        )

        op_R = self.OP_N / df.agg(F.sum("attendances")).collect()[0][0]

        op = (
            df.toPandas()
            .assign(
                attendances=lambda r: rng.poisson(r["attendances"] * op_R),
                tele_attendances=lambda r: rng.poisson(r["tele_attendances"] * op_R),
            )
            .query("(attendances > 0) or (tele_attendances > 0)")
        )

        op["sushrg_trimmed"] = self._hrg_remapping(op["sushrg_trimmed"])
        op["rn"] = [str(uuid.uuid4()) for _ in op.index]

        return op

    @generate_data("birth_factors")
    def _birth_factors(self, df: DataFrame) -> pd.DataFrame:
        return (
            df.drop("dataset")
            .filter(~F.col("variant").startswith("custom_projection_"))
            .toPandas()
            .groupby(["variant", "sex", "age"], as_index=False)
            .mean()
        )

    @generate_data("demographic_factors")
    def _demographic_factors(self, df: DataFrame) -> pd.DataFrame:
        return (
            df.drop("dataset")
            .filter(~F.col("variant").startswith("custom_projection_"))
            .toPandas()
            .groupby(["variant", "sex", "age"], as_index=False)
            .mean()
        )

    @generate_data("hsa_activity_tables")
    def _hsa_activity_tables(self, df: DataFrame) -> pd.DataFrame:
        return (
            df.drop("dataset")
            .toPandas()
            .groupby(["hsagrp", "sex", "age"], as_index=False)
            .mean()
        )


def main():
    logging.basicConfig(level=logging.INFO)
    logger.setLevel(logging.INFO)

    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)

    logging.getLogger("py4j").setLevel(logging.ERROR)

    path = table_names.model_data_path

    fyear = int(sys.argv[1][:4])
    seed = int(sys.argv[2])

    spark = get_spark()

    d = SynthData(fyear, path, seed, spark)
    d.generate()
