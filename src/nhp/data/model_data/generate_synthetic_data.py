import logging
import os
import sys
import uuid
from typing import Callable

import numpy as np
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from nhp.data.get_spark import get_spark

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
    # how many inpatients rows should we target?
    IP_N = 100000

    def __init__(self, fyear: int, path: str, seed: int, spark: SparkSession):
        self._fyear = fyear
        self._dev_path = f"{path}/dev"
        self._synth_path = f"{path}/synth"
        self._seed = seed

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
        df.to_parquet(f"{p}/0.parquet")

    def generate(self) -> None:
        self._ip()
        self._ip_activity_avoidance_stratgegies()
        self._ip_efficiencies_strategies()
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

    @generate_data("ip")
    def _ip(self, df: DataFrame) -> pd.DataFrame:
        ip_R = self.IP_N / df.count()

        df = df.sample(False, ip_R, self._seed)

        ip = df.drop("dataset", "fyear").toPandas()
        ip = ip.assign(sitetret=np.random.choice(["a", "b", "c"], len(ip)))

        ip["sushrg_trimmed"] = self._hrg_remapping(ip["sushrg_trimmed"])

        return ip

    @generate_data("ip_activity_avoidance_strategies")
    def _ip_activity_avoidance_stratgegies(self, df: DataFrame) -> pd.DataFrame:
        ip_df = self.read_synth_file("ip")
        return df.join(ip_df, "rn", "semi").toPandas()

    @generate_data("ip_efficiencies_strategies")
    def _ip_efficiencies_strategies(self, df: DataFrame) -> pd.DataFrame:
        ip_df = self.read_synth_file("ip")
        return df.join(ip_df, "rn", "semi").toPandas()

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
        n_aae_datasets = df.select("dataset").distinct().count()

        df = (
            df.drop("index", "dataset")
            .withColumn("sitetret", F.lit("a"))
            .withColumn("icb", F.when(F.col("is_main_icb"), "A").otherwise("B"))
        )

        aae = (
            df.groupBy(df.drop("arrivals").columns)
            .agg(F.sum("arrivals").alias("arrivals"))
            .toPandas()
            .assign(arrivals=lambda r: rng.poisson(r["arrivals"] / n_aae_datasets))
            .query("arrivals > 0")
        )

        aae["rn"] = [str(uuid.uuid4()) for _ in aae.index]

        return aae

    @generate_data("op")
    def _op(self, df: DataFrame) -> pd.DataFrame:
        rng = np.random.default_rng(self._seed)
        n_op_datasets = df.select("dataset").distinct().count()

        df = (
            df.drop("index", "dataset")
            .withColumn("sitetret", F.lit("a"))
            .withColumn("icb", F.when(F.col("is_main_icb"), "A").otherwise("B"))
        )

        op = (
            df.groupBy(df.drop("attendances", "tele_attendances").columns)
            .agg(
                F.sum("attendances").alias("attendances"),
                F.sum("tele_attendances").alias("tele_attendances"),
            )
            .toPandas()
            .assign(
                attendances=lambda r: rng.poisson(r["attendances"] / n_op_datasets),
                tele_attendances=lambda r: rng.poisson(
                    r["tele_attendances"] / n_op_datasets
                ),
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

    path = sys.argv[1]
    fyear = int(sys.argv[2][:4])
    seed = int(sys.argv[3])

    spark = get_spark()

    d = SynthData(fyear, path, seed, spark)
    d.generate()
