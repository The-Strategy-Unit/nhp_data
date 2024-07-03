"""Activity Mitigators for the NHP model."""

import os
from collections import defaultdict

from databricks.sdk.runtime import dbutils
from pyspark.sql import DataFrame

from mitigators.ip.activity_avoidance import *
from mitigators.ip.efficiency import *

__registered_mitigators = defaultdict(lambda: {})


class Mitigator:
    """Activity Mitigator

    A class that encapsulates the logic for extracting activity mitigators for the NHP model.
    Each mitigator should contain two columns: epikey and sample rate.

    Sample rate should be a float in the range [0, 1].
    Most of the time this value should simply be 1.

    Should be created by decorating a function that generates a mitigator DataFrame with @mitigator.
    """

    __save_path = "/Volumes/su_data/nhp/apc_mitigators"

    def __init__(self, mitigator_type: str, mitigator_name: str, definition: DataFrame):
        self.mitigator_type = mitigator_type
        self.mitigator_name = mitigator_name
        self._definition = definition

    def get(self) -> DataFrame:
        """Get the RDD for this mitigator"""
        return self._definition

    def __repr__(self):
        return str(self)

    def __str__(self):
        return f"Mitigator: {self.mitigator_name} ({self.mitigator_type})"

    def save(self, recreate: bool = False) -> None:
        """Save the mitigator

        :param recreate: delete the existing data and recreate? defaults to False
        :type recreate: bool, optional
        """
        save_path = "/".join(
            [
                self.__save_path,
                f"type={self.mitigator_type}",
                f"strategy={self.mitigator_name}",
            ]
        )
        if recreate:
            dbutils.fs.rm(save_path, True)

        if os.path.exists(save_path):
            return

        self.get().repartition(1).write.mode("overwrite").parquet(save_path)


def mitigator(mitigator_type: str, mitigator_name: str = None):
    """Mitigator Decorator

    A decorator to use for functions that generate a mitigator DataFrame

    :param mitigator_type: _description_
    :type mitigator_type: _type_
    :param mitigator_name: _description_, defaults to None
    :type mitigator_name: _type_, optional
    """

    def decorator(func):
        nonlocal mitigator_name
        if mitigator_name is None:
            mitigator_name = func.__name__.lstrip("_")

        m = Mitigator(mitigator_type, mitigator_name, func())

        if mitigator_name in __registered_mitigators[mitigator_type]:
            raise Exception(  # pylint: disable=broad-exception-raised
                f"duplicate mitigator: {mitigator_name} ({mitigator_type})"
            )

        __registered_mitigators[mitigator_type][mitigator_name] = m

        def wrapper():
            return m

        return wrapper

    return decorator


def activity_avoidance_mitigator(mitigator_name: str = None):
    """Activity Avoidance Mitigator Decorator"""
    return mitigator("activity_avoidance", mitigator_name)


def efficiency_mitigator(mitigator_name: str = None):
    """Efficiency Mitigator Decorator"""
    return mitigator("efficiency", mitigator_name)
