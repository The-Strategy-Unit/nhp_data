"""Activity Mitigators for the NHP model."""

import os
from collections import defaultdict

from databricks.connect import DatabricksSession
from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from raw_data.mitigators.ip.activity_avoidance import *
from raw_data.mitigators.ip.efficiency import *

spark = DatabricksSession.builder.getOrCreate()

__registered_mitigators = defaultdict(lambda: {})


class Mitigator:
    """Activity Mitigator

    A class that encapsulates the logic for extracting activity mitigators for the NHP model.
    Each mitigator should contain two columns: epikey and sample rate.

    Sample rate should be a float in the range [0, 1].
    Most of the time this value should simply be 1.

    Should be created by decorating a function that generates a mitigator DataFrame with @mitigator.
    """

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

    def save(self) -> None:
        """Save the mitigator"""
        print(f"Saving mitigator: {self.mitigator_name} ({self.mitigator_type})")

        # get the mitigators we want to insert
        source = (
            self.get()
            .withColumn("type", F.lit(self.mitigator_type))
            .withColumn("strategy", F.lit(self.mitigator_name))
        )

        # get the table to load the mitigators to
        target = (
            DeltaTable.createIfNotExists(spark)
            .tableName("nhp.raw_data.apc_mitigators")
            .addColumns(source.schema)
            .execute()
        )
        # perform an upsert
        (
            target.alias("target")
            .merge(
                source.alias("source"),
                " and ".join(
                    [
                        "source.epikey = target.epikey",
                        "source.type = target.type",
                        "source.strategy = target.strategy",
                    ]
                ),
            )
            # update the rows that have changed
            .whenMatchedUpdateAll(condition="target.sample_rate != source.sample_rate")
            # insert the rows that previously didn't exist
            .whenNotMatchedInsertAll()
            # delete rows for this strategy that no longer exist
            .whenNotMatchedBySourceDelete(
                condition=" and ".join(
                    [
                        f"target.strategy = '{self.mitigator_name}'",
                        f"target.type = '{self.mitigator_type}'",
                    ]
                )
            )
            .execute()
        )


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
