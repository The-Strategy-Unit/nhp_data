# pylint: disable=line-too-long
"""Excess Bed Days

Sometimes a patient will stay in hospital for longer than expected. 
Expected lengths of stay are calculated for each spell based on the  upper quartile length of stay
for the Healthcare Resource Group (HRG) plus 1.5 times the inter-quartile range of length of stay. 
This is known as the "Trim point". 

Hospitals are reimbursed for additional days in excess of the trim point via an excess bed day
payment. 
Excess Bed Days can also be costly for providers as the payment does not cover the full costs of the
additional stay. 

In some instances, excess bed days may be avoidable; for example, where the excess bed days are as a
result of delayed discharges or suboptimal rehabilitation support.

## Available breakdowns

- Elective (IP-EF-018)
- Emergency (IP-EF-019)

### Data sources

Trim Points are updated annually and can be found in the [National Tariff workbooks][1].

[1]: https://www.england.nhs.uk/publication/past-national-tariffs-documents-and-policies/
"""
# pylint: enable=line-too-long

import pyspark.sql.types as T
from databricks.connect import DatabricksSession
from pyspark.sql import functions as F

from hes_datasets import nhp_apc
from mitigators import efficiency_mitigator

spark = DatabricksSession.builder.getOrCreate()


def _excess_beddays(group):
    ebd = (
        spark.read.option("header", "true")
        .option("delimiter", ",")
        .schema(
            T.StructType(
                [
                    T.StructField("sushrg", T.StringType(), False),
                    T.StructField("elective", T.StringType(), False),
                    T.StructField("emergency", T.StringType(), False),
                ]
            )
        )
        .csv("/Volumes/su_data/nhp/reference_data/hrg_trimpoints.csv")
        .select("sushrg", F.col(group).alias("trimpoint"))
    )

    return (
        nhp_apc.join(ebd, ["sushrg"])
        .filter(F.col("admimeth").startswith(admimeth))
        .filter(F.col("speldur") > F.col("trimpoint"))
        .select("epikey")
        .withColumn("sample_rate", F.lit(1.0))
    )


@efficiency_mitigator()
def _excess_beddays_elective():
    return _excess_beddays("elective")


@efficiency_mitigator()
def _excess_beddays_emergency():
    return _excess_beddays("emergency")
