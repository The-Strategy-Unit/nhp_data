import io

import pandas as pd
import requests
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from nhp.data.get_spark import get_spark
from nhp.data.table_names import table_names


def download_arcgis_results(url: str, where="1=1", fields="*") -> pd.DataFrame:
    """
    Download data from an ArcGIS REST API endpoint and return as a pandas DataFrame.

    This function handles pagination automatically when the response exceeds the transfer limit,
    making multiple requests if necessary to retrieve all features.

    Args:
        url (str): The ArcGIS REST API endpoint URL (e.g., FeatureServer or MapServer query endpoint)
        where (str, optional): SQL WHERE clause to filter features. Defaults to "1=1" (all records)
        fields (str, optional): Comma-separated list of field names to retrieve, or "*" for all fields.
            Defaults to "*"

    Returns:
        pd.DataFrame: DataFrame containing the properties of all features returned from the query.
            Each row represents one feature, with columns corresponding to the feature properties.

    Raises:
        requests.exceptions.HTTPError: If the HTTP request fails
        KeyError: If the response JSON doesn't contain expected 'features' key
    """
    params = {
        "where": where,
        "outFields": fields,
        "f": "geojson",
        "resultRecordCount": 1000,
        "resultOffset": 0,
    }
    features = []
    while True:
        resp = requests.get(url, params=params)
        resp.raise_for_status()
        data = resp.json()
        features.extend(data["features"])
        if (
            "properties" in data
            and "exceededTransferLimit" in data["properties"]
            and data["properties"]["exceededTransferLimit"]
        ):
            params["resultOffset"] = len(features)
        else:
            break

    return pd.DataFrame([i["properties"] for i in features])


def download_lsoa11_to_lsoa21_lookup() -> pd.DataFrame:
    """
    Download the lookup table mapping LSOA 2011 codes to LSOA 2021 codes.

    This function fetches the Lower Layer Super Output Area (LSOA) lookup data
    from the ONS Open Geography Portal, which maps 2011 LSOA codes to their
    corresponding 2021 LSOA codes.

    Returns:
        pd.DataFrame: A DataFrame containing two columns:
            - lsoa11cd: LSOA 2011 codes
            - lsoa21cd: LSOA 2021 codes

    Raises:
        requests.exceptions.HTTPError: If the HTTP request to download the data fails.
        requests.exceptions.RequestException: If there are network-related errors.

    Notes:
        The data is sourced from the ONS Open Geography Portal via ArcGIS Hub.
        The CSV content is decoded using 'utf-8-sig' to handle any BOM characters.
    """
    url = "/".join(
        [
            "https://open-geography-portalx-ons.hub.arcgis.com",
            "api",
            "download",
            "v1",
            "items",
            "cbfe64cc03d74af982c1afec639bafd1",
            "csv",
        ]
    )

    result = requests.get(url, params={"layers": 0})
    result.raise_for_status()

    content = io.StringIO(result.content.decode("utf-8-sig"))

    df = pd.read_csv(content, usecols=["LSOA11CD", "LSOA21CD"])  # ty: ignore[no-matching-overload]
    df.columns = [col.lower() for col in df.columns]

    return df


def get_lsoa11_to_lsoa21_lookup(spark: SparkSession) -> DataFrame:
    """
    Retrieves or creates a lookup table mapping LSOA 2011 codes to LSOA 2021 codes.

    This function checks if the lookup table exists in the Spark catalog. If it doesn't exist,
    it downloads the lookup data, creates a DataFrame, and saves it as a table. If the table
    already exists, it simply reads and returns it.

    Args:
        spark (SparkSession): The active Spark session used to interact with the catalog
            and read/write tables.

    Returns:
        DataFrame: A Spark DataFrame containing the LSOA 2011 to LSOA 2021 lookup data.

    Note:
        The table name is defined in `table_names.reference_lsoa11_to_lsoa21`.
        If the table doesn't exist, `download_lsoa11_to_lsoa21_lookup()` is called to
        retrieve the data.
    """
    table = table_names.reference_lsoa11_to_lsoa21
    if not spark.catalog.tableExists(table):
        df = download_lsoa11_to_lsoa21_lookup()
        spark.createDataFrame(df).write.mode("overwrite").saveAsTable(table)

    return spark.read.table(table)


def create_lsoa21_to_lad23_lookup() -> pd.DataFrame:
    """
    Create a lookup table mapping Lower Layer Super Output Areas (2021) to Local Authority Districts (2023).

    This function downloads geographical lookup data from the ArcGIS REST API that maps
    LSOA 2021 codes to LAD 2023 codes for England and Wales.

    Returns:
        pd.DataFrame: A DataFrame containing the LSOA21 to LAD23 lookup with columns:
            - lsoa21cd: Lower Layer Super Output Area 2021 code
            - lad23cd: Local Authority District 2023 code

    Notes:
        The data is sourced from the Office for National Statistics (ONS) via ArcGIS services.
        Column names are converted to lowercase for consistency.
    """
    lsoa21_to_lad23 = download_arcgis_results(
        "/".join(
            [
                "https://services1.arcgis.com",
                "ESMARspQHYMw9BZ9",
                "arcgis",
                "rest",
                "services",
                "LSOA21_WD23_LAD23_EW_LU",
                "FeatureServer",
                "0",
                "query",
            ]
        ),
        fields="LSOA21CD,LAD23CD",
    )

    lsoa21_to_lad23.columns = [col.lower() for col in lsoa21_to_lad23.columns]
    return lsoa21_to_lad23


def get_lsoa21_to_lad23(spark: SparkSession) -> DataFrame:
    """
    Retrieve or create the LSOA 2021 to LAD 2023 lookup table.

    This function checks if the LSOA 2021 to LAD 2023 lookup table exists in the Spark catalog.
    If the table does not exist, it creates the lookup using create_lsoa21_to_lad23_lookup(),
    converts it to a Spark DataFrame, and saves it as a table. If the table already exists,
    it simply reads and returns it.

    Args:
        spark (SparkSession): The active Spark session used to interact with the catalog
                             and read/write tables.

    Returns:
        DataFrame: A Spark DataFrame containing the LSOA 2021 to LAD 2023 lookup mapping.
    """
    table = table_names.reference_lsoa21_to_lad23
    if not spark.catalog.tableExists(table):
        df = create_lsoa21_to_lad23_lookup()

        spark.createDataFrame(df).write.mode("overwrite").saveAsTable(table)

    return spark.read.table(table)


def create_lsoa11_to_lad23_lookup(spark: SparkSession) -> DataFrame:
    """
    Create a lookup table mapping LSOA 2011 codes to LAD 2023 codes for England.

    This function combines the LSOA 2011 to LSOA 2021 lookup with the LSOA 2021 to LAD 2023
    lookup to create a direct mapping. It filters for English LSOAs only (codes starting with 'E')
    and handles specific edge cases where LSOAs map to multiple LADs by keeping only the primary
    mapping based on geographical analysis.

    Args:
        spark (SparkSession): The active Spark session used to read lookup tables.

    Returns:
        DataFrame: A Spark DataFrame containing the LSOA 2011 to LAD 2023 lookup with columns:
            - lsoa11cd: LSOA 2011 code
            - lad23cd: LAD 2023 code

    Raises:
        AssertionError: If the resulting lookup contains LSOAs mapping to multiple LADs,
            unexpected number of mappings, or unexpected number of LAD23 codes.

    Notes:
        - Filters out 4 specific edge case mappings where >99% of the LSOA area is in a single LAD
        - Validates that each LSOA11 maps to exactly one LAD23
        - Expects 32,844 LSOA11 to LAD23 mappings
        - Expects 296 distinct LAD23 codes
    """
    lsoa11_to_lsoa21 = get_lsoa11_to_lsoa21_lookup(spark)
    lsoa21_to_lad23 = get_lsoa21_to_lad23(spark)

    df = (
        lsoa11_to_lsoa21.filter(F.col("lsoa11cd").startswith("E"))  # ty:ignore[missing-argument, invalid-argument-type]
        .join(lsoa21_to_lad23, "lsoa21cd")
        .select("lsoa11cd", "lad23cd")
        .distinct()
        # handle some specific edge cases:
        # these 4 LSOA11s map to multiple LAD23s, but geospatial analysis shows
        # that >99% of the area for each LSOA11 is within a single LAD23
        .filter(
            ~(
                ((F.col("lsoa11cd") == "E01008187") & (F.col("lad23cd") != "E08000037"))
                | (
                    (F.col("lsoa11cd") == "E01027506")
                    & (F.col("lad23cd") != "E06000057")
                )
                | (
                    (F.col("lsoa11cd") == "E01023964")
                    & (F.col("lad23cd") != "E07000241")
                )
                | (
                    (F.col("lsoa11cd") == "E01023508")
                    & (F.col("lad23cd") != "E07000242")
                )
            )
        )
    )

    # validate that this mapping is valid
    check = df.groupBy("lsoa11cd").count().filter(F.col("count") > 1).count()
    assert check == 0, "Some LSOA11s map to multiple LAD23s"
    assert df.count() == 32844, "Unexpected number of LSOA11 to LAD23 mappings"
    assert df.select("lad23cd").distinct().count() == 296, "Unexpected number of LAD23s"

    return df


def get_lsoa11_to_lad23_lookup(spark: SparkSession) -> DataFrame:
    """
    Retrieve or create the LSOA 2011 to LAD 2023 lookup table.

    This function checks if the LSOA 2011 to LAD 2023 lookup table exists in the Spark catalog.
    If the table does not exist, it creates the lookup using create_lsoa11_to_lad23_lookup(),
    and saves it as a table. If the table already exists, it simply reads and returns it.

    Args:
        spark (SparkSession): The active Spark session used to interact with the catalog
                             and read/write tables.

    Returns:
        DataFrame: A Spark DataFrame containing the LSOA 2011 to LAD 2023 lookup mapping.

    Note:
        The table name is defined in `table_names.reference_lsoa11_to_lad23`.
    """
    table = table_names.reference_lsoa11_to_lad23
    if not spark.catalog.tableExists(table):
        df = create_lsoa11_to_lad23_lookup(spark)
        df.write.mode("overwrite").saveAsTable(table)

    return spark.read.table(table)


def create_lad22_to_lad23_lookup(spark: SparkSession) -> pd.DataFrame:
    """
    Create a lookup table mapping Local Authority District 2022 codes to LAD 2023 codes.

    This function downloads geographical lookup data from the ArcGIS REST API that maps
    LAD 2022 codes to LAD 2023 codes for the United Kingdom.

    Args:
        spark (SparkSession): The active Spark session (unused but kept for consistency).

    Returns:
        pd.DataFrame: A DataFrame containing the LAD22 to LAD23 lookup with columns:
            - lad22cd: Local Authority District 2022 code
            - lad23cd: Local Authority District 2023 code

    Notes:
        The data is sourced from the Office for National Statistics (ONS) via ArcGIS services.
        Column names are converted to lowercase for consistency.
    """
    lad22_to_lad23 = download_arcgis_results(
        "/".join(
            [
                "https://services1.arcgis.com",
                "ESMARspQHYMw9BZ9",
                "arcgis",
                "rest",
                "services",
                "LAD22_LAD23_UK_LU_v1",
                "FeatureServer",
                "0",
                "query",
            ]
        ),
        fields="LAD22CD,LAD23CD",
    )

    lad22_to_lad23.columns = [col.lower() for col in lad22_to_lad23.columns]
    return lad22_to_lad23


def get_lad22_to_lad23_lookup(spark: SparkSession) -> DataFrame:
    """
    Retrieve or create the LAD 2022 to LAD 2023 lookup table.

    This function checks if the LAD 2022 to LAD 2023 lookup table exists in the Spark catalog.
    If the table does not exist, it creates the lookup using create_lad22_to_lad23_lookup(),
    converts it to a Spark DataFrame, and saves it as a table. If the table already exists,
    it simply reads and returns it.

    Args:
        spark (SparkSession): The active Spark session used to interact with the catalog
                             and read/write tables.

    Returns:
        DataFrame: A Spark DataFrame containing the LAD 2022 to LAD 2023 lookup mapping.

    Note:
        The table name is defined in `table_names.reference_lad22_to_lad23`.
    """
    table = table_names.reference_lad22_to_lad23
    if not spark.catalog.tableExists(table):
        df = create_lad22_to_lad23_lookup(spark)
        spark.createDataFrame(df).write.mode("overwrite").saveAsTable(table)

    return spark.read.table(table)


def main():
    """
    Main entry point for generating LSOA and LAD lookup tables.

    This function creates all necessary lookup tables by calling the appropriate
    getter functions. The LSOA11 to LAD23 lookup will trigger creation of dependent
    lookups (LSOA11 to LSOA21 and LSOA21 to LAD23) if they don't already exist.

    The following lookup tables are generated:
        - LSOA 2011 to LAD 2023
        - LAD 2022 to LAD 2023
    """
    spark = get_spark()

    # this will generate the other lsoa lookups as needed
    get_lsoa11_to_lad23_lookup(spark)
    get_lad22_to_lad23_lookup(spark)
