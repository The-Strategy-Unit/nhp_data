"""Process the ODS xml data

Download the ODS data from TRUD and process to get the list of ODS codes to names, along with
current successors for NHS Trusts/Care Trusts/Indepedent sector providers.

In order to run this, you need an API key for [TRUD][1].

Create an account, and then in the [account managment screen][2] you can view your API key.

You will need to subscribe to the [ODS XML Organisation Data][3].

Then, using the databricks CLI you can update the secret using

```
$ databricks secrets put-secret nhp trud_api_key
```

[1]: https://isd.digital.nhs.uk/trud/user/guest/group/0/home
[2]: https://isd.digital.nhs.uk/trud/users/authenticated/filters/0/account/manage
[3]: https://isd.digital.nhs.uk/trud/users/authenticated/filters/0/categories/5/items/341/releases
"""

import io
import xml.etree.ElementTree as ET
import zipfile
from collections import defaultdict
from datetime import datetime, timedelta

import pandas as pd
import requests
from databricks.connect import DatabricksSession
from pyspark.dbutils import DBUtils


def download_ods_full_archive(api_key: str) -> ET.Element:
    """Download the ODS full archive file

    :param api_key: API key for TRUD
    :type api_key: str
    :return: parsed xml tree
    :rtype: ET.Element
    """
    ods_id = 341

    url = f"https://isd.digital.nhs.uk/trud/api/v1/keys/{api_key}/items/{ods_id}/releases?latest"

    response = requests.get(url, timeout=10)
    assert response.status_code == 200

    url = response.json()["releases"][0]["archiveFileUrl"]
    response = requests.get(url, timeout=100)
    assert response.status_code == 200

    ods_data = response.content

    with zipfile.ZipFile(io.BytesIO(ods_data)) as z:
        with z.open("fullfile.zip") as fullfile:
            with zipfile.ZipFile(fullfile) as fullfile_zip:
                filename = fullfile_zip.namelist()[0]
                with fullfile_zip.open(filename) as xml_file:
                    return ET.fromstring(xml_file.read().decode("utf-8"))


def process_successor(x: ET.Element, org_code: str) -> dict:
    """Process successor records

    :param x: xml element
    :type x: ET.Element
    :param org_code: the organisation code that we are processing
    :type org_code: str
    :return: A dictionary containing the successor record
    :rtype: dict
    """
    successor = x.find("Target/OrgId").attrib["extension"]
    if x.find("Type").text == "Predecessor":
        org_code, successor = successor, org_code
    return {
        "from": org_code,
        "to": successor,
        "date": x.find("Date/Start").attrib["value"],
    }


def process_organisation(org: ET.Element) -> dict:
    """Process an organisation record

    :param org: xml element
    :type org: ET.Element
    :return: a dictionary containing the data for the organisation
    :rtype: dict
    """
    org_code = org.find("OrgId").attrib["extension"]

    org_dict = {
        "org_code": org_code,
        "org_name": org.find("Name").text,
        "status": org.find("Status").attrib["value"],
        "primary_role": org.find("Roles/Role[@primaryRole='true']").attrib["id"],
    }

    operational_date = [
        i for i in org.findall("Date") if i.find("Type").get("value") == "Operational"
    ][0]
    org_dict["start_date"] = operational_date.find("Start").get("value")
    end_date = operational_date.find("End")
    if end_date is not None:
        org_dict["end_date"] = end_date.get("value")

    postcode = org.find("GeoLoc/Location/PostCode")
    if postcode is not None:
        org_dict["postcode"] = postcode.text

    org_dict["successors"] = [
        process_successor(i, org_code)
        for i in org.findall("Succs/Succ[Type='Predecessor']")
    ]

    return org_dict


def get_successors_df(processed_orgs: list, ods_df: pd.DataFrame) -> pd.DataFrame:
    """Get the successors data frame

    :param processed_orgs: list of organisation dicts
    :type processed_orgs: list
    :param ods_df: the processed orgs data as a data frame
    :type ods_df: pd.DataFrame
    :return: data frame containing the organisation successors
    :rtype: pd.DataFrame
    """
    successors = defaultdict(lambda: [])

    for i in processed_orgs:
        for j in i["successors"]:
            successors[j["from"]].append((j["to"], j["date"]))

    successors = dict(successors)

    successors = {
        **{
            i["org_code"]: [(None, None)]
            for i in processed_orgs
            if i["org_code"] not in successors
        },
        **successors,
    }

    transitive_closure = []

    for i in successors.keys():
        q = [(i, "1900-01-01")]

        while q:
            j, start_date = q.pop()
            for k, end_date in successors[j]:
                if j == "RW6" and k == "R0A":
                    continue
                transitive_closure.append(
                    {
                        "org_from": i,
                        "org_to": j,
                        "start_date": datetime.strptime(start_date, "%Y-%m-%d").date(),
                        "end_date": (
                            datetime.strptime(end_date, "%Y-%m-%d").date()
                            - timedelta(days=1)
                            if end_date
                            else None
                        ),
                    }
                )
                if k:
                    q.append((k, end_date))

    successors_df = pd.DataFrame(transitive_closure).drop_duplicates()

    # only keep Care Trusts/NHS Trust/Indepedent Care Providers
    org_codes = ods_df.loc[
        ods_df["primary_role"].isin(["RO107", "RO197", "RO172"]), "org_code"
    ]
    successors_df = successors_df[
        successors_df["org_from"].isin(org_codes)
        & successors_df["org_to"].isin(org_codes)
    ]

    # only keep current records
    successors_df = successors_df[successors_df["end_date"].isnull()]

    return successors_df


def get_ods_trusts_and_current_successors(api_key: str) -> pd.DataFrame:
    """Get latest ODS file of trusts and current successors

    :param api_key: TRUD api key
    :type api_key: str
    :return: data frame of ODS trusts with their current successors
    :rtype: pd.DataFrame
    """
    root = download_ods_full_archive(api_key)
    processed_orgs = list(map(process_organisation, root.find(".//Organisations")))

    code_systems = {
        i.get("id"): i.get("displayName")
        for i in root.findall(".//CodeSystems/CodeSystem/concept")
    }

    ods_df = (
        pd.DataFrame(processed_orgs)
        .drop(columns="successors")
        .assign(
            primary_role_description=lambda x: x["primary_role"].apply(code_systems.get)
        )
    )

    successors_df = get_successors_df(processed_orgs, ods_df)

    df_orgs = (
        ods_df.query("(end_date >= '2008-04-01') or end_date.isnull()")
        .query("primary_role.isin(['RO107', 'RO197', 'RO172'])")
        .merge(
            successors_df.drop(columns=["start_date", "end_date"]),
            left_on="org_code",
            right_on="org_from",
            how="inner",
        )
    )

    # we should see no records here, if we do there is an organisation mapped to two orgs
    check = df_orgs.value_counts("org_code") > 1
    assert (
        sum(check) == 0
    ), "organisations have been duplicated when joining to successors"

    return df_orgs


def main():
    spark = DatabricksSession.builder.getOrCreate()
    dbutils = DBUtils(spark)

    API_KEY = dbutils.secrets.get(scope="nhp", key="trud_api_key")
    ods_df = get_ods_trusts_and_current_successors(API_KEY)

    trust_types = spark.read.parquet(
        "/Volumes/nhp/reference/files/trust_types.parquet"
    ).withColumnRenamed("org_code", "org_to")

    df = spark.createDataFrame(ods_df).join(trust_types, "org_to", "left")

    df.write.mode("overwrite").saveAsTable("strategyunit.reference.ods_trusts")
