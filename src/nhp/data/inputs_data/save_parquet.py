import os
import shutil


def save_parquet(df, path):
    # save the dataframe
    df.repartition(1).write.parquet(path)
    # find the created files
    files = os.listdir(path)
    # find just the parquet file
    parquets = list(filter(lambda f: f.endswith(".parquet"), files))
    assert len(parquets) == 1, "Too many parquet files, should only be one."
    parquet = parquets[0]
    # move the parquet file
    if os.path.exists(f"{path}.parquet"):
        os.remove(f"{path}.parquet")
    os.rename(f"{path}/{parquet}", f"{path}.parquet")
    # remove the other files
    shutil.rmtree(path)
