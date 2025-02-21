import pandas as pd

import dagster as dg


@dg.asset
def processed_data():
    df = pd.read_csv("src/resources/data/sample_data.csv")

    df["age_group"] = pd.cut(
        df["age"], bins=[0, 30, 40, 100], labels=["Young", "Middle", "Senior"]
    )

    df.to_csv("src/resources/data/processed_data.csv", index=False)
    return "Data loaded successfully"


## Tell Dagster about the assets that make up the pipeline by
## passing it to the Definitions object
## This allows Dagster to manage the assets' execution and dependencies
defs = dg.Definitions(assets=[processed_data])

