from dagster import op, Field
from pandas import json_normalize
import numpy as np


@op(config_schema={"categories": Field(bool, False, False)})
def transform_data_from_yelp(context, response):
    data = response['businesses']
    df = json_normalize(data, sep="_").drop("categories", axis=1)

    if context.solid_config['categories']:
        categories = json_normalize(data, sep="_", record_path="categories", meta=["id"])
        categories = categories.rename({"alias": "cate_alias", "title": "cate_title"}, axis=1)
        df = df.merge(categories, on="id")
        columns = df.columns.tolist()
        df = df[columns[:3] + columns[-2:] + columns[3:-2]]

    df = df.replace('', np.nan)
    df['price'] = df['price'].fillna("-")
    df['ddi'] = df['phone'].str[:3]

    return df


