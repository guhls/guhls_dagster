from dagster import (
    Field,
    String,
    Out,
    OutputDefinition,
    Output,
    op,
    solid,
    AssetMaterialization,
    MetadataValue,
)
import requests
import pandas as pd

@op(
    config_schema={
        "bucket": Field(String, is_required=True, description="Just the bucket ex: my-bucket"),
        "prefix": Field(String, is_required=True, description="Just prefix ex: my/path/data.parquet"),
        "endpoint": Field(String, is_required=False),
    },
    out={"url_s3": Out()}
)
def df_to_s3(context, df, url):
    bucket = context.op_config.get('bucket')
    prefix = context.op_config.get('prefix')
    endpoint_url = context.op_config.get('endpoint')

    storage_options = {'client_kwargs': {'endpoint_url': endpoint_url}}

    path_s3 = f"s3://{bucket}/{prefix}"

    df.to_parquet(path_s3, storage_options=storage_options)

    context.log_event(
        AssetMaterialization(
            asset_key="dataset",
            description="Result to my storage",
            metadata={
                "text_metadata": "metadata for dataset storage in S3",
                "path": MetadataValue.path(path_s3),
                "dashboard_url": MetadataValue.url(url)
            }
        )
    )

    return Output(path_s3)


@op(config_schema={
    "url": Field(
        String,
        is_required=True,
        default_value="https://hacker-news.firebaseio.com/v0/maxitem.json?print=pretty"
    )
})
def get_data_from_hacker_news_api(context):
    url = context.solid_config.get('url')

    response = requests.get(url)
    response_data = response.json()

    return pd.read_json(response_data)


from dagster import execute_solid
if __name__ == '__main__':
    execute_solid(get_data_from_hacker_news_api, run_config={
        "solids": {
            "get_data_from_hacker_news_api": {
                "config": {
                    "url": "https://hacker-news.firebaseio.com/v0/maxitem.json?print=pretty"
                }
            }
        }
    })
