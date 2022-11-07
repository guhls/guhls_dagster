from dagster import (
    Field,
    String,
    Bool,
    Out,
    Output,
    op,
    job,
    AssetMaterialization,
    MetadataValue,
)
import requests


@op(
    config_schema={
        "bucket": Field(String, is_required=True, description="Just the bucket ex: my-bucket"),
        "prefix": Field(String, is_required=True, description="Just prefix ex: my/path/data.parquet"),
        "endpoint": Field(String, is_required=False),
        "to_parquet": Field(Bool, is_required=False, default_value=False),
    },
)
def df_to_s3(context, df):
    bucket = context.op_config.get('bucket')
    prefix = context.op_config.get('prefix')
    endpoint_url = context.op_config.get('endpoint')

    storage_options = {'client_kwargs': {'endpoint_url': endpoint_url}}

    path_s3 = f"s3://{bucket}/{prefix}"

    if context.op_config.get('to_parquet'):
        df.to_parquet(path_s3, storage_options=storage_options)
    else:
        df.to_csv(path_s3, storage_options=storage_options)

    context.log_event(
        AssetMaterialization(
            asset_key="dataset",
            description="Result to my storage",
            metadata={
                "text_metadata": "metadata for dataset storage in S3",
                "path": MetadataValue.path(path_s3),
            }
        )
    )

    return Output(None)


@op(config_schema={
    "endpoint": Field(
        String,
        is_required=True,
        description="Endpoint fix of the API within path Ex: 'https:subdomain-url/domain.com/'"
    ),
    "path": Field(
        String,
        is_required=True,
        description="Rest of the url that will attached to endpoint Ex: 'search/maxitem'"
    ),
    "params": Field(
        dict,
        is_required=False,
        description="Parameters that go in url requisition"
    ),
    "headers": Field(
        dict,
        is_required=False,
    )
})
def get_data_from_api(context):
    endpoint = context.solid_config.get('endpoint')
    path = context.solid_config.get('path')
    params = context.solid_config.get('params')
    headers = context.solid_config.get('headers')

    url_endpoint = endpoint + path

    resp = requests.get(url_endpoint, params=params, headers=headers).json()
    return resp


if __name__ == '__main__':
    from guhls.yelp.pipelines import yelp_data_pipe
    from dotenv import load_dotenv
    import os

    load_dotenv()
    API_KEY = os.environ.get('API_KEY')
    BUCKET = os.environ.get('BUCKET')
    PREFIX = os.environ.get('PREFIX')

    yelp_data_pipe.execute_in_process(run_config={
        "solids": {
            "get_data_from_api": {
                "config": {
                    "endpoint": "https://api.yelp.com/v3/",
                    "path": "businesses/search",
                    "params": {"term": "bars", "location": "São Paulo"},
                    "headers": {"Authorization": f"Bearer {API_KEY}"}
                }
            },
            "transform_data_from_yelp": {
                "config": {
                    "categories": True
                }
            },
            "df_to_s3": {
                "config": {
                    "bucket": BUCKET,
                    "prefix": PREFIX,
                }
            }
        }
    })
