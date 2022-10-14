from dagster import (
    Field,
    String,
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
        is_required=True,
        description="Parameters that go in url requisition"
    )
})
def get_data_from_api(context):
    endpoint = context.solid_config.get('endpoint')
    path = context.solid_config.get('path')
    params = context.solid_config.get('params')

    url_endpoint = endpoint + path

    if params:
        url_endpoint += "?"
        lst_params = []
        for key, value in params.items():
            lst_params.append(f"{key}={value}")
        url_endpoint += "&".join(lst_params)

    data = requests.get(url_endpoint).json()
    return data


if __name__ == '__main__':
    @job
    def execute_ops():
        get_data_from_api()

    execute_ops.execute_in_process(run_config={
        "solids": {
            "get_data_from_api": {
                "config": {
                    "endpoint": "https://hacker-news.firebaseio.com/v0/",
                    "path": "maxitem.json",
                    "params": {"print": "pretty"}
                }
            }
        }
    })
