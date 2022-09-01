from dagster import Field, String, Output, solid, AssetMaterialization, MetadataValue


@solid(
    config_schema={
        "bucket": Field(String, is_required=True, description="Just the bucket ex: my-bucket"),
        "prefix": Field(String, is_required=True, description="Just prefix ex: my/path/data.parquet"),
        "endpoint": Field(String, is_required=False),
    }
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
                "dashboard_url": MetadataValue.url(
                    url
                )
            }
        )
    )

    yield Output(None)
