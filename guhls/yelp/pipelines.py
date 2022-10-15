from dagster import job
from guhls.common.solids import get_data_from_api, df_to_s3
from guhls.yelp.solids import transform_data_from_yelp


@job
def yelp_data_pipe():
    resp = get_data_from_api()
    df = transform_data_from_yelp(resp)
    df_to_s3(df)
