from dagster import repository
from guhls.NSSDCA.pipelines import nssdca_pipe
from guhls.yelp.pipelines import yelp_data_pipe


@repository
def guhls_repository():
    return [
        nssdca_pipe,
        yelp_data_pipe,
    ]
