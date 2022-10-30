from dagster import repository
from guhls.NSSDCA.pipelines import nssdca_pipe
from guhls.yelp.pipelines import yelp_data_pipe
from guhls.elections.job import elections_job, schedule


@repository
def guhls_repository():
    return [
        nssdca_pipe,
        yelp_data_pipe,
        elections_job,
        schedule,
    ]
