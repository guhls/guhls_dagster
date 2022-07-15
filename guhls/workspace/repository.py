from dagster import repository
from guhls.NSSDCA.pipelines import nssdca_pipe


@repository
def guhls_repository():
    return [
        nssdca_pipe
    ]
