from guhls.NSSDCA.solids import table_html_to_df
from dagster import job


@job
def nssdca_pipe():
    table_html_to_df()
