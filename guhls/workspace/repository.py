from dagster import repository
from guhls.NSSDCA.pipelines import nssdca_pipe
from guhls.yelp.pipelines import yelp_data_pipe
from guhls.elections.job import elections_job, elections_schedule
from guhls.dados_gov.covid_19_vacinacao.job import covid19_vac_pipe


@repository
def guhls_repository():
    return [
        nssdca_pipe,
        yelp_data_pipe,
        elections_job,
        elections_schedule,
        covid19_vac_pipe,
    ]
