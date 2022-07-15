from dagster import sensor, RunRequest
from guhls.dagster_docs.pipelines import job2


@sensor(job=job2)
def job2_sensor():
    should_run = True
    if should_run:
        yield RunRequest(run_key=None, run_config={})
