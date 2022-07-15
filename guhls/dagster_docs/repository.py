from guhls.dagster_docs.assets import asset1, asset2, asset3
from guhls.dagster_docs.pipelines import job3
from guhls.dagster_docs.schedules import job1_schedule
from guhls.dagster_docs.sensors import job2_sensor

from dagster import repository


@repository
def dagster_docs_repository():
    return [
        asset1,
        asset2,
        asset3,
        job1_schedule,
        job2_sensor,
        job3,
    ]
