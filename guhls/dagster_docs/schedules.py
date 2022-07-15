from dagster import ScheduleDefinition
from guhls.dagster_docs.pipelines import job1

job1_schedule = ScheduleDefinition(job=job1, cron_schedule="0 0 * * *")
