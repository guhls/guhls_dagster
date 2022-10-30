from dagster import job, ScheduleDefinition
from guhls.common.solids import df_to_s3
from guhls.elections.ops import get_results_tse, transform_results_tse, notify_results_tse


@job()
def elections_job():
    data = get_results_tse()
    votes, df = transform_results_tse(data)
    notify_results_tse(votes)
    df_to_s3(df)


schedule = ScheduleDefinition(job=elections_job, cron_schedule="5 17-23 * * *", execution_timezone="America/Sao_Paulo")
