from dagster import job, schedule
from guhls.common.solids import df_to_s3
from guhls.elections.ops import get_results_tse, transform_results_tse, notify_results_tse
from guhls.google_sheets.solids import df_to_gsheet
from dagster import static_partitioned_config, build_schedule_from_partitioned_job, daily_partitioned_config, ScheduleDefinition
from datetime import datetime
import os
from dotenv import load_dotenv

keys = ['1']


@static_partitioned_config(partition_keys=keys)
def partition(partition_key):
    load_dotenv()
    return {
        'ops': {
            'get_results_tse': {
                'config': {
                    'url': 'https://resultados.tse.jus.br/oficial/ele2022/545/dados-simplificados/br/br-c0001-e000545-r.json' # noqa
                }
            },
            "df_to_s3": {
                "config": {
                    "bucket": os.environ.get('BUCKET'),
                    "prefix": "data_elections_30-10-22_president.csv",
                }
            }
        }
    }


@job(config=partition)
def elections_job():
    data = get_results_tse()
    votes, df = transform_results_tse(data)
    notify_results_tse(votes)
    df_to_s3(df)
    df_to_gsheet(df)


# partitioned_schedule = build_schedule_from_partitioned_job(
#     elections_job,
# )
#
#
# elections_schedule = ScheduleDefinition(job=elections_job, cron_schedule="* 17-23 * * *", execution_timezone="America/Sao_Paulo")


@schedule(cron_schedule="* 17-23 * * *", execution_timezone="America/Sao_Paulo", job=elections_job)
def elections_schedule():
    request = elections_job.run_request_for_partition(partition_key='1', run_key=None)
    yield request
