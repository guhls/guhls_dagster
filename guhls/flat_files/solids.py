from dagster import op
import os
from pathlib import Path
import subprocess


@op
def get_name():
    return "201812SpotifyData.csv"


@op
def strip_new_lines_csv(context, filename):
    path = Path(__file__).parent.parent.parent
    subprocess.check_output(['wc', '-l', f"{str(path)+'/bin/'+filename}"])


if __name__ == '__main__':
    from dagster import job

    @job
    def execute_job():
        filename = get_name()
        strip_new_lines_csv(filename)

    execute_job.execute_in_process()
