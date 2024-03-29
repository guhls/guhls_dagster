import pandas as pd

from dagster import op, Output, Out


@op(out={"df": Out()})
def planetary_fact_sheet():
    url = "https://nssdc.gsfc.nasa.gov/planetary/factsheet/"

    table = pd.read_html(url)

    table = table[0].transpose()
    df = pd.DataFrame(data=table.values[1:].tolist(), columns=table.values[0].tolist())

    df.columns.values[0] = "Celestial Bodies"
    df = df.drop(df.columns[[-1]], axis=1)

    return Output(df)


if __name__ == '__main__':
    from dagster import job

    @job
    def run_job():
        planetary_fact_sheet()

    run_job.execute_in_process()
