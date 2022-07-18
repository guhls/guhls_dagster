import pandas as pd

from dagster import op


@op
def planetary_fact_sheet(context):
    table = pd.read_html("https://nssdc.gsfc.nasa.gov/planetary/factsheet/")

    table = table[0].transpose()
    df = pd.DataFrame(data=table.values[1:].tolist(), columns=table.values[0].tolist())

    df.columns.values[0] = "Celestial Bodies"
    df = df.drop(df.columns[[-1]], axis=1)

    context.log.info(f"{len(df)} rows in dataframe")

    return df


if __name__ == '__main__':
    from dagster import execute_solid
    execute_solid(planetary_fact_sheet)
