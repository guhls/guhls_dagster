from guhls.NSSDCA.solids import planetary_fact_sheet
from guhls.common.solids import df_to_s3
from guhls.google_sheets.solids import s3_to_gsheet
from dagster import pipeline


@pipeline()
def nssdca_pipe():
    df, url = planetary_fact_sheet()
    df_to_s3(df, url)
    s3_to_gsheet(df)


if __name__ == '__main__':
    from dagster import execute_pipeline

    execute_pipeline(
        nssdca_pipe, run_config={
            "solids": {
                "df_to_s3": {
                    "config": {
                        "bucket": "guhls-dagster",
                        "prefix": "planet_facts.parquet",
                    }
                }
            }
        }
    )
