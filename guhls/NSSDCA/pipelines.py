from guhls.NSSDCA.solids import planetary_fact_sheet
from guhls.common.solids import df_to_s3
from guhls.google_sheets.solids import s3_to_gsheet
from dagster import job


@job
def nssdca_pipe():
    df, url = planetary_fact_sheet()
    url_s3 = df_to_s3(df, url)
    s3_to_gsheet(df, url_s3)


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
