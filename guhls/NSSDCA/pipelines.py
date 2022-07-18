from guhls.NSSDCA.solids import planetary_fact_sheet
from guhls.common.solids import df_to_s3
from guhls.google_sheets.solids import s3_to_gsheet
from dagster import pipeline


@pipeline()
def nssdca_pipe():
    df = planetary_fact_sheet()
    df_to_s3(df)
    s3_to_gsheet(df)


if __name__ == '__main__':
    from dagster import execute_pipeline

    execute_pipeline(
        nssdca_pipe, run_config={
            "solids": {
                "df_to_s3": {
                    "config": {
                        "bucket": "guhls-storage",
                        "prefix": "planet_facts.parquet",
                        "endpoint": "http://192.168.0.107:9000",
                    }
                }
            }
        }
    )
