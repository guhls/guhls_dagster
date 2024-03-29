from guhls.NSSDCA.solids import planetary_fact_sheet
from guhls.common.solids import df_to_s3
from guhls.google_sheets.solids import df_to_gsheet
from dagster import job


@job
def nssdca_pipe():
    df = planetary_fact_sheet()
    df_to_s3(df)
    df_to_gsheet(df)


if __name__ == '__main__':
    nssdca_pipe.execute_in_process(
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
