from dagster import daily_partitioned_config
import datetime as dt


@daily_partitioned_config(start_date=dt.datetime(2022, 11, 1))
def covid19_vac_partition(start, _end):
    return {
            "ops": {
                "get_data_vac_covid19": {
                    "config": {
                        "UF": "SP",
                        "date": start.strftime("%Y-%m-%d")
                    }
                }
            }
        }
