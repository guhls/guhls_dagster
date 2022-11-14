from dagster import job
from guhls.dados_gov.covid_19_vacinacao.op import get_data_vac_covid19, load_covid19_vac_to_s3, modify_covid19_vac_df
from guhls.resources.s3 import s3_resource
from guhls.dados_gov.covid_19_vacinacao.partition import covid19_vac_partition


@job(resource_defs={"s3_resource": s3_resource}, config=covid19_vac_partition)
def covid19_vac_pipe():
    df = get_data_vac_covid19()
    df = modify_covid19_vac_df(df)
    load_covid19_vac_to_s3(df)


if __name__ == '__main__':
    covid19_vac_pipe.execute_in_process(
        run_config={
            "ops": {
                "get_data_vac_covid19": {
                    "config": {
                        "scroll": True,
                        "UF": 'SP',
                        "size": 5,
                        "date": "2022-11-06",
                    }
                }
            }
        }
    )
