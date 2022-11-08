from dagster import job
from guhls.dados_gov.covid_19_vacinacao.op import get_data_vac_covid19, load_vac_covid19_to_s3
from guhls.resources.s3 import s3_resource


@job(resource_defs={"s3_resource": s3_resource})
def covid19_vac_pipe():
    df = get_data_vac_covid19()
    load_vac_covid19_to_s3(df)


if __name__ == '__main__':
    covid19_vac_pipe.execute_in_process(
        run_config={
            "ops": {
                "get_data_vac_covid19": {
                    "config": {
                        "scroll": False,
                        "UF": 'SP',
                        "size": 10,
                    }
                }
            }
        }
    )
