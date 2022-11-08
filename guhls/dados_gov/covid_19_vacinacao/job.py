from dagster import job
from guhls.dados_gov.covid_19_vacinacao.op import get_data_vac_covid19, transform_registry_vac_covid19


@job
def covid19_vac_pipe():
    data = get_data_vac_covid19()
    transform_registry_vac_covid19(data)


if __name__ == '__main__':
    covid19_vac_pipe.execute_in_process(run_config={
        "ops": {
            "get_data_vac_covid19": {
                "config": {
                    "url": "https://imunizacao-es.saude.gov.br/_search",
                    "auth": {"username": "imunizacao_public", "password": "qlto5t&7r_@+#Tlstigi"},
                }
            }
        }
    })
