from dagster import job
from guhls.common.solids import get_data_from_api
from guhls.cities.kingston.op import join_datasets


@job
def kingston_pipe():
    data_lines = get_data_from_api.alias('get_data_lines')()
    data_points = get_data_from_api.alias('get_data_points')()
    join_datasets(data_lines, data_points)


if __name__ == '__main__':
    kingston_pipe.execute_in_process(run_config={
        "ops": {
            "get_data_lines": {
                "config": {
                    "endpoint": 'https://opendatakingston.cityofkingston.ca/',
                    "path": 'api/records/1.0/search',
                    "params": {'dataset': 'capital-planning-lines', 'rows': '999'}
                }
            },
            "get_data_points": {
                "config": {
                    "endpoint": 'https://opendatakingston.cityofkingston.ca/',
                    "path": 'api/records/1.0/search',
                    "params": {'dataset': 'capital-planning-points', 'rows': '999'}
                }
            }
        }
    })
