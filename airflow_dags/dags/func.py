from airflow.decorators import task
from pandas import json_normalize
import requests


def get_data_json(url):
    json = requests.get(url).json()
    return json


@task
def get_columns(json):
    df = json_normalize(data=json)
    print(list(df))


if __name__ == '__main__':
    get_data_json('https://opendatakingston.cityofkingston.ca/api/records/1.0/search/?dataset=capital-planning-lines')
