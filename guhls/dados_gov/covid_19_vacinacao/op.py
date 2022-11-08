from dagster import op, Field
import pandas as pd
import datetime as dt
import requests
import json


@op(config_schema={
    "url": str,
    "size": Field(int, is_required=False, default_value=1000),
    "auth": dict,
    "days_before": Field(
        int,
        is_required=False,
    )
})
def get_data_vac_covid19(context):
    url = context.op_config.get('url')
    size = context.op_config.get('size')
    username = context.op_config.get('auth')['username']
    password = context.op_config.get('auth')['password']

    auth = username, password
    headers = {'Content-Type': 'application/json'}
    params = {"scroll": "1m"}

    days_before = context.op_config.get('days_before', None)
    today = dt.date.today().strftime("%Y-%m-%d")
    if days_before:
        start_date = (dt.date.today() - dt.timedelta(days=days_before)).strftime("%Y-%m-%d")
    else:
        start_date = today

    data = json.dumps(
        {
            "query": {
                "bool": {
                    "must": [
                        {"match": {"estabelecimento_uf": "SP"}}
                    ],
                    "filter": [
                        {"range": {"@timestamp": {"gte": start_date}}}
                    ]
                }
            },
            "sort": [
                {"@timestamp": {"order": "asc", "format": "epoch_millis"}}
            ],
            "size": size
        }
    )

    lst_dfs = []
    while True:
        data_json = requests.request('POST', url=url, auth=auth, data=data, params=params, headers=headers).json()

        if data_json.get('hits', None):
            scroll_id = data_json.get('_scroll_id')
            data = json.dumps({"scroll_id": scroll_id, "scroll": "1m"})
            params = {}
            url += "/scroll"
        else:
            break

        df = pd.json_normalize(data_json['hits'], sep='_', record_path='hits')
        lst_dfs.append(df)

        lst_dates = [
            *map(lambda date: dt.datetime.strptime(date, "%Y-%m-%dT%H:%M:%S.%fZ").date().strftime("%Y-%m-%d"),
                 df['_source_@timestamp'].values.tolist())
        ]

    df_concated = pd.concat(lst_dfs).reset_index(drop=True)
    df_final = df_concated[df_concated['_source_@timestamp'] >= start_date]


@op
def transform_registry_vac_covid19(data):
    df = pd.json_normalize(data['hits'], sep='_', record_path='hits')
    ...
