from dagster import op, Field, Output
import pandas as pd
import datetime as dt
import requests
import json
import io


@op(config_schema={
    "size": Field(int, is_required=False, default_value=10000),
    "scroll": Field(bool, is_required=False, default_value=True, description="Use Paginator"),
    "UF": str,
    "date": Field(
        str,
        is_required=False,
        description="Ex. 2022-11-08"
    )
})
def get_data_vac_covid19(context):
    size = context.op_config['size']
    scroll = context.op_config['scroll']
    UF = context.op_config.get('UF') # noqa
    date = context.op_config.get('date')

    url = "https://imunizacao-es.saude.gov.br/_search"
    auth = ("imunizacao_public", "qlto5t&7r_@+#Tlstigi")  # noqa
    headers = {'Content-Type': 'application/json'}
    params = {"scroll": "1m"}

    if date is None:
        date = dt.date.today().strftime("%Y-%m-%d")

    data = json.dumps(
        {
            "query": {
                "bool": {
                    "must": [
                        {"match": {"estabelecimento_uf": UF.upper()}},
                        {"match": {"@timestamp": date}}
                    ],
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

        if scroll:
            scroll_id = data_json.get('_scroll_id')
            data = json.dumps({"scroll_id": scroll_id, "scroll": "1m"})
            params = {}
            url += "/scroll"
        else:
            return pd.json_normalize(data_json['hits'], sep='_', record_path='hits')

        if data_json.get('hits', None) is None:
            break

        df = pd.json_normalize(data_json['hits'], sep='_', record_path='hits')
        lst_dfs.append(df)

    try:
        df_concated = pd.concat(lst_dfs).reset_index(drop=True)
    except ValueError:
        raise "No objects to concatenate, Verify if has data"

    return df_concated


@op(required_resource_keys={"s3_resource"})
def load_vac_covid19_to_s3(context, df):
    s3 = context.resources.s3_resource()

    date = set(
        df['_source_@timestamp']
        .apply(
            lambda date: dt.datetime.strptime(date, "%Y-%m-%dT%H:%M:%S.%fZ")
            .date()
            .strftime("%Y-%m-%d")
        )
    )
    assert len(date) == 1, "DataFrame has more than one date in the timestamp"

    uf = "".join(set(df['_source_estabelecimento_uf']))

    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, engine='pyarrow', index=False)
    buffer = io.BytesIO(parquet_buffer.getvalue())

    s3.upload_file(
        file=buffer,
        key=f'covid19-vac/{uf}/{"".join(date).split("-")[0]}-{"".join(date).split("-")[1]}/day_{"".join(date)[-1]}.parquet'
    )

    yield Output(None)
