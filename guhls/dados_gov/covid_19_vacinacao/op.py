from dagster import op, Field, Output
import datetime as dt
import pandas as pd
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
            return pd.json_normalize(data_json['hits'], sep='_', record_path='hits').astype("object")

        if data_json.get('hits', None) is None:
            break

        df = pd.json_normalize(data_json['hits'], sep='_', record_path='hits')
        lst_dfs.append(df)

    try:
        df_concated = pd.concat(lst_dfs).reset_index(drop=True).astype("object")
    except ValueError:
        raise "No objects to concatenate, Verify scroll option in config schema"

    return df_concated


@op
def modify_covid19_vac_df(df):
    columns_names = list(df)
    refactor_columns = [
        *map(
            lambda string: string[1:].replace("source_", "").replace("@", "")
            if string.startswith("_") else string, columns_names
        )]

    df.columns = refactor_columns

    df['uf'] = df['estabelecimento_uf']

    date = dt.datetime.strptime(df['timestamp'][0], "%Y-%m-%dT%H:%M:%S.%fZ")
    df['year'] = date.date().strftime("%Y")
    df['month'] = date.date().strftime("%m")
    df['day'] = date.date().strftime("%d")
    return df


@op(required_resource_keys={"s3_resource"})
def load_covid19_vac_to_s3(context, df):
    s3 = context.resources.s3_resource()

    date = dt.datetime.strptime(
        df['timestamp'][0], "%Y-%m-%dT%H:%M:%S.%fZ"
    ).date()

    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, engine='pyarrow', index=False)
    buffer = io.BytesIO(parquet_buffer.getvalue())

    s3.upload_file(
        file=buffer,
        key=f'covid19-vac/'
            f'uf={df["uf"][0]}/'
            f'year={date.year}/'
            f'month={date.strftime("%m")}/'
            f'day={date.strftime("%d")}/'
            f'data.parquet'
    )

    yield Output(None)
