import requests
import pandas as pd
from pandas.core.frame import DataFrame
from dagster import op, Output, Out
import numpy as np
import boto3
from pathlib import Path


@op(config_schema={'url': str})
def get_results_tse(context):
    url = context.op_config.get('url')
    resp = requests.get(url)

    return resp.json()


@op(out={'votes': Out(list), 'df': Out(DataFrame)})
def transform_results_tse(data):
    df_normalize = pd.json_normalize(data['cand'])
    df_normalize = df_normalize.drop('seq', axis=1)
    df_normalize['pvap'] = df_normalize['pvap'].apply(lambda row: row.replace(',', '.'))

    df_cand = df_normalize.loc[:, ['nm', 'cc', 'vap', 'pvap']]
    df_cand = df_cand.rename({'nm': 'Candidato', 'cc': 'Partido', 'vap': 'Votos', 'pvap': '% Votos'}, axis=1)
    df_cand['Partido'] = df_cand['Partido'].apply(lambda row: row.split()[0])

    votes: list = df_cand[['Candidato', 'Votos', '% Votos']].values.tolist()

    yield Output(votes, 'votes')
    yield Output(df_cand, 'df')


@op
def notify_results_tse(votes):
    votes_np = np.array(votes)
    total_votes = votes_np[:, 1].astype('int').sum()

    if len('total_votes') >= 8:
        s3 = boto3.client('s3')
        sns = boto3.client('sns')

        obj = s3.get_object(Bucket='guhls-dagster', Key='votes_to_achieve.csv')
        df = pd.read_csv(obj['Body'])

        root_path = f'{Path(__file__).parent.parent.parent}/bin'

        for i, row in df.iterrows():
            if row['reached'] == '1':
                continue
            elif str(total_votes).startswith(str(row['votes_to_achieve'])):
                df.loc[i, 'reached'] = '1'
                df.to_csv(f'{root_path}/votes_to_achieve.csv')
                s3.upload_object(
                    Bucket='guhls-dagster',
                    FileName=f'{root_path}/votes_to_achieve.csv',
                    Key='votes_to_achieve.csv')
                sns.publish(
                    TopicArn='arn:aws:sns:us-east-1:309927035767:notify_clients',
                    Message=f"""
                        {row['votes_to_achieve']}M Alcançados!
                        {votes_np[0, :][0]}: {votes_np[0, :][1]} de Votos | {votes_np[0, :][2]} 
                        {votes_np[1, :][0]}: {votes_np[1, :][1]} de Votos | {votes_np[0, :][2]}
                    """)


if __name__ == '__main__':
    from dagster import job
    from guhls.common.solids import df_to_s3
    from dotenv import load_dotenv
    import os

    load_dotenv()

    @job
    def run_job():
        data = get_results_tse()
        votes, df = transform_results_tse(data)
        notify_results_tse(votes)
        df_to_s3(df)


    run_job.execute_in_process(run_config={
        'ops': {
            'get_results_tse': {
                'config': {
                    'url': 'https://resultados.tse.jus.br/oficial/ele2022/545/dados-simplificados/br/br-c0001-e000545-r.json' # noqa
                }
            },
            "df_to_s3": {
                "config": {
                    "bucket": os.environ.get('BUCKET'),
                    "prefix": os.environ.get('PREFIX'),
                }
            }
        }
    })
