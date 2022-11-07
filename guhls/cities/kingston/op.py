from dagster import op
import pandas as pd


@op
def join_datasets(context, data_lines, data_points):
    df_lines = pd.json_normalize(data=data_lines, record_path='records')
    df_points = pd.json_normalize(data=data_points, record_path='records')

    column_names = {
        'fields.capital_program_line': 'capital_program',
        'fields.capital_program_point': 'capital_program',
    }
    df_lines = df_lines.rename(column_names, axis=1)
    df_points = df_points.rename(column_names, axis=1)

    df_concated = pd.concat([df_lines, df_points]).reset_index(drop=True)

    df_concated['Completed'] = df_concated['fields.project_status']\
        .apply(lambda row: True if row == 'Project Complete' else False)
