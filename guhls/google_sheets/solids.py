from googleapiclient.discovery import build
from guhls.workspace.credentials.creds import get_creds
from dagster import solid, Output

service_sheets = build('sheets', 'v4', credentials=get_creds())


@solid()
def s3_to_gsheet(df):
    sheet = service_sheets.spreadsheets()

    column = False
    if column:
        sheet.values().clear(
            spreadsheetId='1g7PgVQqFSXcZhySLQahgA0Cz9AvMFVN71RF3F7z1SRk',
            range='pag!A1:V',
        ).execute()

        sheet.values().update(
            spreadsheetId='1g7PgVQqFSXcZhySLQahgA0Cz9AvMFVN71RF3F7z1SRk',
            range='pag!A1:V',
            valueInputOption='USER_ENTERED',
            body={"values": [df.columns.tolist()]}
        ).execute()

    sheet.values().clear(
        spreadsheetId='1g7PgVQqFSXcZhySLQahgA0Cz9AvMFVN71RF3F7z1SRk',
        range='pag!A2:V',
    ).execute()

    sheet.values().append(
        spreadsheetId='1g7PgVQqFSXcZhySLQahgA0Cz9AvMFVN71RF3F7z1SRk',
        range='pag!A2:V',
        valueInputOption='USER_ENTERED',
        body={"values": df.values.tolist()}
    ).execute()

    yield Output(None)
