from googleapiclient.discovery import build
from guhls.workspace.credentials.creds import get_creds
from dagster import op, Output, AssetMaterialization, MetadataValue

service_sheets = build('sheets', 'v4', credentials=get_creds())


@op
def s3_to_gsheet(context, df, url_s3):
    sheet = service_sheets.spreadsheets()
    sheet_id = '1g7PgVQqFSXcZhySLQahgA0Cz9AvMFVN71RF3F7z1SRk'  # noqa

    column = False
    if column:
        sheet.values().clear(
            spreadsheetId=sheet_id,
            range='pag!A1:V',
        ).execute()

        sheet.values().update(
            spreadsheetId=sheet_id,
            range='pag!A1:V',
            valueInputOption='USER_ENTERED',
            body={"values": [df.columns.tolist()]}
        ).execute()

    sheet.values().clear(
        spreadsheetId=sheet_id,
        range='pag!A2:V',
    ).execute()

    sheet.values().append(
        spreadsheetId=sheet_id,
        range='pag!A2:V',
        valueInputOption='USER_ENTERED',
        body={"values": df.values.tolist()}
    ).execute()

    context.log_event(
        AssetMaterialization(
            asset_key="dataset",
            description="S3 to GSheet",
            metadata={
                "text_metadata": "metadata for dataset append in gsheet",
                "path": MetadataValue.path(f"https://docs.google.com/spreadsheets/d/{sheet_id}/"),
                "dashboard_url": MetadataValue.url(url_s3[0].value)
            }
        )
    )

    yield Output(None)
