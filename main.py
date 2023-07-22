from google.cloud import bigquery
from prettytable import PrettyTable
import requests
import json
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
import google
import pandas as pd
import datetime
from croniter import croniter
import pytz


def sendToSlack(request):
    try:
        # Set up BigQuery client
        client = bigquery.Client()

        # Retrieve the query to run from the bq_queries table
        query_job = client.query(f"""
            SELECT *
            FROM `table-name`
        """)
        rows = query_job.result()

        ist = pytz.timezone('Asia/Kolkata')
        now = datetime.datetime.now(ist)
        crontime = datetime.datetime.now(ist) - datetime.timedelta(minutes=1)
        current_hour = now.strftime("%H")
        current_day = now.strftime("%w")

        for row in rows:
            query_string = row['query_string']
            slack_webhook = row['slack_webhook']
            alert_message = row['alert_message']
            code = row['code']
            region = row['region']
            cron_expression = row['cron_time']
            query_name = row['query_name']
            status= row['active_status']
            POC_id = row['POC_slack_id']
            POC_id = "@" + POC_id

            # Check if the current hour and day match the cron expression
            cron = croniter(cron_expression, crontime)
            next_run = cron.get_next(datetime.datetime)
            cron_hour = next_run.strftime("%H")
            cron_day = next_run.strftime("%w")

            if cron_hour == current_hour and cron_day == current_day and status== True:
                # Execute the query and get the results as a pandas dataframe
                if 'LIMIT' not in query_string.upper():
                    if query_string.endswith(';'):
                        query_string = query_string[:-1]
                    query = query_string + ' LIMIT 50'
                else:
                    query = query_string
                try:
                    query_job = client.query(query)
                    rows = query_job.result()
                    df = rows.to_dataframe()
                    if len(df) > 0:
                        to_slack(df, slack_webhook, alert_message, code, region,query_name,POC_id)
                except Exception as e:
                    errormessage = f'Error occured while executing the alert :{code}'
                    send_error_to_slack(str(e),errormessage)

        return "Task execution complete"

    except Exception as e:
        # Send the error message to a default Slack channel
        errormessage =  'Error occured while getting the data from sre-alerts'
        send_error_to_slack(str(e),errormessage)
        return "Task execution interrupted"


def to_slack(df, slack_webhook, alert_message, code, region, query_name, POC_id):
    try:
        # Set up Google Sheets API using the default service account credentials
        credentials, project_id = google.auth.default(
            scopes=['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        )
        drive_service = build('drive', 'v3', credentials=credentials)
        sheets_service = build('sheets', 'v4', credentials=credentials)

        # Create a new Google Sheets file and upload the dataframe as values
        spreadsheet_body = {
            'properties': {
                'title': f'{query_name}'
            }
        }
        permission = {
            'type': 'domain',
            'role': 'writer',
            'domain': 'reputation.com'
        }
        spreadsheet = sheets_service.spreadsheets().create(body=spreadsheet_body).execute()
        spreadsheet_id = spreadsheet['spreadsheetId']
        sheet_id = spreadsheet['sheets'][0]['properties']['sheetId']

        values = [df.columns.values.tolist()] + df.values.tolist()
        for r in values:
            for i, value in enumerate(r):
                if isinstance(value, datetime.date):
                    if pd.isna(value):
                        r[i] = ""
                    else:
                        r[i] = value.strftime('%Y-%m-%d %H:%M:%S')
                           

        body = {
            'values': values
        }

        sheets_service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f'Sheet1!A1',
            valueInputOption='RAW',
            body=body).execute()
        
        drive_service.permissions().create(
            fileId=spreadsheet_id,
            body=permission,
            sendNotificationEmail=False).execute()

        # Get the link of the Google Sheets file
        link = f'https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit#gid={sheet_id}'

        # Send the link to Slack using a webhook
        webhook_url = slack_webhook
        slack_data = {'text': f" <{POC_id}> - {code} {alert_message} {region}\n{link}"}
        response = requests.post(webhook_url, data=json.dumps(slack_data), headers={'Content-Type': 'application/json'})
        return response.text

    except Exception as e:
        # Send the error message to a default Slack channel
        errormessage = f'Error occured while sending data to slack : {code}'
        send_error_to_slack(str(e), errormessage)
        return "Error occurred while sending data to Slack"

        
def send_error_to_slack(e, error_message):
    default_slack_webhook = 'Default slack channel webhook'
    # Send the error message to the default Slack channel
    slack_data = {'text': f"Error occurred: {error_message}\n{e}"}
    response = requests.post(default_slack_webhook, data=json.dumps(slack_data), headers={'Content-Type': 'application/json'})
    return response.text
