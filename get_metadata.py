# import external libraries
from google.oauth2 import service_account
from google.cloud import bigquery

# read the credentials from our file
# scopes are not necessary because we defined them in GCP already
# path to your json key file
KEY_PATH = "steadfast-task-363413-c36fd6845da9.json"
CREDS = service_account.Credentials.from_service_account_file(KEY_PATH, scopes=["https://www.googleapis.com/auth/cloud-platform"])

# the client object will be used to interact with BQ
client = bigquery.Client(credentials=CREDS, project=CREDS.project_id)

def get_columns(tablename):
    columns_qry = """
    SELECT column_name
FROM steadfast-task-363413.staging.INFORMATION_SCHEMA.COLUMNS
WHERE table_name = '%s'"""%tablename
    cols = client.query(columns_qry)
    return cols

