import sys
import pandas as pd
import boto3
import requests
import snowflake.connector
from sqlalchemy import create_engine
import json
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.serialization import load_pem_private_key, load_der_private_key
from requests.auth import HTTPBasicAuth
from datetime import datetime
import pytz

#Function to fetch secrets from Secrets Manager
def get_secrets(secret_names, region_name="us-east-1"):
    secrets = {}
    
    client = boto3.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    
    for secret_name in secret_names:
        try:
            get_secret_value_response = client.get_secret_value(
                SecretId=secret_name)
        except Exception as e:
                raise e
        else:
            if 'SecretString' in get_secret_value_response:
                secrets[secret_name] = get_secret_value_response['SecretString']
            else:
                secrets[secret_name] = base64.b64decode(get_secret_value_response['SecretBinary'])

    return secrets
    
def extract_secret_value(data):
    if isinstance(data, str):
        return json.loads(data)
    return data

secrets = ['snowflake_bizops_user','snowflake_account','snowflake_fivetran_db','snowflake_bizops_role','snowflake_key_pass','snowflake_bizops_wh','jira_api_token','email','jira_survey_api_key']

fetch_secrets = get_secrets(secrets)

extracted_secrets = {key: extract_secret_value(value) for key, value in fetch_secrets.items()}

snowflake_user = extracted_secrets['snowflake_bizops_user']['snowflake_bizops_user']
snowflake_account = extracted_secrets['snowflake_account']['snowflake_account']
snowflake_key_pass = extracted_secrets['snowflake_key_pass']['snowflake_key_pass']
snowflake_bizops_wh = extracted_secrets['snowflake_bizops_wh']['snowflake_bizops_wh']
snowflake_fivetran_db = extracted_secrets['snowflake_fivetran_db']['snowflake_fivetran_db']
snowflake_role = extracted_secrets['snowflake_bizops_role']['snowflake_bizops_role']
jira_api_key = extracted_secrets['jira_api_token']['jira_api_token']
email = extracted_secrets['email']['email']
survey_api_key = extracted_secrets['jira_survey_api_key']['jira_survey_api_key']

password = snowflake_key_pass.encode()

s3_bucket = 'aws-glue-assets-bianalytics'
s3_key = 'BIZ_OPS_ETL_USER.p8'

#Download PEM key from s3
def download_from_s3(bucket, key):
    s3_client = boto3.client('s3')
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        return response['Body'].read()
    except Exception as e:
        print(f"Error downloading from S3: {e}")
        return None

key_data = download_from_s3(s3_bucket, s3_key)

private_key = load_pem_private_key(key_data, password=password)

#Encode PEM key as bytes
private_key_bytes = private_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption()
)

#Today's date
eastern = pytz.timezone('America/New_York')
today = datetime.now(eastern).strftime('%Y-%m-%d')

#Get survey responses from today, store results
results = []

url = f'https://surveys-jsm.servicerocket.io/surveys?projectKey=HHA&responded=true&from={today}&to={today}'

response = requests.get(
    url,
    auth=HTTPBasicAuth(email,jira_api_key),
    headers={
        "Content-Type": "application/json",
        "x-api_key" : survey_api_key
    }
)

json_response = response.json()
results.append(json_response)

#Isolate the number of surveys in the payload to determine if we can proceed
total_surveys = [item['total_surveys_responded'] for item in results if isinstance(item, dict)]
total_surveys = total_surveys[0]

#If there are no survey results in the payload, exit the script
if total_surveys == 0:
    sys.exit(0)

#Parse results, store in dataframe
all_surveys = []
for result in results:
    all_surveys.extend(result['issue_surveys'])

import_df = pd.DataFrame(all_surveys)[['issue_key', 'scale_response', 'comment_response','response_date']]

#Extract keys from payload to list, split into string for use in snowflake query
key_list = import_df['issue_key'].to_list()
key_string = ', '.join(f"'{str(x)}'" for x in key_list)

#Do some data cleanup
import_df['response_date'] = pd.to_datetime(import_df['response_date']).dt.date
column_mapping = {'key':'KEY',
'issue_key': 'KEY',
'scale_response': 'CSAT_SCORE',
'comment_response': 'COMMENT',
'response_date':'RESPONSE_DATE'
} 
import_df_final = import_df.rename(columns=column_mapping)

#Query snowflake table to find existing rows
ctx = snowflake.connector.connect(
    user=snowflake_user,
    account=snowflake_account,
    private_key=private_key_bytes,
    role=snowflake_role,
    warehouse=snowflake_bizops_wh)
    
cs = ctx.cursor()
script = f"""
select * from "{snowflake_fivetran_db}"."JIRA"."CSAT_RESULTS"
where key in ({key_string})
"""
payload = cs.execute(script)
csat_data = pd.DataFrame.from_records(iter(payload), columns=[x[0] for x in payload.description])

#Merge dataframe from payload and snowflake results, only keep net new rows
merged_df = import_df_final.merge(csat_data, left_on='KEY', right_on='KEY',how='left', indicator=True)
new_rows = merged_df[merged_df['_merge'] == 'left_only'].drop(columns=['_merge'])

#If there are no new rows, exit the script
if len(new_rows) == 0:
    sys.exit(0)

#Do some cleanup in prep for import
new_rows.drop(columns=['CSAT_SCORE_y','COMMENT_y','RESPONSE_DATE_y'],inplace=True)
new_rows.rename(columns={'CSAT_SCORE_x':'CSAT_SCORE','COMMENT_x':'COMMENT','RESPONSE_DATE_x':'RESPONSE_DATE'},inplace=True)

#Construct the SQLAlchemy connection string
connection_string = f"snowflake://{snowflake_user}@{snowflake_account}/{snowflake_fivetran_db}/JIRA?warehouse={snowflake_bizops_wh}&role={snowflake_role}&authenticator=externalbrowser"

#Instantiate SQLAlchemy engine with the private key
engine = create_engine(
    connection_string,
    connect_args={
        "private_key": private_key_bytes
    }
)

#Chunk out the data if necessary, import new rows to snowflake table
chunk_size = 1000
chunks = [x for x in range(0, len(new_rows), chunk_size)] + [len(import_df_final)]
table_name = 'CSAT_RESULTS' 

for i in range(len(chunks) - 1):
    new_rows[chunks[i]:chunks[i + 1]].to_sql(table_name, engine, if_exists='append', index=False)
