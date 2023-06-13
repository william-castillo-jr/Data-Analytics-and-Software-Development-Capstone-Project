import time
import requests
import pandas as pd
from datetime import datetime
from datetime import timedelta
from google.cloud import bigquery
from google.oauth2 import service_account

"""
Set the key path to the authentication file path.
Create the authentication credentials.
Finally, create a Client() object and use your credentials to authenticate and list the project.
"""

key_path = "plenary-ellipse-387102-b24908bf43e3.json"

credentials = service_account.Credentials.from_service_account_file(
    key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

client = bigquery.Client(credentials=credentials, 
						 project=credentials.project_id)

#note for team: replace xxx with your API Key
API_KEY = "CwNJOQdf5pzSe0qWBcTUwJwj6seIcQp9Np46Ifff"

#Set your projectID and initialize the BigQuery client
project_id = 'plenary-ellipse-387102'

#Setup dataset and table setup in the BigQuery database, then replace these names with yours
dataset_id = 'solv4x'
table_id = 'row'

url = "https://api.eia.gov/v2/electricity/rto/region-data/data/?api_key=" + API_KEY

"""
note for team--------------------------------
#API route used:
"Electricity",
"Electric Power Operations (Daily And Hourly)"
"Hourly Demand, Demand Forecast, Generation, and Interchange"

here's a link for easy access to website: https://www.eia.gov/opendata/browser/electricity/rto/region-data

current parameters set within 'params' variable below (which can be modified for filtering directly from api)

frequency: hourly

facets[respondent]:
NW - Pacific Northwest,
SCL - Seattle City Light

facets[type]:
D - Demand,
DF - Day-ahead demand forecast,
NG - Net Generation

start & end
2023-04-21T00" - "2023-04-22T00"
--------------------------------------
"""

#determine date to use
now = datetime.today()

# create a time delta object to find two days ago
delta = timedelta(days=2)

# use datetime arithmetic to get two days ago.
two_days_ago = now - delta

# convert datetime to valid format for the API call
def change_to_api_format(dt: datetime) -> str:
  return dt.strftime("%Y-%m-%dT%H")

now_str = change_to_api_format(now)
then_str = change_to_api_format(two_days_ago)

params = {
    "frequency": "hourly",
    "data[0]": "value",
    "facets[respondent][]": ["NW", "SCL"],
    "facets[type][]": ["D", "DF", "NG"],
    "sort[0][column]": "period",
    "sort[0][direction]": "desc",
    "start": then_str,
    "end": now_str,
    "offset": 0,
    "length": 5000
}

print("Fetching data")

#save request in response variable
response = requests.get(url, params=params)

#convert response into json
json_data = response.json()['response']['data']

print("Transforming data")

#flattening the dataset
# Convert the nested JSON object to a flat DataFrame
df = pd.json_normalize(json_data, meta=['period', 'respondent', 'type'])

# Rename columns for clarity
df.columns = ['date', 'respondent', 'respondent_full', 'type', 'type_full', 'value', 'unit']

#convert date string to datetime object, match the formatting.
df['date'] = pd.to_datetime(df['date'], format="%Y-%m-%dT%H")

print(df)

#imports data

print("Updating BigQuery Database")

#start of the sql statement
sql = (f"INSERT INTO `{project_id}.{dataset_id}.{table_id}` VALUES ")
#loop over whole dataframe & adds line to insert statement
for row in df.itertuples():
  #adding onto the sql statment given to BigQuery
  sql = (sql +
  f"('{row.date}','{row.respondent}','{row.respondent_full}','{row.type}','{row.type_full}',{row.value},'{row.unit}'),")
  
#removing final , and adding a ;
sql = (sql[:-1] + ";")
#send over the insert statement to BigQuery
insert_job = client.query(sql)

print("Checking for duplicates")

#code waits 3 seconds for the database to fully update before replacing rows
time.sleep(3)

#creates and replaces our current table with one with no duplicates (could take a while if a ton of rows)
sql = (f"""
create or replace table `{project_id}.{dataset_id}.{table_id}`  as (
  select * except(row_num) from (
      select *,
        row_number() over ( partition by `date`, `respondent`,`respondent_full`,`type`,`type_full`,`value`,`unit` order by `date`, `respondent`,`respondent_full`,`type`,`type_full`,`value`,`unit` ) row_num
      from
      `{project_id}.{dataset_id}.{table_id}` ) t
  where row_num=1
)
""")

#send dupe check to BigQuery
insert_job = client.query(sql)

print("Finished")