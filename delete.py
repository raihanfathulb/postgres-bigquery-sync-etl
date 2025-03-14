# Library
import pandas as pd
from sqlalchemy import create_engine
from google.cloud import bigquery
from google.oauth2 import service_account
import sys

# bigquery
credentials = service_account.Credentials.from_service_account_file('path_to_service_account_file.json')
client = bigquery.Client(credentials=credentials, project='your_project_id')

# dev3
postgres = ('postgresql://username:password@hostname:port/dbname')
connect = create_engine(postgres)
conn = connect.connect()

# Fetch id from dev3
with connect.connect() as conn:
    dflog = pd.read_sql_query('''
    SELECT 
        id
    FROM your_table_name
    WHERE DATE(transaction_date) >= '2024-01-01'
        
    ''', con=conn)

# id from bigquery
sql_query = '''
            SELECT id
            FROM `your_project_id.your_dataset.your_table_name` 
            WHERE DATE(transaction_date) >= '2024-01-01'
'''

query_job = client.query(sql_query)

data = []

for val in query_job.result():
    data.append(dict(val))

df = pd.DataFrame(data)

# List delete
deleted_id = df[~df['id'].isin(dflog['id'])]
list_del = deleted_id['id'].tolist()

if len(list_del) == 0:
    print("Tidak ada ID yang harus dihapus")
    sys.exit()
elif len(list_del) == 1:
    list_del = f"({list_del[0]})"
else:
    list_del = str(tuple(list_del))

print(list_del)

# Delete
query_delete = f'''
                DELETE FROM `your_project_id.your_dataset.your_table_name`
                WHERE id IN {list_del}
'''
query_job = client.query(query_delete)

if query_job.result():
    print(f'Data deleted, {len(deleted_id)} row')
else:
    print('job query failed')
