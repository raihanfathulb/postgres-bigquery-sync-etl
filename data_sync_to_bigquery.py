# import library
import pandas as pd
from sqlalchemy import create_engine
from google.cloud import bigquery
from google.oauth2 import service_account

# Connect to bigquery
credentials = service_account.Credentials.from_service_account_file('path_to_service_account_file.json')
client = bigquery.Client(credentials=credentials, project='your_project_id')

# Connect to postgre dev5
postgres = ('postgresql://username:password@hostname:port/dbname')
connect_source = create_engine(postgres)
con_source = connect_source.connect()

# Get max write_date from bigquery
sql_query = '''
            SELECT MAX(write_date) as max_write_date
            FROM `your_project_id.your_dataset.your_table_name`
'''

query_job = client.query(sql_query)

for val in query_job.result():
    ext_write_date = val['max_write_date']
    print(ext_write_date)


# Fetch data from dev5
with connect_source.connect() as conn:
        df = pd.read_sql(f'''
        SELECT * 
        FROM your_table_name 
        WHERE write_date >= '{ext_write_date}'
        ORDER BY write_date ASC;
        ''',con=conn)

# Table Staging
# Autodetect schema
job_config = bigquery.LoadJobConfig(
    schema = [
    # Column INTEGER
    bigquery.SchemaField('field_1', 'INT64'),
    bigquery.SchemaField('field_2', 'INT64'),
    bigquery.SchemaField('field_3', 'INT64'),
    bigquery.SchemaField('field_4', 'INT64'),
    bigquery.SchemaField('field_5', 'INT64'),
    bigquery.SchemaField('field_6', 'INT64'),
    # Column STRING
    bigquery.SchemaField('field_7', 'STRING'),
    bigquery.SchemaField('field_8', 'STRING'),
    bigquery.SchemaField('field_9', 'STRING'),
    # Column TIMESTAMP
    bigquery.SchemaField('field_10', 'TIMESTAMP'),
    # Column FLOAT
    bigquery.SchemaField('field_11', 'FLOAT64')],
    write_disposition = 'WRITE_TRUNCATE'
)

# Load data to table staging
table_id = 'your_project_id.your_dataset.staging_table'
job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
job.result()

# Get columns
columns = df.columns.tolist()

# String for UPSERT
insert_columns = ', '.join(columns)
insert_values = ', '.join(['S.' + col for col in columns])
update_set = ', '.join([f'T.{col} = S.{col}' for col in columns])

# Merge 
merge_query = f'''
            MERGE INTO `your_project_id.your_dataset.target_table` T
            USING (SELECT * FROM `your_project_id.your_dataset.staging_table`) S
            ON T.transaction_id = S.transaction_id
            WHEN NOT MATCHED THEN
                INSERT ({insert_columns})
                VALUES ({insert_values})
            WHEN MATCHED THEN
                UPDATE SET {update_set};
'''

query_job = client.query(merge_query)

if query_job.result():
    print(f'Job query succeeded, inserted {len(df)} rows')
else:
    print('Job query failed')
