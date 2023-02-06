import os
import pyodbc
import pandas as pd
from google.cloud import bigquery

# variables
currPath = os.getcwd()
loadFile = 'employee_transposed.csv'
loadFile = os.path.join(currPath, 'data_files', loadFile)

proj = 'etl-us'
dataset = 'sample_dataset'
target_table = 'employee_transposed'
table_id = f'{proj}.{dataset}.{target_table}'

# connections

conn = pyodbc.connect(
    'Driver={SQL Server};'
    'Server=EMIL_LAPTOP;'
    'Database=TEST;'
    'Trusted_Connection=yes;'
)

client = bigquery.Client(project = proj)

# create our SQL extract query

sql = 'SELECT * FROM employee'

# extract data

df = pd.read_sql(sql, conn)

# transform data


df.set_index('id', inplace = True)
df = df.stack().reset_index()
df.columns = ['id', 'labels', 'values']


df.to_csv(loadFile, index = False)

# load data
job_config = bigquery.LoadJobConfig(
    skip_leading_rows = 1,
    source_format = bigquery.SourceFormat.CSV,
    autodetect = True
) 

with open(loadFile, 'rb') as file:
    load_job = client.load_table_from_file(
        file,
        table_id,
        job_config = job_config
    )

load_job.result()

# check how many records are loaded

dest_table = client.get_table(table_id)
print(f'You have {dest_table.num_rows} rows in your table')
