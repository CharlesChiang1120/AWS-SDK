#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import json
import boto3
import uuid
from datetime import datetime, timedelta
import os
import time
import re
import csv

#Client
athena_cli = boto3.client('athena')
s3_res = boto3.resource('s3')
s3_cli = s3_res.meta.client

#Env
output_bucket = os.environ['ATHENA_OUTPUT_BUCKET']
chunk = os.environ['CHUNK']
upload_bucket = os.environ['BUCKET_NAME']

#Day before yesterday(2)                                             
day_before_yesterday = datetime.now() - timedelta(days=2)

#Main Function
def lambda_handler(event, context):
    
    #SQL Query
    sql_query_statement = '''
        with clordtrail_log AS (
            SELECT
                json_extract(responseelements,'$.queryExecutionId') AS query_id,
                from_iso8601_timestamp(eventtime) AS datetime
            FROM    
                
            WHERE   
                eventsource='athena.amazonaws.com'
                AND eventname='StartQueryExecution'
                AND json_extract(responseelements, '$.queryExecutionId') is NOT null
                                )
            SELECT *
            FROM   clordtrail_log 
            WHERE  date_diff('day', datetime, current_date) = 1'''
    
    #Partition
    s3_output = 's3://{}/{}/{}/{}/'.format(output_bucket, datetime.now().year, datetime.now().month, datetime.now().day)  
    
    #Athena Query
    batch_query_id = run_query(sql_query_statement, s3_output, max_execution=5)
    print(batch_query_id)
    
    #batch_get_query_execution limited 50 query id
    #Split List into chunk 
    batch_query_chunks = list(divide_chunks(batch_query_id, int(chunk)))
    
    batch_query(batch_query_chunks)
    
def run_query(sql_query, s3_output, max_execution=5):
    
    query_id_list = [ ]
    
    #Run SQL Query
    response = athena_cli.start_query_execution(
                                    QueryString=sql_query,
                                    ResultConfiguration={
                                    'OutputLocation': s3_output
                                                        }
                                                )

    #Query ID
    execution_id = response['QueryExecutionId']
    
    print("QueryExecutionId = "+str(execution_id))
    
    state = 'QUEUED'

    while (max_execution > 0 and state in ['RUNNING', 'QUEUED']):
        max_execution = max_execution - 1
        print("maxexecution=" + str(max_execution))
        response = athena_cli.get_query_execution(QueryExecutionId=execution_id)  

        if 'QueryExecution' in response and                 'Status' in response['QueryExecution'] and                 'State' in response['QueryExecution']['Status']:

                state = response['QueryExecution']['Status']['State']
                print(state)
                
                if state == 'SUCCEEDED':
                    results = athena_cli.get_query_results(QueryExecutionId=execution_id,
                                                                MaxResults=1000)  
                                                                
                    for i in range(1, len(results['ResultSet']['Rows'])):
                        query_id_list.extend(re.findall(r'"([^"]*)"',results['ResultSet']['Rows'][i]['Data'][0].get('VarCharValue')))
                    
                     
                elif state=='FAILED' or state=='CANCELLED':
                    return False
                    
        time.sleep(30)
    
    return query_id_list
    
#Function about split list into chunk
def divide_chunks(batch_query_list:list, chunk:int): 

    for i in range(0, len(batch_query_list), chunk):  
        yield batch_query_list[i: i+chunk] 
  
    
def batch_query(batch_query_chunks):
    
    file = open("/tmp/csv_file.csv", "w")
    temp_csv_file = csv.writer(file, delimiter='|') 
    temp_csv_file.writerow(["Date", "QueryExecutionId", "State", "DataScannedInBytes", "TotalExecutionTimeInMillis", "QueryStatement", "Comment"])
        
    
    for query_id in range(len(batch_query_chunks)):
        print(batch_query_chunks[query_id])
        response=athena_cli.batch_get_query_execution(
                            QueryExecutionIds=batch_query_chunks[query_id]
                                                     )
        
        for i in range(len(response['QueryExecutions'])):
            
            queryexecution=response['QueryExecutions']
            queryexecution_time=datetime.strftime(day_before_yesterday,'%Y-%m-%d')
            
            if len(re.findall( r'\/\*', queryexecution[i]['Query'].replace('\r', '').replace('\n','').replace('|','').replace(' ',''))) != 0:
                temp_csv_file.writerow(
                                    [
                            queryexecution_time,
                            queryexecution[i]['QueryExecutionId'],
                            str(queryexecution[i]['Status']['State']),
                            queryexecution[i]['Statistics']['DataScannedInBytes'],
                            queryexecution[i]['Statistics']['TotalExecutionTimeInMillis'],
                            str(queryexecution[i]['Query'].replace('\r', '').replace('|','').replace(' ','').replace('\n','')),
                            1
                                    ]
                                    )
            elif len(re.findall( r'\-\-',queryexecution[i]['Query'].replace('\r', '').replace('\n', '').replace('|', '').replace(' ', ''))) != 0:
                temp_csv_file.writerow(
                                    [
                            queryexecution_time,
                            queryexecution[i]['QueryExecutionId'],
                            str(queryexecution[i]['Status']['State']),
                            queryexecution[i]['Statistics']['DataScannedInBytes'],
                            queryexecution[i]['Statistics']['TotalExecutionTimeInMillis'],
                            str(queryexecution[i]['Query'].replace('\r', '').replace(' ', '').replace('\n', '').replace('|', '')),
                            1
                                    ]
                                    )
            else:
                temp_csv_file.writerow(
                                    [
                            queryexecution_time,
                            queryexecution[i]['QueryExecutionId'],
                            str(queryexecution[i]['Status']['State']),
                            queryexecution[i]['Statistics']['DataScannedInBytes'],
                            queryexecution[i]['Statistics']['TotalExecutionTimeInMillis'],
                            str(queryexecution[i]['Query'].replace('\r', '').replace(' ', '').replace('\n', '').replace('|', '')),
                            0
                                    ]
                                    )
                
                            
                                    
    file.close()            
                                    
    s3_cli.upload_file('/tmp/csv_file.csv', upload_bucket,'Querydate-{}-{}-{}-Uploadtos3Time-{}-{}-{}.csv'.format(day_before_yesterday.year,                                                                                                              str(day_before_yesterday.month).zfill(2),                                                                                                              str(day_before_yesterday.day).zfill(2),                                                                                                              datetime.now().year,                                                                                                              datetime.now().month,                                                                                                              datetime.now().day))
                                                                                                                
            
    return False
    

    
    

