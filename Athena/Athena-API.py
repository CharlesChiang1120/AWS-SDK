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
athena = boto3.client('athena')
s3 = boto3.client('s3')

#Env
output_bucket = os.environ['ATHENA_OUTPUT_BUCKET']
chunk = os.environ['CHUNK']
upload_bucket = os.environ['BUCKET_NAME']

#Empty List


#Day before yesterday(2)                                             
day_before_yesterday = datetime.now() - timedelta(days= 2)

#Main Function
def lambda_handler(event, context):
    
    #SQL Query
    sql_query = '''
        with clordtrail_log AS (
            SELECT
                json_extract(responseelements,'$.queryExecutionId') AS query_id,
                from_iso8601_timestamp(eventtime) AS datetime
            FROM    
                cloudtrail_logs_909073207319_cloudtrail_log
            WHERE   
                eventsource='athena.amazonaws.com'
                AND eventname='StartQueryExecution'
                AND json_extract(responseelements, '$.queryExecutionId') is NOT null
                                )
            SELECT *
            FROM   clordtrail_log 
            WHERE  date_diff('day', datetime, current_date) = 1
                '''
    
    #Partition
    s3_output = 's3://{}/{}/{}/{}/'.format(output_bucket,datetime.now().year,datetime.now().month,datetime.now().day)  
    
    #Athena Query
    batch_query_id = run_query(sql_query,s3_output, max_execution=5)
    print(batch_query_id)
    print('batch_query_id_total:',len(batch_query_id))
    
    #batch_get_query_execution limited 50 query id
    #Split List into chunk 
    batch_query_chunks = list(divide_chunks(batch_query_id, int(chunk)))
    print(len(batch_query_chunks))
    
    batch_query(batch_query_chunks)
    
def run_query(query, s3_output, max_execution=8):
    
    query_id_list = [ ]
    
    #Run SQL Query
    response = athena.start_query_execution(
                                    QueryString=query,
                                    ResultConfiguration={
                                    'OutputLocation': s3_output
                                                        }
                                                )

    #Query ID
    execution_id = response['QueryExecutionId']
    
    print("QueryExecutionId = " + str(execution_id))
    
    state  = 'QUEUED'

    while (max_execution > 0 and state in ['RUNNING', 'QUEUED']):
        max_execution = max_execution - 1
        print("maxexecution=" + str(max_execution))
        response = athena.get_query_execution(QueryExecutionId=execution_id)  

        if 'QueryExecution' in response and                 'Status' in response['QueryExecution'] and                 'State' in response['QueryExecution']['Status']:

                state = response['QueryExecution']['Status']['State']
                print(state)
                
                if state == 'SUCCEEDED':
                    results = athena.get_query_results(QueryExecutionId=execution_id,
                                                                MaxResults=1000)  
                                                                
                    for i in range(1,len(results['ResultSet']['Rows'])):
                        query_id_list.extend(re.findall(r'"([^"]*)"',results['ResultSet']['Rows'][i]['Data'][0].get('VarCharValue')))
                    
                     
                elif state == 'FAILED' or state == 'CANCELLED':
                    return False
                    
        time.sleep(30)
    
    return query_id_list
    
#Function about split list into chunk
def divide_chunks(batch_query_list:list, chunk): 

    for i in range(0, len(batch_query_list), chunk):  
        yield batch_query_list[i:i + chunk] 
  
    
def batch_query(batch_query_chunks):
    
    for j in range(0,len(batch_query_chunks)):
        print(batch_query_chunks[j])
        response = athena.batch_get_query_execution(
                                    QueryExecutionIds=batch_query_chunks[j]
                                               )
       
         
        f = open("/tmp/csv_file.csv", "w+")
        temp_csv_file = csv.writer(f, delimiter='|') 
        temp_csv_file.writerow(["Date", "QueryExecutionId", "State", "DataScannedInBytes", "TotalExecutionTimeInMillis", "QueryStatement", "Comment" ])
        
        
        for i in range(len(response['QueryExecutions'])):
            if len(re.findall( r'\/\*',response['QueryExecutions'][i]['Query'].replace(' ','').replace('\n','').replace('\"','').replace('|',''))) != 0:
                temp_csv_file.writerow(
                                    [
                            datetime.strftime(day_before_yesterday,'%Y-%m-%d'),
                            response['QueryExecutions'][i]['QueryExecutionId'],
                            '{}'.format(response['QueryExecutions'][i]['Status']['State']),
                            response['QueryExecutions'][i]['Statistics']['DataScannedInBytes'],
                            response['QueryExecutions'][i]['Statistics']['TotalExecutionTimeInMillis'],
                            '{}'.format(response['QueryExecutions'][i]['Query'].replace(' ','').replace('\n','').replace('\"','').replace('|','')[0:500]),
                            1
                                    ]
                                    )
            elif len(re.findall( '\/\*',response['QueryExecutions'][i]['Query'].replace(' ','').replace('\n','').replace('\"','').replace('|',''))) != 0:
                temp_csv_file.writerow(
                                    [
                            datetime.strftime(day_before_yesterday,'%Y-%m-%d'),
                            response['QueryExecutions'][i]['QueryExecutionId'],
                            '{}'.format(response['QueryExecutions'][i]['Status']['State']),
                            response['QueryExecutions'][i]['Statistics']['DataScannedInBytes'],
                            response['QueryExecutions'][i]['Statistics']['TotalExecutionTimeInMillis'],
                            '{}'.format(response['QueryExecutions'][i]['Query'].replace(' ','').replace('\n','').replace('\"','').replace('|','')[0:500]),
                            1
                                    ]
                                    )
            else:
                temp_csv_file.writerow(
                                    [
                            datetime.strftime(day_before_yesterday,'%Y-%m-%d'),
                            response['QueryExecutions'][i]['QueryExecutionId'],
                            '{}'.format(response['QueryExecutions'][i]['Status']['State']),
                            response['QueryExecutions'][i]['Statistics']['DataScannedInBytes'],
                            response['QueryExecutions'][i]['Statistics']['TotalExecutionTimeInMillis'],
                            '{}'.format(response['QueryExecutions'][i]['Query'].replace(' ','').replace('\n','').replace('\"','').replace('|','')[0:500]),
                            0
                                    ]
                                    )
                
                            
                                    
        f.close()            
                                    
        s3.upload_file('/tmp/csv_file.csv', upload_bucket,'Querydate-{}-{}-{}-Uploadtos3Time-{}-{}-{}-{}-{}-{}.csv'.format(day_before_yesterday.year,                                                                                                                            day_before_yesterday.month,                                                                                                                            day_before_yesterday.day,                                                                                                                            datetime.now().year,                                                                                                                            datetime.now().month,                                                                                                                            datetime.now().day,                                                                                                                            datetime.now().hour,                                                                                                                            datetime.now().minute,                                                                                                                            datetime.now().second))
        time.sleep(3)                   
            
    return False

