#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import json
import boto3
import os
import re
import datetime

s3_res = boto3.resource('s3')
s3_cli = s3_res.meta.client

def read_file_gen():
    with open('/tmp/test.txt') as f:
        for line in f:
            yield json.loads(line)

def lambda_handler(event, context):
    
    s3.download_file('Bucket', 'aws-waf-logs', '/tmp/test.txt' )
    it = read_file_gen()

    while True:
        try:
            waflog = next(it)
            
            convert_timestamp = lambda x: datetime.datetime.fromtimestamp(float((str(x).replace(" ", ""))[0:10])).isoformat() if len(str(x)) > 0 else 'NA'
            
            waflog['timestamp']  = convert_timestamp(waflog['timestamp'])
            
            replaceblank = lambda x : str(x).replace(" ", "")
            log_length = lambda x : x if len(str(x)) > 0 else 'NA'
            
            waflog['requestId'] = replaceblank(waflog['httpRequest']['requestId'])
            waflog['requestId'] = log_length(waflog['requestId'])
            
            waflog['action'] = replaceblank(waflog['action'])
            waflog['action'] = log_length(waflog['action'])
            
            waflog['terminatingRuleType'] = replaceblank(waflog['terminatingRuleType'])
            waflog['terminatingRuleType'] = log_length(waflog['terminatingRuleType'])
            
            waflog['webaclId'] = replaceblank(waflog['webaclId'])
            waflog['webaclId'] = log_length(waflog['webaclId'])
            
            waflog = dict((k, waflog[k]) for k in ['timestamp', 'webaclId', 'terminatingRuleType', 'action', 'requestId'] if k in waflog)
                                                    
            with open('/tmp/after.json', 'a', encoding='utf-8') as file:
                file.write(json.dumps(waflog))
                file.write('\n')
                
        except StopIteration:
            break

    s3.upload_file( '/tmp/after.json', 'Bucket', 'after.json')

