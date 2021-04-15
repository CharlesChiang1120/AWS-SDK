#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import json
import boto3
import os
import re
import datetime
time = datetime.datetime.now()

s3 = boto3.client('s3')
FILE_TO_READ = 'aws-waf-logs'

def read_file_gen():
    with open('/tmp/test.json') as f:
        for line in f:
            yield json.loads(line)

def lambda_handler(event, context):
    
    s3.download_file('Bucket', 'aws-waf-logs', '/tmp/test.txt' )
    it = read_file_gen()

    while True:
        try:
            waflog = next(it)
            waflog['requestId'] = str(waflog['httpRequest']['requestId']).replace(" ", "")
            waflog['action'] = str(waflog['action']).replace(" ", "")
            waflog['terminatingRuleType'] = str(waflog['terminatingRuleType']).replace(" ", "")
            waflog['webaclId'] = str(waflog['webaclId']).replace(" ", "")

            if len(str(waflog['timestamp'])) <= 0:
                waflog['timestamp'] = 'NA'
            else:
                waflog['timestamp'] = datetime.datetime.fromtimestamp(float((str(waflog['timestamp']).replace(" ", ""))[0:10])).isoformat()

            if len(waflog['webaclId']) <= 0:
                waflog['webaclId'] = 'NA'
            else:
                pass

            if len(waflog['terminatingRuleId']) <= 0:
                    terminatingRuleId = 'NA'
            else:
                pass

            if len(waflog['terminatingRuleType']) <= 0:
                waflog['terminatingRuleType'] = 'NA'
            else:
                pass

            if len(waflog['action']) <= 0:
                waflog['action'] = 'NA'
            else:
                pass

            if len(waflog['requestId']) <= 0:
                waflog['requestId'] = 'NA'

            waflog = dict((k, waflog[k]) for k in ['timestamp', 'webaclId', 'terminatingRuleType', 'action', 'requestId']
                                                    if k in waflog)
            with open(f'/tmp/after.json', 'a', encoding='utf-8') as g:
                g.write(json.dumps(waflog))
                g.write('\n')
        except StopIteration:
            break

    s3.upload_file( '/tmp/after.json', 'Bucket', 'after.json')

