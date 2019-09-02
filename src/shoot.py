# Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import json
import boto3
import re
from subprocess import Popen, PIPE

def run_emr_clusters(BucketName):
    key = 'request/request.txt'
    s3 = boto3.resource('s3')
    obj = s3.Object(BucketName, key)
    body = obj.get()['Body'].read().decode('utf-8').replace('\n', '')
    try:
        emrCli1 = re.search(r'#emr-cli-1\s*([^\#]+)', body).group(1).replace('\\','')
        emrCli2 = re.search(r'#emr-cli-2\s*([^\#]+)', body).group(1).replace('\\','')
        subscriberEmail = re.search(r'#subscriber-email\s*([^\#]+)', body).group(1)
        table1 = re.search(r'#table1\s*([^\#]+)', body).group(1)
        table2 = re.search(r'#table2\s*([^\#]+)', body).group(1)
        comparingTable = re.search(r'#comparing-table\s*([^\#]+)', body).group(1)
        result = re.search(r'#result\s*([^\#]+)', body).group(1)
    except:
        print("An exception occurred")
    print("""
    #emr_cli_1:  %s
    #emr_cli_2:  %s
    #table1:     %s
    #table1:     %s
    #result:     %s
    #subscriber: %s
    """ % (emrCli1, emrCli2, table1, table2, result, subscriberEmail))
    process = Popen("/var/task/" + emrCli1, shell=True, stdout=PIPE, stderr=PIPE)
    stdout1, stderr1 = process.communicate()
    process = Popen("/var/task/" + emrCli2, shell=True, stdout=PIPE, stderr=PIPE)
    stdout2, stderr2 = process.communicate()
    file_writer(BucketName, 'request/clusterid1', stdout1)
    file_writer(BucketName, 'request/clusterid2', stdout2)
    return stdout1, stderr1, stdout2, stderr2


def file_writer(BucketName, key, contents):
    s3 = boto3.resource('s3')
    object = s3.Object(BucketName, key)
    object.put(Body=contents)


def lambda_handler(event, context):
    ret = 200
    try:
        BucketName = event['Records'][0]['s3']['bucket']['name']
        stdout1, stderr1, stdout2, stderr2 = run_emr_clusters(BucketName)
        print("stdout1: %s, stdout2: %s" % (stdout1, stdout2))
    except:
        print("error occured calling run_emr_clusters()")
        ret = 500
    return {
        'statusCode': ret,
        'body': json.dumps("""{stdout1: %s, stderr1: %s, stdout2: %s, stderr2: %s}"""
                           % (stdout1, stderr1, stdout2, stderr2))
    }

