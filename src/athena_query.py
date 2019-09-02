# Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import json
import boto3
import re

database = 'comp'
athena = boto3.client('athena')

def run_comp_query(BucketName):
    reqKey = 'request/request.txt'
    s3 = boto3.resource('s3')
    obj = s3.Object(BucketName, reqKey)
    body = obj.get()['Body'].read().decode('utf-8').replace('\n', ' ')

    # bucket = 'basketball-tool-bucket-20190818'
    s3Output = 's3://%s/athena_output/' % BucketName
    resultOutput = 's3://%s/result/' % BucketName
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
    responseTable1 = athena_query(table1, s3Output)
    responseTable2 = athena_query(table2, s3Output)
    responseComparingTable = athena_query(comparingTable, s3Output)
    responseResult = athena_query(result, resultOutput)

    status = ('FAILED' or 'CANCELLED') != (get_status(responseTable1['QueryExecutionId']) \
            == get_status(responseTable2['QueryExecutionId']) \
            == get_status(responseComparingTable['QueryExecutionId']) \
            == get_status(responseResult['QueryExecutionId']))
    return responseResult['QueryExecutionId'], status

def get_status(QueryExecutionId):
    res = athena.get_query_execution(QueryExecutionId=QueryExecutionId)
    return res['QueryExecution']['Status']['State']

def athena_query(query, output):
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database
        },
        ResultConfiguration={
            'OutputLocation': output,
        })
    return response

def file_writer(bucket_name, key, contents):
    s3 = boto3.resource('s3')
    object = s3.Object(bucket_name, key)
    object.put(Body=contents)


def lambda_handler(event, context):
    ret = 200
    try:
        BucketName = event['Records'][0]['s3']['bucket']['name']
        QueryExecutionId, status = run_comp_query(BucketName)
        file_writer(BucketName, 'request/queryid', QueryExecutionId)
    except:
        print("error occured calling run_emr_clusters()")
        ret = 500
    return {
        'statusCode': ret,
        'body': json.dumps("{ResultQueryExecutionId: %s, status: %s}" % (QueryExecutionId, status))
    }
