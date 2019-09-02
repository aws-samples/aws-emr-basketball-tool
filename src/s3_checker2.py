# Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import json
import boto3

def file_writer(BucketName, status):
    key = 'status/status.txt'
    s3 = boto3.resource('s3')
    object = s3.Object(BucketName, key)
    object.put(Body=status)
    return status

def lambda_handler(event, context):
    ret = 200
    retString = 'no file'

    s3 = boto3.resource('s3')
    try:
        BucketName = event['Records'][0]['s3']['bucket']['name']
        bucket = s3.Bucket(BucketName)
        size = sum(1 for _ in bucket.objects.filter(Prefix='predict2/'))
        print("predict1 file count: %d" % size)
        if size > 1:
            retString = file_writer(BucketName, 'success')
    except:
        print("error occured calling run_emr_clusters()")
        ret = 500

    return {
        'statusCode': ret,
        'body': json.dumps(retString)
    }