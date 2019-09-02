
import json
import boto3
from botocore.exceptions import ClientError
import re

TARGET_REGION = 'ap-northeast-2'
SES_REGION = 'us-east-1'
SENDER = 'Example <example@example.com>'

def send_email(info):
    RECIPIENT = info['email']
    # CONFIGURATION_SET = "ConfigSet"
    SUBJECT = "EMR Diff Tool"
    BODY_TEXT = ("EMR Diff Tool\r\n"
                 "This email was sent with Amazon SES using the "
                 "AWS SDK for Python (Boto)."
                 )
    BODY_HTML = info['msg']
    CHARSET = "UTF-8"
    client = boto3.client('ses', region_name=SES_REGION)
    try:
        response = client.send_email(
            Destination={
                'ToAddresses': [
                    RECIPIENT,
                ],
            },
            Message={
                'Body': {
                    'Html': {
                        'Charset': CHARSET,
                        'Data': BODY_HTML,
                    },
                    'Text': {
                        'Charset': CHARSET,
                        'Data': BODY_TEXT,
                    },
                },
                'Subject': {
                    'Charset': CHARSET,
                    'Data': SUBJECT,
                },
            },
            Source=SENDER,
            # ConfigurationSetName=CONFIGURATION_SET,
        )
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("Email sent! Message ID:"),
        print(response['MessageId'])


def read_from_requests(BucketName):
    s3 = boto3.resource('s3')
    obj = s3.Object(BucketName, 'request/request.txt')
    reqBody = obj.get()['Body'].read().decode('utf-8').replace('\n', '')
    obj = s3.Object(BucketName, 'request/clusterid1')
    clusterId1Body = obj.get()['Body'].read().decode('utf-8').replace('\n', '')
    obj = s3.Object(BucketName, 'request/clusterid2')
    clusterId2Body = obj.get()['Body'].read().decode('utf-8').replace('\n', '')
    obj = s3.Object(BucketName, 'request/queryid')
    queryIdBody = obj.get()['Body'].read().decode('utf-8').replace('\n', '')
    try:
        subscriberEmail = re.search(r'#subscriber-email\s*([^\#]+)', reqBody).group(1)
        clusterId1 = json.loads(clusterId1Body)['ClusterId']
        clusterId2 = json.loads(clusterId2Body)['ClusterId']
        queryId = queryIdBody
    except:
        print("An exception occurred")
    return subscriberEmail, clusterId1, clusterId2, queryId


def clean_up(BucketName):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(BucketName)
    bucket.objects.filter(Prefix="result").delete()
    bucket.objects.filter(Prefix="status").delete()
    bucket.objects.filter(Prefix="output1").delete()
    bucket.objects.filter(Prefix="output2").delete()
    bucket.objects.filter(Prefix="predict1").delete()
    bucket.objects.filter(Prefix="predict2").delete()
    bucket.objects.filter(Prefix="athena_output").delete()

def getHTMLTable(table_name, header, rows):
    htmlMiddle = "<h3>{}</h3><table>{}</table>"
    tableHead, table = "", ""
    tableItems = []
    endColumn = -1
    for index, head in enumerate(header):
        if head == "End":
            tableHead += "<th bgcolor='#D45B5B'>{}</th>".format(head)
            endColumn = index
        else:
            tableHead += "<th>{}</th>".format(head)
    tableItems.append(tableHead)
    for row in rows:
        tableRow = ""
        for index, item in enumerate(row):
            if index == endColumn:
                tableRow += "<td bgcolor='#D45B5B'>{}</td>".format(item)
            else:
                tableRow += "<td>{}</td>".format(item)
        tableItems.append(tableRow)
    for row in tableItems:
        table += "<tr>{}</tr>".format(row)
    table = htmlMiddle.format(table_name, table)
    return table


def makeMessage(clusterid1, clusterid2, queryid, BucketName):
    # performance compare
    emr = boto3.client('emr')
    emrInfo1 = emr.describe_cluster(ClusterId=clusterid1)
    emrInfo2 = emr.describe_cluster(ClusterId=clusterid2)
    emrHeader = emrInfo1['Cluster'].keys()
    emrRows1 = list(emrInfo1['Cluster'].values())
    emrRows2 = list(emrInfo2['Cluster'].values())
    emrRawTable = list(zip(emrHeader, emrRows1, emrRows2))
    compTable = getHTMLTable("Comparison Target", emrRawTable[0], emrRawTable[1:])

    # output compare
    s3 = boto3.resource('s3')
    obj = s3.Object(BucketName, 'result/%s.csv' % queryid)
    result = obj.get()['Body'].read().decode('utf-8')
    resultHeader = result.split("\n")[0].split(",")
    resultRow = [[]]
    resultRow.append(result.split("\n")[1].split(","))
    resultTable = getHTMLTable("Result Table", resultHeader, resultRow)
    html = "<!DOCTYPE html>\
          <html lang='en'>\
          <head>\
              <title>EMR Diff Tool</title>\
              <style>\
                  table, th, td {\
                      border: 1px solid black;\
                      border-collapse: collapse;\
                      font-size: 10pt;\
                      width: 1000px;\
                  }\
                  th, td {\
                      padding: 5px;\
                      text-align: left;\
                  }\
              </style>\
          </head>\
          <body>" +\
           compTable +\
            resultTable +\
           "</body></html>"
    return html


def lambda_handler(event, context):

    ret = 200
    subscriberEmail = ''
    try:
        BucketName = event['Records'][0]['s3']['bucket']['name']
        subscriberEmail, clusterId1, clusterId2, queryId = read_from_requests(BucketName)
        print(subscriberEmail, clusterId1, clusterId2, queryId)
        msg = makeMessage(clusterId1, clusterId2, queryId, BucketName)
        infos = {'email': subscriberEmail, 'msg': msg}
        send_email(infos)
        clean_up(BucketName)
    except:
        print("error occured calling read_from_requests()")
        ret = 500

    return {
        'statusCode': ret,
        'body': json.dumps("sent: %s" % subscriberEmail)
    }
