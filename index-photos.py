from __future__ import print_function
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import boto3
from datetime import datetime
from decimal import Decimal
import json
import urllib

print('Loading function')

rekognition = boto3.client('rekognition')


# --------------- Helper Functions to call Rekognition APIs ------------------

def detect_labels(bucket, key):
    response = rekognition.detect_labels(Image={"S3Object": {"Bucket": bucket, "Name": key}})
    
    return response

# --------------- Main handler ------------------


def lambda_handler(event, context):
    
    # Get the object from the event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.unquote_plus(event['Records'][0]['s3']['object']['key'].encode('utf8'))
    try:
        
        # Calls rekognition DetectLabels API to detect labels in S3 object
        response = detect_labels(bucket, key)
    
    # Print response to console.
    # print(response)
    
    # return response
    except Exception as e:
        print(e)
        print("Error processing object {} from bucket {}. ".format(key, bucket) +
              "Make sure your object and bucket exist and your bucket is in the same region as this function.")
        raise e
    
    # --------------- es domain ------------------
    
    
    host = ''
    region = 'us-east-2'
    
    service = 'es'
    #    credentials = boto3.Session().get_credentials()

    
    #    awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service)
    awsauth = AWS4Auth(my_access_key, my_secret_key, region, service)
    
    es = Elasticsearch(
                       hosts = [{'host': host, 'port': 443}],
                       http_auth = awsauth,
                       use_ssl = True,
                       verify_certs = True,
                       connection_class = RequestsHttpConnection
                       )
        
                       document = {
                       "objectKey": event['Records'][0]['s3']['object']['key'],
                       "bucket": event['Records'][0]['s3']['bucket']['name'],
                       "createdTimestamp": datetime.now(),
                       "labels": [
                                  response['Labels'][0]['Name'],
                                  response['Labels'][1]['Name'],
                                  response['Labels'][2]['Name']
                                  ]
                       }
                       
                       es.index(index="photos", doc_type="_doc", id="2", body=document)
                       
    print(es.get(index="photos", doc_type="_doc", id="2"))

