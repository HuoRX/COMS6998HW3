from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import json
import boto3

def lambda_handler(event, context):
    # TODO implement
    bot = boto3.client('lex-runtime', region_name = 'us-east-1')
    
    text = event['q']
    
    ###### for lex #####
    response = bot.post_text(
                             botName='SearchPhotos',
                             botAlias='$LATEST',
                             userId='test',
                             sessionAttributes={
                             'id': 'user1'
                             },
                             requestAttributes={},
                             inputText=text
                             )
                             keywords = []
                             
                             #### for elastic search ####
                             
                             host = # For example, my-test-domain.us-east-1.es.amazonaws.com
                             region = 'us-east-2' # e.g. us-west-1
                             
                             service = 'es'
                             #credentials = boto3.Session().get_credentials()
                             #awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service)
                             
                             
                             awsauth = AWS4Auth(my_access_key, my_secret_key,
                                                region, service)
                             es = Elasticsearch(
                                                hosts = [{'host': host, 'port': 443}],
                                                http_auth = awsauth,
                                                use_ssl = True,
                                                verify_certs = True,
                                                connection_class = RequestsHttpConnection
                                                )
                             
                             if response['dialogState'] == 'ReadyForFulfillment':
                                 #print("entered1")
                                 keywords.append(response['slots']['labelOne'])
                                 if response['slots']['labelTwo']:
                                     keywords.append(response['slots']['labelTwo'])
                                         print(keywords)
                                         #search for index
                                         results = []
                                         for key in keywords:
                                             body = {
                                                 "query":{
                                                     "match":{
                                                         "labels":key
                                                         }
                                                         }
                                                             }
                                                                 search_result = es.search(index="photos", doc_type="_doc", body=body)
                                                                 results.append(search_result)
                                                                     #print(results)
                                                                     final_response = []
                                                                     i = 0
                                                                         for result in results:
                                                                             result = result['hits']['hits'][i]['_source']
                                                                             url = "/"+result['objectKey']
                                                                             labels = result['labels']
                                                                             final = {
                                                                                 "url":url,
                                                                                     "labels":labels
                                                                                         }
                                                                                             final_response.append(final)
                                                                                             i+=1
                                                                                                 print(final_response)
                                                                                                 final_response = {
                                                                                                     "results":final_response
                                                                                                     }
                                                                                                     #result = '!find!'.join(result)
else:
    final_response = response['message']
    
    
    
    
    return {
        'statusCode': 200,
        'body': json.dumps(final_response)
}

