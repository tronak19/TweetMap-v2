from __future__ import print_function

import boto3
from elasticsearch import Elasticsearch, RequestsHttpConnection
import json
from requests_aws4auth import AWS4Auth
from watson_developer_cloud import NaturalLanguageUnderstandingV1
from watson_developer_cloud.natural_language_understanding_v1 import Features, SentimentOptions

print('TweetProcessing Lambda Function Running')

cred = boto3.session.Session().get_credentials()

host = ''

awsauth = AWS4Auth(cred.access_key, cred.secret_key, 'us-east-2', 'es', session_token=cred.token)

es = Elasticsearch(
	hosts=[{'host': host, 'port': 443}],
	http_auth=awsauth,
	use_ssl=True,
	verify_certs=True,
	connection_class=RequestsHttpConnection
)

natural_language_understanding = NaturalLanguageUnderstandingV1(
	version='2017-02-27',
	username='f2c869cc-b62d-4cd2-8038-94c09439924b',
	password='HNYizDorCV1W')

def lambda_handler(event, context):
	message = event['Records'][0]['Sns']['Message']
	tweets = json.loads(message)
	try:
		es_count = es.count(index="tweet-index")['count']
	except:
		es_count = 0
	
	batch = []
	current_batch_count = 0

	for tweet_dict in tweets:
		tweet_dict['sentiment'] = sentiment_analysis(tweet_dict['tweet_text'])
		batch.append(tweet_dict)
		res = es.index(index="tweet-index", doc_type='tweet', id=es_count, body=tweet_dict)
		es_count+=1
		current_batch_count+=1
		print("res = ", res)
	
	print("es_count = ", es_count)
	print("current_batch_count = ", current_batch_count)
	print(batch)

	return batch

def sentiment_analysis(data):
	try:
		response = natural_language_understanding.analyze(text=data, features=Features(sentiment=SentimentOptions()))
		return response['sentiment']['document']['label']
	except:
		return 'unknown'