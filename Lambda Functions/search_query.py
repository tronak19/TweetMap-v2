import boto3
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import json

print('SearchQuery Lambda Function Invoked')

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

def search_query(search_term):
	search_results = []

	query = json.dumps({
		"query": {
			"match": {
				"tweet_text": search_term
			}
		}
	})

	res = es.search(index="tweet-index", body=query, size=10000, from_=0)
	for hit in res['hits']['hits']:
		search_results.append(hit["_source"])
	return search_results

def lambda_handler(event, context):
	search_term = event['params']['querystring']['search']
	print("search_term = ", search_term)
	results = search_query(search_term)
	print(results)
	return json.dumps({'results': results})