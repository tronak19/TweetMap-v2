import boto3
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

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
print(es.info())

es.indices.delete(index='tweet-index', ignore=[400, 404])