from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

aws_access_key_id = ''
aws_secret_access_key = ''
region = 'us-east-2'

host = ''
awsauth = AWS4Auth(aws_access_key_id, aws_secret_access_key, region, 'es')

es = Elasticsearch(
	hosts=[{'host': host, 'port': 443}],
	http_auth=awsauth,
	use_ssl=True,
	verify_certs=True,
	connection_class=RequestsHttpConnection
)
print(es.info())


# doc = {
# 	'author': 'Ronak',
# 	'text': 'Elasticsearch Test',
# }

# res = es.index(index="test-index", doc_type='tweet', id=1, body=doc)
# print(res['created'])

# res = es.get(index="tweet-index", doc_type='tweet', id=5)
# print(res['_source'])

# es.indices.refresh(index="test-index")

# res = es.search(index="test-index", body={"query": {"match_all": {}}})
# print("Got %d Hits:" % res['hits']['total'])
# for hit in res['hits']['hits']:
# 	print("%(author)s: %(text)s" % hit["_source"])

es.indices.delete(index='tweet-index', ignore=[400, 404])