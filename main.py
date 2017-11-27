from __future__ import print_function

import boto3
from elasticsearch import Elasticsearch, RequestsHttpConnection
import json
from requests_aws4auth import AWS4Auth
import tweepy
from watson_developer_cloud import NaturalLanguageUnderstandingV1
from watson_developer_cloud.natural_language_understanding_v1 import Features, SentimentOptions

consumer_key =  ''
consumer_secret = ''
access_token = ''
access_token_secret = ''

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
	version='',
	username='',
	password='')

sns = boto3.resource('sns')
sqs = boto3.resource('sqs')
queue = sqs.get_queue_by_name(QueueName='tweets-queue')

#Set counter for number of Tweets
counter = 10

class StreamListener(tweepy.StreamListener):
	
	def __init__(self, api):
		self.api = api
		super(tweepy.StreamListener, self).__init__()
		self.count = 0

	def on_data(self, tweet):
		# while True:
		while self.count<counter:
			response = queue.send_message(MessageBody=tweet)
			self.count+=1
			return True
		else:
			return False

	def on_error(self, status_code):
		print("status_code = ",status_code)
		if status_code == 420:
			return False

def twitter_stream():
	auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
	auth.set_access_token(access_token, access_token_secret)
	api = tweepy.API(auth)
	stream_listener = StreamListener(api)
	stream = tweepy.Stream(auth=api.auth, listener=stream_listener)
	stream.filter(locations=[-180,-90,180,90], languages=['en'])

def process_tweets(tweet):
	if 'lang' in tweet and tweet['lang'] != None:
		language = tweet['lang']
	else:
		language = 'NaN'

	if 'id' in tweet:
		tweet_id = tweet['id']
	else:
		tweet_id = 'NaN'

	if 'text' in tweet:
		tweet_text = tweet['text']
	else:
		tweet_text = 'NaN'

	if 'user' in tweet:
		user_profile_image_url = tweet['user']['profile_image_url_https']
		user_screen_name = tweet['user']['screen_name']
	else:
		user_profile_image_url = 'NaN'
		user_screen_name = 'NaN'

	if 'coordinates' in tweet and tweet['coordinates'] != None:
		latitude = float(tweet['coordinates']['coordinates'][1])
		longitude = float(tweet['coordinates']['coordinates'][0])
	elif 'place' in tweet and tweet['place']!=None:
		latitude = float(float(tweet['place']['bounding_box']['coordinates'][0][1][1]
									+ tweet['place']['bounding_box']['coordinates'][0][3][1])/2)
		longitude = float(float(tweet['place']['bounding_box']['coordinates'][0][1][0]
									+ tweet['place']['bounding_box']['coordinates'][0][3][0])/2)
	else:
		latitude = 999
		longitude = 999

	return language, latitude, longitude, tweet_id, tweet_text, user_profile_image_url, user_screen_name

def sentiment_analysis(data):
	try:
		response = natural_language_understanding.analyze(text=data, features=Features(sentiment=SentimentOptions()))
		return response['sentiment']['document']['label']
	except:
		return 'unknown'

def index_tweets(tweets):
	# tweets = json.loads(message)
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
	
	print("index_tweets batch:")
	print(batch)
	return batch

def retrieve_tweets():
	messages = []
	tweets = []
	
	for i in range(counter):
		messages+=queue.receive_messages()
	count = 0
	
	for message in messages:
		tweet = json.loads(message.body)
		tweet_dict = {}
		tweet_dict['language'], tweet_dict['latitude'], tweet_dict['longitude'], tweet_dict['tweet_id'], tweet_dict['tweet_text'], tweet_dict['user_profile_image_url'], tweet_dict['user_screen_name'] = process_tweets(tweet)
		tweets.append(tweet_dict)
		count+=1
		message.delete()
	print("count = ", count)
	
	response = sns.Topic('arn:aws:sns:us-east-2:013336224536:tweets-channel').publish(Message = json.dumps(tweets), MessageAttributes = {})
	print(response)

	return tweets

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

if __name__ == '__main__':
	twitter_stream()
	tweets = retrieve_tweets()
	batch = index_tweets(tweets)
	search_term = 'in'
	results = search_query(search_term)
	print("search_query results:")
	print(results)
	print("After JSON Dumps:")
	print(json.dumps({'results': results}))