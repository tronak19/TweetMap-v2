from django.conf import settings
from django.http import JsonResponse
from django.shortcuts import render
from elasticsearch import Elasticsearch, RequestsHttpConnection
from multiprocessing import Process
from requests_aws4auth import AWS4Auth
import json
import math
import time
import tweepy

consumer_key =  ''
consumer_secret = ''
access_token = ''
access_token_secret = ''

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

tweets = []
trends = []

class StreamListener(tweepy.StreamListener):
	
	def __init__(self, api):
		self.api = api
		super(tweepy.StreamListener, self).__init__()
		self.count = settings.COUNT

	def on_data(self, tweet):
		# while True:
		while self.count<=1000:
			res = es.index(index="tweet-index", doc_type='tweet', id=self.count, body=tweet)
			self.count+=1
			return True

	def on_error(self, status_code):
		print("status_code = ",status_code)
		if status_code == 420:
			return False

def ajax_process(request):
	if settings.COUNT<=100000:
		for i in range(settings.COUNT, settings.COUNT+100):
			res = es.get(index="tweet-index", doc_type='tweet', id=i)
			tweets.append(res['_source'])
		settings.COUNT+=100
		
		languages = []
		latitude = []
		longitude = []
		tweet_id = []
		tweet_text = []
		user_profile_image_url = []
		user_screen_name = []
		
		languages, latitude, longitude, tweet_id, tweet_text, user_profile_image_url, user_screen_name = process_tweets(tweets)
		
		return JsonResponse({"tweet_data" : tweets, "latitude": latitude, "longitude": longitude, "languages": languages,
			"user_profile_image_url": user_profile_image_url, "user_screen_name": user_screen_name, "tweet_id": tweet_id, "tweet_text": tweet_text})
	else:
		return JsonResponse({})

def index(request):
	if settings.FLAG==0:
		settings.FLAG = 1
		p1 = Process(target = twitter_stream)
		p1.start()	
	return render(request, 'TweetMap/index.html', {})

def process_tweets(tweet_list):
	languages = []
	latitude = []
	longitude = []
	tweet_id = []
	tweet_text = []
	user_profile_image_url = []
	user_screen_name = []
	
	languages = list(map(lambda tweet: tweet['lang'] if 'lang' in tweet
						and tweet['lang'] != None else 'NaN', tweet_list))

	tweet_id = list(map(lambda tweet: tweet['id'] if 'id' in tweet
						else 'NaN', tweet_list))
	
	tweet_text = list(map(lambda tweet: tweet['text'] if 'text' in tweet
						else 'NaN', tweet_list))

	user_profile_image_url = list(map(lambda tweet: tweet['user']['profile_image_url_https'] if 'user' in tweet
						else 'NaN', tweet_list))
	
	user_screen_name = list(map(lambda tweet: tweet['user']['screen_name'] if 'user' in tweet
						else 'NaN', tweet_list))
	
	latitude = list(map(lambda tweet: tweet['coordinates']['coordinates'][1]
						if 'coordinates' in tweet and tweet['coordinates'] != None
						else float(float(tweet['place']['bounding_box']['coordinates'][0][1][1]
									+ tweet['place']['bounding_box']['coordinates'][0][3][1])/2)
						if 'place' in tweet and tweet['place']!=None 
						else 'NaN', tweet_list))

	longitude = list(map(lambda tweet: tweet['coordinates']['coordinates'][0]
						if 'coordinates' in tweet and tweet['coordinates'] != None 
						else float(float(tweet['place']['bounding_box']['coordinates'][0][1][0]
									+ tweet['place']['bounding_box']['coordinates'][0][3][0])/2)
						if 'place' in tweet and tweet['place']!=None else 'NaN', tweet_list))

	longitude = list(map(float, longitude))
	longitude = [999 if math.isnan(i) else i for i in longitude]
	latitude = list(map(float, latitude))
	latitude = [999 if math.isnan(i) else i for i in latitude]

	return languages, latitude, longitude, tweet_id, tweet_text, user_profile_image_url, user_screen_name

def search_query(request):
	search_results = []
	if request.method == "POST":
		search_term = request.POST.get('search_bar')
		dropdown_option = 0
		is_dropdown_search = request.POST.get('is_dropdown_search')
		dropdown_dict = {'cat':1,'dog':2,'cricket':3,'football':4,'modi':5,'trump':6,'song':7,'vacation':8,'offer':9,'sale':10}
		
		if is_dropdown_search=="1":
			dropdown_option = dropdown_dict.get(search_term)
		
		query = json.dumps({
			"query": {
				"match": {
					"text": search_term
				}
			}
		})
		res = es.search(index="tweet-index", body=query)
		for hit in res['hits']['hits']:
			search_results.append(hit["_source"])
		
		languages = []
		latitude = []
		longitude = []
		tweet_id = []
		tweet_text = []
		user_profile_image_url = []
		user_screen_name = []

		if len(search_results)>0:
			languages, latitude, longitude, tweet_id, tweet_text, user_profile_image_url, user_screen_name = process_tweets(search_results)

	return render(request, 'TweetMap/results.html',
		{'latitude': latitude, 'longitude': longitude,'languages': languages, 'dropdown_option':dropdown_option,
		'user_profile_image_url': user_profile_image_url, 'user_screen_name': user_screen_name, 'tweet_id': tweet_id, 'tweet_text': tweet_text})

def twitter_stream():
	auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
	auth.set_access_token(access_token, access_token_secret)
	api = tweepy.API(auth)
	trends = api.trends_place(1)
	# for location in trends:
	# 	for trend in location["trends"]:
			# trends.append(trend["name"])
			# print(trend["name"])
	stream_listener = StreamListener(api)
	stream = tweepy.Stream(auth=api.auth, listener=stream_listener)
	stream.filter(locations=[-180,-90,180,90])