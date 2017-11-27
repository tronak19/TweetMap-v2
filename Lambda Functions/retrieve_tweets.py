import boto3
import json

print('RetrieveTweets Lambda Function Invoked')

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

def publish_tweets():
	sns = boto3.resource('sns')
	sqs = boto3.resource('sqs')
	queue = sqs.get_queue_by_name(QueueName='tweets-queue')
	
	messages = []
	tweets = []
	
	for i in range(50):
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
	
	response = sns.Topic('').publish(Message = json.dumps(tweets), MessageAttributes = {})
	print(response)
	

def lambda_handler(event, context):
	publish_tweets()