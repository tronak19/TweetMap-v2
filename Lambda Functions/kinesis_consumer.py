import boto3
import json
import time

print("KinesisConsumer Running")

def get_kinesis_data():
	elasticache = boto3.client('elasticache')
	kinesis = boto3.client('kinesis', region_name='us-east-2')
	response = kinesis.describe_stream(StreamName='TweetStream')
	shard_id = response['StreamDescription']['Shards'][0]['ShardId']
	shard_iterator_response = kinesis.get_shard_iterator(StreamName='TweetStream', ShardId=shard_id, ShardIteratorType='TRIM_HORIZON')

	resource_name = ''
	elasticache_response = elasticache.list_tags_for_resource(ResourceName=resource_name)
	
	if len(elasticache_response['TagList'])==0:
		shard_iterator = shard_iterator_response['ShardIterator']
		elasticache.add_tags_to_resource(ResourceName=resource_name, Tags=[{'Key': 'shard_iterator', 'Value': shard_iterator}])
	else:
		shard_iterator = elasticache_response['TagList'][0]['Value']

	count = 0
	tweets = []
	try:
		record_response = kinesis.get_records(ShardIterator=shard_iterator, Limit=100)
		for record in record_response['Records']:
			last_sequence_number = record['SequenceNumber']
			tweets.append(json.loads(record['Data']))
			count += 1
		shard_iterator = record_response['NextShardIterator']

	except kinesis.exceptions.ProvisionedThroughputExceededException:
		print("ProvisionedThroughputExceededException")

	except kinesis.exceptions.ExpiredIteratorException:
		print("ExpiredIteratorException")
		shard_iterator_response = kinesis.get_shard_iterator(StreamName='TweetStream', ShardId=shard_id, ShardIteratorType='AFTER_SEQUENCE_NUMBER', StartingSequenceNumber=last_sequence_number)
		shard_iterator = shard_iterator_response['ShardIterator']

	elasticache.remove_tags_from_resource(ResourceName=resource_name, TagKeys=['shard_iterator'])
	elasticache.add_tags_to_resource(ResourceName=resource_name, Tags=[{'Key': 'shard_iterator', 'Value': shard_iterator}])

	print("get_kinesis_data count = ", count)
	print("shard_iterator = ", shard_iterator)
	
	return tweets

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

def publish_tweets(tweets_list):
	sns = boto3.resource('sns')
	count = 0
	tweets = []
	for tweet in tweets_list:
		tweet = json.loads(tweet)
		tweet_dict = {}
		tweet_dict['language'], tweet_dict['latitude'], tweet_dict['longitude'], tweet_dict['tweet_id'], tweet_dict['tweet_text'], tweet_dict['user_profile_image_url'], tweet_dict['user_screen_name'] = process_tweets(tweet)
		tweets.append(tweet_dict)
		count+=1
	print("count = ", count)
	
	response = sns.Topic('').publish(Message = json.dumps(tweets), MessageAttributes = {})
	print(response)

def lambda_handler(event, context):
	tweets = get_kinesis_data()
	publish_tweets(tweets)