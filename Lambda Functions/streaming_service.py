import boto3
import tweepy

print('StreamingService Lambda Function Initiated')

consumer_key =  ''
consumer_secret = ''
access_token = ''
access_token_secret = ''

sqs = boto3.resource('sqs')
queue = sqs.get_queue_by_name(QueueName='tweets-queue')

class StreamListener(tweepy.StreamListener):
	
	def __init__(self, api):
		self.api = api
		super(tweepy.StreamListener, self).__init__()

	def on_data(self, tweet):
		while True:
			response = queue.send_message(MessageBody=tweet)
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

def lambda_handler(event, context):
	print("Streaming Service Started")
	twitter_stream()