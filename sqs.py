import boto3
import tweepy

consumer_key =  ''
consumer_secret = ''
access_token = ''
access_token_secret = ''

aws_access_key_id = ''
aws_secret_access_key = ''
region = 'us-east-2'

sqs = boto3.resource('sqs')

tweets = []

class StreamListener(tweepy.StreamListener):
	
	def __init__(self, api):
		self.api = api
		super(tweepy.StreamListener, self).__init__()
		self.count = 0

	def on_data(self, tweet):
		# while True:
		while self.count<=1000:
			queue = sqs.get_queue_by_name(QueueName='test')
			response = queue.send_message(MessageBody=tweet)
			self.count+=1
			return True

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
	stream.filter(locations=[-180,-90,180,90])

def create_queue():
	sqs = boto3.resource('sqs')
	queue = sqs.create_queue(QueueName='test', Attributes={'DelaySeconds': '5'})
	print(queue.url)
	print(queue.attributes.get('DelaySeconds'))

def list_queues():
	sqs = boto3.resource('sqs')
	for queue in sqs.queues.all():
		print(queue.url)

def use_queue():
	sqs = boto3.resource('sqs')
	queue = sqs.get_queue_by_name(QueueName='test')
	response = queue.send_message(MessageBody='world')
	print(response.get('MessageId'))
	print(response.get('MD5OfMessageBody'))

if __name__ == '__main__':
	twitter_stream()