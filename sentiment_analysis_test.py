from __future__ import print_function
import json
from watson_developer_cloud import NaturalLanguageUnderstandingV1
# from watson_developer_cloud.natural_language_understanding_v1 import Features, EntitiesOptions, KeywordsOptions, SentimentOptions
from watson_developer_cloud.natural_language_understanding_v1 import Features, SentimentOptions


natural_language_understanding = NaturalLanguageUnderstandingV1(
	version='',
	username='',
	password='')

response = natural_language_understanding.analyze(
	text='Bruce Banner is the Hulk and Bruce Wayne is BATMAN! '
		 'Superman fears not Banner, but Wayne.',
	# features=Features(entities=EntitiesOptions(), keywords=KeywordsOptions(), sentiment=SentimentOptions()))
  features=Features(sentiment=SentimentOptions()))

# print(json.dumps(response, indent=2))
# print(json.dumps(response['sentiment']['document']['label']))
print(response['sentiment']['document']['label'])