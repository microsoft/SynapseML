import requests
from pprint import pprint
import os
import time
import text_analytics_docs as docs

if 'TEXT_ANALYTICS_API_KEY' in os.environ:
    subscription_key = os.environ['TEXT_ANALYTICS_API_KEY']
else:
    print("\nSet the TEXT_ANALYTICS_API_KEY environment variable.\n**Restart your shell or IDE for changes to take effect.**")
    sys.exit()
if 'TEXT_ANALYTICS_ENDPOINT' in os.environ:
    endpoint = os.environ['TEXT_ANALYTICS_ENDPOINT']
else:
    print("\nSet the TEXT_ANALYTICS_ENDPOINT environment variable.\n**Restart your shell or IDE for changes to take effect.**")
    sys.exit()

def extractKeyPhrases(verbose=False):
	if verbose:
		print("========= EXTRACT KEY PHRASES ============")
	keyphrase_url = endpoint + "/text/analytics/v2.1/keyphrases"
	documents = docs.extractKeyPhrases
	headers = {"Ocp-Apim-Subscription-Key": subscription_key}
	
	last_time = time.time()
	response = requests.post(keyphrase_url, headers=headers, json=documents)
	runtime = time.time()-last_time

	key_phrases = response.json()
	if verbose:
		pprint(key_phrases)
		print("Time: %6fs" % runtime)
	return runtime

def detectLanguage(verbose=False):
	if verbose:
		print("========= DETECT LANGUAGE ============")

	language_api_url = endpoint + "/text/analytics/v2.1/languages"
	documents = docs.detectLanguage
	headers = {"Ocp-Apim-Subscription-Key": subscription_key}

	last_time = time.time()
	response = requests.post(language_api_url, headers=headers, json=documents)
	runtime = time.time()-last_time

	languages = response.json()
	if verbose:
		pprint(entities)
		print("Time: %6fs" % runtime)
	return runtime

def sentimentAnalysis(verbose=False):
	if verbose:
		print("========= SENTIMENT ANALYSIS ============")

	sentiment_url = endpoint + "/text/analytics/v2.1/sentiment"
	documents = docs.sentimentAnalysis
	headers = {"Ocp-Apim-Subscription-Key": subscription_key}

	last_time = time.time()
	response = requests.post(sentiment_url, headers=headers, json=documents)
	runtime = time.time()-last_time

	sentiments = response.json()
	if verbose:
		pprint(entities)
		print("Time: %6fs" % runtime)
	return runtime