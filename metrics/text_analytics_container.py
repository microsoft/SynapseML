import requests
import json
import time
import text_analytics_docs as docs

""" Note: Before running, make sure a Text Analytics docker container is pulled
	and running. """

PORT = 5000

link = "http://localhost:%d/status" % PORT

def checkAPIStatus():
	status = requests.get(link)
	apiStatus = status.json()["apiStatus"]
	return apiStatus == "Valid"

def extractKeyPhrases(verbose=False):
	if verbose:
		print("========= EXTRACT KEY PHRASES ============")
	last_time = time.time()
	headers = {
	    'accept': 'application/json',
	    'Content-Type': 'application/json',
	}
	data = json.dumps(docs.extractKeyPhrases)
	response = requests.post('http://localhost:5000/text/analytics/v2.0/keyPhrases', data=data, headers=headers)
	runtime = time.time() - last_time
	if verbose:
		print(response.text)
		print("Status: %d" % response.status_code)
		print("Time: %6f" % runtime)
	return runtime

def detectLanguage(verbose=False):
	if verbose:
		print("========= DETECT LANGUAGE ============")
	last_time = time.time()
	headers = {
	    'accept': 'application/json',
	    'Content-Type': 'application/json',
	}
	data = json.dumps(docs.detectLanguage)
	response = requests.post('http://localhost:5000/text/analytics/v2.0/language', data=data, headers=headers)
	runtime = time.time() - last_time
	if verbose:
		print(response.text)
		print("Status: %d" % response.status_code)
		print("Time: %6f" % runtime)
	return runtime

def sentimentAnalysis(verbose=False):
	if verbose:
		print("========= SENTIMENT ANALYSIS ============")
	last_time = time.time()
	headers = {
	    'accept': 'application/json',
	    'Content-Type': 'application/json',
	}
	data = json.dumps(docs.sentimentAnalysis)
	response = requests.post('http://localhost:5000/text/analytics/v2.0/sentiment ', data=data, headers=headers)
	runtime = time.time() - last_time
	if verbose:
		print(response.text)
		print("Status: %d" % response.status_code)
		print("Time: %6f" % runtime)
	return runtime
