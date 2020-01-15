import requests
import json
import time
import text_analytics_docs as docs

""" Note: Before running, make sure a Text Analytics docker container is pulled
	and running. """

PORT = 5000

link = "http://localhost:%d/status" % PORT
headers = {
    'accept': 'application/json',
    'Content-Type': 'application/json',
}

def checkAPIStatus():
	status = requests.get(link)
	apiStatus = status.json()["apiStatus"]
	return apiStatus == "Valid"

def extractKeyPhrases(verbose=False):
	if verbose:
		print("========= EXTRACT KEY PHRASES ============")
	data = json.dumps(docs.extractKeyPhrases)

	last_time = time.time()
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
	data = json.dumps(docs.detectLanguage)

	last_time = time.time()
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
	data = json.dumps(docs.sentimentAnalysis)
	last_time = time.time()
	response = requests.post('http://localhost:5000/text/analytics/v2.0/sentiment ', data=data, headers=headers)
	runtime = time.time() - last_time
	if verbose:
		print(response.text)
		print("Status: %d" % response.status_code)
		print("Time: %6f" % runtime)
	return runtime
