import os
from time import time
import requests
import pprint

class Service:
	def __init__(self, name, endpoint, sunscriptionKey, functions=[]):
		self.name = name
		self.endpoint = endpoint
		self.sunscriptionKey = sunscriptionKey
		self.functions = functions

	def getName(self):
		return self.name

	def update(self, name):
		pass

class APIService(Service):
	def __init__(self, name):
		self.super.__init__()


class Function:
	def __init__(self, name, suffix, parent):
		self.name = name
		self.suffix = suffix

	def getName(self):
		return self.name

	def run(self, verbose=False):
		url = self.parent.endpoint + self.suffix
		documents = docs.extractKeyPhrases #######
		headers = {"Ocp-Apim-Subscription-Key": self.parent.subscription_key}
		
		last_time = time()
		response = requests.post(url, headers=headers, json=documents)
		runtime = time()-last_time

		value = response.json()
		if verbose:
			pprint(value)
			print("Time: %6fs" % runtime)
		return runtime


def getServices():
	names = ["text_analytics", "computer_vision"]
	services = {}
	# check if all keys exist
	for name in names:
		assert(names.upper() + "_API_KEY" in os.environ, "\nSet the %s_API_KEY environment variable.\n**Restart your shell or IDE for changes to take effect.**" % os.environ, names.upper())
		assert(names.upper() + "_ENDPOINT" in os.environ, "\nSet the %s_ENDPOINT environment variable.\n**Restart your shell or IDE for changes to take effect.**" % os.environ, names.upper())

	text_analytics_service = new Service(
		name = "text_analytics",
		endpoint = os.environ['TEXT_ANALYTICS_ENDPOINT'],
		subscriptionKey = os.environ['TEXT_ANALYTICS_API_KEY']
	)

	text_analytics_functions = [
		new Function(
			name = "extractKeyPhrases",
			suffix = "/text/analytics/v2.1/keyphrases",
			parent = text_analytics_service
			),
		new Function("detectLanguage"),
		new Function("sentimentAnalysis")]
		
	services["text_analytics"] = text_analytics_service

	return services
