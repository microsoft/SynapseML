import matplotlib.pyplot as plt 
import numpy as np
import text_analytics_container as container
import text_analytics_api as api
import sys

results_directory = "results/text_analytics"
imgs_directory = "imgs"
num_trials = 20

def getMeans():
	container_means = []
	api_means = []

	with open(results_directory + "/extract_key_phrases.txt", "r") as fp:
		container_means.append(float(fp.readline().split("\t")[1]))
		api_means.append(float(fp.readline().split("\t")[1]))


	with open(results_directory + "/detect_language.txt", "r") as fp:
		container_means.append(float(fp.readline().split("\t")[1]))
		api_means.append(float(fp.readline().split("\t")[1]))

	with open(results_directory + "/sentiment_analysis.txt", "r") as fp:
		container_means.append(float(fp.readline().split("\t")[1]))
		api_means.append(float(fp.readline().split("\t")[1]))

	return container_means, api_means

def plot():
	labels = ['Extract Key Phrases', "Language Detection", "Sentiment Analysis"]
	container_means, api_means = getMeans()

	x = np.arange(len(labels))  
	width = 0.35 

	fig, ax = plt.subplots()
	rects1 = ax.bar(x - width/2, container_means, width, label='Container')
	rects2 = ax.bar(x + width/2, api_means, width, label='API')

	ax.set_ylabel('Runtimes (s)')
	ax.set_title('Text Analytics Container vs. API Runtimes')
	ax.set_xticks(x)
	ax.set_xticklabels(labels)
	ax.legend()

	def autolabel(rects):
	    """Attach a text label above each bar in *rects*, displaying its height."""
	    for rect in rects:
	        height = rect.get_height()
	        ax.annotate('{}'.format(height),
	                    xy=(rect.get_x() + rect.get_width() / 2, height),
	                    xytext=(0, 3),  # 3 points vertical offset
	                    textcoords="offset points",
	                    ha='center', va='bottom')


	autolabel(rects1)
	autolabel(rects2)

	fig.tight_layout()

	plt.savefig(imgs_directory + "/text_analytics.png")

	plt.show()

def extractKeyPhrases():
	containerExtractKeyPhrases = [container.extractKeyPhrases() for i in range(num_trials)]
	apiExtractKeyPhrases = [api.extractKeyPhrases() for i in range(num_trials)]
	containerExtractKeyPhrasesAverage = sum(containerExtractKeyPhrases)/len(containerExtractKeyPhrases)
	apiExtractKeyPhrasesAverage = sum(apiExtractKeyPhrases)/len(apiExtractKeyPhrases)
	print("Container extractKeyPhrases runtime: %6f" % containerExtractKeyPhrasesAverage)
	print("API extractKeyPhrases runtime: %6f" % apiExtractKeyPhrasesAverage)

	with open(results_directory + "/extract_key_phrases.txt", "w+") as fp:
		fp.write("Container:\t%10f\n" % containerExtractKeyPhrasesAverage)
		fp.write("API:\t%10f" % apiExtractKeyPhrasesAverage)

def detectLanguage():
	containerDetectLanguage = [container.detectLanguage() for i in range(num_trials)]
	apiDetectLanguage = [api.detectLanguage() for i in range(num_trials)]
	containerDetectLanguageAverage = sum(containerDetectLanguage)/len(containerDetectLanguage)
	apiDetectLanguageAverage = sum(apiDetectLanguage)/len(apiDetectLanguage)
	print("Container detectLanguage runtime: %6f" % containerDetectLanguageAverage)
	print("API detectLanguage runtime: %6f" % apiDetectLanguageAverage)

	with open(results_directory + "/detect_language.txt", "w+") as fp:
		fp.write("Container:\t%10f\n" % containerDetectLanguageAverage)
		fp.write("API:\t%10f" % apiDetectLanguageAverage)

def sentimentAnalysis():
	containerSentimentAnalysis = [container.sentimentAnalysis() for i in range(num_trials)]
	apiSentimentAnalysis = [api.sentimentAnalysis() for i in range(num_trials)]
	containerSentimentAnalysisAverage = sum(containerSentimentAnalysis)/len(containerSentimentAnalysis)
	apiSentimentAnalysisAverage = sum(apiSentimentAnalysis)/len(apiSentimentAnalysis)
	print("Container sentimentAnalysis runtime: %6f" % containerSentimentAnalysisAverage)
	print("API sentimentAnalysis runtime: %6f" % apiSentimentAnalysisAverage)

	with open(results_directory + "/sentiment_analysis.txt", "w+") as fp:
		fp.write("Container:\t%10f\n" % containerSentimentAnalysisAverage)
		fp.write("API:\t%10f" % apiSentimentAnalysisAverage)



if __name__ == "__main__":
	if "--plot" in sys.argv:
		plot()
	elif container.checkAPIStatus():
		if "--language" in sys.argv:
			detectLanguage()
		elif "--keyphrases" in sys.argv:
			extractKeyPhrases()
		elif "--sentiment" in sys.argv:
			sentimentAnalysis()
		else:
			print("Error: Please specify a function to update. Valid options are --language, --keyphrases, --sentiment, or --plot to show the resulting graph.")
	else:
		print("API Status Invalid.")