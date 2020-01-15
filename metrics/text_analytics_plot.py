import pandas as pd 
import numpy as np 
import matplotlib.pyplot as plt 
from text_analytics_base import *
import seaborn as sns

def main_plot(container_means, api_means, container_stds, api_stds, labels):
	x = np.arange(len(labels))  
	width = 0.35 

	fig, ax = plt.subplots()
	rects1 = ax.bar(x - width/2, container_means, width, label='Container', yerr=container_stds)
	rects2 = ax.bar(x + width/2, api_means, width, label='API', yerr=api_stds)

	ax.set_ylabel('Runtimes (s)')
	ax.set_title('Text Analytics Container vs. API Runtimes')
	ax.set_xticks(x)
	ax.set_xticklabels(labels)
	ax.set_yscale("log")
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

	plt.savefig(imgs_directory + "/text_analytics_"+location.lower()+".png")

	plt.show()

def generate_plots():
	df_keyphrase = pd.read_csv(results_directory + "/extract_key_phrases.csv")
	df_language = pd.read_csv(results_directory + "/detect_language.csv")
	df_sentiment = pd.read_csv(results_directory + "/sentiment_analysis.csv")
	df_keyphrase["Function"] = "Extract Key Phrases"
	df_language["Function"] = "Language Detection"
	df_sentiment["Function"] = "Sentiment Analysis"

	dfs = [df_keyphrase, df_language, df_sentiment]

	labels = ['Extract Key Phrases', "Language Detection", "Sentiment Analysis"]

	num_results = min(len(df_language), len(df_keyphrase), len(df_sentiment))//2
	print("Number of results: %d" % num_results)

	container_means = [round(df[df["Type"] == "Container"][:num_results]["Runtime"].describe()['mean'],6) for df in dfs]
	api_means = [round(df[df["Type"] == "API"][:num_results]["Runtime"].describe()['mean'],6) for df in dfs]

	container_stds = [round(df[df["Type"] == "Container"][:num_results]["Runtime"].describe()['std'],6) for df in dfs]
	api_stds = [round(df[df["Type"] == "API"][:num_results]["Runtime"].describe()['std'],6) for df in dfs]

	types = ["Container", "API"]
	fig, axs = plt.subplots(2,3)
	for i in range(3):
		for j in range(2):
			df = dfs[i]
			type_ = types[j]
			axs[j,i].set_title(labels[i] + "\n" + type_)
			axs[j,i].hist(df[df["Type"] == type_]["Runtime"][:num_results])
	fig.tight_layout()

	plt.savefig(imgs_directory + "/text_analytics_"+location.lower()+"_dist.png")


	sns.set(style="whitegrid")

	types = ["Container", "API"]
	fig, axs = plt.subplots(2,3)
	for i in range(3):
		for j in range(2):
			df = dfs[i]
			type_ = types[j]
			axs[j,i].set_title(labels[i] + "\n" + type_)
			sns.violinplot(df[df["Type"] == type_]["Runtime"][:num_results], 
				ax=axs[j,i], inner="quart")

	fig.tight_layout()
	plt.savefig(imgs_directory + "/text_analytics_"+location.lower()+"_violin_dist.png")


	main_plot(container_means, api_means, container_stds, api_stds, labels)

	sns.set(style="whitegrid", palette="pastel", color_codes=True)

	df = pd.concat([df_keyphrase, df_language, df_sentiment], axis=0)
	print(df.head())
	print(df.columns)
	# Draw a nested violinplot and split the violins for easier comparison
	sns.violinplot(x="Function", y="Runtime", hue="Type",
	               split=True, inner="quart",
	               # palette={"Yes": "y", "No": "b"},
	               data=df)
	sns.despine(left=True)
	fig.tight_layout()
	plt.savefig(imgs_directory + "/text_analytics_"+location.lower()+"_violin_dist_split.png")

	# Initialize the figure
	f, ax = plt.subplots()
	sns.despine(bottom=True, left=True)

	# Show each observation with a scatterplot
	sns.stripplot(x="Function", y="Runtime", hue="Type",
	              data=df, dodge=True, jitter=True,
	              alpha=.25, zorder=1)
	fig.tight_layout()
	plt.savefig(imgs_directory + "/text_analytics_"+location.lower()+"_stripplot.png")



if __name__ == "__main__":
	print("Current location: %s. You can edit state variables (location, directory, etc.) in text_analytics_base.py" % location)
	generate_plots()