#!/usr/bin/env python
# coding: utf-8

# # Exploring Art across Culture and Medium with Fast, Conditional, k-Nearest Neighbors
# 
# <img src="https://mmlspark.blob.core.windows.net/graphics/art/cross_cultural_matches.jpg"  width="600"/>
# 
# This notebook serves as a guideline for match-finding via k-nearest-neighbors. In the code below, we will set up code that allows queries involving cultures and mediums of art amassed from the Metropolitan Museum of Art in NYC and the Rijksmuseum in Amsterdam.

# ### Overview of the BallTree
# The structure functioning behind the kNN model is a BallTree, which is a recursive binary tree where each node (or "ball") contains a partition of the points of data to be queried. Building a BallTree involves assigning data points to the "ball" whose center they are closest to (with respect to a certain specified feature), resulting in a structure that allows binary-tree-like traversal and lends itself to finding k-nearest neighbors at a BallTree leaf.

# #### Setup
# Import necessary Python libraries and prepare dataset.

# In[4]:


from pyspark.sql.types import *
from pyspark.ml.feature import Normalizer
from pyspark.sql.functions import lit, array, array_contains, udf, col, struct
from mmlspark.nn import ConditionalKNN, ConditionalKNNModel
from PIL import Image
from io import BytesIO

import requests
import numpy as np
import matplotlib.pyplot as plt


# Our dataset comes from a table containing artwork information from both the Met and Rijks museums. The schema is as follows:
# 
# - **id**: A unique identifier for a piece of art
#   - Sample Met id: *388395* 
#   - Sample Rijks id: *SK-A-2344* 
# - **Title**: Art piece title, as written in the museum's database
# - **Artist**: Art piece artist, as written in the museum's database
# - **Thumbnail_Url**: Location of a JPEG thumbnail of the art piece
# - **Image_Url** Location of an image of the art piece hosted on the Met/Rijks website
# - **Culture**: Category of culture that the art piece falls under
#   - Sample culture categories: *latin american*, *egyptian*, etc
# - **Classification**: Category of medium that the art piece falls under
#   - Sample medium categories: *woodwork*, *paintings*, etc
# - **Museum_Page**: Link to the work of art on the Met/Rijks website
# - **Norm_Features**: Embedding of the art piece image
# - **Museum**: Specifies which museum the piece originated from

# In[6]:


# loads the dataset and the two trained CKNN models for querying by medium and culture
df = spark.read.parquet("wasbs://publicwasb@mmlspark.blob.core.windows.net/met_and_rijks.parquet")
display(df.drop("Norm_Features"))


# #### Define categories to be queried on
# We will be using two kNN models: one for culture, and one for medium. The categories for each grouping are defined below.

# In[8]:


from pyspark.sql.types import BooleanType

#mediums = ['prints', 'drawings', 'ceramics', 'textiles', 'paintings', "musical instruments","glass", 'accessories', 'photographs',  "metalwork", 
#           "sculptures", "weapons", "stone", "precious", "paper", "woodwork", "leatherwork", "uncategorized"]

mediums = ['paintings', 'glass', 'ceramics']

#cultures = ['african (general)', 'american', 'ancient american', 'ancient asian', 'ancient european', 'ancient middle-eastern', 'asian (general)', 
#            'austrian', 'belgian', 'british', 'chinese', 'czech', 'dutch', 'egyptian']#, 'european (general)', 'french', 'german', 'greek', 
#            'iranian', 'italian', 'japanese', 'latin american', 'middle eastern', 'roman', 'russian', 'south asian', 'southeast asian', 
#            'spanish', 'swiss', 'various']

cultures = ['japanese', 'american', 'african (general)']

# Uncomment the above for more robust and large scale searches!

classes = cultures + mediums

medium_set = set(mediums)
culture_set = set(cultures)
selected_ids = {"AK-RBK-17525-2", "AK-MAK-1204", "AK-RAK-2015-2-9"}

small_df = df.where(udf(lambda medium, culture, id_val: (medium in medium_set) or (culture in culture_set) or (id_val in selected_ids), BooleanType())("Classification", "Culture", "id"))

small_df.count()


# ### Define and fit ConditionalKNN models
# Below, we create ConditionalKNN models for both the medium and culture columns; each model takes in an output column, features column (feature vector), values column (cell values under the output column), and label column (the quality that the respective KNN is conditioned on).

# In[10]:


medium_cknn = (ConditionalKNN()
  .setOutputCol("Matches")
  .setFeaturesCol("Norm_Features")
  .setValuesCol("Thumbnail_Url")
  .setLabelCol("Classification")
  .fit(small_df))


# In[11]:


culture_cknn = (ConditionalKNN()
  .setOutputCol("Matches")
  .setFeaturesCol("Norm_Features")
  .setValuesCol("Thumbnail_Url")
  .setLabelCol("Culture")
  .fit(small_df))


# #### Define matching and visualizing methods
# 
# After the intial dataset and category setup, we prepare methods that will query and visualize the conditional kNN's results. 
# 
# `addMatches()` will create a Dataframe with a handful of matches per category.

# In[13]:


def add_matches(classes, cknn, df):
  results = df
  for label in classes:
    results = (cknn.transform(results.withColumn("conditioner", array(lit(label))))
                 .withColumnRenamed("Matches", "Matches_{}".format(label)))
  return results


# `plot_urls()` calls `plot_img` to visualize top matches for each category into a grid.

# In[15]:


def plot_img(axis, url, title):
  response = requests.get(url)
  img = Image.open(BytesIO(response.content)).convert('RGB')
  axis.imshow(img, aspect="equal")
  if title is not None: axis.set_title(title,  fontsize=4)
  axis.axis("off")

def plot_urls(url_arr, titles, filename):
  nx, ny = url_arr.shape
  
  plt.figure(figsize=(nx*5, ny*5), dpi=1600)
  fig, axes = plt.subplots(ny,nx)
  
  # reshape required in the case of 1 image query
  if len(axes.shape) == 1:
    axes = axes.reshape(1, -1)
    
  for i in range(nx):
     for j in range(ny):
          if j == 0:
            plot_img(axes[j, i], url_arr[i,j], titles[i])
          else:
            plot_img(axes[j, i],  url_arr[i,j], None)
            
  plt.savefig(filename, dpi=1600) # saves the results as a PNG

  display(plt.show())


# ### Putting it all together
# Below, we define `test_all()` to take in the data, CKNN models, the art id values to query on, and the file path to save the output visualization to. The medium and culture models were previously trained and loaded.

# In[17]:


# main method to test a particular dataset with two CKNN models and a set of art IDs, saving the result to filename.png

def test_all(data, cknn_medium, cknn_culture, test_ids, root):
  is_nice_obj = udf(lambda obj: obj in test_ids, BooleanType())
  test_df = data.where(is_nice_obj("id"))
  
  results_df_medium = add_matches(mediums, cknn_medium, test_df)
  results_df_culture = add_matches(cultures, cknn_culture, results_df_medium)
  
  results = results_df_culture.collect()
  
  original_urls = [row["Thumbnail_Url"] for row in results]
  
  culture_urls = [ [row["Matches_{}".format(label)][0]["value"] for row in results] for label in cultures]
  culture_url_arr = np.array([original_urls] + culture_urls)[:, :]
  plot_urls(culture_url_arr, ["Original"] + cultures, root + "matches_by_culture.png")
  
  medium_urls = [ [row["Matches_{}".format(label)][0]["value"] for row in results] for label in mediums]
  medium_url_arr = np.array([original_urls] + medium_urls)[:, :]
  plot_urls(medium_url_arr, ["Original"] + mediums, root + "matches_by_medium.png")
  
  return results_df_culture


# ### Demo
# The following cell performs batched queries given desired image IDs and a filename to save the visualization.
# 
# 
# <img src="https://mmlspark.blob.core.windows.net/graphics/art/cross_cultural_matches.jpg"  width="600"/>

# In[19]:


# sample query
result_df = test_all(small_df, medium_cknn, culture_cknn, selected_ids, root=".")

