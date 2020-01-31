import numpy as np
from sklearn.linear_model import LinearRegression
from joblib import dump
from azureml.core.run import Run
import os
from azureml.core import Experiment, Workspace
from azureml.train.estimator import Estimator

X = np.array([[1, 1], [1, 2], [2, 2], [2, 3]])
y = np.dot(X, np.array([1, 2])) + 3
reg = LinearRegression().fit(X, y)

outputdir = "outputs/"
os.makedirs(outputdir, exist_ok=True)
dump(reg, outputdir + "my_c00l_classifier.joblib") 
