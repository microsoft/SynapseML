#!/usr/bin/env python
# coding: utf-8

# In[ ]:



import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from datetime import datetime 
from sklearn import metrics
from sklearn.metrics import roc_auc_score
from sklearn.metrics import confusion_matrix
import seaborn as sns
import keras.backend as k
penaliser_controller=0.25
# Importing the dataset
df = pd.read_csv("data/wisconsin_breast_cancer_dataset.csv")

#Rename Dataset to Label to make it easy to understand
df = df.rename(columns={'Diagnosis':'Label'})


####### Replace categorical values with numbers########
df['Label'].value_counts()

#Define the dependent variable that needs to be predicted (labels)
y = df["Label"].values

# Encoding categorical data
from sklearn.preprocessing import LabelEncoder
labelencoder = LabelEncoder()
Y = labelencoder.fit_transform(y) # M=1 and B=0

#Define x and normalize values

#Define the independent variables. Let's also drop Gender, so we can normalize other data
X = df.drop(labels = ["Label", "ID"], axis=1) 


feature_names = np.array(X.columns)  #Convert dtype string?


from sklearn.preprocessing import StandardScaler
scaler = StandardScaler()
scaler.fit(X)
X = scaler.transform(X)

##Split data into train and test to verify accuracy after fitting the model. 
from sklearn.model_selection import train_test_split
X_train, X_test, y_train, y_test = train_test_split(X, Y, test_size=0.2, random_state=42)

#####################################################
#Light GBM

import lightgbm as lgb
d_train = lgb.Dataset(X_train, label=y_train)

def penalisation_loss(penaliser_controller,y_true, y_pred):
  error = k.square(y_true - y_pred)         #(batch_size,2)
  #finalError = penaliser_controller*error +(penaliser_controller-1) *error
  #finalError= k.sum(finalError , axis=1)
    # calculate loss, using y_pred
  return k.mean(k.maximum((penaliser_controller*error),(penaliser_controller-1) *error))

# https://lightgbm.readthedocs.io/en/latest/Parameters.html
lgbm_params = {'learning_rate':0.05, 'boosting_type':'gbdt',    #Try dart for better accuracy
              'objective':'binary',
              'metric':['auc', 'penalisation_loss'],
              'num_leaves':100,
              'max_depth':10}

start=datetime.now()
clf = lgb.train(lgbm_params, d_train, 50) #50 iterations. Increase iterations for small learning rates
stop=datetime.now()
execution_time_lgbm = stop-start
#print("LGBM execution time is: ", execution_time_lgbm)

#Prediction on test data
y_pred_lgbm=clf.predict(X_test)

#convert into binary values 0/1 for classification
for i in range(0, X_test.shape[0]):
    if y_pred_lgbm[i]>=.5:       # setting threshold to .5
       y_pred_lgbm[i]=1
    else:  
       y_pred_lgbm[i]=0
       
#Print accuracy
#print ("Accuracy with LGBM = ", metrics.accuracy_score(y_pred_lgbm,y_test))

       
#Confusion matrix

cm_lgbm = confusion_matrix(y_test, y_pred_lgbm)
sns.heatmap(cm_lgbm, annot=True)


#print("AUC score with LGBM is: ", roc_auc_score(y_pred_lgbm,y_test))


###################################

import xgboost as xgb 
dtrain=xgb.DMatrix(X_train,label=y_train)


#setting parameters for xgboost
parameters={'max_depth':10, 
            'objective':'binary:logistic',
            'eval_metric':'auc',
            'learning_rate':.05}


start = datetime.now() 
xg=xgb.train(parameters, dtrain, 50) 
stop = datetime.now()

#Execution time of the model 
execution_time_xgb = stop-start 
#print("XGBoost execution time is: ", execution_time_xgb)

#now predicting the model on the test set 
dtest=xgb.DMatrix(X_test)
y_pred_xgb = xg.predict(dtest) 

#Converting probabilities into 1 or 0  
for i in range(0, X_test.shape[0]): 
    if y_pred_xgb[i]>=.5:       # setting threshold to .5 
       y_pred_xgb[i]=1 
    else: 
       y_pred_xgb[i]=0  

cm_xgb = confusion_matrix(y_test, y_pred_xgb)
sns.heatmap(cm_xgb, annot=True)

#print ("Accuracy with XGBoost= ", metrics.accuracy_score(y_pred_xgb, y_test))
#print("AUC score with XGBoost is: ", roc_auc_score(y_pred_xgb, y_test))

################
#SUMMARY
print("################################################")
print("LGBM execution time is: ", execution_time_lgbm)
print("XGBoost execution time is: ", execution_time_xgb)
print("################################################")
print ("Accuracy with LGBM = ", metrics.accuracy_score(y_pred_lgbm,y_test))
print ("Accuracy with XGBoost= ", metrics.accuracy_score(y_pred_xgb, y_test))
print("################################################")
print("AUC score with LGBM is: ", roc_auc_score(y_pred_lgbm,y_test))
print("AUC score with XGBoost is: ", roc_auc_score(y_pred_xgb, y_test))

