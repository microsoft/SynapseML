# Notes

Followed [this tutorial](https://docs.microsoft.com/en-us/azure/machine-learning/tutorial-deploy-models-with-aml) to
deploy sample image classification model to to test `fit` function in `AMLEstimator.scala`. Deployment via notebook 
failed because of version issue with sklearn (only supports 0.20 or earlier). Workaround: Downloaded files from tutorial
and ran locally as a script. 

[Link to deployed model]()