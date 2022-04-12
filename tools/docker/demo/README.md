# demo Dockerfile


This dockerfile can be used to run notebooks on a local docker image. The image houses all dependencies required to experiment synapseml and all notebooks under the path [here](../../notebook/features)

# Build the image
To build the docker image, run the following command:

```
$ cd SynapseML
$ docker build . -f tools/docker/demo/Dockerfile -t synapseml
```

# Run the image
```
$ docker run -ti synapseml jupyter notebook ./
```
And then on a browser, open localhost:8888 to open jupyter terminal and play with the notebooks.