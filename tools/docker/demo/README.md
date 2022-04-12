# demo Dockerfile
This dockerfile can be used to run notebooks on a local docker image. The image houses all dependencies required to experiment synapseml and all notebooks [here](https://github.com/microsoft/SynapseML/tree/master/notebooks/features)

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
And then on a browser,
- Open [localhost:8888](https://localhost:8888) to open jupyter terminal and experiment with the notebooks.
- Open [localhost:4040]() to see the Spark Dashboard.