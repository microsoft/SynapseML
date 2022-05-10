# demo Dockerfile
This dockerfile can be used to run notebooks on a local docker image. The image houses all dependencies required to experiment synapseml and all notebooks [here](https://github.com/microsoft/SynapseML/tree/master/notebooks/features)

# Build the image
To build the docker image from the current tip of `master` branch, run:

```
docker build . -f tools/docker/demo/Dockerfile -t synapseml
```

If you wish to build the image from a specific version SynapseML version, run:
```
docker build . --build-arg SYNAPSEML_VERSION=<YOUR-VERSION-HERE> -f tools/docker/demo/Dockerfile -t synapseml:<VERSION-TAG>
```

eg.

For building image with SynapseML version 0.9.5, run:
```
docker build . --build-arg SYNAPSEML_VERSION=0.9.5 -f tools/docker/demo/Dockerfile -t synapseml:0.9.5
```

# Run the image
```
$ docker run -ti synapseml jupyter notebook ./
```
And then on a browser,
- Open [localhost:8888](https://localhost:8888) to open jupyter terminal and experiment with the notebooks.
- Open [localhost:4040]() to see the Spark Dashboard.