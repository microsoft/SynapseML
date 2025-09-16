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

For building image with SynapseML version 1.0.14, run:
```
docker build . --build-arg SYNAPSEML_VERSION=1.0.14 -f tools/docker/demo/Dockerfile -t synapseml:1.0.14
```

# Run the image
```
docker run -p4040:4040 -p8888:8888  -ti synapseml jupyter notebook ./
```
And then on a browser,
- Open [localhost:8888](https://localhost:8888) to open jupyter terminal and experiment with the notebooks.
- Open [localhost:4040]() to see the Spark Dashboard.
