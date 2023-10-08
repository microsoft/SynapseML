---
title: Docker Setup
sidebar_label: Docker Setup
description: Docker Setup
---

## Quickstart: install and run the Docker image

Begin by installing [Docker for your OS][docker-products].  Then, to get the
SynapseML image and run it, open a terminal (PowerShell/cmd on Windows) and run

```bash
docker run -it -p 8888:8888 mcr.microsoft.com/mmlspark/release
```

In your browser, go to <http://localhost:8888/> —you'll see the Docker image
EULA, and once you accept it, the Jupyter notebook interface will start.  To
skip this step, add `-e ACCEPT_EULA=yes` to the Docker command:

```bash
docker run -it -p 8888:8888 -e ACCEPT_EULA=y mcr.microsoft.com/mmlspark/release
```

You can now select one of the sample notebooks and run it, or create your own.

> Note: The EULA is needed only for running the SynapseML Docker image; the
> source code is released under the MIT license (see the [LICENSE](https://github.com/microsoft/SynapseML/blob/master/LICENSE)
> file).

## Running a specific version

In the preceding docker command, `mcr.microsoft.com/mmlspark/release` specifies the project and image name that you
want to run.  There's another component implicit here: the _tsag_ (=
version) that you want to use. Specifying it explicitly looks like
`mcr.microsoft.com/mmlspark/release:0.11.3` for the `0.11.3` tag.

Leaving `mcr.microsoft.com/mmlspark/release` by itself has an implicit `latest` tag, so it's
equivalent to `mcr.microsoft.com/mmlspark/release:latest`.  The `latest` tag is identical to the
most recent stable SynapseML version.  You can see the current [synapsemltags] on
our [Docker Hub repository][mmlspark-dockerhub].

## A more practical example

The previous section had a rather simplistic command.  A more complete command
that you'll probably want to use can look as follows:

```bash
docker run -it --rm \
           -p 127.0.0.1:80:8888 \
           -v ~/myfiles:/notebooks/myfiles \
           mcr.microsoft.com/mmlspark/release:0.11.3
```

In this example, backslashes are for readability; you
can enter the command as one long line if you like.  In PowerShell, the `myfiles` local
path and line breaks looks a little different:

    docker run -it --rm `
               -p 127.0.0.1:80:8888 `
               -v C:\myfiles:/notebooks/myfiles `
               mcr.microsoft.com/mmlspark/release:0.11.3

Let's break this command and go over the meaning of each part:

-   **`-it`**

    This command uses a combination of `-i` and `-t` (which could also be specified as
    `--interactive --tty`).  Combining these two flags means that the
    image is running interactively, which in this example means that you can see
    messages that the server emits, and it also makes it possible to use
    `Ctrl+C` to shut down the Jupyter notebook server.

-   **`--rm`**

    When Docker runs any image, it creates a _container_ to hold any further
    filesystem data for files that were created or modified.  If you ran the above
    quickstart command, you can see the container that is left behind with `docker
    container list -a`.  You can reclaim such containers with `docker container rm
    <id/>`, or reclaim all containers from stopped run with `docker container
    prune`, or even more generally, reclaim all unused Docker resources with
    `docker system prune`.

    Back to `--rm`: this flag tells Docker to discard the image when the image
    exits, which means that any data created while
    running the image is discarded when the run is done. But see the description
    of the `-v` flag.

-   **`-e ACCEPT_EULA=y`**

    The `-e` flag is used to set environment variables in the running container.
    In this case, we use it to bypass the EULA check.  More flags can be
    added for other variables, for example, you can add a `-e
    MMLSPARK_JUPYTER_PORT=80` to change the port that the Jupyter server listens
    to.

-   **`-p 127.0.0.1:80:8888`**

    The Jupyter server in the SynapseML image listens to port 8888, but that is
    normally isolated from the actual network.  Previously, we have used `-p
    8888:8888` to say that we want to map port 8888 (LHS) on our actual machine to
    port 8888 (RHS) in the container.  One problem with this is that `8888` might
    be hard to remember, but a more serious problem is that your machine now
    serves the Jupyter interface to any one on your network.

    This more complete example resolves these issues: we replaced `8888:8888` with
    `80:8888` so HTTP port 80 goes to the container's running Jupyter (making just
    <http://localhost/> work); and we also added a `127.0.0.1:` prefix to make the
    Jupyter inteface available only from your own machine rather than the whole network.

    You can repeat this flag to forward additional ports similarly.  For example,
    you can expose some of the [Spark ports], for example: `-p 127.0.0.1:4040:4040`.

-   **`-v ~/myfiles:/notebooks/myfiles`**

    As described earlier, we're using `--rm` to remove the container when the run
    exits, which is usually fine since pulling out files from these containers can
    be a little complicated.  Instead, we use the -v flag to map a directory from
    your machine (the `~/myfiles` on the LHS) to a directory that is available
    inside the running container.  Any modifications to this directory that are
    done by the Docker image are performed directly on the actual directory.

    The local directory follows the local filename conventions, so on
    Windows you'd use a Windows-looking path.  On Windows, you also need to share
    the drive you want to use in the [Docker settings].

    The path on the right side is used inside the container and it's therefore a
    Linux path.  The SynapseML image runs Jupyter in the `/notebooks` directory, so
    it's a good place for making your files available conveniently.

    This flag can be used more than once, to make several directories available in
    the running container.  Both paths must be absolute, so if you want to specify
    a path relatively, you can use something like `-v
    $PWD/myfiles:/notebooks/myfiles`.

    With such directory sharing in place, you can create/edit notebooks, and code
    in notebooks can use the shared directory for additional data, for example:

    ```python
    data = spark.read.csv('myfiles/mydata.csv')
    ...
    model.write().overwrite().save('myfiles/myTrainedModel.mml')
    ```

-   **`mcr.microsoft.com/mmlspark/release:0.11.3`**

    Finally, this argument specifies an explicit version tag for the image that we want to
    run.

## Running the container as a server

An alternative to running the Docker image interactively with `-it` is running
it in a "detached" mode, as a server, using the `-d` (or `--detach`) flag. 
A second flag that may be useful here is `--name`, which gives a convenient
label to the running image:

```bash
docker run -d --name my-synapseml ...flags... mcr.microsoft.com/mmlspark/release
```

When running in this mode, you can use

-   `docker stop my-synapseml`:  to stop the image

-   `docker start my-synapseml`: to start it again

-   `docker logs my-synapseml`:  to see the log output it produced

## Running other commands in an active container

Another useful `docker` command is `exec`, which runs a command in the context
of an _existing_ active container.  To use it, you specify the container name
and the command to run.  For example, with an already running detached container
named my-synapseml, you can use

```bash
docker exec -it my-synapseml bash
```

to start a shell in the context of the server, roughly equivalent to starting a
terminal in the Jupyter interface.

Other common Linux executables can be used, for example:

```bash
docker exec -it my-synapseml top
docker exec my-synapseml ps auxw
```

(`ps` doesn't need `-it` since it's not an interactive command.)

These commands can be used with interactive containers too, and `--name` can be
used to make them easy to target.  If you don't use `--name`, Docker assigns a
random name to the container; you can use `docker ps` to see it. You can
also get the container IDs to use instead of names.

Remember that the command given to `docker exec` is running in the context of
the running container: you can only run executables that exist in the container,
and the run is subject to the same resource restrictions (FS/network access,
etc.) as the container.  The SynapseML image is based on a rather basic Ubuntu
installation (the `ubuntu` image from Docker Hub).

## Running other Spark executables

`docker run` can accept another optional argument after the image name,
specifying an alternative executable to run instead of the default launcher that
fires up the Jupyter notebook server.  Using this extra argument you can use the
Spark environment directly in the container:

```bash
docker run -it ...flags... mcr.microsoft.com/mmlspark/release bash
```

This command starts the container with bash instead of Jupyter.  This environment
has all of the Spark executables available in its `$PATH`.  You still need to
specify the command-line flags that load the SynapseML package, but there are
convenient environment variables that hold the required package and repositories
to use:

```bash
pyspark --repositories "$MML_M2REPOS" --packages "$MML_PACKAGE" --master "local[*]"
```

Many of the above listed flags are useful in this case too, such as mapping work
directories with `-v`.

## Updating the SynapseML image

New releases of SynapseML are published from time to time, and they include a new
Docker image.  As an image consumer, you'll normally not notice such new
versions: `docker run` will download an image if a copy of it doesn't exist
locally, but if it does, then `docker run` will blindly run it, _without_
checking for new tags that were pushed.

Hence you need to explicitly tell Docker to check for a new version
and pull it if one exists.  You do so with the `pull` command:

```bash
docker pull mcr.microsoft.com/mmlspark/release
```

Since we didn't specify an explicit tag here, `docker` adds the implied
`:latest` tag, and checks the available `mcr.microsoft.com/mmlspark/release` image with this tag
on Docker Hub.  When it finds a different image with this tag, it will fetch a
copy to your machine, changing the image that an unqualified
`mcr.microsoft.com/mmlspark/release` refers to.

Docker normally knows only about the tags that it fetched, so if you've always
used `mcr.microsoft.com/mmlspark/release` to refer to the image without an explicit version tag,
then you wouldn't have the version-tagged image too.  Once the tag is updated,
the previous version will still be in your system, only without any tag.  Using
`docker images` to list the images in your system will now show you two images
for `mcr.microsoft.com/mmlspark/release`, one with a tag of `latest` and one with no tag, shown
as `<none>`.  Assuming that you don't have active containers (including detached
ones), `docker system prune` will remove this untagged image, reclaiming the
used space.

If you've used an explicit version tag, then it will still exist after a new
pull, which means that you can continue using this version.  If you
used an unqualified name first and then a version-tagged one, Docker will fetch
both tags. Only the second fetch is fast since it points to content that
was already loaded.  In this case, doing a `pull` when there's a new version
will fetch the new `latest` tag and change its meaning to the newer version, but
the older version will still be available under its own version tag.

Finally, if there are such version-tagged older versions that you want to get
rid of, you can use `docker images` to check the list of installed images and
their tags, and `docker rmi <name>:<tag>` to remove the unwanted ones.

## A note about security

Executing code in a Docker container can be unsafe if the running user is
`root`.  For this reason, the SynapseML image uses a proper username instead.  If
you still want to run as root (for instance, if you want to `apt install` an
another ubuntu package), then you should use `--user root`.  This mode can be useful
when combined with `docker exec` to perform administrative work while the image
continues to run as usual.

## Further reading

This text briefly covers some of the useful things that you can do with the
SynapseML Docker image (and other images in general).  You can find much more
documentation [online](https://docs.docker.com/).

[docker-products]: http://www.docker.com/products/overview/

[mmlspark tags]: https://hub.docker.com/r/microsoft/mmlspark/tags/

[mmlspark-dockerhub]: https://hub.docker.com/r/microsoft/mmlspark/

[Spark ports]: https://spark.apache.org/docs/latest/security.html#configuring-ports-for-network-security

[Docker settings]: https://docs.docker.com/docker-for-windows/#docker-settings
