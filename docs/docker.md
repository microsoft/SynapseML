# Using the MMLSpark Docker Image

## Quickstart: install and run the Docker image

Begin by installing [Docker for your OS][docker-products].  Then, to get the
MMLSpark image and run it, open a terminal (powershell/cmd on Windows) and run

   ```bash
   docker run -it -p 8888:8888 mcr.microsoft.com/mmlspark/release
   ```

In your browser, go to <http://localhost:8888/> — you'll see the Docker image
EULA, and once you accept it, the Jupyter notebook interface will start.  To
skip this step, add `-e ACCEPT_EULA=yes` to the Docker command:

   ```bash
   docker run -it -p 8888:8888 -e ACCEPT_EULA=y mcr.microsoft.com/mmlspark/release
   ```

You can now select one of the sample notebooks and run it, or create your own.

> Note: The EULA is needed only for running the MMLSpark Docker image; the
> source code is released under the MIT license (see the [LICENSE](../LICENSE)
> file).


## Running a specific version

In the above, `mcr.microsoft.com/mmlspark/release` specifies the project and image name that you
want to run.  There is another component implicit here which is the *tag* (=
version) that you want to use — specifying it explicitly looks like
`mcr.microsoft.com/mmlspark/release:0.17` for the `0.17` tag.

Leaving `mcr.microsoft.com/mmlspark/release` by itself has an implicit `latest` tag, so it is
equivalent to `mcr.microsoft.com/mmlspark/release:latest`.  The `latest` tag is identical to the
most recent stable MMLSpark version.  You can see the current [mmlspark tags] on
our [Docker Hub repository][mmlspark-dockerhub].


## A more practical example

The previous section had a rather simplistic command.  A more complete command
that you will probably want to use can look as follows:

   ```bash
   docker run -it --rm \
              -e ACCEPT_EULA=y \
              -p 127.0.0.1:80:8888 \
              -v ~/myfiles:/notebooks/myfiles \
              mcr.microsoft.com/mmlspark/release:0.17
   ```

In this example, backslashes are used to break things up for readability; you
can enter it as one long like.  Note that in powershell, the `myfiles` local
path and line breaks looks a little different:

   ```
   docker run -it --rm `
              -e ACCEPT_EULA=y `
              -p 127.0.0.1:80:8888 `
              -v C:\myfiles:/notebooks/myfiles `
              mcr.microsoft.com/mmlspark/release:0.17
   ```

Let's break this command and go over the meaning of each part:

* **`-it`**

  This is a combination of `-i` and `-t` (which could also be specified as
  `--interactive --tty`).  The combination of these two flags mean that the
  image is running interactively, which in this example means that you can see
  messages that the server prints out, and it also makes it possible to use
  `Ctrl+C` to shut down the Jupyter notebook server.

* **`--rm`**

  When Docker runs any image, it creates a *container* to hold any additional
  filesystem data for files that were created or modified.  If you ran the above
  quickstart command, you can see the container that is left behind with `docker
  container list -a`.  You can reclaim such containers with `docker container rm
  <id>`, or reclaim all containers from stopped run with `docker container
  prune`, or even more generally, reclaim all unused Docker resources with
  `docker system prune`.

  Back to `--rm`: this flag tells Docker to discard the image when the image
  exits.  You should be aware that this means that any data created while
  running the image is discarded when the run is done — but see the description
  of the `-v` flag below.

* **`-e ACCEPT_EULA=y`**

  The `-e` flag is used to set environment variables in the running container.
  In this case, we use it to bypass the EULA check.  Additional flags can be
  added for other variables, for example, you can add a `-e
  MMLSPARK_JUPYTER_PORT=80` to change the port that the Jupyter server listens
  to.

* **`-p 127.0.0.1:80:8888`**

  The Jupyter server in the MMLSpark image listens to port 8888 — but that is
  normally isolated from the actual network.  Previously, we have used `-p
  8888:8888` to say that we want to map port 8888 (LHS) on our actual machine to
  port 8888 (RHS) in the container.  One problem with this is that `8888` might
  be hard to remember, but a more serious problem is that your machine now
  serves the Jupyter interface to any one on your network.

  This more complete example resolves these issues: we replaced `8888:8888` with
  `80:8888` so HTTP port 80 goes to the container's running Jupyter (making just
  <http://localhost/> work); and we also added a `127.0.0.1:` prefix which means
  that this is available only from your own machine rather than being exposed.

  You can repeat this flag to forward additional ports similarly.  For example,
  you can expose some of the [Spark ports], e.g.,: `-p 127.0.0.1:4040:4040`.

* **`-v ~/myfiles:/notebooks/myfiles`**

  As described above, we're using `--rm` to remove the container when the run
  exits.  This is usually fine since pulling out files from these containers can
  be a little complicated.  Instead, we use this flag to map a directory from
  your machine (the `~/myfiles` on the LHS) to a directory that is available
  inside the running container.  Any modifications to this directory that are
  done by the Docker image are performed directly on the actual directory.

  Note that the local directory follows the local filename conventions, so on
  Windows you'd use a Windows-looking path.  On Windows you also need to share
  the drive you want to use in the [Docker settings].

  The path on the right side is used inside the container and it is therefore a
  Linux path.  The MMLSpark image runs Jupyter in the `/notebooks` directory, so
  it is a good place for making your files available conveniently.

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

* **`mcr.microsoft.com/mmlspark/release:0.17`**

  Finally, this specifies an explicit version tag for the image that we want to
  run.


## Running the container as a server

An alternative to running the Docker image interactively with `-it` is running
it in a "detached" mode, as a server, using the `-d` (or `--detach`) flag.  An
additional flag that is useful for this is `--name` that gives a convenient
label to the running image:

   ```bash
   docker run -d --name my-mmlspark ...flags... mcr.microsoft.com/mmlspark/release
   ```

When running in this mode, you can use

* `docker stop my-mmlspark`:  to stop the image

* `docker start my-mmlspark`: to start it again

* `docker logs my-mmlspark`:  to see the log output it produced


## Running other commands in an active container

Another useful `docker` command is `exec`, which runs a command in the context
of an *existing* active container.  To use it, you specify the container name,
and the command to run.  For example, with a detached container started as
above, you can use

   ```bash
   docker exec -it my-mmlspark bash
   ```

to start a shell in the context of the server, roughly equivalent to starting a
terminal in the Jupyter interface.

Other common Linux executables can be used, e.g.,

   ```bash
   docker exec -it my-mmlspark top
   docker exec my-mmlspark ps auxw
   ```

(Note that `ps` does not need `-it` since it's not an interactive command.)

These commands can be used with interactive containers too, and `--name` can be
used to make them easy to target.  If you don't use `--name`, Docker assigns a
random name to the container; you can use `docker ps` to see it --- and you can
also get the container IDs and use those instead of names.

Remember that the command given to `docker exec` is running in the context of
the running container: you can only run executables that exist in the container,
and the run is subject to the same resource restrictions (FS/network access,
etc) as the container.  The MMLSpark image is based on a rather basic Ubuntu
installation (the `ubuntu` image from Docker Hub).


## Running other Spark executables

`docker run` can accept another optional argument after the image name,
specifying an alternative executable to run instead of the default launcher that
fires up the Jupyter notebook server.  This makes it possible to use the Spark
environment directly in the container if you start it as:

   ```bash
   docker run -it ...flags... mcr.microsoft.com/mmlspark/release bash
   ```

This starts the container with bash instead of Jupyter.  This environment has
all of the Spark executables available in its `$PATH`.  You still need to
specify the command-line flags that load the MMLSpark package, but there are
convenient environment variables that hold the required package and repositories
to use:

   ```bash
   pyspark --repositories "$MML_M2REPOS" --packages "$MML_PACKAGE" --master "local[*]"
   ```

Many of the above listed flags are useful in this case too, such as mapping work
directories with `-v`.


## Updating the MMLSpark image

New releases of MMLSpark are published from time to time, and they include a new
Docker image.  As an image consumer, you will normlly not notice such new
versions: `docker run` will download an image if a copy of it does not exist
locally, but if it does, then `docker run` will blindly run it, *without*
checking for new tags that were pushed.

This means that you need to explicitly tell Docker to check for a new version
and pull it if one exists.  You do this with the `pull` command:

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

If you have used an explicit version tag, then it will still exist after a new
pull, which means that you can continue using this version.  Note that if you
used an unqualified name first and then a version-tagged one, Docker will fetch
both tags, only the second fetch is fast since the it points to contents that
was already loaded.  In this case, doing a `pull` when there's a new version
will fetch the new `latest` tag and change its meaning to the newer version, but
the older version will still be available under its own version tag.

Finally, if there are such version-tagged older versions that you want to get
rid of, you can use `docker images` to check the list of installed images and
their tags, and `docker rmi <name>:<tag>` to remove the unwanted ones.


## A note about security

Executing code in a Docker container can be unsafe if the running user is
`root`.  For this reason, the MMLSpark image uses a proper username instead.  If
you still want to run as root (e.g., if you want to `apt install` an additional
ubuntu package), then you should use `--user root`.  This can be useful when
combined with `docker exec` too do such administrative work while the image
continues to run as usual.


## Further reading

This text covers very briefly some of the useful things that you can do with the
MMLSpark Docker image (and other images in general).  You can find much more
documentation [online](https://docs.docker.com/).



[docker-products]:    http://www.docker.com/products/overview/
[mmlspark tags]:      https://hub.docker.com/r/microsoft/mmlspark/tags/
[mmlspark-dockerhub]: https://hub.docker.com/r/microsoft/mmlspark/
[Spark ports]:        https://spark.apache.org/docs/latest/security.html#configuring-ports-for-network-security
[Docker settings]:    https://docs.docker.com/docker-for-windows/#docker-settings
