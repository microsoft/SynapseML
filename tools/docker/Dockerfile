# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

#META# echo FROM ${MML_DOCKER_BASE_IMAGE:-ubuntu:16.04}

#META# echo LABEL maintainer=\"$SUPPORT_EMAIL\"

#META# H="/home/mmlspark"; echo ENV \
  USER="mmlspark" HOME="$H" PATH="$H/bin:\$PATH"

RUN : '==== create a user (and also hide the random hostname in the prompt)' \
 && echo 'PS1='\''\u:\w\$ '\' >> "/etc/skel/.bashrc" \
 && useradd -c "Microsoft ML for Apache Spark" -U -G root -d "$HOME" -m "$USER" \
 && : '==== install needed packages' \
 && apt-get update --fix-missing \
 && apt-get install -y curl unzip bzip2 libopenmpi1.10 libgomp1 libunwind8 \
                       libtiff5 libpng12-0 libjasper1 \
 && apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR $HOME
USER $USER:$USER

# add the premade runtime environment
ADD mmlspark.tgz ..

#BUILD# -t "${MML_DOCKER_TARGET_TAG:-mmlspark}:old-spark-keep-for-cache"
#META# rm -f "mmlspark.tgz" # avoid re-hashing a big file

#META# H="/home/mmlspark"; echo ENV \
  MML_VERSION=$(printf "%q" "$MML_VERSION") \
  MML_BUILD_INFO=$(printf "%q" "$MML_BUILD_INFO") \
  MMLSPARK_JUPYTER_PORT="8888" \
  MML_M2REPOS="file:$H/mml-m2,$MAVEN_URL" \
  MML_PACKAGE="com.microsoft.ml.spark:mmlspark_$SCALA_VERSION:$MML_VERSION"

ADD notebooks.tgz  /
ADD mml-m2.tgz .
ADD bin.tgz .

RUN : '==== make the notebooks convenient to access and have the right date' \
 && find "/notebooks" -execdir touch "{}" + \
 && ln -s /notebooks . \
 && : '==== disable the jupyter security token' \
 && mkdir ".jupyter" \
 && echo "c.NotebookApp.token = ''" > ".jupyter/jupyter_notebook_config.py" \
 && : '==== pre-populate the ivy cache' \
 && bash -c ". ./.mmlspark_profile; \
      spark-shell --master \"local[*]\" \
        --repositories \"$MML_M2REPOS\" --packages \"$MML_PACKAGE\" < /dev/null"
#BUILD#
# #SQUASH# (would be good, but: https://github.com/moby/moby/issues/31983)

#META# R() { docker run --rm -h tmphost $BUILDXTAG bash -c "$*"; }
#META# echo "ENV $(comm -13 <(R "bin/print-envvars") <(R ". .bashrc; bin/print-envvars") | tr '\n' ' ')"

# use CMD to get both "docker run this" and "docker run this command" work
EXPOSE $MMLSPARK_JUPYTER_PORT
CMD ["bin/launcher"]
