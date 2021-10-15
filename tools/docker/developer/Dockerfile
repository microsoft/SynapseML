FROM ubuntu:18.04

RUN apt-get -qq update \
    && apt-get -qq upgrade \
    && apt-get -qq -y install curl bzip2 \
    && curl -sSL https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -o /tmp/miniconda.sh \
    && bash /tmp/miniconda.sh -bfp /usr/local \
    && rm -rf /tmp/miniconda.sh \
    && conda install -y python=3 \
    && conda update conda \
    && apt-get install default-jdk -y \
    && apt-get install apt-transport-https \
    && echo "deb https://dl.bintray.com/sbt/debian /" | tee -a /etc/apt/sources.list.d/sbt.list \
    && apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2EE0EA64E40A89B84B2DF73499E82A75642AC823 \
    && apt-get update \
    && apt-get install sbt -y \
    && curl -sL https://aka.ms/InstallAzureCLIDeb | bash \
    && apt-get -qq -y remove curl bzip2 \
    && apt-get -qq -y autoremove \
    && apt-get autoclean \
    && rm -rf /var/lib/apt/lists/* /var/log/dpkg.log \
    && conda clean --all --yes

ENV PATH /opt/conda/bin:$PATH
ENV JAVA_HOME /usr/lib/jvm/java-1.8.0-openjdk-amd64

RUN mkdir -p /__w/1
WORKDIR /__w/1
RUN git clone https://github.com/Microsoft/SynapseML.git s \
    && cd s \
    && git checkout build-refactor
WORKDIR /__w/1/s

ADD environment.yaml .
RUN conda env create -f environment.yaml && conda clean --all --yes
ADD . .

ENV SBT_OPTS="${SBT_OPTS} -Dsbt.io.jdktimestamps=true"
RUN sbt setup
RUN conda init bash
RUN echo "source activate synapseml" > ~/.bashrc

# RUN /bin/bash -c "sbt testPython"


