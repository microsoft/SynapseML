FROM mcr.microsoft.com/onebranch/cbl-mariner/build:2.0

USER root

SHELL ["/bin/bash", "--login", "-c"]

RUN mkdir -p /etc/micromamba

RUN wget -qO- https://aka.ms/install-artifacts-credprovider.sh | bash && \
    curl -Ls https://micro.mamba.pm/api/micromamba/linux-64/latest | tar -xvj bin/micromamba && \
    ./bin/micromamba shell init -s bash -p ~/micromamba  # this writes to your .bashrc file && \
    ./bin/micromamba config append channels conda-forge && \
    ./bin/micromamba config set channel_priority strict

RUN curl -fL https://github.com/coursier/coursier/releases/latest/download/cs-x86_64-pc-linux.gz | gzip -d > cs && chmod +x cs && ./cs setup --apps scala,scalac,sbt --yes && \
    echo "export PATH=$PATH:/root/.local/share/coursier/bin" >> ~/.bashrc

RUN tdnf -y update && \
    tdnf -y install msopenjdk-11 vi conda && \
    python3.9 -m pip install -U --force-reinstall charset-normalizer  && \
    python3.9 -m pip install conda-pack && \
    tdnf clean all 

RUN git config --global core.autocrlf true

COPY environment.yml /etc/micromamba/environment.yml
RUN micromamba env create -f /etc/micromamba/environment.yml -y

COPY website/environment.yml /etc/micromamba/website_environment.yml
RUN micromamba env create -f /etc/micromamba/website_environment.yml -y

RUN echo "micromamba activate synapseml" >> ~/.bashrc

WORKDIR /