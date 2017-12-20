FROM ubuntu:16.04

RUN apt-get update\
	&& apt-get install -y git curl bzip2 wget tar unzip\
	&& apt-get install -y libpng12-0 libjasper1 libopenmpi1.10 libgomp1\
	&& apt-get install -qq -y --fix-missing sudo software-properties-common libxext-dev libxrender-dev libxslt1.1 libxtst-dev libgtk2.0-0 libcanberra-gtk-module 

ADD install_intellij.sh install_intellij.sh

RUN apt-get install -y libfontconfig1 libxrender1 libxtst6 libxi6

RUN chmod +777 ./install_intellij.sh; sync; ./install_intellij.sh


RUN apt-get install -y openssh-server
RUN mkdir /var/run/sshd

RUN echo 'root:root' |chpasswd

RUN sed -ri 's/^PermitRootLogin\s+.*/PermitRootLogin yes/' /etc/ssh/sshd_config \
 && sed -ri 's/UsePAM yes/#UsePAM yes/g' /etc/ssh/sshd_config \
 && echo "AddressFamily inet" >> /etc/ssh/sshd_config 

EXPOSE 22

CMD    ["/usr/sbin/sshd", "-D"]
