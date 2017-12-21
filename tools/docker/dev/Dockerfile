FROM ubuntu:16.04

# line 5 is general requirements, 6-8 is for IntelliJ, 9 is for ssh
RUN apt-get update \
	&& apt-get install -y git curl bzip2 wget tar unzip \                    
	libpng12-0 libjasper1 libopenmpi1.10 libgomp1 libxext-dev \
	libxrender-dev libxslt1.1 libxtst-dev libgtk2.0-0 libcanberra-gtk-module \
	libfontconfig1 libxrender1 libxtst6 libxi6 \
	openssh-server


ADD install_intellij.sh install_intellij.sh

RUN apt-get install -y libfontconfig1 libxrender1 libxtst6 libxi6

RUN chmod +777 ./install_intellij.sh; sync; ./install_intellij.sh

RUN mkdir /var/run/sshd

RUN echo "root:root" | chpasswd

RUN sed -ri 's/^PermitRootLogin\s+.*/PermitRootLogin yes/' /etc/ssh/sshd_config \
 && sed -ri 's/UsePAM yes/#UsePAM yes/g' /etc/ssh/sshd_config \
 && echo "AddressFamily inet" >> /etc/ssh/sshd_config 

EXPOSE 22

CMD    ["/usr/sbin/sshd", "-D"]
