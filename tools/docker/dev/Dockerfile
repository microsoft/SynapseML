FROM ubuntu:16.04

# Install requirements
# line 6 is general requirements, 7-9 is for IntelliJ, 10 is for ssh
RUN apt-get update \
	&& apt-get install -y git curl bzip2 wget tar unzip \                    
	libpng12-0 libjasper1 libopenmpi1.10 libgomp1 libxext-dev \
	libxrender-dev libxslt1.1 libxtst-dev libgtk2.0-0 libcanberra-gtk-module \
	libfontconfig1 libxrender1 libxtst6 libxi6 \
	openssh-server

# Install intellij
COPY install_intellij.sh .
RUN ./install_intellij.sh

# Set up SSH and X forwarding
RUN mkdir "/var/run/sshd" \
	&& echo "root:root" | chpasswd \
	&& sed -ri "s/^PermitRootLogin\s+.*/PermitRootLogin yes/" "/etc/ssh/sshd_config" \
 	&& sed -ri "s/UsePAM yes/#UsePAM yes/g" "/etc/ssh/sshd_config" \
 	&& echo "AddressFamily inet" >> "/etc/ssh/sshd_config" 
EXPOSE 22

# Set home environment for runme
RUN echo "export HOME=/root/mmlspark_home/env" >> "/root/.bashrc"

# launch SSH server
CMD ["/usr/sbin/sshd", "-D"]
