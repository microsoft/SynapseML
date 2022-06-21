#!/usr/bin/env bash
firewall-cmd --zone=public --add-port=8000-10000/tcp --permanent
firewall-cmd --reload
echo "GatewayPorts yes" >> /etc/ssh/sshd_config
service ssh --full-restart
