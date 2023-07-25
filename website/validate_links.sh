#!/bin/bash

echo "Starting the website..."
yarn start &
sleep 60
echo "Crawling the website to check for dead links..."
wget --spider -r http://localhost:3000/SynapseML 2>&1 | grep -B 2 '404 Not Found'
pkill -f "yarn start"
