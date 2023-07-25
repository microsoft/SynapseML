#!/bin/bash

# Function to check if the website is up and running
check_website_status() {
  max_retries=30
  retries=0
  while [ $retries -lt $max_retries ]; do
    http_status=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:3000/SynapseML)
    if [ $http_status -eq 200 ]; then
      echo "Website is up and running!"
      break
    else
      echo "Waiting for the website to deploy... (Attempt $((retries+1)) of $max_retries)"
      sleep 5 # Adjust the sleep duration as needed
      retries=$((retries+1))
    fi
  done

  if [ $retries -eq $max_retries ]; then
    echo "Website deployment failed. Exiting."
    exit 1
  fi
}

# Function to crawl the website and check for dead links
crawl_for_deadlinks() {
  echo "Crawling the website to check for dead links..."
  wget --spider -r http://localhost:3000/SynapseML 2>&1 | grep -B 2 '404 Not Found'
}

# Start the website
echo "Starting the website..."
yarn start &

# Wait for the website to deploy
check_website_status

# Crawl the website for dead links
crawl_for_deadlinks

# Stop the server after the crawl is complete (you may need to adjust this command based on your specific setup)
# For example, if you are using a different web server like Node.js' http-server, replace the command accordingly.
pkill -f "yarn start"
