#!/bin/bash
SCRIPT_PATH=$(realpath "$0")
SCRIPT_DIR=$(dirname "$SCRIPT_PATH")
DOCKER_CONTEXT=$(realpath "$SCRIPT_DIR/../../..")
docker build -t my_image_name -f "$SCRIPT_DIR/Dockerfile" "$DOCKER_CONTEXT"
