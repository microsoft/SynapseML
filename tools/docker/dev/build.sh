#!/bin/bash
docker build -f ./Dockerfile -t mmlspark-dev .
docker run -p 2222:22 -v "$(pwd)../../../../:/root/mmlspark" mmlspark-dev
