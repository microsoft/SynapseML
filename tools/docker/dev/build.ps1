docker build -f .\Dockerfile -t mmlspark-dev .
docker run -d -p 2222:22 -v "$(pwd)..\..\..\..\..\:/root/mmlspark_home" mmlspark-dev
