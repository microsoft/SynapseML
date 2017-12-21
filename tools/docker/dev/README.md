# Docker-based Development Setup Instructions

MMLSpark is currently working on making the ./runme build experience work natively on windows.This page is to fill the gap in the meantime using docker.

## Prerequisites

1. Install [Docker](https://www.docker.com/docker-windows)
2. Enable [Docker mounted volumes](https://blogs.msdn.microsoft.com/stevelasker/2016/06/14/configuring-docker-for-windows-volumes/)
3. For ssh with X-forwarding install [MobaXTerm](https://mobaxterm.mobatek.net/download.html). Other X clients will probably work as well.
4. Boost your docker memory limit to something like 5GB for sbt

## Git Setup

1. Make a new folder to house your MMLSpark install and environment

```bash
    mkdir mmlspark_home
    cd mmlspark_home 
    mkdir env # Make a folder to hold the requirements
```

2. Fork the repository on Github so you can make PRs (use the botton in the top right corner)

3. Stop your git from converting linux line endings. You can do this on git install, or after install using:
```bash
  git config --global core.autocrlf false
```

4. If you want to be able to pull updates from the Azure branch, 
clone your fork using bash (bash on windows for windows) and add the main repo as a remote called "upstream":
```bash
  git clone git@github.com:<YOUR FORK NAME>/mmlspark.git
  git remote add upstream https://github.com/Azure/mmlspark.git
  git fetch upstream
```

## Docker Setup

1) If you are using windows, run the following powershell script. If you are using other systems, use `build.sh`:
```bash
   cd <MMLSPARK HOME LOCATION>\mmlspark\tools\docker\windows
   ./build.ps1
```

## SSH Setup

1. Open MobaXTerm and SSH to your running docker image:
    * session type: SSH
    * host: "localhost"
    * port: "2222"
    * Username: "root"
    * Password: "root"

## Build MMLSpark
1. Build and use like you are working on a regular linux machine:
```bash
cd mmlspark_home/mmlspark
./runme # this first call sets up the environment
./runme # this second call builds and tests
```
To skip testing use `./runme TESTS="none"`

## Intellij Setup

1. Launch Intellij `idea & `

2. Setup Intellij
    * Go through standard setup, install Scala plugin, set colors to Darcula if you are a real hacker
    * OPEN the mmlspark folder
    * Go to Project Settings and set the project SDK to `/root/lib/jdk`
    * Open `src/build.sbt` and click Import Project in the top right corner
    * Test the setup by running one of our tests in IntelliJ 
    (ex: image-transformer/src/test/scala/ImageTransformerSuite.scala, right click the test class and click `run tests`)

