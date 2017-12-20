# Windows Development Setup Instructions

MMLSpark is currently working on making the ./runme build experience work natively on windows.This page is to fill the gap in the meantime using docker.

## Prerequisites

1) Install [Docker for Windows](https://www.docker.com/docker-windows)
2) Enable [Docker mounted volumes](https://rominirani.com/docker-on-windows-mounting-host-directories-d96f3f056a2c)
3) For ssh with X-forwarding install [MobaXTerm](https://mobaxterm.mobatek.net/download.html). Other X clients will probably work as well.
4) If you have issues with running `./runme` you might have to boost your docker memory limit to something like 5GB

## Git Setup

1) Fork the repository on Github so you can make PRs

2) Stop your git from converting linux line endings:
```bash
  git config --global core.autocrlf false
```

3) Clone your fork using git bash and add the main repo as a remote called "upstream":
```bash
  git clone git@github.com:[[YOUR FORK NAME]]/mmlspark.git
  git remote add upstream https://github.com/Azure/mmlspark.git
  git fetch upstream
```

## Docker Setup

1) Open up a powershell window, navigate to the build script and run:
```bash
   cd [[MMLSPARK CLONE LOCATION]]\tools\docker\windows
   ./build.ps1
```

## SSH Setup

1) Open MobaXTerm and SSH to your running docker image:
    * session type: SSH
    * host: "localhost"
    * port: "2222"
    * UN: "root"
    * PW: "root"

## Build MMLSpark
1) Build and use like a linux VM:
```bash
cd mmlspark
./runme TESTS="none" # this first call sets up the environment
./runme TESTS="none" # this second call builds with sbt
```

## Intellij Setup

1) Launch intellij ```idea & ```

2) Setup intellij
    * Go through standard setup, install scala plugin, set colors to Darcula if you are a real hacker
    * OPEN the mmlspark folder
    * Go to Project Settings and set the project SDK to `/root/lib/jdk`
    * Open `src/build.sbt` and click Import Project in the top right corner
    * Test the setup by running one of our tests in IntelliJ

