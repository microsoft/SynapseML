# MMLSpark Development

1) [Install SBT](https://www.scala-sbt.org/1.x/docs/Setup.html)
    - Make sure to download JDK 1.8 if you dont have it
2) Git Clone Repository
    - `git clone https://github.com/Azure/mmlspark.git`
    - `git checkout build-refactor`
3) Run sbt to compile and grab datasets
    - `sbt compile test:compile getDatasets`
4) [Install IntelliJ](https://www.jetbrains.com/idea/download)
5) Configure IntelliJ
    - OPEN the mmlspark directory
    - click on build.sbt and import project (install scala/sbt plugins if needed)
