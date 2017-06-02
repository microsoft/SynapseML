//> DependsOn: core

Extras.noJar

// Running this project will load all jars, which will fail if they're
// all "provided".  This magic makes it as if the "provided" is not
// there for the run task.  See https://github.com/sbt/sbt-assembly and
// http://stackoverflow.com/questions/18838944/
run in Compile :=
  Defaults
    .runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))
    .evaluated
