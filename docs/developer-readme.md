# MMLSpark

## Repository Layout

* `runme`:    main build entry point
* `src/`:     scala and python sources
  - `core/`:  shared functionality
  - `project/`: sbt build-related materials
* `tools/`:   build-related tools


## Build

### Build Environment

Currently, this code is developed and built on Linux.  The main build entry
point, `./runme`, will install the needed packages.  When everything is
installed, you can use `./runme` again to do a build.


### Development

From now on, you can continue using `./runme` for builds.  Alternatively, use
`sbt full-build` to do the build directly through SBT.  The output will show
the individual steps that are running, and you can use them directly as usual
with SBT.  For example, use `sbt "project foo-bar" test` to run the tests of
the `foo-bar` sub-project, or `sbt ~compile` to do a full compilation step
whenever any file changes.

Note that the SBT environment is set up in a way that makes *all* code in
`com.microsoft.ml.spark` available in the Scala console that you get when you
run `sbt console`.  This can be a very useful debugging tool, since you get to
play with your code in an interactive REPL.

Every once in a while the installed libraries will be updated.  In this case,
executing `./runme` will update the libraries, and the next run will do a build
as usual.  If you're using `sbt` directly, it will warn you whenever there was
a change to the library configurations.

Note: the libraries are all installed in `$HOME/lib` with a few
executable symlinks in `$HOME/bin`.  The environment is configured in
`$HOME/.mmlspark_profile` which will be executed whenever a shell starts.
Occasionally, `./runme` will tell you that there was an update to the
`.mmlspark_profile` file --- when this happens, you can start a new shell
to get the updated version, but you can also apply the changes to your
running shell with `. ~/.mmlspark_profile` which will evaluate its
contents and save a shell restart.


## Adding a Module

To add a new module, create a directory with an appropriate name, and in the
new directory create a `build.sbt` file.  The contents of `build.sbt` is
optional, and can be completely empty: its presence will make the build include
your directory as a sub-project which gets included in SBT work.

You can put the usual SBT customizations in your `build.sbt`, for example:

    version := "1.0"
    name := "A Useful Module"

In addition, there are a few utilities in `Extras` that can be useful to
specify some things.  Currently, there is only one such utility:

    Extras.noJar

putting this in your `build.sbt` indicates that no `.jar` file should be
created for your sub-project in the `package` step.  (Useful, for example, for
build tools and test-only directories.)

Finally, whenever SBT runs it generates an `autogen.sbt` file that specifies
the sub-projects.  This file is generated automatically so there is no need to
edit a central file when you add a module, and therefore customizing what
appears in it is done via "meta comments" in your `build.sbt`.  This is
currently used to specify dependencies for your sub-project --- in most cases
you will want to add this:

    //> DependsOn: core

to use the shared code in the `common` sub-project.
