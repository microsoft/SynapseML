# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

################################################################################
# Environment Configuration
# (See the `defvar` documentation in "utils.sh" too.)

# Make it possible to have a local installation by setting HOME
defvar -xp HOME; mkdir -p "$HOME"

# Definition of things that need to be installed.  Each one is followed by misc
# settings, where some of the settings can be computed from others.  The used
# settings are:
# * ver: The version of the library.  This version can be used in other settings
#   by using "<{ver}>", it is also available in the `.setup` and `.init` hooks
#   as "$ver".
# Note: actually all keys can be used with "<{key}>" potentially with bash-like
# variable substitutions (similar to `defvar -d`).
# * lib: The name of the directory (in ~/lib) to install to, defaults to the
#   library name in lowercase.
# * envvar: An environment variable prefix to set to the library's version and
#   installation directory.  Defaults to the library name in uppercase.  If this
#   is "FOO", then the two variables set are $FOO_VERSION and $FOO_HOME.
# * url: The installer URL.
# * sha256: The expected sha256 of the installer file.
# * instcmd: The installation command for an `sh` installer (should have `"$1"`
#   somewhere for the installer file or more likely `bash "$1"`, will be
#   `eval`uated).  This must be set for sh installers.
# * exes: Executables to symlink to ~/bin.
# * vers: Version info in a format of "cmd|pattern" where the `cmd` part is the
#   command to run to get the version (after the library is installed), and the
#   output pattern (usually with "<{ver}>", sometimes can also have shell glob
#   patterns like "*").  The pattern should identify a complete line in the
#   output of `cmd`.
# * bindir: The (relative) sub-directory in which executables are found,
#   defaults to "bin".
# * prereq: Prerequisite information in a format of "cmd|msg", where cmd is a
#   shell command to run (its output will not be shown), and a message to show
#   in case of failure.  The message cannot contain "|"s.  `has_libs` is useful
#   in this context.
# * where: A list of contexts where the library is needed; the contexts are:
#   "devel" for developer installation, "build" for just building (eg, on the
#   build server), "runtime" for libraries that are needed it a user
#   environment.
# In addition, further library-specific setup operations can be put in functions
# named "<library-name>.setup" and "<library-name>.init".  Both are functions
# run after the library is already populated and its envvar is set and it run in
# its directory (can cd elsewhere), but before executable symlinks are made.
# The .setup function is called to perform setup operation after installation,
# and the .init function is always called when runme starts, so it's useful to
# initialize the environment.

# First, the common container definition
defvar MAIN_STORAGE "mmlspark"
defvar MAIN_RESOURCE_GROUP "mmlspark-rg"
# to use the storage directly replace: "azureedge" -> "blob.core.windows"
_main_url() { echo "https://$MAIN_STORAGE.azureedge.net/$1"; }
# The base URL for our installables
defvar INSTALLER_URL "$(_main_url "installers")"
# Directory for caching installers; if it is empty then no caching is used
defvar -p INSTALLER_CACHE_DIR "$HOME/.mmlspark_cache"

INSTALLATIONS=(

  Java ver: "1.8.0" lib: "jdk"
  url:    "https://cdn.azul.com/zulu/bin/zulu8.28.0.1-jdk8.0.163-linux_x64.tar.gz"
  sha256: "85b81652b3fe8cfb0a2cfb835988672bde7844f613dae5b9487b5b44921a1afd"
  exes:   "java javac jar javadoc"
  vers:   "java -version|openjdk version \"<{ver}>_*\""
  where:  "devel runtime build"

  SBT ver: "1.1.1"
  url:    "https://github.com/sbt/sbt/releases/download/v<{ver}>/sbt-<{ver}>.tgz"
  sha256: "8a9072155578f06c861be406e7f9fe989b3770d8da4069dd3cb5ad6c6d25c03b"
  exes:   "sbt"
  vers:   "sbt -no-colors sbtVersion|?info? <{ver}>"
  where:  "devel build"

  Spark ver: "2.4.0"
  url:    "https://archive.apache.org/dist/spark/spark-<{ver}>/spark-<{ver}>-bin-hadoop2.7.tgz"
  sha256: "c93c096c8d64062345b26b34c85127a6848cff95a4bb829333a06b83222a5cfa"
  exes:   "spark-shell spark-sql spark-submit spark-class pyspark sparkR"
  vers:   "spark-shell --version|* version <{ver}>"
  where:  "devel runtime build"

  Conda ver: "4.3.31"
  url:    "https://repo.continuum.io/miniconda/Miniconda3-<{ver}>-Linux-x86_64.sh"
  sha256: "5551f01f436b6409d467412c33e12ecc4f43b5e029290870f8fdeca403c274e6"
  instcmd: 'PYTHONPATH="" bash "$1" -b -f -p "$PWD"'
  exes:   "python python3 ipython ipython3 jupyter conda pip"
  vers:   "PYTHONDONTWRITEBYTECODE=true conda --version|conda <{ver}>"
  where:  "devel runtime build"

  DataSets ver: "2019-05-02"
  url:    "$INSTALLER_URL/datasets-<{ver}>.tgz"
  sha256: "20baed47a4ac790788ab66f491ea7152189e6d54f4272c5e36205b03f375c27e"
  vers:   "cat version|<{ver}>"
  where:  "devel build"

  CNTK ver: "2.4" dashver: "<{ver//./-}>"
  url:    "$INSTALLER_URL/CNTK-<{dashver}>-Linux-64bit-CPU-Only.tar.gz"
  sha256: "2ed7917d426025d7dd722c7d7fda5f55e6bbec7a293a3bfc4cb163c10b4b27f6"
  exes:   "cntk"
  vers:   "cat version.txt|CNTK-<{dashver}>"
  prereq: "has_libs libpng12.so.0 libjasper.so.1|libpng12 and libjasper1 are required"
  bindir: "cntk/bin"
  where:  "devel build"

  DockerBuildx ver: "0.0.1"
  url:  "https://github.com/Microsoft/docker-buildx/archive/v<{ver}>.tar.gz"
  sha256: "bac3d0036224f4279fc553031849c548296cfae432b3212ea21b2089703b290e"
  exes: "docker-buildx"
  vers: "docker-buildx -V|<{ver}>"
  bindir: "."
  where: "devel build"

)

# $TESTS holds the specification of tests to run.  The syntax is a list of
# `tag`, `+tag` or `-tag`, separated by commas and/or spaces; and `tag` is
# equivalent to `+tag`.  The semantics of the specs mimicks the scala semantics
# for tags: we run tests that are tagged with `+tag`s, but not `-tag`s, and if
# there are no `+tag`s then run all tests except for `-tag`s.  `all` and `none`
# behave as you'd expect, but they can have additional benefits (e.g., `none`
# will avoid even compiling the tests); avoid using them with other tags.  The
# default is `+scala,-extended` for local builds, and `all` for server builds.
# The value is normalized to hold comma-separated `+tag` or `-tag`, except for
# a single `all`/`none` which don't get a sign prefix.  $PUBLISH similarly
# holds the specification of things to publish.
defvar -x TESTS   ""
defvar -x PUBLISH ""
if [[ -z "$TESTS" ]]; then
  if [[ "$BUILDMODE" = "server" ]]; then TESTS="all"; else TESTS="+scala,-extended"; fi
fi
if [[ -z "$PUBLISH" ]]; then
  if [[ "$BUILDMODE" = "server" ]]; then PUBLISH="-demo,-docker"; else PUBLISH="none"; fi
fi
# Tag definitions for $TESTS
deftag scala
deftag extended
  deftag buildserver extended
  deftag python extended
  deftag e2e extended
deftag linuxonly
# Tag definitions for $PUBLISH
map deftag storage maven pip r docs demo docker

defvar -p SRCDIR          "$BASEDIR/src"
defvar -p BUILD_ARTIFACTS "$BASEDIR/BuildArtifacts"
defvar -p TEST_RESULTS    "$BASEDIR/TestResults"

# Specific installation functions

SBT.setup() {
  local f="$SRCDIR/project/build.properties" txt="sbt.version=$SBT_VERSION"
  if [[ ! -e "$f" ]]; then echo "$txt" > "$f"; return; fi
  if [[ "$(< "$f")" != "$txt" ]]; then failwith "$f exists with unexpected contents"; fi
}
defvar SCALA_VERSION "2.11"
defvar SCALA_FULL_VERSION "$SCALA_VERSION.8"
SBT.init() {
  defvar -E SCALA_VERSION "$SCALA_VERSION"
  defvar -E SCALA_FULL_VERSION "$SCALA_FULL_VERSION"
}

Spark.setup() {
  if [[ -e "conf/hive-site.xml" ]]; then failwith "conf/hive-site.xml exists"; fi
  { echo "<configuration>"
    echo "  <property>"
    echo "    <name>javax.jdo.option.ConnectionURL</name>"
    echo "    <value>jdbc:derby:memory:databaseName=metastore_db;create=true</value>"
    echo "    <description>the URL of the Derby Server database</description>"
    echo "  </property>"
    echo "  <property>"
    echo "    <name>javax.jdo.option.ConnectionDriverName</name>"
    echo "    <value>org.apache.derby.jdbc.EmbeddedDriver</value>"
    echo "  </property>"
    echo "</configuration>"
  } > "conf/hive-site.xml"
  cd "jars"
  # Patch the Spark jars: add hadoop-azure and azure-storage to make WASB access
  # work.  Ideally, we would just add `hadoop-azure` to the SBT dependencies,
  # but that collides with the hadoop version that comes with Spark (see comment
  # in "src/project/build.scala").  When/if spark is updated for a newer hadoop,
  # then go back to the sbt route.
  local mvn="http://central.maven.org/maven2"
  _curl -O "$mvn/com/microsoft/azure/azure-storage/2.0.0/azure-storage-2.0.0.jar"
  _curl -O "$mvn/org/apache/hadoop/hadoop-azure/2.7.3/hadoop-azure-2.7.3.jar"
}
Spark.init() {
  local f; for f in "python/lib/"*.zip; do
    envinit_eval \
      '[[ ":$PYTHONPATH:" != *":$SPARK_HOME/'"$f"':"* ]]' \
      '&& export PYTHONPATH="$PYTHONPATH:$SPARK_HOME/'"$f"'"'
  done
}

Conda.setup() {
  show section "Installing Conda & Packages"
  _ cp "$TOOLSDIR/mmlspark-packages.spec" .
  # Use `--no-update-deps` to avoid updating everything (including conda &
  # python) to latest versions; and `--no-deps` is to avoid dependencies that we
  # know are not needed, such as QT.
  _ ./bin/conda install --name "root" --no-update-deps --no-deps --yes \
      --quiet --file "mmlspark-packages.spec"
  if [[ "$BUILDMODE" != "runtime" ]]; then
    # xmlrunner: tests; wheel: pip builds; sphinx*, recommonmark: pydoc builds
    ./bin/pip install "xmlrunner" "wheel" "sphinx" "sphinx_rtd_theme" "recommonmark"
  else
    show section "Minimizing conda directory"
    collect_log=2 _ ./bin/conda uninstall -y tk
    collect_log=2 _ ./bin/conda clean -y --all
    _rm "pkgs"
    show command "rm lib/libmkl_....so"
    rm -f lib/libmkl_{,vml_}{def,sequential,cmpt,mc{,2,3},avx512{,_mic}}.so
    show command "rm **/*.pyc"
    rm -rf **/__pycache__/
    rm -f **/*.pyc
    show command "strip **/*.so"
    # note: running this without output and ignore its exit status, so it can
    # fail silently (its stderr is verbose with files it can't strip, and it
    # does return an error)
    strip **/*.so > /dev/null 2>&1
  fi
}

_add_to_ld_library_path() {
  envinit_eval \
    '[[ ":$LD_LIBRARY_PATH:" != *":'"$1"':"* ]]' \
    '&& export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:'"$1"'"'
}
_req_library_so() { # file.so libname
  { /sbin/ldconfig -p | grep -q "$1"; } ||
    failwith "$1 missing, try apt-get install $2"
}
CNTK.init() {
  _req_library_so "libmpi_cxx.so" "libopenmpi1.10"
  _req_library_so "libgomp.so" "libgomp1"
  _add_to_ld_library_path '$CNTK_HOME/cntk/lib'
  _add_to_ld_library_path '$CNTK_HOME/cntk/dependencies/lib'
}

# Storage for build artifacts
defvar STORAGE_CONTAINER "buildartifacts"
defvar -X STORAGE_URL    "$(_main_url "$STORAGE_CONTAINER")"

# Container for docs and maven/pip/r packages
defvar DOCS_CONTAINER    "docs"
defvar DOCS_URL          "$(_main_url "$DOCS_CONTAINER")"
defvar MAVEN_CONTAINER   "maven"
defvar -xX MAVEN_URL     "$(_main_url "$MAVEN_CONTAINER")"
defvar -dX MAVEN_PACKAGE "com.microsoft.ml.spark:mmlspark_$SCALA_VERSION:<{MML_VERSION}>"
defvar PIP_CONTAINER     "pip"
defvar -xX PIP_URL       "$(_main_url "$PIP_CONTAINER")"
defvar -dX PIP_PACKAGE   "mmlspark-<{MML_VERSION}>-py2.py3-none-any.whl"
defvar R_CONTAINER       "rrr"
defvar -xX R_URL         "$(_main_url "$R_CONTAINER")"
defvar -dX R_PACKAGE     "mmlspark-<{MML_VERSION}>.zip"

# Public contact email
defvar -x SUPPORT_EMAIL "mmlspark-support@microsoft.com"

# The following should generally not change

PROFILE_FILE="$HOME/.mmlspark_profile"
CONF_TRACK_FILE="$HOME/.mmlspark_installed_libs"
ENV_INIT_FILES=(".profile" # first: write here if none of these files exist
                ".bash_profile" ".bash_login" ".bashrc" ".zprofile" ".zshrc")
LIB_VERSION_FILE="MMLSPARK_INSTALLED-README.txt"

CURL_FLAGS="-f --location --retry 20 --retry-max-time 60 --connect-timeout 120"
CURL_FLAGS="$CURL_FLAGS --speed-limit 10 --speed-time 120"
if [[ "$BUILDMODE" = "server" ]]; then CURL_FLAGS="$CURL_FLAGS --silent --show-error"
else CURL_FLAGS="$CURL_FLAGS --progress-bar"; fi

envinit_eval '[[ ":$PATH:" != *":$HOME/bin:"* ]] && export PATH="$HOME/bin:$PATH"'
envinit_commands+=(
  'ldpaths="$(ldconfig -v 2> /dev/null | while read -r line; do
    if [[ "$line" = *: ]]; then echo -n "$line"; fi; done)"'
  '[[ ":$LD_LIBRARY_PATH:" != *":$ldpaths"* ]] && export LD_LIBRARY_PATH="$ldpaths$LD_LIBRARY_PATH"'
)
