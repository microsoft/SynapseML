# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

################################################################################
# Utilities

# ---< defvar [opts] var val >--------------------------------------------------
# Use this to define customize-able variables (no effect if it exists).
# Additional arguments are concatenated (wihtout spaces) to make long value
# settings look nice.  Use "-x" to export the variable, "-p" to resolve the
# value to an absolute path from where we are, "-f" to set the value even if
# it's already set.  You can also use "-d" to define values with delayed
# references to other variables using "...<{var}>..." -- these will be replaced
# at the end of processing the config file.
_delayed_vars=()
defvar() {
  local opts=""; while [[ "x$1" == "x-"* ]]; do opts+="${1:1}"; shift; done
  local var="$1" val v; shift
  if [[ "$opts" == *"f"* || -z "${!var+x}" ]]; then
    val=""; for v; do val+="$v"; done; printf -v "$var" "%s" "$val"; fi
  if [[ "$opts" == *"p"* && "x${!var}" != "/"* ]]; then
    printf -v "$var" "%s" "$(realpath -m "${!var}")"; fi
  if [[ "$opts" == *"x"* ]]; then export "$var"; fi
  if [[ "$opts" == *"d"* ]]; then _delayed_vars+=( "$var" ); fi
}
_replace_delayed_vars() {
  local var val pfx sfx change=1
  for var in "${_delayed_vars[@]}"; do
    val="${!var}"
    while [[ "$val" = *"<{"*"}>"* ]]; do
      pfx="${val%%"<{"*}"; val="${val#*"<{"}"
      sfx="${val#*"}>"}"; val="${val%%"}>"*}"
      val="$pfx${!val}$sfx"
      printf -v "$var" "%s" "$val"
    done
  done
}

# Parse `X=Y` arguments, stop at a "--"
while [[ "$#" -gt 0 ]]; do case "$1" in
  ( *"="* ) defvar -f "${1%%=*}" "${1#*=}" ;;
  ( "--"  ) shift; break ;;
  ( "-h" | "--help" | "help" )
    text="$(<"$BASEDIR/runme")"
    text="${text#*$'\n'+(#)$'\n# '}"; text="${text%$'\n'+(#)$'\n'*}"
    text="${text//$'\n'#?( )/$'\n'}"
    echo "$text"
    exit
    ;;
  ( * ) echo "WARNING: ignoring unrecognized argument \"$1\"" 1>&2; sleep 1 ;;
esac; shift; done

# ---< @ bash-file arg... >-----------------------------------------------------
# Similar to `script` for loading a bash library file, except that the path is
# relative to the file that used `@`.
@ () {
  local lib="$1" srcdir="$(dirname ${BASH_SOURCE[1]})"; shift
  lib="$(cd "$srcdir"; realpath "$lib")"
  if [[ ! -r "$lib" ]]; then failwith "lib: file not found, $lib"; fi
  . "$lib" "$@"
}

# VSTS:
# Details on available environment variables:
#   https://www.visualstudio.com/en-us/docs/build/define/variables
# to color output, start an output line with "##[<tag>]", these are
# known tags: section, command, error, warning, debug; also, there are
# various meta "##vso[...]" instructions, see:
#   https://github.com/Microsoft/vsts-tasks/blob/master/docs/authoring/commands.md

# ---< show tag message... >----------------------------------------------------
# Display a message type classified by the given tag, in a way that is proper
# for the build context: on the build server use VSO magic outputs.  Accepted
# tags are: "section", "warning", "command", "debug", "error", or "-" for
# generic output.  $hide_in_log can be set to a string holding sensitive
# information that should be hidden in the output.  The display uses "$HOME"
# instead of the actual value whenever it appears.
first_show="Y"
hide_in_log=""
show() {
  local tag="$1"; shift
  if [[ "$first_show" = "Y" ]]; then first_show="N";
  elif [[ "x$tag" = "xsection" ]]; then echo ""
  fi
  if [[ "x$tag" = "x-" ]]; then tag=""
  elif [[ "$BUILDMODE" = "server" ]]; then tag="##[$tag]"
  else case "$tag" in
    ( "section" ) tag="===>>> " ;;
    ( "warning" ) tag="*** "    ;;
    ( "command" ) tag="$ "      ;;
    ( "debug"   ) tag=">> "     ;;
    ( "error"   ) tag="!!! "    ;;
    ( * ) failwith "this script is broken, don't know about display tag: $tag" ;;
  esac; fi
  local line="$tag${*//"$HOME"/\$HOME}"
  if [[ "$hide_in_log" != "" ]]; then
    line="$tag${*//"$hide_in_log"/[...]}"
  fi
  echo "$line"
}

# ---< failwith message... >----------------------------------------------------
# Abort the run with the given error message.
failwith() { show error "Error: $*" 1>&2; exit 1; }

_killed_handler() { echo ""; failwith "Aborting..."; }
builtin trap _killed_handler 2 3 9 15

# ---< map cmd arg... >---------------------------------------------------------
# Apply $cmd on each of the arguments.
map() { local cmd="$1" arg; shift; for arg; do $cmd "$arg"; done; }

# ---< echo_exit message... >---------------------------------------------------
# Echo a message on exit.
_exit_strings=()
_show_exit_strings() { map echo "${_exit_strings[@]}"; }
trap _show_exit_strings 0
echo_exit() { _exit_strings+=("$*"); }

# protection from mistakingly overwriting traps in scriplets
trap() { failwith "cannot overwrite traps (in \"trap $*\")"; }

# ---< qstr [-not-dollar] str... >----------------------------------------------
# Quotes the input as a shell-parsable string, also using $HOME instead of its
# value (better than printf with "%q" which tends to uglingly backslash spaces).
# If "-not-dollar" then avoid quoting dollar signs.
qstr() {
  local replace='\ " ` $'
  if [[ "x$1" = "x-not-dollar" ]]; then replace='\ " `'; shift; fi
  local str="$*" ch
  for ch in $replace; do str="${str//"$ch"/\\$ch}"; done
  echo "\"${str//$HOME/\$HOME}\""
}

# ---< maybe_qstr str... >------------------------------------------------------
# Quotes the input as a shell-parsable string (using qstr) only if needed.
maybe_qstr() {
  local str="$*"
  if [[ "$(printf "%q" "$str")" = "$str" ]]; then echo "$str"; else qstr "$str"; fi
}

# ---< _ [flags] cmd arg... >---------------------------------------------------
# Run the given $cmd very carefuly.  Exit on error, unless flags have "-a".
# Normally, the command is shown (using "show command") unless flags have "-q".
# If $collect_log is set to 1 then instead of showing the command's stdout it is
# captured in $collected_logs (which can also be used to suppress showing the
# output), or set it to 2 to capture both stdout and stderr (this is better than
# redirecting to /dev/null since that will swallow failure messages as well).
collect_log=0 collected_log=""
declare -A known_exes
_() {
  local verbose=1 abortonfail=1
  while [[ "x$1" = "x-"* ]]; do
    case "${1#-}" in
      ( "q" ) verbose=0 ;;
      ( "a" ) abortonfail=0 ;;
      ( * ) failwith "internal error, unknown flag for '_': $1"
    esac
    shift
  done
  local sets=()
  while [[ "$1" =~ ^[A-Za-z_][A-Za-z_0-9]*= ]]; do sets+=( "$1" ); shift; done
  local cmd="$1"; shift
  local exe="${known_exes[$cmd]}"
  if [[ "$exe" = "" ]]; then
    exe="$(type -p "$cmd")"
    if [[ "$exe" = "" && "$(type -t "$cmd")" != "" ]]; then exe="$cmd"; fi
    if [[ "$exe" = "" ]]; then failwith "could not find executable: $cmd"; fi
    known_exes[$cmd]="$exe"
  fi
  if ((verbose)); then
    local to_show="" x
    for x in "${sets[@]}"; do to_show+=" ${x%%=*}=$(maybe_qstr "${x#*=}")"; done
    for x in "$cmd" "$@"; do to_show+=" $(maybe_qstr "$x")"; done
    show command "${to_show:1}"
  fi
  args=( "$@" )
  __run_it__() {
    case $collect_log in
      ( 2 ) collected_log="$("$exe" "${args[@]}" 2>&1)" ;;
      ( 1 ) collected_log="$("$exe" "${args[@]}")"      ;;
      ( * )                  "$exe" "${args[@]}"        ;;
    esac
  }
  if [[ "${#sets[@]}" = 0 ]]; then __run_it__
  else # can't put "x=y"s in a variable, so use eval
    local pfx=""
    for x in "${sets[@]}"; do pfx+="${x%%=*}=$(printf "%q" "${x#*=}") "; done
    eval "${pfx}__run_it__"
  fi
  local ret=$?
  if [[ $ret != 0 && $abortonfail -ge 1 ]]; then failwith "failure when running $cmd $*"
  else return $ret; fi
}

# ---< __ cmd arg... >----------------------------------------------------------
# Convenient shorthand for "_ -q cmd arg..."
__()  { _ -q "$@"; } # same, but no command display

# ---< ___ cmd arg... >---------------------------------------------------------
# Convenient shorthand for "_ -q -a cmd arg..."
___() { _ -q -a "$@"; } # same, but command display or aborting on failure

# ---< _rm path... >------------------------------------------------------------
# Removes a file or directory if it exists.
_rm_() {
  if [[ -d "$1" ]]; then _ rm -rf "$1"; elif [[ -e "$1" ]]; then _ rm -f "$1"; fi;
}
_rm() { map _rm_ "$@"; }

# ---< _md dir >----------------------------------------------------------------
# Create a directory (with -p) if it doesn't exist.
_md_() { if [[ ! -d "$1" ]]; then _ mkdir -p "$1"; fi; }
_md() { map _md_ "$@"; }

# ---< _mcd dir >---------------------------------------------------------------
# Create a directory (with -p) and cd into it.
_mcd() { _md "$1"; _ cd "$1"; }

# ---< _rmcd dir >--------------------------------------------------------------
# Same as _mcd, removing the directory if it exists.
_rmcd() { _rm "$1"; _mcd "$1"; }

# ---< get_suffix path >--------------------------------------------------------
# Prints the suffix of a given path.  Properly deal with filenames that begin
# with a "." and with multiple suffixes (like ".tar.gz"); suffixes are
# alphanumeric with at least one alphabetic character.
get_suffix() {
  rx="[^/](([.][a-zA-Z0-9_]*[a-zA-Z][a-zA-Z0-9_]*)+)$"
  if [[ "$1" =~ $rx ]]; then echo "${BASH_REMATCH[1]:1}"; fi
}

# ---< call_ifdef [_] fun arg... >----------------------------------------------
# If the named function exists, calls it with the given arguments.  Calls only
# functions, not external executables or builtins.
call_ifdef() {
  local pfx=""; if [[ "$1" = "_" ]]; then pfx="_"; shift; fi
  local fun="$1"; shift
  if [[ "$(type -t "$fun")" = "function" ]]; then $pfx "$fun" "$@"; fi
}

# ---< deftag tag [supertag] >--------------------------------------------------
# Define a tag, possibly as a subtag of supertag (which defaults to `all`).
# Note that tags are shared for both `$TESTS` and `$PUBLISH`.
declare -A _tag_parent
deftag() {
  if [[ -n "$2" && "$2" != "all" && -z "${_tag_parent[$2]}" ]]; then
    failwith "deftag: unknown parent tag, $2"
  fi
  _tag_parent[$1]="${2:-all}"
}

# ---< should what tag... >-----------------------------------------------------
# Returns a zero (success) status if `tag` should be "what"-ed (tested or
# published) according to $TESTS or $PUBLISH, or one (failure) status otherwise.
# If more than one tag is given, follow scalatest semantics: succeed if at least
# one of the tags is included, and none are excluded.  Convenient to use as:
# `should test foo && run_foo_test`.
_get_tag_value() {
  local ret="${info[$1]}"
  if [[ "$ret" = "" ]]; then
    if [[ -z "${_tag_parent[$1]}" ]]; then ret="."
    else ret="$(_get_tag_value "${_tag_parent[$1]}")"; fi
    info[$1]=$ret
  fi
  echo $ret
}
_get_valid_tag_value() {
  if [[ "$1" != "all" && "$1" != "none" && -z "${_tag_parent[$1]}" ]]; then
    failwith "should: unknown tag, $1  {{${_tag_parent[$1]}}}"
  fi
  _get_tag_value "$1"
}
_has_tag() {
  local -n info="$1"; shift
  if (($# == 0)); then failwith "should: missing tag(s)"; fi
  # mimic the scalatest logic: needs one tag included and none excluded
  local r="" t; for t; do r+="$(_get_valid_tag_value "$t")"; done
  [[ "$r" = *1* && "$r" != *0* ]]
}
should() {
  what="$1"; shift
  case "$what" in
    ( "test" | "publish" ) _has_tag "_${what}_info" "$@" ;;
    ( * ) failwith "should: unknown tag info, $what" ;;
  esac
}

# ---< get_install_info libname key >-------------------------------------------
# Print the value for $key in the setup section of $libname.  Properly deals
# with various default values.
get_install_info() {
  local ret="${_install_info[$1.$2]}"
  if [[ "$ret" = "" ]]; then
    case "$2" in
      ( "lib"    ) ret="${1,,}" ;;
      ( "envvar" ) ret="${1^^}" ;;
      ( "bindir" ) ret="bin"    ;;
    esac
    if [[ "$ret" != "" ]]; then
      _install_info[$1.$2]="$ret"; _replace_ver_in_info $1.$2
    fi
  fi
  echo "$ret"
}

# ---< set_install_info_vars libname key... >-----------------------------------
# Get the value for each $key in the setup section of $libname, and set the
# variable whose name is $key to this value.
set_install_info_vars() {
  local libname="$1" var val; shift
  for var; do
    printf -v "$var" "%s" "$(get_install_info "$libname" "$var")"
  done
}

# ---< env_eval str... >--------------------------------------------------------
# Evaluate an expression and make sure that it's also included in the
# user's environment.  The commands are held in $envinit_commands which
# can be added to if you want to include something in the environment
# but not evaluate it right now.  (This is written out by code in "install.sh".)
envinit_commands=('export MMLSPARK_PROFILE="yes"')
envinit_eval() { envinit_commands+=("$*"); eval "$*"; }

# ---< setenv var val >---------------------------------------------------------
# Set an environment variable; include the setting in the user environment too.
setenv() { envinit_eval "export $1=$(qstr "$2")"; }

# ---< get_runtime_hash >-------------------------------------------------------
# Prints out a hash of the currently configured runtime environment.  The hash
# depends on the relevant bits of configuration, including a .setup and .init
# function definitions, if any.
get_runtime_hash() {
  local hash="$(
    for libname in "${install_packages[@]}"; do
      set_install_info_vars "$libname" lib sha256 instcmd exes where
      if [[ " $where " != *" runtime "* ]]; then continue; fi
      printf "%s\n" "$libname" "$lib" "$sha256" "$instcmd" "$exes" \
             "$(declare -f "$libname.setup" "$libname.init")"
    done | sha256sum)"
  echo "${hash%% *}"
}

# ---< azblob verb arg... >-----------------------------------------------------
# Same as "az storage blob <verb> --account-name $MAIN_STORAGE arg..."
azblob() {
  local verb="$1"; shift
  az storage blob "$verb" --account-name "$MAIN_STORAGE" "$@"
}

# ------------------------------------------------------------------------------
# Internal functions follow

# Parse tag specs, used for $TESTS
_parse_tags() {
  local -n tags="$1" info="$2"
  tags="${tags,,}"; tags="${tags// /,}"
  while [[ "$tags" != "${tags//,,/,}" ]]; do tags="${tags//,,/,}"; done
  tags="${tags#,}"; tags="${tags%,}"; tags=",$tags"
  while [[ "$tags" =~ (.*)","([^+-].*) ]]; do # just "tag" is the same as "+tag"
    tags="${BASH_REMATCH[1]},+${BASH_REMATCH[2]}"; done
  tags="${tags#,}"
  if [[ "$tags," =~ [+-], ]]; then
    failwith "empty tag in \$$1"
  elif [[ "$tags" =~ [+-]([a-zA-Z0-9_]*[^a-zA-Z0-9_,][^,]*) ]]; then
    failwith "bad \$$1 tag name: ${BASH_REMATCH[1]}"
  fi
  local t pos=0 ts="$tags"
  ts="${ts//,/ }" ts="${ts//+/1}"; ts="${ts//-/0}"
  for t in $ts; do [[ ${t:0:1} = 1 ]] && pos=1; info[${t:1}]=${t:0:1}; done
  if ((!pos)); then info[all]=${info[all]:-1}; fi # no positives => all
  if [[ "$tags" == "+"@("all"|"none") ]]; then tags="${tags:1}"; fi
}
declare -A _test_info _publish_info
_parse_TESTS()   { _parse_tags TESTS   _test_info;    }
_parse_PUBLISH() { _parse_tags PUBLISH _publish_info; }

# Defines $MML_VERSION and $MML_BUILD_INFO
_used_preexisting_version=0
_set_build_info() {
  local info version is_latest
  # make it possible to avoid running git
  if [[ -n "$MML_BUILD_INFO" && -n "$MML_VERSION" && -n "$MML_LATEST" ]]; then
    info="$MML_BUILD_INFO"; version="$MML_VERSION"; is_latest="$MML_LATEST"
  else
    local owd="$PWD"; cd "$BASEDIR"
    # sanity checks for version tags
    local t rx="(0|[1-9][0-9]*)"; rx="^v$rx[.]$rx([.][1-9][0-9]*)?$"
    for t in $(git tag -l); do
      if [[ ! "$t" =~ $rx ]]; then failwith "found a bad tag name \"$t\""; fi
    done
    # collect git information that is used for both MML_VERSION and MML_LATEST
    local tag="$(git describe --abbrev=0)"
    local head="$(git rev-parse HEAD)"
    # (note: prefer origin/master since VSTS doesn't update master)
    local branch="$(git merge-base HEAD refs/remotes/origin/master 2> /dev/null \
                    || git merge-base HEAD refs/heads/master)"
    local tagref="$(git rev-parse "refs/tags/$tag^{commit}")"
    # MML_VERSION
    if [[ -r "$BUILD_ARTIFACTS/version" ]]; then
      # if there is a built version, use it, so that we don't get a new version
      # after commits are made (but it'll get reset in ./runme since it
      # recreates the $BUILD_ARTIFACTS directory)
      _used_preexisting_version=1
      version="$(< "$BUILD_ARTIFACTS/version")"
    else
      # generate a version string (that works for pip wheels too) as follows:
      # 1. main version, taken from the most recent version tag
      #    (that's all if we're building this tagged version)
      version="${tag#v}"
      # 2. ".dev" + number of commits on master since the tag, unless
      #    we're right on the tag
      if [[ "$tagref" != "$head" ]]; then
        version+=".dev$(git rev-list --count "$tag..$branch")"
      fi
      # 3. if building a branch or building locally:
      #    "+" + number of commits on top of master ".g" + abbreviated sha1
      if [[ "$branch" != "$head" || "$BUILDMODE" != "server" ]]; then
        version+="+$(git rev-list --count "$branch..$head")"
        version+=".g$(git rev-parse --short "$head")"
      fi
      # 4. ".local" for local builds, ".dirty" for a dirty tree
      if [[ "$BUILDMODE" != "server" ]]; then version+=".local"; fi
      if ! git diff-index --quiet HEAD --; then version+=".dirty"; fi
    fi
    # MML_BUILD_INFO
    if [[ "$BUILDMODE" != "server" || "$AGENT_ID" = "" ]]; then
      info="Local build: ${USERNAME:-$USER} ${BASEDIR:-$PWD}"
      local line
      info+="$(
        git branch --no-color -vv --contains HEAD --merged | \
          while read line; do
            if [[ "x$line" = "x*"* ]]; then
              line="${line#"* "}";
              if [[ "$line" = *\[*\]* ]]; then line="${line%%\]*}]"; fi
              if [[ "$line" = *\(*\)* ]]; then line="${line%%)*})"; fi
              echo "//$line"; fi; done)"
      if ! git diff-index --quiet HEAD; then info+=" (dirty)"; fi
    else
      local branch="${BUILD_SOURCEBRANCH#refs/heads/}"
      # drop the commit sha1 for builds that are on the main line
      if [[ "$BUILDPR:$branch" = ":master" && ! -r "$BUILD_ARTIFACTS/version" ]]; then
        version="${version%+g[0-9a-f][0-9a-f]*}"
      fi
      info="$BUILD_REPOSITORY_NAME/$branch@${BUILD_SOURCEVERSION:0:8}"
      info+="; $BUILD_DEFINITIONNAME#$BUILD_BUILDNUMBER"
    fi
    info="$version: $info"
    # MML_LATEST
    # "yes" when building an exact version which is the latest on origin/master
    local latest="$(git describe --abbrev=0 "$branch")"
    if [[ "$version" = "${tag#v}" && "$tag" = "$latest" ]]
    then is_latest="yes"; else is_latest="no"; fi
    #
    cd "$owd"
  fi
  defvar -x MML_VERSION    "$version"
  defvar -x MML_BUILD_INFO "$info"
  defvar    MML_LATEST     "$is_latest"
}
# To be called when re-creating $BUILD_ARTIFACTS
_reset_build_info() {
  if ((!_used_preexisting_version)); then return; fi
  unset MML_BUILD_INFO MML_VERSION MML_LATEST; _used_preexisting_version=0
  _set_build_info
}

# Parse $INSTALLATIONS info
declare -A _install_info
install_packages=()
_parse_install_info() {
  local key="" libname="" x keys=1
  for x in "${INSTALLATIONS[@]}"; do
    if [[ "$key" != "" ]];     then _install_info[${libname}.${key%:}]="$x" key=""
    elif [[ "$x" = *: ]];      then key="$x" keys=1
    elif [[ "$x" != [A-Z]* ]]; then failwith "bad package name: $x"
    elif ((!keys)); then failwith "install entry with no keys: $libname"
    else libname="$x"; key=""; keys=0; install_packages+=("$x")
    fi
  done
  # replace "<{ver}>"s
  for x in "${!_install_info[@]}"; do _replace_ver_in_info "$x"; done
}
_replace_ver_in_info() { # lib.field
  _install_info[$1]="${_install_info[$1]//"<{ver}>"/"${_install_info[${1%%.*}.ver]}"}"
}

_post_config() {
  _set_build_info
  _parse_install_info
  _parse_TESTS
  _parse_PUBLISH
  _replace_delayed_vars
}
