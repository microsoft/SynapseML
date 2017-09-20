# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

################################################################################
# Environment Installation

inst_work_done=""
_note_work() { # what, as something that can be shown in "_ done"
  # prefer bigger work as the label to keep
  for what in "Install" "Update" "Environment update"; do
    if [[ "$what" = "$1" ]]; then inst_work_done="$1"; fi
    if [[ "$what" = "$inst_work_done" ]]; then return; fi
  done
}

_verify_version() { # [-q] what; prints error if mismatched version
  # "-q" => quick mode: just check that the version file matches
  local quick="N"; if [[ "x$1" = "x-q" ]]; then quick="Y"; shift; fi
  libname="$1"; shift
  local lib vers ver; set_install_info_vars "$libname" lib vers ver
  local dir="$HOME/lib/$lib"
  if [[ ! -d "$dir" ]]; then
    echo "$libname is not installed (missing dir: $dir)"; return
  fi
  local vcmd="" pat1 pat2 actual line ver_file="$dir/$LIB_VERSION_FILE"
  if [[ "$vers" != "" ]]; then vcmd="${vers%%|*}"; vers="${vers#*|}"; fi
  if [[ "$quick" = "Y" && -r "$ver_file" ]]; then
    read -r actual < "$ver_file"
    if [[ "$actual" = "$ver" ]]; then return; fi # look for $ver here!
  fi
  if [[ "$vcmd" = "" ]]; then echo "no version information"; return; fi
  actual="$(cd "$dir"; ___ $vcmd 2>&1)"
  if [[ $'\n'"$actual"$'\n' != *$'\n'$vers$'\n'* ]]; then # $vers can have globs
    printf 'unexpected %s version,\n  wanted:\n  | %s\n  got:\n  | %s' \
           "$libname" "$vers" "${actual//$'\n'/$'\n  | '}"
  fi
}

_do_envinits() {
  cd "$HOME"
  local cmd script="" f="$PROFILE_FILE" add_init="N" orig_profile="$MMLSPARK_PROFILE"
  # create the init script
  for cmd in "${envinit_commands[@]}"; do
    script+="$cmd"$'\n'
    local var="" val
    if [[ "$cmd" = "export "*"="* ]]; then
      var="${cmd#export }"; var="${var%%=*}"; val="$(qstr "${!var}")"
    fi
    # print only commands and var settings that change values
    if [[ "$var" = "" || "$cmd" != "export $var=$val" ]]; then show command "$cmd"; fi
  done
  eval "$script"
  if   [[ ! -e "$f" ]]; then add_init="Y"; show section "Creating $f"
  elif [[ "${script%$'\n'}" != "$(<"$f")" ]]; then show section "Updating $f"
  else return; fi
  _note_work "Environment update"
  show command "...init code... > \"$f\""
  echo -n "$script" > "$f"
  if [[ "$add_init" = "N" ]]; then return; fi
  if [[ "$orig_profile" = "yes" ]]; then
    show warning "There was no $f file, but \$MMLSPARK_PROFILE is set,"
    show warning "so the environment was modified in an unexpected way and"
    show warning "therefore no shell init files are modified."
    return
  fi
  show section "Adding environment initialization"
  local file haveit="N" text fh="$(qstr "$f")"
  local cmd="[[ \"\$MMLSPARK_PROFILE\" != \"\" ]] || . $fh"
  local qcmd="$(qstr "$cmd")"
  for file in "${ENV_INIT_FILES[@]}"; do
    if [[ ! -r "$file" ]]; then continue; fi
    haveit="Y" # either it's there or we're adding it
    text="$(< "$file")"
    if [[ "$text" = *"$cmd"* ]]; then continue; fi
    show command "...added init line... > \"$file\""
    # add it at the top since some init files have `return`s in the middle
    echo "$cmd"$'\n\n'"$text" > "$file"
  done
  if [[ "$haveit" = "N" ]]; then
    show command "echo $qcmd > \"${ENV_INIT_FILES[0]}\""
    echo "$cmd" > "${ENV_INIT_FILES[0]}"
  fi
  show - ""
  show warning "I made your shell initialization load $f, but this"
  show warning "shell is still not initialized.  Enter \"source $f\""
  show warning "to do so, or start a new terminal."
}

_unpack_tgz() {
  _ tar xzf "$1" --strip-components=1
}

_unpack_zip() {
  local restore_opt="$(shopt -p dotglob)"; shopt -s dotglob
  _ unzip -q "$1"
  local paths=( * )
  if [[ "${#paths[@]}" != "1" || ! -d "${paths[0]}" ]]; then
    failwith "empty archive or archive with multiple toplevel directories, $1"
  fi
  show command "mv ${paths[0]}/* .; rmdir ${paths[0]}"
  local tmp="...install-tmp-$$"
  mv "${paths[0]}" "$tmp"
  mv "$tmp"/* .
  rmdir "$tmp"
  $restore_opt
}

_unpack_sh() {
  if [[ "x$instcmd" != "x" ]]; then eval "_ $instcmd"
  else failwith "sh package without instcmd: $1"; fi
}

_retrieve_file() { # url file sha256
  # Retrieve the $url into $file with a cache left in $INSTALLER_CACHE_DIR; the
  # file will actually be a symlink to the cache; if $INSTALLER_CACHE_DIR is
  # empty no cache is used; verify sha256 checksum; only verified files are
  # cached; files in the cache are assumed to be valid.
  local url="$1" target="$2" sha256="$3"; shift 3
  local cache="$INSTALLER_CACHE_DIR/$(basename "$target")"
  if [[ -n "$INSTALLER_CACHE_DIR" && -r "$cache" && -r "$cache.sha256"
        && "$(< "$cache.sha256")" = "$sha256" ]]; then
    _ ln -sf "$cache" "$target"; return
  fi
  _curl --output "$target" "$url"
  local sha256sum="$(__ sha256sum "$target")"; sha256sum="${sha256sum%% *}"
  if [[ "x$sha256sum" = "x" ]]; then failwith "could not get sha256 checksum"; fi
  if [[ "$sha256sum" != "$sha256" ]]; then
    failwith "sha256 checksum failed for $target (retrieved from $url)"
  fi
  if [[ -z "$INSTALLER_CACHE_DIR" ]]; then return; fi
  _md "$INSTALLER_CACHE_DIR"
  _ mv "$target" "$cache"; echo "$sha256" > "$cache.sha256"
  _ ln -s "$cache" "$target"
}

_install() { # libname
  libname="$1"; shift
  local lib envvar url sha256 instcmd exes vers ver bindir prereq where
  set_install_info_vars "$libname" \
        lib envvar url sha256 instcmd exes vers ver bindir prereq where
  if [[ ( "$BUILDMODE" = "server"  && " $where " != *" build "*   )
     || ( "$BUILDMODE" = "runtime" && " $where " != *" runtime "* )
     || (                             " $where " != *" devel "*   ) ]]; then return
  fi
  local dir="$HOME/lib/$lib"
  defvar -E "${envvar}_VERSION" "$ver"
  defvar -xe "${envvar}_HOME"    "$dir"
  if [[ "x$prereq" != "x" ]] && ! eval "${prereq%|*}" > /dev/null 2>&1; then
    failwith "$libname: prerequisite failure: ${prereq##*|}"
  fi
  if [[ "$(_verify_version -q "$libname")" = "" ]]; then
    cd "$dir"; call_ifdef "$libname.init" # can use $ver
    return
  fi
  local update="N" Op; if [[ -r "$dir/$LIB_VERSION_FILE" ]]; then update="Y"; fi
  if [[ "$update" = "Y" ]]; then Op="Updating"; _note_work "Update"
  else Op="Installing"; _note_work "Install"; fi
  # avoid output up to here, so there's nothing unless we actually do something
  show section "$Op $libname v$ver in $dir"
  show command export "${envvar}_VERSION=$ver"
  show command export "${envvar}_HOME=$dir"
  if [[ "$update" = "Y" && "$(_verify_version "$libname")" = "" ]]; then
    show warning "Looks like $libname was already updated, noting new version"
    _ cd "$dir"
  else
    if [[ "$update" = "Y" ]]; then show warning "Removing $dir!"; _rm "$dir"; fi
    if [[ -d "$dir" ]]; then failwith "directory exists, please remove it: $dir"; fi
    local sfx="$(get_suffix "$url")"; if [[ "$sfx" = "tar.gz" ]]; then sfx="tgz"; fi
    local file="/tmp/$lib.$sfx"
    _retrieve_file "$url" "$file" "$sha256"
    _mcd "$dir"
    if [[ "$(type -t _unpack_$sfx)" = "function" ]]; then _unpack_$sfx "$file"
    else failwith "unknown package file suffix: $sfx"; fi
    _rm "$file"
  fi
  map call_ifdef "$libname.setup" "$libname.init" # can use $ver
  if [[ "$setup_function" != "" ]]; then _ "$setup_function"; fi
  show command "...text... > $(qstr "$LIB_VERSION_FILE")"
  { echo "$ver"
    echo ""
    echo "This directory has an installation of $libname v$ver"
    echo "It has been created by the MMLSpark build script: as long as this file"
    echo "exists, the build script is allowed to remove it for version updates"
    echo "when needed.  Please do not modify it."
  } > "$dir/$LIB_VERSION_FILE"
  _ cd "$HOME/bin"
  local exe
  for exe in $exes; do _ ln -sf "../lib/$lib/$bindir/$exe" "$exe"; done
  if [[ "$vers" != "" ]]; then
    show debug "verifying $libname installation version"
    local err="$(_verify_version "$libname")"
    if [[ "$err" != "" ]]; then _rm -rf "$dir"; failwith "$err"; fi
  fi
}

# Main entry point
_install_environment() {
  # Common directories
  _md "$HOME/bin" "$HOME/lib"
  # Installations
  map _install "${install_packages[@]}"
  _ cd "$BASEDIR"
  _rm "$CONF_TRACK_FILE"
  # Set vars and setup environment initialization
  _do_envinits
}
