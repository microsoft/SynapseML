# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

################################################################################
# Build

# Since developers usually work in the IDE, most of the build mechanics is done
# by SBT.

_generate_description() {
  if [[ "$BUILDMODE" != "server" || "$AGENT_ID" = "" ]]; then return; fi
  show section "Generating Build.md"
  show command "... > $(qstr "$BUILD_ARTIFACTS/Build.md")"
  eval echo "\"$(< "$RUNMEDIR/info.tmpl")\"" > "$BUILD_ARTIFACTS/Build.md"
  # upload the generated description lazily on exit, so we can add info lines below
  echo_exit "##vso[task.uploadsummary]$BUILD_ARTIFACTS/Build.md"
}
_add_to_description() { # fmt arg...
  if [[ ! -e "$BUILD_ARTIFACTS/Build.md" ]]; then return; fi
  { echo ""; printf "$@"; } >> "$BUILD_ARTIFACTS/Build.md"
}
_publish_description() {
  # note: this does not depend on "should publish storage"
  if [[ ! -e "$BUILD_ARTIFACTS/Build.md" ]]; then return; fi
  _ azblob upload -c "$STORAGE_CONTAINER" \
                  -f "$BUILD_ARTIFACTS/Build.md" -n "$MML_VERSION/Build.md"
}

_postprocess_sbt_log() {
  # Adapts the SBT output to work nicely with the VSTS build, most of the work
  # is for the SPARK output logs
  local line rx tag text
  local IFS="" # preserve whitespaces
  # Prefix finding regexp
  rx=$'^(\e[[0-9]+m)?\[?(\e[[0-9]+m)??'
  rx+=$'(warning|WARNING|warn|WARN|info|INFO|error|ERROR)'
  rx+=$'(\e[[0-9]+m)?\]?(\e[[0-9]+m)? *(.*)'
  while read -r line || [[ -n "$line" ]]; do
    # Drop time stamps from SPARK output lines
    line="${line#[0-9][0-9]/[0-9][0-9]/[0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9] }"
    # Highlight a prefix of "[warning]" with optional brackets, and the same
    # for "warn"s, "error"s and "info"s (for info, just drop the prefix); do
    # that for uppercase also, but *not* mixed since spark shows a line that
    # starts with "Info provided"
    if [[ "${line}" =~ $rx ]]; then
      tag="${BASH_REMATCH[3],,}"
      if [[ "$tag" = "warn" ]]; then tag="warning"
      elif [[ "$tag" = "info" ]]; then tag="-"
      fi
      # preserve the line (with escape sequences) when in interactive mode
      if [[ "${BUILDMODE}${BASH_REMATCH[1]}" != "server" ]]; then text="$line"
      else text="${BASH_REMATCH[6]}"
      fi
      show "$tag" "$text"
    else
      echo "$line"
    fi
  done
}

_prepare_build_artifacts() {
  show section "Preparing Build"
  _rm "$BUILD_ARTIFACTS" "$TEST_RESULTS"
  _reset_build_info
  _ mkdir -p "$BUILD_ARTIFACTS/sdk" "$TEST_RESULTS"
  _ cp -a "$BASEDIR/LICENSE" "$BUILD_ARTIFACTS"
  _ cp -a "$BASEDIR/LICENSE" "$BUILD_ARTIFACTS/sdk"
  echo "$MML_VERSION" > "$BUILD_ARTIFACTS/version"
  local paths
  # copy only the test notebooks from notebooks/tests to the local test
  # directory -- running all notebooks is covered better by the E2E tests
  for paths in "samples:$BUILD_ARTIFACTS/notebooks" "tests:$TEST_RESULTS/notebook_tests"; do
    _ "$BASEDIR/tools/notebook/postprocess.py" "$BASEDIR/notebooks/${paths%%:*}" "${paths#*:}"
  done
}

_sbt_run() { # sbt-args...
  local flags=""; if [[ "$BUILDMODE" = "server" ]]; then flags="-no-colors"; fi
  # temporary hack around the sbt+bash problem in v1.0.0,
  # (should be fixed in 1.0.3, and then this should be removed)
  local stty_settings=""
  if [[ "$SBT_VERSION" > "1.0.2" ]]; then failwith "Time to remove the stty workaround"
  elif [[ "$BUILDMODE" != "server" ]]; then stty_settings="$(stty -g)"
  fi
  (set -o pipefail; export BUILD_ARTIFACTS TEST_RESULTS
   _ sbt $flags "$@" < /dev/null 2>&1 | _postprocess_sbt_log)
  local ret=$?
  if [[ -n "$stty_settings" ]]; then stty "$stty_settings"; fi
  if ((ret != 0)); then exit $ret; fi
}

_sbt_build() {
  show section "Running SBT Build"
  local owd="$PWD" restore_opt="$(shopt -p nullglob)"; shopt -s nullglob
  cd "$SRCDIR"
  local rmjars=( **/"target/scala-"*/!(*"-$MML_VERSION"@(|-*))".jar" )
  $restore_opt
  if [[ "${#rmjars[@]}" != "0" ]]; then
    show command "rm **/target/...stale-jars"
    __ rm "${rmjars[@]}"
  fi
  local TESTS="$TESTS"
  if ! should test scala; then TESTS="none"
  else # Hide the "+scala" tag
    TESTS=",$TESTS,"; TESTS="${TESTS//,+scala,/,}"; TESTS="${TESTS#,}"; TESTS="${TESTS%,}"
    if [[ "$TESTS" = "" ]]; then TESTS="all"; fi
  fi
  _sbt_run "full-build"
  show section "Sorting assembly jar for the maven repo"
  # leave only the -assembley jars under the proper name (and the pom files)
  local f; for f in "$BUILD_ARTIFACTS/packages/m2/"**; do case "$f" in
    ( *-@(javadoc|sources).jar@(|.md5|.sha1) ) _rm "$f" ;;
    ( *-assembly.jar@(|.md5|.sha1) ) _ mv "$f" "${f//-assembly.jar/.jar}" ;;
  esac; done
  cd "$owd"
}

_upload_package_to_storage() { # name, pkgdir, container
  show section "Publishing $1 Package"
  _ azblob upload-batch --source "$BUILD_ARTIFACTS/packages/$2" --destination "$3"
  case "$1" in
  ( "Maven" )
    _add_to_description '* **Maven** package uploaded, use `%s` and `%s`.\n' \
                        "--packages $MAVEN_PACKAGE" "--repositories $MAVEN_URL"
    ;;
  ( "PIP" )
    _add_to_description '* **PIP** package [uploaded](%s/%s).\n' \
                        "$PIP_URL" "$PIP_PACKAGE"
    ;;
  ( "R" )
    _add_to_description '* **R** package [uploaded](%s/%s).\n' \
                        "$R_URL" "$R_PACKAGE"
    ;;
  esac
}

_e2e_script_action() { # config-name script-name file-name
  declare -n cluster="$1_CLUSTER_NAME" group="$1_RESOURCE_GROUP"; shift
  local script_name="$1" file="$2"; shift 2
  local url="$STORAGE_URL/$MML_VERSION/$file"
  collect_log=1 \
    _ azure hdinsight script-action create "$cluster" -g "$group" \
            -n "$script_name" -u "$url" -t "headnode;workernode"
  echo "$collected_log"
  if [[ ! "$collected_log" =~ "Operation state: "+"Succeeded" ]]; then
    failwith "script action failed"
  fi
}
e2ekey=""
_e2e_ssh() {
  local cmd keyfile rm_pid ret
  cmd=("ssh"); if [[ "$1" = "scp" ]]; then cmd=("$1"); shift; fi
  if [[ "$_e2e_key" = "" ]]; then
    e2ekey="$(__ az keyvault secret show --vault-name mmlspark-keys --name testcluster-ssh-key)"
    e2ekey="${e2ekey##*\"value\": \"}"; e2ekey="${e2ekey%%\"*}"; e2ekey="${e2ekey//\\n/$'\n'}"
  fi
  keyfile="/dev/shm/k$$"; touch "$keyfile"; chmod 600 "$keyfile"; echo "$e2ekey" > "$keyfile"
  cmd+=(-o "StrictHostKeyChecking=no" -i "$keyfile")
  if [[ "${cmd[0]}" = "ssh" ]]; then
    { sleep 30; rm -f "$keyfile"; } &
    rm_pid="$!"
    _ -a "${cmd[@]}" "$@"; ret="$?"
    kill -9 "$rm_pid" > /dev/null 2>&1; rm -f "$keyfile"
  elif [[ "${cmd[0]}" = "scp" ]]; then
    _ -a "${cmd[@]}" "$@"; ret="$?"
    rm -f "$keyfile"
  fi
  return $ret
}
_e2e_tests() {
  show section "Running E2E Tests"
  _e2e_script_action "E2E" "Install MML to E2E Cluster" "install-mmlspark.sh"
  _e2e_script_action "E2E" "Setup authorized-keys for E2E" "setup-test-authkey.sh"
  local shost="$E2E_CLUSTER_SSH" sdir="$CLUSTER_SDK_DIR/notebooks/hdinsight"
  _e2e_ssh scp -p "$TEST_RESULTS/notebook_tests/hdinsight/"* "$shost:$sdir"
  _e2e_ssh scp -p "$BASEDIR/tools/notebook/tester/"* "$shost:$sdir"
  _e2e_ssh -t -t "$shost" \
           ". /usr/bin/anaconda/bin/activate; \
            cd \"$sdir\"; rm -rf \"../local\"; \
            ./parallel_run.sh 2 \"TestNotebooksOnHdi.py\""
  local ret="$?"
  _e2e_ssh scp "$shost:$sdir/TestResults/*" "$TEST_RESULTS"
  if ((ret != 0)); then failwith "E2E test failures"; fi
}

_publish_to_demo_cluster() {
  show section "Installing Demo Cluster"
  _e2e_script_action "DEMO" "Install MML to Demo Cluster" "install-mmlspark.sh"
  _add_to_description '* Demo cluster updated.\n'
}

_publish_docs() {
  @ "../pydocs/publish"
  _add_to_description '* Documentation [uploaded](%s).\n' "$DOCS_URL/$MML_VERSION"
  if [[ "$MML_LATEST" = "yes" ]]; then
    # there is no api for copying to a different path, so re-do the whole thing,
    # but first, delete any paths that are not included in the new contents
    local d f
    for d in "scala" "pyspark"; do
      __ azblob list --container-name "$DOCS_CONTAINER" --prefix "$d/" -o tsv | cut -f 3
    done | while read -r f; do
      if [[ -e "$BUILD_ARTIFACTS/docs/$f" ]]; then continue; fi
      echo -n "deleting $f..."
      if collect_log=1 __ azblob delete --container-name "$DOCS_CONTAINER" -n "$f" > /dev/null
      then echo " done"; else echo " failed"; failwith "deletion of $f failed"; fi
    done
    @ "../pydocs/publish" --top
    _add_to_description '* Also copied as [toplevel documentation](%s).\n' "$DOCS_URL"
  fi
}

_publish_to_dockerhub() {
  @ "../docker/build-docker"
  local auth user pswd
  __ docker logout > /dev/null
  auth="$(__ az keyvault secret show --vault-name mmlspark-keys --name dockerhub-auth)"
  auth="${auth##*\"value\": \"}"; auth="${auth%%\"*}"; auth="${auth%\\n}"
  auth="$(base64 -d <<<"$auth")"
  user="${auth%%:*}" pswd="${auth#*:}"
  ___ docker login -u "$user" -p "$pswd" > /dev/null
  unset user pass auth
  local txt=""
  # mmlspark       -> microsoft/mmlspark:X.Y,       microsoft/mmlspark:latest
  # mmlspark-$mode -> microsoft/mmlspark:$mode-X.Y, microsoft/mmlspark:$mode
  local mode
  for mode in "" "plus" "gpu" "plus-gpu"; do
    local itag="mmlspark${mode:+-}${mode}:latest" otag otags
    otag="microsoft/mmlspark:${mode}${mode:+-}$MML_VERSION"
    otag="${otag//+/_}"; otags=("$otag")
    if [[ "$MML_LATEST" = "yes" ]]; then otags+=("microsoft/mmlspark:${mode:-latest}"); fi
    show section "Pushing to Dockerhub as ${otags[*]}"
    show - "Image info:"
    local info="$(docker images "$itag")"
    if [[ "$info" != *$'\n'* ]]; then failwith "tag not found: $itag"; fi
    echo "  | ${info//$'\n'/$'\n  | '}"
    for otag in "${otags[@]}"; do
      txt+=", \`$otag\`"
      show - "Pushing \"$otag\""
      _ docker tag "$itag" "$otag"
      _ docker push "$otag"
      _ docker rmi "$otag"
    done
  done
  __ docker logout > /dev/null
  _add_to_description '* Docker hub images pushed: %s.\n' "${txt:2}"
}

_upload_artifacts_to_VSTS() {
  if [[ "$BUILDMODE" != "server" ]]; then return; fi
  show section "Uploading Build Artifacts to VSTS"
  local f d
  for f in "$BUILD_ARTIFACTS/"**/*; do
    if [[ -d "$f" ]]; then continue; fi
    f="${f#$BUILD_ARTIFACTS}"; d="${f%/*}"
    echo "##vso[artifact.upload artifactname=Build$d]$BUILD_ARTIFACTS/$f"
  done
}

_upload_artifacts_to_storage() {
  show section "Uploading Build Artifacts to Storage"
  local tmp="/tmp/mmlbuild-$$" # temporary place for uploads
  mkdir -p "$tmp"
  ( cd "$BUILD_ARTIFACTS"
    _ zip -qr9 "$tmp/$(basename "$BUILD_ARTIFACTS.zip")" * )
  local f txt
  local varlinerx="^(.*)# +<=<= .*? =>=>(.*)\$"
  for f in "$TOOLSDIR/hdi/"*; do
    txt="$(< "$f")"
    if [[ "$txt" =~ $varlinerx ]]; then
      txt="${BASH_REMATCH[1]}$(_show_gen_vars)${BASH_REMATCH[2]}"
    fi
    echo "$txt" > "$tmp/$(basename "$f")"
  done
  _ azblob upload-batch --source "$tmp" --destination "$STORAGE_CONTAINER/$MML_VERSION"
  _rm "$tmp"
  _add_to_description \
    '* **HDInsight**: Copy the link to %s to setup this build on a cluster.\n' \
    "[this Script Action]($STORAGE_URL/$MML_VERSION/install-mmlspark.sh)"
}

_full_build() {
  show section "Building ($MML_VERSION)"
  _ cd "$BASEDIR"
  _prepare_build_artifacts
  _generate_description
  _publish_description # publish first version, in case of failures
  _sbt_build
  _ ln -sf "$(realpath --relative-to="$HOME/bin" "$TOOLSDIR/bin/mml-exec")" \
           "$HOME/bin"
  @ "../pydocs/build"
  @ "../pip/generate-pip"
  if [[ "$PUBLISH" != "none" ]]; then
    _ az account show > /dev/null # fail if not logged-in to azure
  fi
  # basic publish steps that happen before testing
  should publish maven && _upload_package_to_storage "Maven" "m2"  "$MAVEN_CONTAINER"
  should publish pip   && _upload_package_to_storage "PIP"   "pip" "$PIP_CONTAINER"
  should publish r     && _upload_package_to_storage "R"     "R"   "$R_CONTAINER"
  should publish storage && _upload_artifacts_to_storage
  # tests
  should test python     && @ "../pytests/auto-tests"
  should test python     && @ "../pytests/notebook-tests"
  should test e2e        && _e2e_tests
  # publish steps that should happen only for successful tests
  should publish docs    && _publish_docs
  should publish demo    && _publish_to_demo_cluster
  should publish docker  && _publish_to_dockerhub
  _upload_artifacts_to_VSTS
  # upload updated Build.md after all of the additions were made
  _publish_description
  # tag with "Publish" if we published everything and had no failures
  if [[ "$PUBLISH" = "all" ]]; then echo "##vso[build.addbuildtag]Publish"; fi
  return 0
}
