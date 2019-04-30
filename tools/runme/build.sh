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

_from_keyvault(){ # secretName, variableName
  local secret="$(__ az keyvault secret show --vault-name mmlspark-keys --name $1)"
  secret="${secret##*\"value\": \"}"; secret="${secret%%\"*}"
  export $2="$secret"
}

_sbt_run() { # sbt-args...
  if [[ "$BUILDMODE" = "server" ]]; then
    _from_keyvault powerbi-url MML_POWERBI_URL
    _from_keyvault face-api-key FACE_API_KEY
    _from_keyvault text-api-key TEXT_API_KEY
    _from_keyvault vision-api-key VISION_API_KEY
    _from_keyvault bing-image-search-key BING_IMAGE_SEARCH_KEY
    _from_keyvault azure-search-key AZURE_SEARCH_KEY
    _from_keyvault anomaly-api-key ANOMALY_API_KEY
    _from_keyvault speech-api-key SPEECH_API_KEY
  fi

  local flags=""; if [[ "$BUILDMODE" = "server" ]]; then flags="-no-colors"; fi
  (set -o pipefail; export BUILD_ARTIFACTS TEST_RESULTS
   _ sbt $flags "$@" < /dev/null 2>&1 | _postprocess_sbt_log) \
    || exit $?
}

_sbt_build() {
  show section "Running SBT Build"
  local owd="$PWD"
  cd "$SRCDIR"
  local rmjars=( **/"target/scala-"*/!(*"-$MML_VERSION"@(|-*))".jar" )
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

_sbt_adb_notebooks() {
  show section "Running ADB Notebooks"
  if [[ "$BUILDMODE" = "server" ]]; then
    _from_keyvault adb-token MML_ADB_TOKEN
  fi

  local owd="$PWD"
  cd "$SRCDIR"
  _sbt_run "core-test-fuzzing/it:test"
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
  for mode in "" "gpu"; do
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
  cd "$BUILD_ARTIFACTS/../"
  zip -r BuildArtifacts.zip "$BUILD_ARTIFACTS/"
  echo "##vso[artifact.upload artifactname=BuildArtifacts]$(pwd)/BuildArtifacts.zip"
}

_upload_artifacts_to_storage() {
  show section "Uploading Build Artifacts to Storage"
  local tmp="/tmp/mmlbuild-$$" # temporary place for uploads
  mkdir -p "$tmp"
  ( cd "$BUILD_ARTIFACTS"
    _ zip -qr9 "$tmp/$(basename "$BUILD_ARTIFACTS.zip")" * )
  local f txt target
  local varlinerx="^(.*)# +<=<= .*? =>=>(.*)\$"
  for f in "$TOOLSDIR/"{hdi,deployment}"/"*; do
    target="$tmp/$(basename "$f")"
    if [[ -e "$target" ]]; then
      failwith "duplicate file intended for $STORAGE_CONTAINER: $(basename "$f")";
    fi
    txt="$(< "$f")"
    if [[ "$txt" =~ $varlinerx ]]; then
      local sfx="${target##*.}"
      txt="${BASH_REMATCH[1]}$(_show_gen_vars "$sfx")${BASH_REMATCH[2]}"
    fi
    # might be useful to allow <{...}> substitutions: _replace_var_substs txt
    echo "$txt" > "$target"
  done
  _ azblob upload-batch --source "$tmp" --destination "$STORAGE_CONTAINER/$MML_VERSION"
  _rm "$tmp"
  _add_to_description \
    '* **HDInsight**: Copy the link to %s to setup this build on a cluster.\n' \
    "[this Script Action]($STORAGE_URL/$MML_VERSION/install-mmlspark.sh)"
  portal_link() {
    local url="$STORAGE_URL/$MML_VERSION/$1"
    url="${url//\//%2F}"; url="${url//:/%3A}"; url="${url//+/%2B}"
    url="https://portal.azure.com/#create/Microsoft.Template/uri/$url"
    printf "([in the portal](%s))" "$url"
  }
  _add_to_description \
    '* **ARM Template**: Use %s to setup a cluster with a gpu %s.\n' \
    "[this ARM Template]($STORAGE_URL/$MML_VERSION/deploy-main-template.json)" \
    "$(portal_link "deploy-main-template.json")"
  _add_to_description \
    '  - **HDI Sub-Template**: Use the %s for just the hdi deployment %s.\n' \
    "[HDI sub-template]($STORAGE_URL/$MML_VERSION/spark-cluster-template.json)" \
    "$(portal_link "spark-cluster-template.json")"
  _add_to_description \
    '  - **GPU VM Sub-Template**: Use the %s for just the gpu vm deployment %s.\n' \
    "[GPU sub-template]($STORAGE_URL/$MML_VERSION/gpu-vm-template.json)" \
    "$(portal_link "gpu-vm-template.json")"
  _add_to_description \
    '  - **Convenient Deployment Script**: Download %s or %s, create a %s, and run as\n\n%s\n' \
    "[this bash script]($STORAGE_URL/$MML_VERSION/deploy-arm.sh)" \
    "[this powershell script]($STORAGE_URL/$MML_VERSION/deploy-arm.ps1)" \
    "paramers file [based on this template]($STORAGE_URL/$MML_VERSION/deploy-parameters.template)" \
    "        ./deploy-arm.sh ... -p <your-parameters>"
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
  should test e2e        && _sbt_adb_notebooks
  # publish steps that should happen only for successful tests
  should publish docs    && _publish_docs
  should publish docker  && _publish_to_dockerhub
  _upload_artifacts_to_VSTS
  # upload updated Build.md after all of the additions were made
  _publish_description
  # tag with "Publish" if we published everything and had no failures
  if [[ "$PUBLISH" = "all" ]]; then echo "##vso[build.addbuildtag]Publish"; fi
  return 0
}
