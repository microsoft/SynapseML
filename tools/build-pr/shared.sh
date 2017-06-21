# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

set -e

cd "$BASEDIR"

if [[ "$BUILDPR" = "" ]]; then :
elif [[ "$BUILDPR" = *[^0-9]* ]]; then
  echo "ERROR: \$BUILDPR should be a number, got: \"$BUILDPR\"" 1>&2
  exit 1
fi

T=""
_get_T() {
  if [[ "x$T" = "x" ]]; then
    T="$(__ az keyvault secret show --vault-name mmlspark-keys --name github-auth \
         | jq -r ".value" | base64 -d)"
  fi
}

declare -A api_cache
api() {
  local call="$1"; shift
  local curlargs=() x use_cache=1 json=""
  while (($# > 0)); do
    x="$1"; shift
    if [[ "x$x" = "x-" ]]; then break; else use_cache=0; curlargs+=("$x"); fi;
  done
  if ((use_cache)); then json="${api_cache["${call} ${curlargs[*]}"]}"; fi
  if [[ -z "$json" ]]; then
    _get_T
    json="$(curl --silent --show-error -H "AUTHORIZATION: bearer ${T#*:}" \
                 "https://api.github.com/repos/Azure/mmlspark/$call" \
                 "${curlargs[@]}")"
    if ((use_cache)); then api_cache["${call} ${curlargs[*]}"]="$json"; fi
  fi
  if (($# == 0)); then echo "$json"; else jq -r "$@" <<<"$json"; fi
}

jsonq() { # text...; quotes the text as a json string
  jq --null-input --arg txt "$*" '$txt'
}

VURL="${SYSTEM_TASKDEFINITIONSURI%/}/$SYSTEM_TEAMPROJECT"
VURL+="/_build/index?buildId=$BUILD_BUILDID&_a=summary"
GURL="" SHA1="" REPO="" REF=""

get_pr_info() {
  if [[ "$SHA1" != "" ]]; then return; fi
  SHA1="$(api "pulls/$BUILDPR" - '.head.sha')"
  REPO="$(api "pulls/$BUILDPR" - '.head.repo.full_name')"
  REF="$( api "pulls/$BUILDPR" - '.head.ref')"
  GURL="$(api "pulls/$BUILDPR" - '.html_url')"
}

# post a status, only if we're running all tests
post_status() { # state text
  if [[ "$TESTS" != "all" ]]; then return; fi
  get_pr_info
  local status='"context":"build-pr","state":"'"$1"'","target_url":"'"$VURL"'"'
  api "statuses/$SHA1" -d '{'"$status"',"description":'"$(jsonq "$2")"'}' > /dev/null
}

# post a comment with the given text and link to the build; remember its id
post_comment() { # text [more-text...]
  local text="[$1]($VURL)"; shift; local more_text="$*"
  if [[ "$more_text" != "" ]]; then text+=$'\n\n'"$more_text"; fi
  api "issues/$BUILDPR/comments" -d '{"body":'"$(jsonq "$text")"'}' - '.id' \
      > "$PRDIR/comment-id"
}

# delete the last posted comment
delete_comment() {
  if [[ ! -r "$PRDIR/comment-id" ]]; then return; fi
  api "issues/comments/$(< "$PRDIR/comment-id")" -X DELETE
}
