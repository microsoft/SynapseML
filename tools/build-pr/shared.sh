# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

set -e

cd "$BASEDIR"

# For private builds: <user>/<repo>/<branch>
privatebuildrx="([^a-zA-Z0-9][^a-zA-Z0-9-]*)"
privatebuildrx+="/([a-zA-Z0-9+-]+)/([a-zA-Z0-9/+-]+)"

if [[ "$BUILDPR" = "" ]]; then :
elif [[ "$BUILDPR" =~ $privatebuildrx ]]; then :
elif [[ "$BUILDPR" = *[^0-9]* ]]; then
  echo "ERROR: \$BUILDPR should be a number, got: \"$BUILDPR\"" 1>&2
  exit 1
fi

T=""
_get_T() {
  if [[ -z "$T" ]]; then
    T="$(__ az keyvault secret show --vault-name mmlspark-keys --name github-auth \
         | jq -r ".value" | base64 -d)"
  fi
}

declare -A api_cache
api() {
  local repo="Azure/mmlspark"
  if [[ "$1" = "-r" ]]; then repo="$2"; shift 2; fi
  local call="$1"; shift
  local curlargs=() x use_cache=1 json=""
  while (($# > 0)); do
    x="$1"; shift
    if [[ "$x" = "-" ]]; then break; else use_cache=0; curlargs+=("$x"); fi;
  done
  if ((use_cache)); then json="${api_cache["${repo} ${call} ${curlargs[*]}"]}"; fi
  if [[ -z "$json" ]]; then
    _get_T
    json="$(curl --silent --show-error -H "AUTHORIZATION: bearer ${T#*:}" \
                 "https://api.github.com/repos/$repo/$call" \
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
  if [[ "$BUILDPR" = */*/* ]]; then # private builds
    REPO="${BUILDPR%%/*}"; BUILDPR="${BUILDPR#*/}"; REPO="$REPO/${BUILDPR%%/*}"
    REF="${BUILDPR#*/}"; BUILDPR="0"
    GURL="https://github.com/$REPO/tree/$REF"
    SHA1="$(api -r "$REPO" "git/refs/heads/$REF" - '.object.sha // empty')"
    if [[ -z "$SHA1" ]]; then failwith "no such repo/ref: $REPO/$REF"; fi
  else # plain pr builds
    if [[ "$(api "pulls/$BUILDPR" - '.state')" != "open" ]]; then
      failwith "PR#$BUILDPR is not open"
    fi
    SHA1="$(api "pulls/$BUILDPR" - '.head.sha // empty')"
    if [[ -z "$SHA1" ]]; then failwith "no such PR: $BUILDPR"; fi
    REPO="$(api "pulls/$BUILDPR" - '.head.repo.full_name // empty')"
    REF="$( api "pulls/$BUILDPR" - '.head.ref // empty')"
    GURL="$(api "pulls/$BUILDPR" - '.html_url // empty')"
  fi
}

# post a status, only if we're running all tests
post_status() { # state text
  if [[ "$BUILDPR" = "0" ]]; then return; fi
  if [[ "$TESTS" != "all" ]]; then return; fi
  local status='"context":"build-pr","state":"'"$1"'","target_url":"'"$VURL"'"'
  api "statuses/$SHA1" -d '{'"$status"',"description":'"$(jsonq "$2")"'}' > /dev/null
}

# post a comment with the given text and link to the build; remember its id
post_comment() { # text [more-text...]
  if [[ "$BUILDPR" = "0" ]]; then return; fi
  local text="[$1]($VURL)"; shift; local more_text="$*"
  if [[ "$more_text" != "" ]]; then text+=$'\n\n'"$more_text"; fi
  api "issues/$BUILDPR/comments" -d '{"body":'"$(jsonq "$text")"'}' - '.id' \
      > "$PRDIR/comment-id"
}

# delete the last posted comment
delete_comment() {
  if [[ "$BUILDPR" = "0" ]]; then return; fi
  if [[ ! -r "$PRDIR/comment-id" ]]; then return; fi
  api "issues/comments/$(< "$PRDIR/comment-id")" -X DELETE
}
