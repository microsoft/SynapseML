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
GURL="$(api "pulls/$BUILDPR" - '.html_url')"
