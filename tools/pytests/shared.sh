# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

_pytest() {
  local ret=0 tmperr="/tmp/pytest-stderr-$$" line
  # capture stderr and show it on failure, because it looks like log4j is using
  # stderr directly, which bypasses unittest's capture of stderr
  TEST_RESULTS="$TEST_RESULTS" \
  "$TOOLSDIR/bin/mml-exec" spark-submit "$@" 2> "$tmperr" || {
    ret=$?
    echo "Standard error for the above failure:"
    cat "$tmperr" | while read -r line; do printf "  | %s\n" "$line"; done
  }
  rm -f "$tmperr"
  return $ret
}
