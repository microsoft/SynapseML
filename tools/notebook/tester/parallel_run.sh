#!/usr/bin/env bash

# Use this script to run the tests in N parallel processes, where
# notebooks are split among them.  It would have been better to combine
# python's unittest with multiprocessing (create a process pool, use
# p.map), but that will be more work, especially for dealing with the
# xml output.

# Arguments: proc_num py_file [args...]
proc_num="$1"; shift
py_file="$1";  shift

prefix_lines() { # pfx
  local line
  while read -r line; do printf "%s| %s\n" "$1" "${line%$'\r'}"; done
}

onerun() { # id [args...]
  local id="$1"; shift
  # dump the prefixed output on the correct fd; use script to fake a tty
  # so the python process would show progress.
  PROC_SHARD="$id" \
      script -qefc "$(printf "%q " python "$py_file" "$@")" /dev/null \
      1> >(prefix_lines "$id") 2> >(prefix_lines "$id" 1>&2)
}

procs=()
for ((i=1; i <= proc_num; i++)); do onerun "$i/$proc_num" "$@" & procs+=($!); done

status=0
for p in "${procs[@]}"; do wait "$p" || status="$?"; done
exit $status
