#!/usr/bin/env bash
# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

. "$(dirname "${BASH_SOURCE[0]}")/../../runme"

map deftag a b c
num=0 fail=0

try() {
  local test="$*"
  local flags="${test%% => *}" expect="${test##* => }"
  flags=$(echo $flags)
  local res=$(TESTS="$flags"; unset _test_info; declare -A _test_info
              _parse_TESTS
              should test a && echo A
              should test b && echo B
              should test c && echo C
              should test a b && echo AB)
  res=$(echo { $res })
  ((num++))
  if [[ "$expect" != "$res" ]]; then
    ((fail++))
    echo "FAIL: TEST=\"$flags\": expected $expect, got $res"
  fi
}

report() {
  if ((fail == 0)); then echo "All tests passed"; exit 0
  else echo "$fail/$num tests failed"; exit 1; fi
}

# The following is an exhaustive list of all a/b/c options, verified with
# scalatest.  To try it:
#     import org.scalatest.{FunSuite, Tag}
#     object A extends Tag("a"); object B extends Tag("b"); object C extends Tag("c")
#     class ExampleSpec extends FunSuite {
#       test("A", A) {}; test("B", B) {}; test("C", C) {}; test("AB", A, B) {}
#     }
# and then in sbt use -n for + and -l for -, eg: test-only * -- -n a -n b -l c

try "         => { A B C AB }"
try "+a +b +c => { A B C AB }"
try "+a +b    => { A B AB }"
try "+a +b -c => { A B AB }"
try "      -c => { A B AB }"
try "+a    +c => { A C AB }"
try "   +b +c => { B C AB }"
try "+a       => { A AB }"
try "+a    -c => { A AB }"
try "   +b    => { B AB }"
try "   +b -c => { B AB }"
try "-a       => { B C }"
try "-a +b +c => { B C }"
try "   -b    => { A C }"
try "+a -b +c => { A C }"
try "+a -b    => { A }"
try "   -b -c => { A }"
try "+a -b -c => { A }"
try "-a +b    => { B }"
try "-a    -c => { B }"
try "-a +b -c => { B }"
try "-a -b    => { C }"
try "      +c => { C }"
try "-a    +c => { C }"
try "   -b +c => { C }"
try "-a -b +c => { C }"
try "-a -b -c => { }"

report
