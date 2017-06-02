#!/usr/bin/env bash
# Copyright (C) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See LICENSE in project root for information.

. "$(dirname "${BASH_SOURCE[0]}")/../../runme" "$@"
main() {

local srcdir="$TOOLSDIR/pip"
local destdir="$BUILD_ARTIFACTS/packages/pip"
local tempdir="$destdir/mmlspark"
local wheelfile="$destdir/$PIP_PACKAGE"

# Create the package structure in the temp packaging directory
_rmcd "$tempdir"
_ cp "$srcdir/"* .; _rm *.sh
_ unzip -q "$BUILD_ARTIFACTS/sdk/mmlspark.zip"
_ cp "$BASEDIR/LICENSE" "LICENSE.txt"

# Create the package
_ python setup.py bdist_wheel --universal -d "$destdir"
if [[ -r "$wheelfile" ]]; then show - "Generated wheel: $wheelfile"
else failwith "expected wheel file missing: $wheelfile"; fi

# Cleanup
_ cd "$destdir"
_rm "$tempdir"

}
main "$@"
