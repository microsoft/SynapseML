/**
 * Copyright (c) 2017-present, Facebook, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

const {parseQuery, getOptions} = require('loader-utils');
const {loader} = require('webpack');
const {truncate} = require('./cookbookUtils');

module.exports = function(fileString) {
  const callback = this.async();

  const {truncateMarker} = getOptions(this);

  let finalContent = fileString;

  // Truncate content if requested (e.g: file.md?truncated=true)
  const {truncated} = this.resourceQuery && parseQuery(this.resourceQuery);
  if (truncated) {
    finalContent = truncate(fileString, truncateMarker);
  }
  return callback && callback(null, finalContent);
};
