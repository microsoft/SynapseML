#!/usr/bin/env node
// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

"use strict";

let [fs, path, util] = ["fs", "path", "util"].map(require);

class CheckError extends Error {};
let bad = (...message) => {
  throw new CheckError("| " + require("util").format(...message)
                                .replace(/\n/g, "\n| "));
};

let escapeNonASCII = str =>
  str.replace(/[^\0-\x7e]/g,
              x => `\\u${x.charCodeAt(0).toString(16).padStart(4,"0")}`);
let normalString = x =>
  escapeNonASCII(JSON.stringify(x, null, 1) + "\n");

let type = x => x === null ? "null" : Array.isArray(x) ? "array" : typeof x;
let assertType = (x, t, path) => {
  if (type(x) != t) bad(`${path}: bad type, expected ${t}, got ${type(x)}`);
};

let _ = x => true;
let ___ = x => true;
let _eq = expected => (value, path) => {
  assertType(value, type(expected), path);
  if (value !== expected)
    bad("%s: bad value,\n  expected: %j\n  got:      %j",
        path, expected, value);
};
let _arr = expected => (value, path) => {
  assertType(value, "array", path);
  if (expected.length != value.length)
    bad(`%s: bad array length, expected %s, got %s`,
        path, expected.length, value.length);
  value.forEach((v, i) => _verifier(expected[i])(v, `${path}[${i}]`));
};
let _arrOf = expected => (value, path) => {
  assertType(value, "array", path);
  value.forEach((v, i) => _verifier(expected)(v, `${path}[${i}]`));
};
let _obj = expected => (value, path) => {
  assertType(value, "object", path);
  let keys = Object.keys(expected);
  let _keys = keys.join(", "), _vkeys = Object.keys(value).join(", ");
  if (_keys != _vkeys)
    bad("%s: bad keys:\n  expected: %s\n  got:      %s",
       path, _keys, _vkeys);
  keys.forEach(k => _verifier(expected[k])(value[k], `${path}.${k}`));
};
let _or = (...expecteds) => (value, path) => {
  let errors = expecteds.map(expected => {
    let e = null;
    try { _verifier(expected)(value, path); return null; }
    catch (err) { if (!(err instanceof CheckError)) throw err;
                  else return err; }
  }).filter(x => x !== null)
  if (errors.length == expecteds.length)
    bad("OR(" + expecteds.length + "):\n" +
        errors.map(e => (e.message)).join("\n"));
}

let _verifier = expected => {
  switch (type(expected)) {
    case "function": return expected;
    case "array":    return _arr(expected)
    case "object":   return _obj(expected);
    default:         return _eq(expected);
  }
}

function check(file, expected) {
  process.stdout.write(`Checking ${path.basename(file)}... `);
  let str = fs.readFileSync(file, "utf8");
  let x = JSON.parse(str);
  { let nstr = normalString(x);
    if (str != nstr) {
      fs.writeFileSync("/tmp/check.json", nstr);
      bad("not normalized (indentation?)\n  compare with \"/tmp/check.json\"");
    }
  }
  _verifier(expected)(x, "TOP");
  process.stdout.write(`OK\n`);
}

function checkAll(files, expected) {
  try { files.forEach(file => check(file, expected)); }
  catch (err) {
    if (!(err instanceof CheckError)) throw err;
    process.stdout.write(`FAIL!\n${err.message}\n`);
  }
}

let _cellMetadata =
  _or({},
      {"mml-deploy": "local",     collapsed: false},
      {"mml-deploy": "hdinsight", collapsed: true});
let _srcStrings = _arrOf((value, path) => {
  assertType(value, "string", path);
  if (value.endsWith(" ")) bad(`${path}: bad string, ends with a space`);
  if (value.length > 120)  bad(`${path}: bad string, too long`);
  if (value.match(/\n./))  bad(`${path}: bad string, \\n not at EOS`);
  if (value.match(/ \n/))  bad(`${path}: bad string, space before \\n`);
});

let getNotebooks = () => {
  let rx = /\.ipynb$/;
  let scanDir = (dir) => {
    let paths = fs.readdirSync(dir).map(p => path.join(dir,p));
    let dirs  = paths.filter(p => fs.statSync(p).isDirectory());
    let files = paths.filter(p => dirs.indexOf(p) < 0);
    return files.filter(f => rx.test(f)).concat(...(dirs.map(scanDir)));
  };
  let baseDir = path.dirname(path.dirname(path.dirname(__filename)));
  return scanDir(baseDir);
};

let notebooks =
  (process.argv.length > 2) ? process.argv.slice(2) : getNotebooks();

checkAll(
  notebooks,
  {cells:
   _arrOf(_or({cell_type: "markdown", metadata: _cellMetadata,
               source: _srcStrings},
              {cell_type: "code", execution_count: null,
               metadata: _cellMetadata, outputs: [],
               source: _srcStrings})),
   metadata: {"anaconda-cloud": {},
              kernelspec: {display_name: "Python [default]",
                           language: "python",
                           name: "python3"},
              language_info: _},
   nbformat: 4,
   nbformat_minor: 0});
