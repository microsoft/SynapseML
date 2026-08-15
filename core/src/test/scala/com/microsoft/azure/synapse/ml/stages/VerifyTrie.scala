// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package com.microsoft.azure.synapse.ml.stages

import com.microsoft.azure.synapse.ml.core.test.base.TestBase

class VerifyTrie extends TestBase {

  test("Trie.apply creates from map") {
    val t = Trie(Map("hello" -> "hi"))
    assert(t.get('h').isDefined)
  }

  test("put and get for single characters") {
    val t = new Trie().put("a", "b")
    assert(t.get('a').isDefined)
  }

  test("mapText replaces matching substrings") {
    val t = Trie(Map("hello" -> "hi"))
    assert(t.mapText("hello there") === "hi there")
  }

  test("mapText with no matches returns original text") {
    val t = Trie(Map("xyz" -> "abc"))
    assert(t.mapText("hello world") === "hello world")
  }

  test("mapText with multiple replacements") {
    val t = Trie(Map("hello" -> "hi", "world" -> "earth"))
    assert(t.mapText("hello world.") === "hi earth.")
  }

  test("putAll adds all entries") {
    val t = new Trie().putAll(Map("a" -> "1", "b" -> "2"))
    assert(t.get('a').isDefined)
    assert(t.get('b').isDefined)
  }

  test("get returns None for missing key") {
    val t = new Trie()
    assert(t.get('z').isEmpty)
  }

  test("mapText with overlapping keys longer key wins") {
    val t = Trie(Map("he" -> "X", "hello" -> "Y"))
    assert(t.mapText("hello.") === "Y.")
  }
}
