package com.example

import scala.collection.immutable.TreeMap


class Machine() {
  private var map: TreeMap[HashKey, HashValue] = new TreeMap[HashKey, HashValue]()

  def add(key: HashKey, value: HashValue): Unit = {
    map = map + (key -> value)
  }

  def get(hashKey: HashKey): Option[HashValue] = {
    map.get(hashKey)
  }

  def getRange(hashRange: HashRange): Seq[(HashKey, HashValue)] ={
    map.range(hashRange.minHash, hashRange.maxHash).toSeq
  }

  def keepOnly(hashRanges: Seq[HashRange]): Seq[(HashKey, HashValue)] = {
    val keepOnly: TreeMap[HashKey, HashValue] =
      hashRanges
      .map(range => map.range(range.minHash, range.maxHash))
      .fold(map.empty) { (tree1, tree2) => tree1 ++ tree2 }

    val dropped = map.filter { case (k, v) => !keepOnly.contains(k) }

    map = keepOnly

    dropped.toSeq
  }
}
