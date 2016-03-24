package com.example

import scala.collection.immutable.TreeMap


class Machine[TValue](val id: String) {
  private var map: TreeMap[HashKey, TValue] = new TreeMap[HashKey, TValue]()

  def add(key: HashKey, value: TValue): Unit = {
    //printf("%s; Adding %s -> %s\n", id, key, value)

    map = map + (key -> value)
  }

  def get(hashKey: HashKey): Option[TValue] = {
    map.get(hashKey)
  }

  def getValuesInHashRange(hashRange: HashRange): Seq[(HashKey, TValue)] ={
    map.range(hashRange.minHash, hashRange.maxHash).toSeq
  }

  def keepOnly(hashRanges: Seq[HashRange]): Seq[(HashKey, TValue)] = {
    val keepOnly: TreeMap[HashKey, TValue] =
      hashRanges
      .map(range => map.range(range.minHash, range.maxHash))
      .fold(map.empty) { (tree1, tree2) => tree1 ++ tree2 }

    val dropped = map.filter { case (k, v) => !keepOnly.contains(k) }

    map = keepOnly

    //printf("%s, Keeping %s, Emitting %s\n", id, map, dropped)

    dropped.toSeq
  }
}
