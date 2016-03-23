package com.example

import scala.collection.immutable.TreeMap
import scala.collection.mutable
import scala.util.Random

case class HashValue(value: String) extends AnyRef

case class HashKey(key: Int) extends AnyRef with Ordered[HashKey] {
  override def compare(that: HashKey): Int = key.compare(that.key)
}

case class HashRange(minHash: HashKey, maxHash: HashKey) extends Ordered[HashRange] {
  override def compare(that: HashRange): Int = minHash.compare(that.minHash)
}

class ConsistentHasher(replicas: Int) {

  private var primaries: TreeMap[HashRange, Machine] = TreeMap()
  private val replicants                             =
    new mutable.HashMap[Machine, mutable.Set[HashRange]]() with mutable.MultiMap[Machine, HashRange]

  private var ringCount: Int = 0

  def removeMachine(machine: Machine): Unit = {
    ringCount -= 1

    val newMachines = primaries.filter { case (_, m) => m.eq(machine) }.values.toSeq

    replicants.remove(machine)

    rebalance(newMachines)
  }

  def removeRandomMachine(): Unit = {
    removeMachine(getPrimary(new HashKey(new Random().nextInt())))
  }

  private def inRange(hashKey: HashKey, hashRange: HashRange): Boolean =
    hashKey >= hashRange.minHash && hashKey < hashRange.maxHash

  private def getPrimary(hashKey: HashKey): Machine = {
    var previous: HashRange = primaries.head._1
    for ((hashRange, value) <- primaries.drop(1)) {
      if (inRange(hashKey, hashRange)) {
        return value
      }

      previous = hashRange
    }

    primaries.last._2
  }

  private def addPrimary(hashRange: HashRange, machine: Machine) = {
    primaries = primaries + (hashRange -> machine)
  }

  private def addSecondary(hashRange: HashRange, machine: Machine): Unit = {
    replicants.addBinding(machine, hashRange)
  }

  private def getSecondaries(hashKey: HashKey): Seq[Machine] = {
    val filter =
      replicants.filter {
                          case (machine, ranges) =>
                            ranges.exists(inRange(hashKey, _))
                        }

    filter.keys.toSeq
  }

  def get(hashKey: HashKey): Option[HashValue] = {
    getReplicas(hashKey)
    .map(_.get(hashKey))
    .collectFirst { case Some(x) => x }
  }

  private def getClosest(range: HashRange, newRanges: Seq[HashRange]): HashRange = {
    newRanges
    .minBy(f => (f.maxHash.key - range.maxHash.key) - (f.minHash.key - range.minHash.key))
  }

  def addMachine(): Unit = {
    ringCount += 1

    val newMachine = new Machine()

    val oldMachines: Iterable[Machine] = primaries.values

    val allMachines = Seq(newMachine) ++ oldMachines

    rebalance(allMachines)
  }

  private def getRangeFor(machine: Machine): HashRange = {
    primaries.find { case (range, m) => m.equals(machine) }.get._1
  }

  private def getNewSecondaries(machines : Seq[Machine]) = {
    val ranges: Stream[HashRange] = Stream.continually(defineRanges(machines.size).flatMap(List.fill(replicas)(_))).flatten

    val infiteMachines: Stream[Machine] = Stream.continually(machines).flatten

    val newSecondaries = new mutable.HashMap[Machine, mutable.Set[HashRange]]() with mutable.MultiMap[Machine, HashRange]

    ranges
      .zip(infiteMachines)
      .slice(1, machines.size * replicas + 1)
      .foreach {
                 case (range, machine) =>
                   newSecondaries.addBinding(machine, range)
               }

    newSecondaries
  }

  private def rebalance(allMachines: Seq[Machine]): Unit = {
    primaries = primaries.empty

    val newRanges = defineRanges(ringCount)

    val zip: Seq[(HashRange, Machine)] = newRanges
                                         .zip(allMachines)

    // build the new primary and secondaries list
    zip
    .foreach {
               case (range, machine) =>
                 primaries = primaries + (range -> machine)
             }

    replicants.clear()

    getNewSecondaries(allMachines).foreach(replicants += _)

    zip
    .flatMap {
               case (range, machine) =>

                 // emit the things no longer held here
                 machine.keepOnly(Seq(range) ++ replicants(machine))
             }
    .foreach(k => put(k._2))
  }

  def put(hashValue: HashValue): HashKey = {
    val key: HashKey = new HashKey(Math.abs(hashValue.hashCode()))

    getReplicas(key).foreach(_.add(key, hashValue))

    key
  }

  private def getReplicas(hashKey: HashKey): Seq[Machine] = {
    Seq(getPrimary(hashKey)) ++ getSecondaries(hashKey)
  }

  private def defineRanges(totalMachines: Int): Seq[HashRange] = {
    val interval = Int.MaxValue / totalMachines

    Range(0, totalMachines)
    .map(num => (interval * num, interval * (num + 1)))
    .map(t => new HashRange(new HashKey(t._1), new HashKey(t._2)))
  }
}

