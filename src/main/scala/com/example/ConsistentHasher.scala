package com.example

import scala.collection.immutable.TreeMap
import scala.collection.mutable
import scala.util.Random

case class HashValue(value: String) extends AnyRef

case class HashKey(key: Int) extends AnyRef with Ordered[HashKey] {
  override def compare(that: HashKey): Int = key.compare(that.key)
}

object HashKey {
  def safe(key: Int) = new HashKey(Math.abs(key))
}

case class HashRange(minHash: HashKey, maxHash: HashKey) extends Ordered[HashRange] {
  override def compare(that: HashRange): Int = minHash.compare(that.minHash)
}

class ConsistentHasher[TKey, TValue](replicas: Int) {

  private var primaries: TreeMap[HashRange, Machine[TValue]] = TreeMap()
  private val replicants                                     =
    new mutable.HashMap[Machine[TValue], mutable.Set[HashRange]]() with mutable.MultiMap[Machine[TValue], HashRange]

  private var id: Int = 0

  def removeMachine(machine: Machine[TValue]): Unit = {
    val responsibleRange: HashRange = getRangeForMachine(machine)

    val newMachines = primaries.filter { case (_, m) => !m.eq(machine) }.values.toSeq

    replicants.remove(machine)

    reassignParitions(newMachines)

    /*
    For the ranges the old box covered, ask the secondaries
    for the values and re-emit them
     */
    val secondariesFor: Seq[Machine[TValue]] = getSecondariesFor(responsibleRange)

    secondariesFor
    .flatMap(_.getValuesInHashRange(responsibleRange))
    .distinct
    .foreach { case (k, v) => put(k, v) }
  }

  def removeRandomMachine(): Unit = {
    removeMachine(getPrimaryFor(new HashKey(new Random().nextInt())))
  }

  def get(hashKey: TKey): Option[TValue] = {
    val key = HashKey.safe(hashKey.hashCode())

    val first: Option[TValue] = getReplicas(key)
                                   .map(_.get(key))
                                   .collectFirst { case Some(x) => x }

    if (first.isDefined)
      first
    else
      None
  }

  def addMachine(): Machine[TValue] = {
    //println("Adding machine")
    id += 1

    val newMachine = new Machine[TValue]("machine-" + id)

    val oldMachines: Iterable[Machine[TValue]] = primaries.values

    val allMachines = Seq(newMachine) ++ oldMachines

    reassignParitions(allMachines)

    /*
    For all hash ranges the new machine is told to be a replicant for
    find the primary for that range, and grab all primary values from them
     */
    val newMachineReplicationRanges: mutable.Set[HashRange] = replicants(newMachine)

    newMachineReplicationRanges
    .foreach(responseRange => {
      getPrimaryFor(responseRange)
      .getValuesInHashRange(responseRange)
      .foreach { case (k, v) => newMachine.add(k, v) }
    })

    newMachine
  }

  def put(key: TKey, value: TValue): Unit = {
    val hashkey = HashKey.safe(key.hashCode())

    put(hashkey, value)
  }

  private def put(hashkey: HashKey, value: TValue): Unit = {
    getReplicas(hashkey).foreach(_.add(hashkey, value))
  }

  private def getPrimaryFor(responseRange: HashRange): Machine[TValue] = {
    primaries.find { case (range, m) => range == responseRange }.get._2
  }

  private def overlaps(range: HashRange, hashRange: HashRange): Boolean =
    range.minHash >= hashRange.minHash ||
    range.maxHash < hashRange.maxHash

  private def getPrimariesFor(hashRange: HashRange): Seq[Machine[TValue]] = {
    primaries
    .filter {
              case (range, machine) => overlaps(range, hashRange)
            }
    .values.toSeq
  }

  private def getPrimaryFor(hashKey: HashKey): Machine[TValue] = {
    var previous: HashRange = primaries.head._1
    for ((hashRange, value) <- primaries.drop(1)) {
      if (inRange(hashKey, hashRange)) {
        return value
      }

      previous = hashRange
    }

    primaries.last._2
  }

  private def getSecondariesFor(range: HashRange): Seq[Machine[TValue]] = {
    val filter =
      replicants.filter {
                          case (machine, ranges) =>
                            ranges.exists(r => overlaps(r, range))
                        }
    filter.keys.toSeq
  }

  private def getSecondaries(hashKey: HashKey): Seq[Machine[TValue]] = {
    val filter =
      replicants.filter {
                          case (machine, ranges) =>
                            ranges.exists(inRange(hashKey, _))
                        }

    filter.keys.toSeq
  }

  private def inRange(hashKey: HashKey, hashRange: HashRange): Boolean =
    hashKey >= hashRange.minHash && hashKey < hashRange.maxHash

  private def getRangeForMachine(machine: Machine[TValue]): HashRange = {
    primaries.find { case (range, m) => m.eq(machine) }.get._1
  }

  private def getNewSecondaries(machines: Seq[Machine[TValue]]) = {
    val replicatedRanges: Seq[HashRange] = defineRanges(machines.size).flatMap(Seq.fill(replicas)(_))

    val ranges: Stream[HashRange] = Stream.continually(replicatedRanges).flatten

    val infiteMachines: Stream[Machine[TValue]] = Stream.continually(machines).flatten

    val newSecondaries =
      new mutable.HashMap[Machine[TValue], mutable.Set[HashRange]]()
          with mutable.MultiMap[Machine[TValue], HashRange]

    ranges
    .zip(infiteMachines)
    .slice(1, machines.size * replicas + 1)
    .foreach {
               case (range, machine) =>
                 newSecondaries.addBinding(machine, range)
             }

    newSecondaries
  }

  /**
    * Assign new partitions to machines and assign new secondaries to partitions
    *
    * For any machine that covers a partition it doesn't own anymore
    * re-distribute those messages
    *
    * @param allMachines
    */
  private def reassignParitions(allMachines: Seq[Machine[TValue]]): Unit = {
    primaries = primaries.empty

    val newRanges = defineRanges(allMachines.size)

    val zip: Seq[(HashRange, Machine[TValue])] = newRanges
                                         .zip(allMachines)

    // build the new primary list
    zip
    .foreach {
               case (range, machine) =>
                 primaries = primaries + (range -> machine)
             }

    replicants.clear()

    // reassign who is responsible for what secondary
    getNewSecondaries(allMachines).foreach(replicants += _)

    // tell all primaries to drop everything except for whats in their
    // primary and in their secondary
    // everything they emit, re-add which should get picked up by
    // the new primaries and secondaries
    zip
    .flatMap {
               case (range, machine) =>

                 // emit the things no longer held here
                 machine.keepOnly(Seq(range) ++ replicants(machine))
             }
    .foreach(k => put(k._1, k._2))
  }

  private def getReplicas(hashKey: HashKey): Seq[Machine[TValue]] = {
    Seq(getPrimaryFor(hashKey)) ++ getSecondaries(hashKey)
  }

  private def defineRanges(totalMachines: Int): Seq[HashRange] = {
    val interval = Int.MaxValue / totalMachines

    Range(0, totalMachines)
    .map(num => (interval * num, interval * (num + 1)))
    .map(t => new HashRange(new HashKey(t._1), new HashKey(t._2)))
  }
}

