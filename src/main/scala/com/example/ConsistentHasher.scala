package com.example

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

  private var partitions: Seq[(HashRange, Machine[TValue])] = Seq.empty

  private var id: Int = 0

  def removeMachine(machine: Machine[TValue]): Unit = {
    val remainingMachines = partitions.filter { case (r, m) => !m.eq(machine) }.map(_._2)

    partitions = getPartitions(remainingMachines.distinct)

    redistribute(partitions)
  }

  def removeRandomMachine(): Unit = {
    val idx: Int = new Random().nextInt(partitions.size)

    val machineSequence: Seq[Machine[TValue]] = partitions.map(_._2)

    removeMachine(machineSequence(idx))
  }

  def get(hashKey: TKey): Option[TValue] = {
    val key = HashKey.safe(hashKey.hashCode())

    getReplicas(key)
    .map(_.get(key))
    .collectFirst { case Some(x) => x }
  }

  def redistribute(newPartitions: Seq[(HashRange, Machine[TValue])]) = {
    newPartitions.groupBy { case (range, machine) => machine }
    .flatMap { case (machine, ranges) => machine.keepOnly(ranges.map(_._1)) }
    .foreach { case (k, v) => put(k, v) }
  }

  def addMachine(): Machine[TValue] = {
    //println("Adding machine")
    id += 1

    val newMachine = new Machine[TValue]("machine-" + id)

    val oldMachines = partitions.map(_._2).distinct

    partitions = getPartitions(Seq(newMachine) ++ oldMachines)

    redistribute(partitions)

    newMachine
  }

  def put(key: TKey, value: TValue): Unit = {
    val hashkey = HashKey.safe(key.hashCode())

    put(hashkey, value)
  }

  private def put(hashkey: HashKey, value: TValue): Unit = {
    getReplicas(hashkey).foreach(_.add(hashkey, value))
  }

  private def getPartitions(machines: Seq[Machine[TValue]]): Seq[(HashRange, Machine[TValue])] = {
    val replicatedRanges: Seq[HashRange] = Stream.continually(defineRanges(machines.size)).flatten

    val infiteMachines: Stream[Machine[TValue]] = Stream.continually(machines.flatMap(List.fill(replicas)(_))).flatten

    replicatedRanges
    .zip(infiteMachines)
    .take(machines.size * replicas)
    .toList
  }

  private def getReplicas(hashKey: HashKey): Seq[Machine[TValue]] = {
    partitions
    .filter { case (range, machine) => hashKey >= range.minHash && hashKey < range.maxHash }
    .map(_._2)
  }

  private def defineRanges(totalMachines: Int): Seq[HashRange] = {
    val interval = Int.MaxValue / totalMachines

    Range(0, totalMachines)
    .map(num => (interval * num, interval * (num + 1)))
    .map(t => new HashRange(new HashKey(t._1), new HashKey(t._2)))
  }
}

