package com.example

import com.example.hashing.Ring

import scala.collection.mutable
import scala.util.Random

case class HashValue(value: String) extends AnyRef

case class HashKey(key: Int) extends AnyRef

package object hashing {
  type Ring = Int
}

class Machine() {
  private val map: mutable.Map[HashKey, HashValue] = mutable.Map[HashKey, HashValue]()

  def add(hashKey: HashKey, hashValue: HashValue): Unit = {
    map.put(hashKey, hashValue)
  }

  def get(hashKey: HashKey): Option[HashValue] = {
    map.get(hashKey)
  }

  def sync(allowedInReplica: (HashKey) => Boolean,
           onElement: ((HashKey, HashValue)) => Unit) : Unit = {
    map
    .map(kv => {
      onElement.apply(kv)

      kv
    })
    .filter { case (k, v) => !allowedInReplica.apply(k) }
    .foreach {
               case (k, v) =>
                 map.remove(k)
             }
  }
}

class ConsistentHasher(replicas: Int) {
  private val machines: mutable.ArrayBuffer[Machine] = mutable.ArrayBuffer()

  def removeMachine(machine: Machine): Unit = {
    machines.remove(machines.indexOf(machine))

    rebalance()
  }

  def removeRandomMachine(): Unit = {
    machines.remove(new Random().nextInt(machines.size))

    rebalance()
  }

  def getMachines = machines

  def get(hashKey: HashKey): Option[HashValue] = {
    getReplicas(hashKey)
    .map(machines(_).get(hashKey))
    .collectFirst { case Some(x) => x }
  }

  def addMachine(): Unit = {
    machines += new Machine()

    rebalance()
  }

  private def rebalance(): Unit = {
    var idx = 0

    for (machine <- machines) {
      val allowedInReplica: (HashKey) => Boolean = key => {
        getReplicas(key).contains(idx)
      }

      val replicate: ((HashKey, HashValue)) => Unit = {
        case (key, v) =>
          val replicas: Seq[Ring] = getReplicas(key)
          if (!replicas.forall(machines(_).get(key).isDefined)) {
            replicas.foreach(machines(_).add(key, v))
          }
      }

      machine.sync(allowedInReplica, replicate)

      idx += 1
    }
  }

  def put(hashValue: HashValue): HashKey = {

    val key: HashKey = new HashKey(Math.abs(hashValue.hashCode()))

    getReplicas(key).foreach(ring => machines(ring).add(key, hashValue))

    key
  }

  private def getReplicas(hashKey: HashKey): Seq[Ring] = {
    val replicaCount: Int = Math.min(replicas, machines.size)

    Range(0, replicaCount).map(offset => ((hashKey.key % machines.size) + offset) % machines.size)
  }
}

