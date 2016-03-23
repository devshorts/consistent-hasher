import java.util.UUID

import com.example.{ConsistentHasher, HashKey, HashValue, Machine}
import org.scalatest._

import scala.util.Random

class ConsistentHashTester extends FlatSpec with Matchers {
  "Hasher" should "add" in {
    val hasher: ConsistentHasher = new ConsistentHasher(2)

    val firstMachine = hasher.addMachine()

    val data1: HashValue = new HashValue("data1")
    val data1key: HashKey = HashKey.safe(data1.hashCode())

    hasher.put(data1key, data1)

    hasher.get(data1key).get should equal(data1)

    val second: Machine = hasher.addMachine()
    val third: Machine = hasher.addMachine()

    hasher.removeMachine(second)

    val data2: HashValue = new HashValue("data2")
    val data2key: HashKey = HashKey.safe(data2.hashCode())

    hasher.put(data2key, data2)

    hasher.get(data1key).get should equal(data1)
    hasher.get(data2key).get should equal(data2)
  }

  it should "replicate" in {
    val hasher: ConsistentHasher = new ConsistentHasher(2)

    hasher.addMachine()

    val foo: HashValue = new HashValue("foo")

    val key: HashKey = HashKey.safe(foo.hashCode())

    hasher.put(key, foo)

    Range(0, 10).foreach(i => {
      val added: Int = new Random().nextInt(50) + 1

      Range(0, added).foreach(_ => hasher.addMachine())

      val bar: HashValue = new HashValue(UUID.randomUUID().toString)

      val key2: HashKey = HashKey.safe(bar.hashCode())

      hasher.put(key2 ,bar)

      val removeCount: Int = Math.max(1, new Random().nextInt(added - 2))

      Range(0, removeCount).foreach(_ => hasher.removeRandomMachine())

      hasher.get(key2).get should equal(bar)
    })
  }
}
