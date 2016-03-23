import java.util.UUID

import com.example.{ConsistentHasher, HashKey, HashValue, Machine}
import org.scalatest._

import scala.util.Random

class ConsistentHashTester extends FlatSpec with Matchers {
  "Hasher" should "add" in {
    val hasher: ConsistentHasher = new ConsistentHasher(2)

    val firstMachine = hasher.addMachine()

    val foo: HashValue = new HashValue("foo")

    val key: HashKey = HashKey.safe(foo.hashCode())

    hasher.put(key, foo)

    hasher.get(key).get should equal(foo)

    val second: Machine = hasher.addMachine()
    val third: Machine = hasher.addMachine()
    hasher.removeMachine(second)

    val jizz: HashValue = new HashValue("jizz")
    val put: HashKey = HashKey.safe(jizz.hashCode())

    hasher.put(put, jizz)

    hasher.get(key).get should equal(foo)
    hasher.get(put).get should equal(jizz)
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
