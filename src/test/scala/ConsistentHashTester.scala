import java.util.UUID

import com.example.ConsistentHasher
import org.scalatest._

import scala.util.Random

class ConsistentHashTester extends FlatSpec with Matchers {
  "Hasher" should "add" in {
    val hasher = new ConsistentHasher[String, String](2)

    val firstMachine = hasher.addMachine()

    hasher.put("key1", "data1")

    hasher.get("key1").get should equal("data1")

    val second = hasher.addMachine()
    val third = hasher.addMachine()

    hasher.removeMachine(second)

    hasher.put("key2", "data2")

    hasher.get("key1").get should equal("data1")
    hasher.get("key2").get should equal("data2")
  }

  it should "replicate" in {
    val hasher = new ConsistentHasher[String, String](2)

    hasher.addMachine()

    hasher.put("key", "foo")

    Range(0, 20).foreach(i => {
      val added: Int = new Random().nextInt(50) + 1

      Range(0, added).foreach(_ => hasher.addMachine())

      val bar = UUID.randomUUID().toString

      hasher.put(bar, bar)

      val removeCount: Int = new Random().nextInt(Math.max(1, added - 2))

      Range(0, removeCount).foreach(_ => hasher.removeRandomMachine())

      hasher.get(bar).get should equal(bar)
    })
  }
}
