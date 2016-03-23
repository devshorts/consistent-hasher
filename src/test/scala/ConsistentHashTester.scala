import com.example.{ConsistentHasher, HashKey, HashValue}
import org.scalatest._

class ConsistentHashTester extends FlatSpec with Matchers {
  "Hasher" should "add" in {
    val hasher: ConsistentHasher = new ConsistentHasher(2)

    hasher.addMachine()

    val foo: HashValue = new HashValue("foo")

    val key: HashKey = hasher.put(foo)

    val value: HashValue = hasher.get(key).get

    value should equal(foo)
  }

  it should "replicate" in {
    val hasher: ConsistentHasher = new ConsistentHasher(2)

    hasher.addMachine()
    hasher.addMachine()

    val foo: HashValue = new HashValue("foo")

    val key: HashKey = hasher.put(foo)

    Range(0, 100).foreach(i => {
      hasher.get(key).get should equal(foo)

      hasher.removeRandomMachine()

      hasher.get(key).get should equal(foo)

      hasher.addMachine()

      hasher.get(key).get should equal(foo)
    })
  }
}
