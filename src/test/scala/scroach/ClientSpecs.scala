package scroach

import com.twitter.finagle.Httpx
import com.twitter.io.Charsets
import com.twitter.util.{Future, Await}
import scroach.Scroach._
import scroach.proto.{IsolationType, HttpKv}

import scala.collection.JavaConverters._
import java.io.InputStreamReader
import java.util.concurrent.atomic.AtomicReference

import com.google.common.io.CharStreams
import org.scalatest.{Matchers, FlatSpec, Suite, BeforeAndAfterAll}

trait CockroachCluster extends BeforeAndAfterAll { self: Suite =>

  private[this] class Cluster(hostAndPort: String) {

    val endpoint = Httpx.newClient(hostAndPort, "cockroach").toService

    def stop() = Cluster.stop()
  }

  private[this] object Cluster {
    def stop() {
      new ProcessBuilder("/bin/bash", "local_cluster.sh", "stop")
        .directory(new java.io.File("src/test/scripts"))
        .start().waitFor
    }
    def apply() = {

      stop()

      val process = new ProcessBuilder("/bin/bash", "local_cluster.sh", "start")
        .directory(new java.io.File("src/test/scripts").getAbsoluteFile)
        .redirectOutput(ProcessBuilder.Redirect.PIPE)
        .start()

      println("Starting cockroach cluster")
      val output = CharStreams.readLines(new InputStreamReader(process.getInputStream)).asScala

      if(process.waitFor == 0) {
        val hostname = output.last
        println(s"Cockroach cluster available at $hostname")
        new Cluster(hostname)
      } else throw new RuntimeException("failed to start cockroach cluster: " + output.mkString("\n"))
    }
  }

  private[this] val isntance = new AtomicReference[Cluster]()

  def cluster() = Option(isntance.get).get.endpoint

  override def beforeAll() {
    isntance.set(Cluster())
  }

  override def afterAll() {
    Option(isntance.get()).foreach(_.stop())
  }
}

class ClientSpec extends FlatSpec with CockroachCluster with Matchers {

  def randomBytes = util.Random.alphanumeric.take(20).map(_.toByte).toArray

  def withClient(test: Client => Future[Any]) = {
    Await.result {
      test(KvClient(HttpKv(cluster()), "root"))
    }
  }

  "A Client" should "read None when value not present" in withClient { client =>
    val key = randomBytes
    for {
      got <- client.get(key)
    } yield {
      got should be('empty)
    }
  }

  it should "correctly respond to contains" in withClient { client =>
    val key = randomBytes
    val value = randomBytes
    for {
      empty <- client.contains(key)
      _ <- client.put(key, value)
      exists <- client.contains(key)
    } yield {
      empty should be(false)
      exists should be(true)
    }
  }

  it should "read its own writes" in withClient { client =>
    val key = randomBytes
    val value = randomBytes
    for {
      _ <- client.put(key, value)
      got <- client.get(key)
    } yield {
      // TODO: factor these two lines out (custom matcher?)
      got should be ('defined)
      got.get should equal (value)
    }
  }

  it should "delete a single value" in withClient { client =>
    val key = randomBytes
    val value = randomBytes
    for {
      _ <- client.put(key, value)
      exists <- client.get(key)
      _ <- client.delete(key)
      gone <- client.get(key)
    } yield {
      exists should be ('defined)
      gone should be ('empty)
    }
  }

  it should "delete a range" in withClient { client =>
    val keys = for(i <- 0 until 100) yield {
      f"$i%03d".getBytes(Charsets.Utf8)
    }

    val from = 24
    val to = 42
    val exists = keys.zipWithIndex.map { case(_, i) => i < from || i >= to }

    for {
      // TODO: use batch endpoint when available
      _ <- Future.collect(keys.map(k => client.put(k, randomBytes)).toSeq)
      _ <- client.deleteRange(keys(from), keys(to))
      values <- Future.collect(keys.map(k => client.get(k)))
    } yield {
      values zip exists foreach { case(v, e) =>
        v.isDefined should be (e)
      }
    }
  }

  it should "compare and set when compare succeeds" in withClient { client =>
    val key = randomBytes
    val first = randomBytes
    val second = randomBytes

    for {
      _ <- client.compareAndSet(key, None, Some(first))
      isFirst <- client.get(key)
      _ <- client.compareAndSet(key, Some(first), Some(second))
      isSecond <- client.get(key)
      _ <- client.compareAndSet(key, Some(second), None)
      isNone <- client.get(key)
    } yield {
      isFirst should be ('defined)
      isFirst.get should equal(first)
      isSecond should be ('defined)
      isSecond.get should equal(second)
      isNone should be ('empty)
    }
  }

  it should "fail when compare fails" in withClient { client =>
    val key = randomBytes
    val first = randomBytes
    val second = randomBytes

    client.compareAndSet(key, Some(first), Some(second))
      .map { _ =>
        throw new RuntimeException("compareAndSet expected to fail")
      }
      .handle {
        case ConditionFailedException(actual) => {
          actual should be ('empty)
        }
      }
  }

  it should "correctly handle counters" in withClient { client =>
    val key = randomBytes
    for {
      everything <- client.increment(key, 42)
      nothing <- client.increment(key, -42)

      _ <- client.put(key, -42)
      stillNothing <- client.increment(key, 42)
    } yield {
      everything should be (42)
      nothing should be (0)
      stillNothing should be (0)
    }
  }

  it should "handle snapshot isolation" in withClient { client =>
    val key = randomBytes
    val value = randomBytes
    for {
      _ <- client.delete(key)
      tx <- client.tx(IsolationType.SNAPSHOT) { kv =>
        val rich = KvClient(kv, user())
        for {
          _ <- rich.put(key, value)
          inner <- rich.get(key)
          outter <- client.get(key)
        } yield {
          inner should be ('defined)
          inner.get should equal (value)
          outter should be ('empty)
          inner
        }
      }
      got <- client.get(key)
    } yield {
      got should be ('defined)
      got.get should equal (value)
    }
  }

}
