package scroach

import com.twitter.finagle.{Httpx, Name}
import com.twitter.io.Charsets
import com.twitter.util.Await
import scroach.Scroach._
import scroach.proto.{IsolationType, HttpKv}

import scala.collection.JavaConverters._
import java.io.InputStreamReader
import java.util.concurrent.atomic.AtomicReference

import com.google.common.io.CharStreams
import org.scalatest.{Matchers, FlatSpec, Suite, BeforeAndAfterAll}

trait CockroachCluster extends BeforeAndAfterAll { self: Suite =>

  class Cluster(hostAndPort: String) {

    val endpoint = Httpx.newClient(hostAndPort, "cockroach").toService

    def stop(): Unit = {
      val process = new ProcessBuilder("/bin/bash", "local_cluster.sh", "stop")
        .directory(new java.io.File("src/test/scripts"))
        .start()
      process.waitFor
    }
  }
  object Cluster {
    def apply() = {
      val process = new ProcessBuilder("/bin/bash", "local_cluster.sh", "start")
        .directory(new java.io.File("src/test/scripts").getAbsoluteFile)
        .redirectOutput(ProcessBuilder.Redirect.PIPE)
        .start()

      println("Starting cockroach cluster")
      val output = CharStreams.readLines(new InputStreamReader(process.getInputStream)).asScala

      if(process.exitValue == 0) {
        val hostname = output.last
        println(s"Cockroach cluster available at $hostname")
        new Cluster(hostname)
      } else throw new RuntimeException("failed to start cockroach cluster: " + output.mkString("\n"))
    }
  }

  private[this] val isntance = new AtomicReference[Cluster]()

  def cluster() = Option(isntance.get).get

  override def beforeAll() {
    isntance.set(Cluster())
  }

  override def afterAll() {
    Option(isntance.get()).foreach(_.stop())
  }
}

class ClientSpec extends FlatSpec with CockroachCluster with Matchers {

  val Key = "Cockroach".getBytes(Charsets.Utf8)
  val Value = "Hello".getBytes(Charsets.Utf8)

  "A Client" should "handle snapshot isolation" in {
    val client = KvClient(HttpKv(cluster().endpoint), "root")

    Await.result {
      for {
        _ <- client.delete(Key)
        tx <- client.tx(IsolationType.SNAPSHOT) { kv =>
          val rich = KvClient(kv, user())
          for {
            _ <- rich.put(Key, Value)
            inner <- rich.get(Key)
            outter <- client.get(Key)
          } yield {
            inner should be ('defined)
            inner.get should equal (Value)
            outter should be ('empty)
            inner
          }
        }
        got <- client.get(Key)
      } yield {
        got should be ('defined)
        got.get should equal (Value)
      }
    }

  }

}
