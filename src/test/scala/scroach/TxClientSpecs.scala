package scroach

import com.google.protobuf.ByteString
import com.twitter.finagle.Service
import com.twitter.util.{Await, Future}
import scroach.proto._

class TxClientSpecs extends ScroachSpec {

  "A TxClient" should "always increment ts" in {
    val timestamps = Seq(
      Timestamp(0,0) -> Timestamp(10,0),
      Timestamp(10,0) -> Timestamp(10,1),
      Timestamp(10,1) -> Timestamp(10,0),
      Timestamp(10,1) -> Timestamp(20,1),
      Timestamp(20,1) -> Timestamp(20,1),
      Timestamp(20,1) -> Timestamp(0,0),
      Timestamp(20,1) -> Timestamp(20,1)
    )

    val kv = new TestKv {
      private[this] var requestIdx = 0
      override val putEndpoint = Service.mk { (req: PutRequest) =>
        val (expTs, newTs) = timestamps(requestIdx)
        val respTx = req.header.txn.map { tx =>
          tx.timestamp.getOrElse(Timestamp(0, 0)) should be(expTs)
          tx.copy(id = tx.id orElse Some(ByteString.copyFromUtf8("tx-id")), timestamp = Some(newTs))
        }
        requestIdx += 1
        Future.value(PutResponse(ResponseHeader(txn = respTx)))
      }
    }

    val key = randomBytes
    KvClient(kv, "root").tx() { txClient =>
      timestamps.map { ts =>
        Await.result(txClient.put(key, randomBytes))
      }
      Future.Done
    } handle {
      case _: UnsupportedOperationException => ()
    }
  }
}
