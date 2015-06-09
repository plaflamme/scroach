package scroach

import com.google.protobuf.ByteString
import com.twitter.finagle.Service
import com.twitter.util.{Await, Future}
import cockroach.proto._

class TxClientSpecs extends ScroachSpec {

  def timestamp(wall: Long, logical: Int) = Timestamp().withWallTime(wall).withLogical(logical)

  "A TxClient" should "always increment ts" in {
    val timestamps = Seq(
      timestamp(0,0) -> timestamp(10,0),
      timestamp(10,0) -> timestamp(10,1),
      timestamp(10,1) -> timestamp(10,0),
      timestamp(10,1) -> timestamp(20,1),
      timestamp(20,1) -> timestamp(20,1),
      timestamp(20,1) -> timestamp(0,0),
      timestamp(20,1) -> timestamp(20,1)
    )

    val kv = new TestKv {
      private[this] var requestIdx = 0
      override val putEndpoint = Service.mk { (req: PutRequest) =>
        val (expTs, newTs) = timestamps(requestIdx)
        val respTx = req.header.txn.map { tx =>
          tx.timestamp.getOrElse(timestamp(0, 0)) should be(expTs)
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

  it should "reset tx on abort" in {

    def txAbortError(respTx: Transaction) = {
      Error()
        .withTransactionRestart(TransactionRestart.BACKOFF)
        .withDetail(
          ErrorDetail()
            .withTransactionAborted(
              TransactionAbortedError().withTxn(respTx)
            )
        )
    }

    val kv = new TestKv {
      private[this] var first = true
      override val endTxEndpoint = Service.mk { (req: EndTransactionRequest) =>
        req.header.txn match {
          case Some(txn) => {
            txn.id should be ('empty)
            if(first) {
              first = false
              val respTx = txn.copy(id = Some(ByteString.copyFromUtf8("tx-id")), timestamp = Some(timestamp(10, 0)), priority = Some(4))
              Future.value(EndTransactionResponse(header = ResponseHeader(error = Some(txAbortError(respTx)), txn = Some(respTx))))
            } else {
              // Client is expected to use the provided tx priority for future transactions
              txn.priority.value should be(4)
              val respTx = txn.copy(id = Some(ByteString.copyFromUtf8("tx-id2")), timestamp = Some(timestamp(10, 0)))
              Future.value(EndTransactionResponse(header = ResponseHeader(txn = Some(respTx))))
            }
          }
          case None => Future.exception(new IllegalStateException())
        }
      }
    }

    var tries = 0
    Await.result {
      KvClient(kv, "root").tx() { txClient =>
        tries = tries + 1
        Future.Done
      }
    }
    tries should be(2)
  }
}
