package scroach

import com.google.protobuf.MessageLite
import com.twitter.io.{BufInputStream, Buf}

package object proto {

  implicit class RichMessageLite[M <: MessageLite](val msg: M) extends AnyVal {
    def toBuf: Buf = {
      Buf.ByteArray.Owned(msg.toByteArray)
    }
  }

  implicit def bufToInputStream(buf: Buf): BufInputStream = new BufInputStream(buf)

  /**
   * Extracts the bytes from a Value
   */
  object BytesValue {
    def unapply(value: Value): Option[Bytes] = {
      value match {
        case Value(Some(bytes), _, _, _, _) => Some(bytes.toByteArray)
        case _ => None
      }
    }
  }

  trait CockroachRequest[M <: MessageLite] {
    val header: RequestHeader
    def tx(txn: Transaction): M
  }

  trait CockroachResponse[M <: MessageLite] {
    def tx(): Option[Transaction]
  }

  implicit class RichContainsRequest(req: ContainsRequest) extends CockroachRequest[ContainsRequest] {
    val header = req.header
    def tx(txn: Transaction) = {
      req.copy(header = req.header.copy(txn = Some(txn)))
    }
  }
  implicit class RichContainsResponse(res: ContainsResponse) extends CockroachResponse[ContainsResponse] {
    def tx() = {
      res.header.txn
    }
  }
  implicit class RichGetRequest(req: GetRequest) extends CockroachRequest[GetRequest] {
    val header = req.header
    def tx(txn: Transaction) = {
      req.copy(header = req.header.copy(txn = Some(txn)))
    }
  }
  implicit class RichGetResponse(res: GetResponse) extends CockroachResponse[GetResponse] {
    def tx() = {
      res.header.txn
    }
  }
  implicit class RichPutRequest(req: PutRequest) extends CockroachRequest[PutRequest] {
    val header = req.header
    def tx(txn: Transaction) = {
      req.copy(header = req.header.copy(txn = Some(txn)))
    }
  }
  implicit class RichPutResponse(res: PutResponse) extends CockroachResponse[PutResponse] {
    def tx() = {
      res.header.txn
    }
  }
  implicit class RichConditionalPutRequest(req: ConditionalPutRequest) extends CockroachRequest[ConditionalPutRequest] {
    val header = req.header
    def tx(txn: Transaction) = {
      req.copy(header = req.header.copy(txn = Some(txn)))
    }
  }
  implicit class RichConditionalPutResponse(res: ConditionalPutResponse) extends CockroachResponse[ConditionalPutResponse] {
    def tx() = {
      res.header.txn
    }
  }
  implicit class RichIncrementRequest(req: IncrementRequest) extends CockroachRequest[IncrementRequest] {
    val header = req.header
    def tx(txn: Transaction) = {
      req.copy(header = req.header.copy(txn = Some(txn)))
    }
  }
  implicit class RichIncrementResponse(res: IncrementResponse) extends CockroachResponse[IncrementResponse] {
    def tx() = {
      res.header.txn
    }
  }
  implicit class RichDeleteRequest(req: DeleteRequest) extends CockroachRequest[DeleteRequest] {
    val header = req.header
    def tx(txn: Transaction) = {
      req.copy(header = req.header.copy(txn = Some(txn)))
    }
  }
  implicit class RichDeleteResponse(res: DeleteResponse) extends CockroachResponse[DeleteResponse] {
    def tx() = {
      res.header.txn
    }
  }
  implicit class RichDeleteRangeRequest(req: DeleteRangeRequest) extends CockroachRequest[DeleteRangeRequest] {
    val header = req.header
    def tx(txn: Transaction) = {
      req.copy(header = req.header.copy(txn = Some(txn)))
    }
  }
  implicit class RichDeleteRangeResponse(res: DeleteRangeResponse) extends CockroachResponse[DeleteRangeResponse] {
    def tx() = {
      res.header.txn
    }
  }
  implicit class RichScanRequest(req: ScanRequest) extends CockroachRequest[ScanRequest] {
    val header = req.header
    def tx(txn: Transaction) = {
      req.copy(header = req.header.copy(txn = Some(txn)))
    }
  }
  implicit class RichScanResponse(res: ScanResponse) extends CockroachResponse[ScanResponse] {
    def tx() = {
      res.header.txn
    }
  }
  implicit class RichReapQueueRequest(req: ReapQueueRequest) extends CockroachRequest[ReapQueueRequest] {
    val header = req.header
    def tx(txn: Transaction) = {
      req.copy(header = req.header.copy(txn = Some(txn)))
    }
  }
  implicit class RichReapQueueResponse(res: ReapQueueResponse) extends CockroachResponse[ReapQueueResponse] {
    def tx() = {
      res.header.txn
    }
  }
  implicit class RichEnqueueMessageRequest(req: EnqueueMessageRequest) extends CockroachRequest[EnqueueMessageRequest] {
    val header = req.header
    def tx(txn: Transaction) = {
      req.copy(header = req.header.copy(txn = Some(txn)))
    }
  }
  implicit class RichEnqueueMessageResponse(res: EnqueueMessageResponse) extends CockroachResponse[EnqueueMessageResponse] {
    def tx() = {
      res.header.txn
    }
  }
  implicit class RichEndTransactionRequest(req: EndTransactionRequest) extends CockroachRequest[EndTransactionRequest] {
    val header = req.header
    def tx(txn: Transaction) = {
      req.copy(header = req.header.copy(txn = Some(txn)))
    }
  }
  implicit class RichEndTransactionResponse(res: EndTransactionResponse) extends CockroachResponse[EndTransactionResponse] {
    def tx() = {
      res.header.txn
    }
  }

  implicit class TimestampOrder(val x: Timestamp) extends AnyVal with Ordered[Timestamp] {
    def compare(y: Timestamp): Int = {
      lazy val less = x.wallTime < y.wallTime || (x.wallTime == y.wallTime && x.logical < y.logical)
      lazy val equal = x.wallTime == y.wallTime && x.logical == y.logical

      if(less) -1 else if(equal) 0 else 1
    }
  }

}
