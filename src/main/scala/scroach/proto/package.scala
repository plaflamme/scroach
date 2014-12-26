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

  /**
   * Matches the header of a response that has no error set
   */
  object NoError {
    def unapply(h: ResponseHeader) = {
      h match {
        case ResponseHeader(None, _, _) => Some(h)
        case _ => None
      }
    }
  }
  object HasError {
    def unapply(h: ResponseHeader) = {
      h match {
        case ResponseHeader(Some(err), _, _) => Some(err)
        case _ => None
      }
    }
  }

  trait CockroachRequest[M <: MessageLite] {
    val header: RequestHeader
    def batched: RequestUnion
    def tx(txn: Transaction): M
  }

  trait CockroachResponse[M <: MessageLite] {
    val header: ResponseHeader
  }

  implicit class RichContainsRequest(req: ContainsRequest) extends CockroachRequest[ContainsRequest] {
    val header = req.header
    def batched = RequestUnion(contains = Some(req))
    def tx(txn: Transaction) = {
      req.copy(header = req.header.copy(txn = Some(txn)))
    }
  }
  implicit class RichContainsResponse(res: ContainsResponse) extends CockroachResponse[ContainsResponse] {
    val header = res.header
  }
  implicit class RichGetRequest(req: GetRequest) extends CockroachRequest[GetRequest] {
    val header = req.header
    def batched = RequestUnion(get = Some(req))
    def tx(txn: Transaction) = {
      req.copy(header = req.header.copy(txn = Some(txn)))
    }
  }
  implicit class RichGetResponse(res: GetResponse) extends CockroachResponse[GetResponse] {
    val header = res.header
  }
  implicit class RichPutRequest(req: PutRequest) extends CockroachRequest[PutRequest] {
    val header = req.header
    def batched = RequestUnion(put = Some(req))
    def tx(txn: Transaction) = {
      req.copy(header = req.header.copy(txn = Some(txn)))
    }
  }
  implicit class RichPutResponse(res: PutResponse) extends CockroachResponse[PutResponse] {
    val header = res.header
  }
  implicit class RichConditionalPutRequest(req: ConditionalPutRequest) extends CockroachRequest[ConditionalPutRequest] {
    val header = req.header
    def batched = RequestUnion(conditionalPut = Some(req))
    def tx(txn: Transaction) = {
      req.copy(header = req.header.copy(txn = Some(txn)))
    }
  }
  implicit class RichConditionalPutResponse(res: ConditionalPutResponse) extends CockroachResponse[ConditionalPutResponse] {
    val header = res.header
  }
  implicit class RichIncrementRequest(req: IncrementRequest) extends CockroachRequest[IncrementRequest] {
    val header = req.header
    def batched = RequestUnion(increment = Some(req))
    def tx(txn: Transaction) = {
      req.copy(header = req.header.copy(txn = Some(txn)))
    }
  }
  implicit class RichIncrementResponse(res: IncrementResponse) extends CockroachResponse[IncrementResponse] {
    val header = res.header
  }
  implicit class RichDeleteRequest(req: DeleteRequest) extends CockroachRequest[DeleteRequest] {
    val header = req.header
    def batched = RequestUnion(delete = Some(req))
    def tx(txn: Transaction) = {
      req.copy(header = req.header.copy(txn = Some(txn)))
    }
  }
  implicit class RichDeleteResponse(res: DeleteResponse) extends CockroachResponse[DeleteResponse] {
    val header = res.header
  }
  implicit class RichDeleteRangeRequest(req: DeleteRangeRequest) extends CockroachRequest[DeleteRangeRequest] {
    val header = req.header
    def batched = RequestUnion(deleteRange = Some(req))
    def tx(txn: Transaction) = {
      req.copy(header = req.header.copy(txn = Some(txn)))
    }
  }
  implicit class RichDeleteRangeResponse(res: DeleteRangeResponse) extends CockroachResponse[DeleteRangeResponse] {
    val header = res.header
  }
  implicit class RichScanRequest(req: ScanRequest) extends CockroachRequest[ScanRequest] {
    val header = req.header
    def batched = RequestUnion(scan = Some(req))
    def tx(txn: Transaction) = {
      req.copy(header = req.header.copy(txn = Some(txn)))
    }
  }
  implicit class RichScanResponse(res: ScanResponse) extends CockroachResponse[ScanResponse] {
    val header = res.header
  }
  implicit class RichReapQueueRequest(req: ReapQueueRequest) extends CockroachRequest[ReapQueueRequest] {
    val header = req.header
    def batched = RequestUnion(reapQueue = Some(req))
    def tx(txn: Transaction) = {
      req.copy(header = req.header.copy(txn = Some(txn)))
    }
  }
  implicit class RichReapQueueResponse(res: ReapQueueResponse) extends CockroachResponse[ReapQueueResponse] {
    val header = res.header
  }
  implicit class RichEnqueueMessageRequest(req: EnqueueMessageRequest) extends CockroachRequest[EnqueueMessageRequest] {
    val header = req.header
    def batched = RequestUnion(enqueueMessage = Some(req))
    def tx(txn: Transaction) = {
      req.copy(header = req.header.copy(txn = Some(txn)))
    }
  }
  implicit class RichEnqueueMessageResponse(res: EnqueueMessageResponse) extends CockroachResponse[EnqueueMessageResponse] {
    val header = res.header
  }
  implicit class RichBatchRequest(req: BatchRequest) extends CockroachRequest[BatchRequest] {
    val header = req.header
    def batched = ???
    def tx(txn: Transaction) = {
      req.copy(header = req.header.copy(txn = Some(txn)))
    }
  }
  implicit class RichBatchResponse(res: BatchResponse) extends CockroachResponse[BatchResponse] {
    val header = res.header
  }
  implicit class RichEndTransactionRequest(req: EndTransactionRequest) extends CockroachRequest[EndTransactionRequest] {
    val header = req.header
    def batched = RequestUnion(endTransaction = Some(req))
    def tx(txn: Transaction) = {
      req.copy(header = req.header.copy(txn = Some(txn)))
    }
  }
  implicit class RichEndTransactionResponse(res: EndTransactionResponse) extends CockroachResponse[EndTransactionResponse] {
    val header = res.header
  }

  implicit class TimestampOrder(val x: Timestamp) extends AnyVal with Ordered[Timestamp] {
    def compare(y: Timestamp): Int = {
      lazy val less = x.wallTime < y.wallTime || (x.wallTime == y.wallTime && x.logical < y.logical)
      lazy val equal = x.wallTime == y.wallTime && x.logical == y.logical

      if(less) -1 else if(equal) 0 else 1
    }
  }

}
