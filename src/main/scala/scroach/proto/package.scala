package scroach

import cockroach.proto._
import com.trueaccord.scalapb.GeneratedMessage
import com.twitter.io.{BufInputStream, Buf}

package object proto {

  implicit class RichGeneratedMessage[M <: GeneratedMessage](val msg: M) extends AnyVal {
    def toBuf: Buf = {
      Buf.ByteArray.Owned(msg.toByteArray)
    }
  }

  implicit def bufToInputStream(buf: Buf): BufInputStream = new BufInputStream(buf)

  /**
   * Extracts the bytes from a Value
   */
  object BytesValue {
    def unapply(value: Value): Option[Option[Bytes]] = {
      value match {
        case Value(Some(bytes), _, _, _) => Some(Some(bytes.toByteArray))
        case Value(None, _, _, _) => Some(None)
        case _ => None
      }
    }
  }
  object CounterValue {
    def unapply(value: Value): Option[Option[Long]] = {
      value match {
        case Value(Some(bytes), _, _, _) if(bytes.size == 8) => Some(Some(bytes.toByteArray.decodeLong))
        case Value(None, _, _, _) => Some(None)
        case _ => None
      }
    }
  }

  /**
   * Matches the header of a response that has no error set
   */
  object NoError {
    def unapply(header: Option[ResponseHeader]) = {
      header match {
        case Some(h@ResponseHeader(None, _, _)) => Some(h)
        case Some(_) => None // no error
        case None => throw new IllegalStateException("missing header")
      }
    }
  }
  object HasError {
    def unapply(header: Option[ResponseHeader]) = {
      header match {
        case Some(ResponseHeader(Some(err), _, _)) => Some(err)
        case Some(_) => None // no error
        case None => throw new IllegalStateException("missing header")
      }
    }
  }
  object ConditionFailed {
    def unapply(header: Option[ResponseHeader]) = {
      header match {
        case Some(ResponseHeader(Some(err), _, _)) if(err.getDetail.value.conditionFailed.isDefined) => err.getDetail.value.conditionFailed
        case _ => None
      }
    }
  }

  trait CockroachRequest[M <: GeneratedMessage] {
    val header: RequestHeader
    def batched: RequestUnion
    def tx(txn: Transaction): M
  }

  trait CockroachResponse[M <: GeneratedMessage] {
    val header: ResponseHeader
  }

  implicit class RichGetRequest(req: GetRequest) extends CockroachRequest[GetRequest] {
    val header = req.header.get
    def batched = RequestUnion(RequestUnion.Value.Get(req))
    def tx(txn: Transaction) = {
      req.withHeader(header.withTxn(txn))
    }
  }
  implicit class RichGetResponse(res: GetResponse) extends CockroachResponse[GetResponse] {
    val header = res.header.get
  }
  implicit class RichPutRequest(req: PutRequest) extends CockroachRequest[PutRequest] {
    val header = req.header.get
    def batched = RequestUnion(RequestUnion.Value.Put(req))
    def tx(txn: Transaction) = {
      req.withHeader(header.withTxn(txn))
    }
  }
  implicit class RichPutResponse(res: PutResponse) extends CockroachResponse[PutResponse] {
    val header = res.header.get
  }
  implicit class RichConditionalPutRequest(req: ConditionalPutRequest) extends CockroachRequest[ConditionalPutRequest] {
    val header = req.header.get
    def batched = RequestUnion(RequestUnion.Value.ConditionalPut(req))
    def tx(txn: Transaction) = {
      req.withHeader(header.withTxn(txn))
    }
  }
  implicit class RichConditionalPutResponse(res: ConditionalPutResponse) extends CockroachResponse[ConditionalPutResponse] {
    val header = res.header.get
  }
  implicit class RichIncrementRequest(req: IncrementRequest) extends CockroachRequest[IncrementRequest] {
    val header = req.header.get
    def batched = RequestUnion(RequestUnion.Value.Increment(req))
    def tx(txn: Transaction) = {
      req.withHeader(header.withTxn(txn))
    }
  }
  implicit class RichIncrementResponse(res: IncrementResponse) extends CockroachResponse[IncrementResponse] {
    val header = res.header.get
  }
  implicit class RichDeleteRequest(req: DeleteRequest) extends CockroachRequest[DeleteRequest] {
    val header = req.header.get
    def batched = RequestUnion(RequestUnion.Value.Delete(req))
    def tx(txn: Transaction) = {
      req.withHeader(header.withTxn(txn))
    }
  }
  implicit class RichDeleteResponse(res: DeleteResponse) extends CockroachResponse[DeleteResponse] {
    val header = res.header.get
  }
  implicit class RichDeleteRangeRequest(req: DeleteRangeRequest) extends CockroachRequest[DeleteRangeRequest] {
    val header = req.header.get
    def batched = RequestUnion(RequestUnion.Value.DeleteRange(req))
    def tx(txn: Transaction) = {
      req.withHeader(header.withTxn(txn))
    }
  }
  implicit class RichDeleteRangeResponse(res: DeleteRangeResponse) extends CockroachResponse[DeleteRangeResponse] {
    val header = res.header.get
  }
  implicit class RichScanRequest(req: ScanRequest) extends CockroachRequest[ScanRequest] {
    val header = req.header.get
    def batched = RequestUnion(RequestUnion.Value.Scan(req))
    def tx(txn: Transaction) = {
      req.withHeader(header.withTxn(txn))
    }
  }
  implicit class RichScanResponse(res: ScanResponse) extends CockroachResponse[ScanResponse] {
    val header = res.header.get
  }
  implicit class RichBatchRequest(req: BatchRequest) extends CockroachRequest[BatchRequest] {
    val header = req.header.get
    def batched = ???
    def tx(txn: Transaction) = {
      req.withHeader(header.withTxn(txn))
    }
  }
  implicit class RichBatchResponse(res: BatchResponse) extends CockroachResponse[BatchResponse] {
    val header = res.header.get
  }
  implicit class RichEndTransactionRequest(req: EndTransactionRequest) extends CockroachRequest[EndTransactionRequest] {
    val header = req.header.get
    def batched = RequestUnion(RequestUnion.Value.EndTransaction(req))
    def tx(txn: Transaction) = {
      req.withHeader(header.withTxn(txn))
    }
  }
  implicit class RichEndTransactionResponse(res: EndTransactionResponse) extends CockroachResponse[EndTransactionResponse] {
    val header = res.header.get
  }

  implicit class TimestampOrder(val x: Timestamp) extends AnyVal with Ordered[Timestamp] {
    def compare(y: Timestamp): Int = {
      val less = x.wallTime.get < y.wallTime.get || (x.wallTime == y.wallTime && x.logical.get < y.logical.get)
      lazy val equal = x.wallTime == y.wallTime && x.logical == y.logical

      if(less) -1 else if(equal) 0 else 1
    }
  }

}
