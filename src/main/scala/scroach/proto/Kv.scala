package scroach.proto

import java.io.InputStream
import java.util.concurrent.atomic.AtomicReference

import com.google.protobuf.MessageLite
import com.twitter.finagle.{Filter, SimpleFilter, Service}
import com.twitter.finagle.httpx.{RequestBuilder, Response, Request}

trait Kv {
  val containsEndpoint: Service[ContainsRequest, ContainsResponse]
  val getEndpoint: Service[GetRequest, GetResponse]
  val putEndpoint: Service[PutRequest, PutResponse]
  val casEndpoint: Service[ConditionalPutRequest, ConditionalPutResponse]
  val incrementEndpoint: Service[IncrementRequest, IncrementResponse]
  val deleteEndpoint: Service[DeleteRequest, DeleteResponse]
  val deleteRangeEndpoint: Service[DeleteRangeRequest, DeleteRangeResponse]
  val scanEndpoint: Service[ScanRequest, ScanResponse]
  val reapQueueEndpoint: Service[ReapQueueRequest, ReapQueueResponse]
  val enqueueEndpoint: Service[EnqueueMessageRequest, EnqueueMessageResponse]
  val batchEndpoint: Service[BatchRequest, BatchResponse]
  val endTxEndpoint: Service[EndTransactionRequest, EndTransactionResponse]
}

case class CockroachException[M <: MessageLite](error: Error, response: M) extends Exception(error.toString)

/**
 * Wraps another Kv instance adding transactional semantics to requests and responses.
 *
 * @param kv the non-transaction kv instance
 * @param name the transaction's name
 * @param isolation the requested isolation level
 */
case class TxKv(kv: Kv, name: String = util.Random.alphanumeric.take(20).mkString, isolation: IsolationType.EnumVal = IsolationType.SERIALIZABLE) extends Kv {

  private[this] val tx = new AtomicReference[Transaction](Transaction(name = Some(name), isolation = Some(isolation)))

  private[this] def merge(old: Transaction, niu: Transaction): Transaction = {
    if(old.id.isEmpty) niu
    else {
      val builder = old.toBuilder
      if(niu.status.map(_ != TransactionStatus.PENDING).getOrElse(false))
        builder.setStatus(niu.status.get)
      if(old.`epoch`.isEmpty || old.`epoch`.get < niu.`epoch`.get)
        builder.setEpoch(niu.`epoch`.get)
      if(old.timestamp.isEmpty || old.timestamp.get < niu.timestamp.get)
        builder.setTimestamp(niu.timestamp.get)
      if(old.origTimestamp.isEmpty || old.origTimestamp.get < niu.origTimestamp.get)
        builder.setOrigTimestamp(niu.origTimestamp.get)
      builder.setMaxTimestamp(niu.maxTimestamp.get)
      builder.setCertainNodes(niu.certainNodes.get)
      builder.setPriority(math.max(old.priority.getOrElse(0), niu.priority.get))
      builder.build
    }
  }

  private[this] case class TxFilter[Req <: MessageLite <% CockroachRequest[Req], Res <: MessageLite <% CockroachResponse[Res]]() extends SimpleFilter[Req, Res] {
    def apply(req: Req, service: Service[Req, Res]) = {
      service(req.tx(tx.get))
        .map { response =>
          response.header match {
            case ResponseHeader(Some(err), _, Some(txn)) if(err.transactionAborted.isDefined) => {
              tx.set(Transaction(name = Some(name), isolation = Some(isolation), priority = txn.priority))
            }
            case ResponseHeader(_, _, Some(niu)) => tx.set(merge(tx.get, niu))
            case _ => ()
          }
          response
        }
    }
  }

  val containsEndpoint = TxFilter[ContainsRequest, ContainsResponse]() andThen kv.containsEndpoint
  val getEndpoint = TxFilter[GetRequest, GetResponse]() andThen kv.getEndpoint
  val putEndpoint = TxFilter[PutRequest, PutResponse]() andThen kv.putEndpoint
  val casEndpoint = TxFilter[ConditionalPutRequest, ConditionalPutResponse]() andThen kv.casEndpoint
  val incrementEndpoint = TxFilter[IncrementRequest, IncrementResponse]() andThen kv.incrementEndpoint
  val deleteEndpoint = TxFilter[DeleteRequest, DeleteResponse]() andThen kv.deleteEndpoint
  val deleteRangeEndpoint = TxFilter[DeleteRangeRequest, DeleteRangeResponse]() andThen kv.deleteRangeEndpoint
  val scanEndpoint = TxFilter[ScanRequest, ScanResponse]() andThen kv.scanEndpoint
  val reapQueueEndpoint = TxFilter[ReapQueueRequest, ReapQueueResponse]() andThen kv.reapQueueEndpoint
  val enqueueEndpoint = TxFilter[EnqueueMessageRequest, EnqueueMessageResponse]() andThen kv.enqueueEndpoint
  val batchEndpoint = TxFilter[BatchRequest, BatchResponse]() andThen kv.batchEndpoint
  val endTxEndpoint = TxFilter[EndTransactionRequest, EndTransactionResponse]() andThen kv.endTxEndpoint
}

/**
 * A Kv implementation that uses HTTP as a transport.
 * TODO: consider using StackClient so users can stack custom layers such as logging
 */
case class HttpKv(client: Service[Request, Response]) extends Kv {

  private[this] case class LoggingFilter[Req, Res]() extends SimpleFilter[Req,Res] {
    def apply(req: Req, service: Service[Req,Res]) = {
      println(s"req == $req")
      service(req)
        .respond { res =>
          println(s"res == $res")
        }
    }
  }
  private[this] case class ProtobufFilter[Req <: MessageLite, Res <: MessageLite <% CockroachResponse[Res]](cmd: String, parse: (InputStream) => Res) extends Filter[Req, Res, Request, Response] {
    def apply(req: Req, service: Service[Request, Response]) = {
      val request = RequestBuilder
        .create()
        .url(s"http://hostname/kv/db/$cmd") // TODO: get rid of fake hostname here.
        .addHeader("Content-Type", "application/x-protobuf")
        .addHeader("Accept", "application/x-protobuf")
        .buildPost(req.toBuf)

      service(request)
        .map { response =>
          parse(response.content)
        }
    }
  }
  val containsEndpoint = newEndpoint[ContainsRequest, ContainsResponse]("Contains", ContainsResponse.parseFrom)
  val getEndpoint = newEndpoint[GetRequest, GetResponse]("Get", GetResponse.parseFrom)
  val putEndpoint = newEndpoint[PutRequest, PutResponse]("Put", PutResponse.parseFrom)
  val casEndpoint = newEndpoint[ConditionalPutRequest, ConditionalPutResponse]("ConditionalPut", ConditionalPutResponse.parseFrom)
  val incrementEndpoint = newEndpoint[IncrementRequest, IncrementResponse]("Increment", IncrementResponse.parseFrom)
  val deleteEndpoint = newEndpoint[DeleteRequest, DeleteResponse]("Delete", DeleteResponse.parseFrom)
  val deleteRangeEndpoint = newEndpoint[DeleteRangeRequest, DeleteRangeResponse]("DeleteRange", DeleteRangeResponse.parseFrom)
  val scanEndpoint = newEndpoint[ScanRequest, ScanResponse]("Scan", ScanResponse.parseFrom)
  val reapQueueEndpoint = newEndpoint[ReapQueueRequest, ReapQueueResponse]("ReapQueue", ReapQueueResponse.parseFrom)
  val enqueueEndpoint = newEndpoint[EnqueueMessageRequest, EnqueueMessageResponse]("EnqueueMessage", EnqueueMessageResponse.parseFrom)
  val batchEndpoint = newEndpoint[BatchRequest, BatchResponse]("Batch", BatchResponse.parseFrom)
  val endTxEndpoint = newEndpoint[EndTransactionRequest, EndTransactionResponse]("EndTransaction", EndTransactionResponse.parseFrom)

  private[this] def newEndpoint[Req <: MessageLite, Res <: MessageLite <% CockroachResponse[Res]](name: String, parseFrom: (InputStream) => Res): Service[Req, Res] = {
    // TODO: retrying semantics for 429 and possibly 500s
    LoggingFilter[Req, Res] andThen ProtobufFilter[Req, Res](name, parseFrom) andThen client
  }
}
