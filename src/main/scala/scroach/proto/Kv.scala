package scroach
package proto

import java.io.InputStream
import java.util.concurrent.atomic.AtomicReference

import cockroach.proto._
import com.trueaccord.scalapb.GeneratedMessage
import com.twitter.finagle.{Filter, SimpleFilter, Service}
import com.twitter.finagle.httpx.{RequestBuilder, Response, Request}
import com.twitter.logging.Level
import com.twitter.util.{Duration, Stopwatch, Return, Throw}

trait Kv {
  val getEndpoint: Service[GetRequest, GetResponse]
  val putEndpoint: Service[PutRequest, PutResponse]
  val casEndpoint: Service[ConditionalPutRequest, ConditionalPutResponse]
  val incrementEndpoint: Service[IncrementRequest, IncrementResponse]
  val deleteEndpoint: Service[DeleteRequest, DeleteResponse]
  val deleteRangeEndpoint: Service[DeleteRangeRequest, DeleteRangeResponse]
  val scanEndpoint: Service[ScanRequest, ScanResponse]
  val batchEndpoint: Service[BatchRequest, BatchResponse]
  val endTxEndpoint: Service[EndTransactionRequest, EndTransactionResponse]
}

case class CockroachException[M <: GeneratedMessage](error: Error, response: M) extends Exception(error.toString)

/**
 * Wraps another Kv instance adding transactional semantics to requests and responses.
 *
 * @param kv the non-transaction kv instance
 * @param name the transaction's name
 * @param isolation the requested isolation level
 */
case class TxKv(kv: Kv, name: String = util.Random.alphanumeric.take(20).mkString, isolation: IsolationType = IsolationType.SERIALIZABLE) extends Kv {

  private[this] val tx = new AtomicReference[Transaction](Transaction(name = Some(name), isolation = Some(isolation)))

  private[this] def max[T <% Ordered[T]](l: Option[T], r: Option[T]) = {
    import scala.math.Ordering.Implicits._
    if(l > r) l else r
  }

  private[this] def merge(niu: Transaction) = synchronized {
    val old = tx.get
    tx.set {
      if(old.id.isEmpty) niu
      else {
        old.copy(
          timestamp = max(old.timestamp, niu.timestamp),
          origTimestamp = max(old.origTimestamp, niu.origTimestamp),
          epoch = max(old.epoch, niu.epoch),
          priority = max(old.priority, niu.priority),
          status = niu.status.filter(_ != TransactionStatus.PENDING) orElse old.status,
          maxTimestamp = niu.maxTimestamp,
          certainNodes = niu.certainNodes
        )
      }
    }
  }

  private[this] case class TxFilter[Req <: GeneratedMessage <% CockroachRequest[Req], Res <: GeneratedMessage <% CockroachResponse[Res]]() extends SimpleFilter[Req, Res] {
    def apply(req: Req, service: Service[Req, Res]) = {
      service(req.tx(tx.get))
        .map { response =>
          response.header match {
            case ResponseHeader(Some(err), _, Some(txn)) if(err.getDetail.value.transactionAborted.isDefined) => {
              tx.set(Transaction(name = Some(name), isolation = Some(isolation), priority = txn.priority))
            }
            case ResponseHeader(_, _, Some(niu)) => merge(niu)
            case _ => ()
          }
          response
        }
    }
  }

  val getEndpoint = TxFilter[GetRequest, GetResponse]() andThen kv.getEndpoint
  val putEndpoint = TxFilter[PutRequest, PutResponse]() andThen kv.putEndpoint
  val casEndpoint = TxFilter[ConditionalPutRequest, ConditionalPutResponse]() andThen kv.casEndpoint
  val incrementEndpoint = TxFilter[IncrementRequest, IncrementResponse]() andThen kv.incrementEndpoint
  val deleteEndpoint = TxFilter[DeleteRequest, DeleteResponse]() andThen kv.deleteEndpoint
  val deleteRangeEndpoint = TxFilter[DeleteRangeRequest, DeleteRangeResponse]() andThen kv.deleteRangeEndpoint
  val scanEndpoint = TxFilter[ScanRequest, ScanResponse]() andThen kv.scanEndpoint
  val batchEndpoint = TxFilter[BatchRequest, BatchResponse]() andThen kv.batchEndpoint
  val endTxEndpoint = TxFilter[EndTransactionRequest, EndTransactionResponse]() andThen kv.endTxEndpoint
}

/**
 * A Kv implementation that uses HTTP as a transport.
 * TODO: consider using StackClient so users can stack custom layers such as logging
 */
case class HttpKv(client: Service[Request, Response]) extends Kv {

  private[this] case class LoggingFilter[Req <: GeneratedMessage <% CockroachRequest[Req], Res <: GeneratedMessage <% CockroachResponse[Res]](cmd: String) extends SimpleFilter[Req,Res] {

    implicit class OptionLogging[T](opt: Option[T]) {
      def toLog: String = opt.fold("") { _.toString }
    }
    implicit class TimestampLogging(opt: Option[Timestamp]) {
      def toLog: String = opt.fold("") { ts => f"${ts.getWallTime.toDouble/1E9}%.09f,${ts.getLogical}" }
    }
    def hdrString(header: RequestHeader) = {
      val user = header.user.toLog
      val priority = header.userPriority.toLog
      s"(user=$user pri=$priority)"
    }
    def txnString(transaction: Option[Transaction]) = {
      for {
        txn <- transaction
        name <- txn.name
        priority = txn.priority.toLog
        iso = txn.isolation.toLog
        state = txn.status.toLog
        epoch = txn.epoch.toLog
        ts = txn.timestamp.toLog
        orig = txn.origTimestamp.toLog
        max = txn.maxTimestamp.toLog
      } yield s"(tx=$name isolation=$iso pri=$priority state=$state epoch=$epoch ts=$ts orig=$orig max=$max) "
    } getOrElse("")

    def errString(error: Option[Error]) = {
      for {
        err <- error
        msg = err.message.toLog
        retry = err.retryable.toLog
        restart = err.transactionRestart.toLog
        detail <- err.detail
        typ = detail.value match {
          case ErrorDetail.Value.Empty => "Unknown"
          case _: ErrorDetail.Value.ConditionFailed => "ConditionFailed"
          case _: ErrorDetail.Value.LeaseRejected => "LeaseRejected"
          case _: ErrorDetail.Value.NotLeader => "NotLeader"
          case _: ErrorDetail.Value.OpRequiresTxn => "OpRequiresTxn"
          case _: ErrorDetail.Value.RangeKeyMismatch => "RangeKeyMismatch"
          case _: ErrorDetail.Value.RangeNotFound => "RangeNotFound"
          case _: ErrorDetail.Value.ReadWithinUncertaintyInterval => "ReadWithinUncertaintyInterval"
          case _: ErrorDetail.Value.TransactionAborted => "TransactionAborted"
          case _: ErrorDetail.Value.TransactionPush => "TransactionPush"
          case _: ErrorDetail.Value.TransactionRetry => "TransactionRetry"
          case _: ErrorDetail.Value.TransactionStatus => "TransactionStatus"
          case _: ErrorDetail.Value.WriteIntent => "WriteIntent"
          case _: ErrorDetail.Value.WriteTooOld => "WriteTooOld"
        }
      } yield s"(error=$typ retry=$retry restart=$restart)"
    } getOrElse("ok")

    def logRequest(nonce: String, request: Req) = {
      val hdrStr = hdrString(request.header)
      val txStr = txnString(request.header.txn)
      ScroachLog.trace(f"[$nonce]         ->$cmd%-14s $txStr")
    }

    def logResponse(nonce: String, response: Res, duration: Duration) = {
      val status = Seq(
        txnString(response.header.txn),
        errString(response.header.error)
      ) filter(_.nonEmpty) mkString(" ")

      ScroachLog.trace(f"[$nonce] ${duration.inMillis}%5dms <-$cmd%-14s $status")
    }

    def logException(nonce: String, exception: Throwable, duration: Duration) = {
      ScroachLog.trace(f"[$nonce] ${duration.inMillis}%5d <-$cmd%-14s ${exception.getMessage}")
    }

    def apply(req: Req, service: Service[Req,Res]) = {
      val nonce = scala.util.Random.alphanumeric.take(8).mkString
      val watch = Stopwatch.start()
      logRequest(nonce, req)
      service(req)
        .respond {
          case Return(res) => logResponse(nonce, res, watch())
          case Throw(e) => logException(nonce, e, watch())
        }
    }
  }
  private[this] case class ProtobufFilter[Req <: GeneratedMessage <% CockroachRequest[Req], Res <: GeneratedMessage <% CockroachResponse[Res]](cmd: String, parse: (InputStream) => Res) extends Filter[Req, Res, Request, Response] {
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

  val getEndpoint = newEndpoint[GetRequest, GetResponse]("Get", GetResponse.parseFrom)
  val putEndpoint = newEndpoint[PutRequest, PutResponse]("Put", PutResponse.parseFrom)
  val casEndpoint = newEndpoint[ConditionalPutRequest, ConditionalPutResponse]("ConditionalPut", ConditionalPutResponse.parseFrom)
  val incrementEndpoint = newEndpoint[IncrementRequest, IncrementResponse]("Increment", IncrementResponse.parseFrom)
  val deleteEndpoint = newEndpoint[DeleteRequest, DeleteResponse]("Delete", DeleteResponse.parseFrom)
  val deleteRangeEndpoint = newEndpoint[DeleteRangeRequest, DeleteRangeResponse]("DeleteRange", DeleteRangeResponse.parseFrom)
  val scanEndpoint = newEndpoint[ScanRequest, ScanResponse]("Scan", ScanResponse.parseFrom)
  val batchEndpoint = newEndpoint[BatchRequest, BatchResponse]("Batch", BatchResponse.parseFrom)
  val endTxEndpoint = newEndpoint[EndTransactionRequest, EndTransactionResponse]("EndTransaction", EndTransactionResponse.parseFrom)

  private[this] def newEndpoint[Req <: GeneratedMessage <% CockroachRequest[Req], Res <: GeneratedMessage <% CockroachResponse[Res]](name: String, parseFrom: (InputStream) => Res): Service[Req, Res] = {
    val cockroach = ProtobufFilter[Req, Res](name, parseFrom) andThen client
    if(ScroachLog.isLoggable(Level.TRACE)) LoggingFilter[Req, Res](name) andThen cockroach else cockroach
  }
}
