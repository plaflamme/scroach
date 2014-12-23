package scroach

import java.io.InputStream
import java.net.InetSocketAddress
import java.util.concurrent.atomic.AtomicReference

import com.google.protobuf.{MessageLite, ByteString}
import com.twitter.app.App
import com.twitter.concurrent.{SpoolSource, Spool}
import com.twitter.finagle._
import com.twitter.finagle.httpx.{Request, Response, RequestBuilder}
import com.twitter.io.Charsets
import com.twitter.util.{ Future, Await}
import proto._

import scala.util

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
  val endTxEndpoint: Service[EndTransactionRequest, EndTransactionResponse]

}

// TODO: consider using StackClient so users can stack custom layers such as logging
case class HttpKv(client: Service[Request, Response]) extends Kv {

  private[this] case class LoggingFilter[Req, Res]() extends SimpleFilter[Req,Res] {
    def apply(req: Req, service: Service[Req,Res]) = {
      println(s"req == $req")
      service(req)
        .map { res =>
          println(s"res == $res")
          res
        }
    }
  }
  private[this] case class ProtobufFilter[Req <: MessageLite, Res <: MessageLite](cmd: String, parse: (InputStream) => Res) extends Filter[Req, Res, Request, Response] {
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
  val endTxEndpoint = newEndpoint[EndTransactionRequest, EndTransactionResponse]("EndTransaction", EndTransactionResponse.parseFrom)

  private[this] def newEndpoint[Req <: MessageLite, Res <: MessageLite](name: String, parseFrom: (InputStream) => Res): Service[Req, Res] = {
    // TODO: retrying semantics for 429 and possibly 500s
    LoggingFilter[Req, Res] andThen ProtobufFilter[Req, Res](name, parseFrom) andThen client
  }
}

trait TxResult
case object TxComplete extends TxResult
case object TxAbort extends TxResult
case object TxRetry extends TxResult

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
          response.tx() match {
            case Some(niu) => {
              tx.set(merge(tx.get, niu))
              response
            }
            case None => response
          }
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
  val endTxEndpoint = TxFilter[EndTransactionRequest, EndTransactionResponse]() andThen kv.endTxEndpoint

}

// TODO: handle bytes vs. counter values.
trait Client {
  def contains(key: Bytes): Future[Boolean]
  def get(key: Bytes): Future[Option[Bytes]]
  def put(key: Bytes, value: Bytes): Future[Unit]
  def compareAndSet(key: Bytes, previous: Option[Bytes], value: Option[Bytes]): Future[Unit]
  def increment(key: Bytes, amount: Long): Future[Long]
  def delete(key: Bytes): Future[Unit]
  def deleteRange(from: Bytes, to: Bytes, maxToDelete: Long = 0): Future[Long]
  def scan(from: Bytes, to: Bytes, bacthSize: Int = 256): Future[Spool[(Bytes, Bytes)]]
  def enqueueMessage(key: Bytes, message: Bytes): Future[Unit]
  def reapQueue(key: Bytes, maxItems: Int): Future[Seq[Bytes]]
  def tx[T](isolation: IsolationType.EnumVal = IsolationType.SERIALIZABLE)(f: Kv => Future[T]): Future[T]
}

case class DefaultClient(kv: Kv, user: String) extends Client {
  private[this] val someUser = Some(user)
  private[this] def header(key: Bytes) = {
    RequestHeader(user = someUser, key = Option(key).map(ByteString.copyFrom(_)))
  }

  def contains(key: Bytes): Future[Boolean] = {
    val req = ContainsRequest(header = header(key))
    kv.containsEndpoint(req)
      .map {
      case ContainsResponse(_, Some(contains)) => contains
      case ContainsResponse(ResponseHeader(None, _, _), None) => false
      case r => throw new RuntimeException(r.toString) // TODO: better error semantics
    }
  }

  def get(key: Bytes): Future[Option[Bytes]] = {
    val req = GetRequest(header = header(key))
    kv.getEndpoint(req)
      .map {
        case GetResponse(_, Some(Value(bytes, _, _, _, _))) => bytes.map(_.toByteArray)
        case GetResponse(ResponseHeader(None, _, _), None) => None
        case r => throw new RuntimeException(r.toString) // TODO: better error semantics
      }
  }

  def put(key: Bytes, value: Bytes): Future[Unit] = {
    val req = PutRequest(header = header(key), value = Some(Value(bytes = Some(ByteString.copyFrom(value)))))
    kv.putEndpoint(req).unit // TODO: error handling
  }

  def compareAndSet(key: Bytes, previous: Option[Bytes], value: Option[Bytes]): Future[Unit] = {
    // TODO: verify the semantics of the params:
    // expValue == None means value should be missing or should it be Some(Value(None)) ?
    // value is required, how do you compare-and-delete?
    val req = ConditionalPutRequest(
      header = header(key),
      expValue = previous.map(p => Value(bytes = Some(ByteString.copyFrom(p)))),
      value = Value(bytes = value.map(ByteString.copyFrom(_)))
    )
    kv.casEndpoint(req).unit // TODO: error handling
  }

  def increment(key: Bytes, amount: Long): Future[Long] = {
    val req = IncrementRequest(
      header = header(key),
      increment = amount
    )
    kv.incrementEndpoint(req)
      .map {
        case IncrementResponse(_, Some(newValue)) => newValue
        case IncrementResponse(ResponseHeader(Some(err), _, _), _) => throw new RuntimeException(err.toString) // TODO: better error semantics
      }
  }

  def delete(key: Bytes): Future[Unit] = {
    val deleteRequest = DeleteRequest(header = header(key))
    kv.deleteEndpoint(deleteRequest).unit // TODO: error handling
  }

  def deleteRange(from: Bytes, to: Bytes, maxToDelete: Long): Future[Long] = {
    require(maxToDelete > 0, "maxToDelete must be >= 0")
    if(maxToDelete == 0) Future.value(0) // short-circuit
    else {
      val h = header(from).copy(endKey = Some(ByteString.copyFrom(to)))
      val req = DeleteRangeRequest(header = h, maxEntriesToDelete = maxToDelete)
      kv.deleteRangeEndpoint(req)
        .map {
          case DeleteRangeResponse(_, Some(deleted)) => deleted
          case DeleteRangeResponse(ResponseHeader(Some(err), _, _), _) => throw new RuntimeException(err.toString) // TODO: better error semantics
        }
    }
  }

  def scan(from: Bytes, to: Bytes, batchSize: Int): Future[Spool[(Bytes, Bytes)]] = {
    require(batchSize > 0, "batchSize must be > 0")

    def scanBatch(start: Bytes): Future[Seq[(Bytes, Bytes)]] = {
      if(from >= to) Future.value(Seq.empty) // short-circuit
      else {
        val h = header(start).copy(endKey = Some(ByteString.copyFrom(to)))
        val req = ScanRequest(header = h, maxResults = batchSize)
        kv.scanEndpoint(req)
          .map {
            case ScanResponse(ResponseHeader(None, _, _), rows) => rows.collect {
              case KeyValue(key, BytesValue(bytes)) => (key.toByteArray, bytes)
            }
            case ScanResponse(ResponseHeader(Some(err), _, _), _) => throw new RuntimeException(err.toString) // TODO: better error semantics
          }
      }
    }

    // TODO: lazy scan
    val spool = new SpoolSource[(Bytes, Bytes)]()
    scanBatch(from)
      .flatMap { values =>
        if(values.nonEmpty) {
          values.map(spool.offer(_))
          scanBatch(values.last._1.next)
        } else {
          spool.close()
          Future.Done
        }
      }
    spool()
  }

  def reapQueue(key: Bytes, maxItems: Int): Future[Seq[Bytes]] = {
    require(maxItems > 0, "maxItems must be > 0")
    val req = ReapQueueRequest(header = header(key), maxResults = maxItems.toLong)
    kv.reapQueueEndpoint(req)
      .map {
        case ReapQueueResponse(ResponseHeader(None, _, _), values) => values.collect {
          case BytesValue(bytes) => bytes
        }
      }
  }

  def enqueueMessage(key: Bytes, message: Bytes): Future[Unit] = {
    val req = EnqueueMessageRequest(header = header(key), msg = Value(bytes = Some(ByteString.copyFrom(message))))
    kv.enqueueEndpoint(req).unit // TODO: error handling
  }

  def tx[T](isolation: IsolationType.EnumVal = IsolationType.SERIALIZABLE)(f: Kv => Future[T]): Future[T] = {
    val txKv = TxKv(kv, isolation = isolation)
    def tryTx(): Future[T] = { // TODO: limit and backoff
      f(txKv)
        .flatMap { result =>
          val endRequest = EndTransactionRequest(header = header(null), commit = Some(true)) // TODO: get rid of null here
          txKv.endTxEndpoint(endRequest)
            .map {
              case EndTransactionResponse(ResponseHeader(Some(err), _, _), _) if(err.transactionRetry.isDefined) => TxRetry
              case EndTransactionResponse(ResponseHeader(Some(err), _, _), _) if(err.transactionAborted.isDefined) => TxAbort
              case EndTransactionResponse(ResponseHeader(None, _, _), _) => TxComplete
            }
            .map { _ -> result}
        }
        .flatMap {
          case (TxComplete, result) => Future.value(result)
          case (TxRetry, _) => tryTx()
          case _ => Future.exception(new RuntimeException) // TODO: handle TxAbort
        }
    }
    tryTx()
  }
}

object Scroach extends App {

  val hosts = flag("hosts", Seq(new InetSocketAddress(8080)), "Host:port of cockroach node(s)")
  val user = flag("user", "root", "Cockroach username.")

  val key = "Cockroach".getBytes(Charsets.Utf8)
  val value = "Hello".getBytes(Charsets.Utf8)

  def main() {

    val kv = {
      val http = Httpx.newClient(Name.bound(hosts(): _*), "cockroach").toService
      closeOnExit(http)
      HttpKv(http)
    }

    val client = DefaultClient(kv, user())

    def print(msg: String, v: Option[Bytes]) = {
      println(msg + (v.fold("None") { new String(_, Charsets.Utf8)}))
    }

    Await.result {
      for {
        _ <- client.delete(key)
        tx <- client.tx(IsolationType.SNAPSHOT) { kv =>
          val rich = DefaultClient(kv, user())
          for {
            _ <- rich.put(key, value)
            got <- rich.get(key)
            _ = print(s"in tx: ", got)
            out <- client.get(key)
            _ = print("out tx: ", out)
          } yield got
        }
        got <- client.get(key)
        _ = print("after tx: ", got)
      } yield ()
    }

  }
}
