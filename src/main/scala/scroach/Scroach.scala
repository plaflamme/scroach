package scroach

import java.net.InetSocketAddress

import com.google.protobuf.{MessageLite, ByteString}
import com.twitter.app.App
import com.twitter.concurrent.{SpoolSource, Spool}
import com.twitter.conversions.time._
import com.twitter.finagle._
import com.twitter.finagle.service.Backoff
import com.twitter.util._
import proto._

// TODO: handle bytes vs. counter values. For example get for a counter would return Future[Long]
// TODO: handle timestamps. Potentially wrap Value into a a union?
trait Client {
  def contains(key: Bytes): Future[Boolean]
  def get(key: Bytes): Future[Option[Bytes]]
  def put(key: Bytes, value: Bytes): Future[Unit]
  def put(key: Bytes, value: Long): Future[Unit]
  def compareAndSet(key: Bytes, previous: Option[Bytes], value: Option[Bytes]): Future[Unit]
  def increment(key: Bytes, amount: Long): Future[Long]
  def delete(key: Bytes): Future[Unit]
  def deleteRange(from: Bytes, to: Bytes, maxToDelete: Long = Long.MaxValue): Future[Long]
  def scan(from: Bytes, to: Bytes, bacthSize: Int = 256): Future[Spool[(Bytes, Bytes)]]
  def enqueueMessage(key: Bytes, message: Bytes): Future[Unit]
  def reapQueue(key: Bytes, maxItems: Int): Future[Seq[Bytes]]
  def tx[T](isolation: IsolationType.EnumVal = IsolationType.SERIALIZABLE)(f: Kv => Future[T]): Future[T]
}

sealed trait Batch[+A] {

  type BatchRunner = Seq[RequestUnion] => Future[Seq[Try[ResponseUnion]]]

  import Batch.{Pending, Complete}

  def map[B](f: A => B): Batch[B]

  def flatMap[B](f: A => Batch[B]): Batch[B]

  def run(r: BatchRunner): Future[A]

  def join[B](b: Batch[B]) = Batch.join(this, b)

  def join[B, C](b: Batch[B], c: Batch[C]) = Batch.join(this, b, c)

  def zip[B](other: Batch[B]): Batch[(A,B)] = (this, other) match {
    case (Complete(a), Complete(b)) => {
      Complete(
        for { x <- a; y <- b} yield (x,y)
      )
    }
    case (Pending(r, h), c@Complete(_)) => Pending(r, h andThen { o => o zip c })
    case (c@Complete(_), Pending(r, h)) => Pending(r, h andThen { o => c zip o })
    case (Pending(l, lh), Pending(r, rh)) => {
      Pending(l ++ r, { out =>
        lh(out.take(l.size)) zip rh(out.drop(l.size))
      })
    }
  }

}

object Batch {

  def single(req: RequestUnion): Batch[ResponseUnion] = {
    Pending(Seq(req), { r => Complete(r.head) })
  }

  def const[A](a: A): Batch[A] = Complete(Return(a))

  def exception[A](ex: Throwable): Batch[A] = Complete(Throw[A](ex))

  def collect[T](batches: Seq[Batch[T]]): Batch[Seq[T]] = {
    batches.size match {
      case 0 => const(Seq.empty)
      case 1 => batches.head.map { v => Seq(v)}
      case n => {
        val (left, right) = batches.splitAt(n / 2)
        (collect(left) zip collect(right)).map { case (l, r) => l ++ r}
      }
    }
  }

  def join[A,B](a: Batch[A], b: Batch[B]): Batch[(A,B)] = {
    a zip b
  }

  def join[A,B,C](a: Batch[A], b: Batch[B], c: Batch[C]): Batch[(A,B,C)] = {
    Batch.collect(Seq(a,b,c))
      .map { case Seq(a,b,c) => (a.asInstanceOf[A], b.asInstanceOf[B], c.asInstanceOf[C]) }
  }
  // TODO: write joins with a macro?

  private final case class Complete[A](value: Try[A]) extends Batch[A] {

    def map[B](f: A => B): Batch[B] = {
      Complete(value.map(f))
    }

    def flatMap[B](f: A => Batch[B]): Batch[B] = {
      value match {
        case Return(a) => f(a)
        case Throw(t) => Batch.exception(t)
      }
    }

    override def run(f: BatchRunner): Future[A] = Future.const(value)
  }

  private final case class Pending[A](reqs: Seq[RequestUnion], handler: Seq[Try[ResponseUnion]] => Batch[A]) extends Batch[A] {
    def map[B](f: A => B): Batch[B] = {
      Pending(reqs, handler andThen { _.map(f) })
    }

    def flatMap[B](f: A => Batch[B]): Batch[B] = {
      Pending(reqs, handler andThen { _.flatMap(f) })
    }

    override def run(f: BatchRunner): Future[A] = {
      for {
        out <- f(reqs)
        a <- handler(out).run(f)
      } yield a
    }

  }

}

trait BatchClient {
  def contains(key: Bytes): Batch[Boolean]
  def get(key: Bytes): Batch[Option[Bytes]]
  def put(key: Bytes, value: Bytes): Batch[Unit]
  def put(key: Bytes, value: Long): Batch[Unit]
  def compareAndSet(key: Bytes, previous: Option[Bytes], value: Option[Bytes]): Batch[Unit]
  def increment(key: Bytes, amount: Long): Batch[Long]
  def delete(key: Bytes): Batch[Unit]
  def deleteRange(from: Bytes, to: Bytes, maxToDelete: Long = Long.MaxValue): Batch[Long]
  // TODO: scan, we can make a simple version (req -> res), but I need to figure out how Spool interacts with batching
  def enqueueMessage(key: Bytes, message: Bytes): Batch[Unit]
  def reapQueue(key: Bytes, maxItems: Int): Batch[Seq[Bytes]]
  // TODO: endTx, is the transaction on the batch or on individual requests?
  // If it's on the batch, then constructing a BatchClient with a TxKv would "just work"
  // If it's on individual requests, then that's a lot more complicated since we would need to replay portions of batches to handle tx retries.

  def run[A](batch: Batch[A]): Future[A]
}

private[scroach] object ResponseHandlers {

  def handler[M <: MessageLite <% CockroachResponse[M], T](f: M => T): M => T = { res =>
    res.header match {
      case HasError(err) => throw new CockroachException(err, res)
      case NoError(_) => f(res)
    }
  }

  def noOpHandler[M <: MessageLite <% CockroachResponse[M]] = handler[M, Unit] { _ => () }

  val contains = handler[ContainsResponse, Boolean] {
    case ContainsResponse(_, Some(contains)) => contains
    case ContainsResponse(NoError(_), None) => false
  }
  val get = handler[GetResponse, Option[Bytes]] {
    case GetResponse(_, Some(BytesValue(bytes))) => Some(bytes)
    case GetResponse(NoError(_), Some(Value(None, _, _, _, _))) => None
    case GetResponse(NoError(_), None) => None
  }
  val put = noOpHandler[PutResponse]
  val cas: ConditionalPutResponse => Unit = {
    case ConditionalPutResponse(HasError(err), Some(actual)) => throw ConditionFailedException(actual.bytes.map(_.toByteArray))
    case res@ConditionalPutResponse(HasError(err), None) => throw CockroachException(err, res)
    case _ => ()
  }
  val increment = handler[IncrementResponse, Long] {
    case IncrementResponse(_, Some(newValue)) => newValue
  }
  val delete = noOpHandler[DeleteResponse]
  val deleteRange = handler[DeleteRangeResponse, Long] {
    case DeleteRangeResponse(_, Some(deleted)) => deleted
  }
  val reapQueue = handler[ReapQueueResponse, Seq[Bytes]] {
    case ReapQueueResponse(NoError(_), values) => values.collect {
      case BytesValue(bytes) => bytes
    }
  }
  val enqueueMessage = noOpHandler[EnqueueMessageResponse]
}

case class KvBatchClient(kv: Kv, user: String) extends BatchClient {
  private[this] val someUser = Some(user)
  private[this] def header() = {
    RequestHeader(user = someUser)
  }
  private[this] def header(key: Bytes) = {
    RequestHeader(user = someUser, key = Option(key).map(ByteString.copyFrom(_)))
  }

  def batch[Req <: MessageLite, Res, R](req: CockroachRequest[Req], ex: ResponseUnion => Option[Res], handler: Res => R) = {
    Batch.single(req.batched)
      .map { ex(_) }
      .map {
        case Some(response) => handler(response)
        case None => throw new RuntimeException("invalid response type")
      }
  }
  def contains(key: Bytes): Batch[Boolean] = {
    batch(ContainsRequest(header = header(key)), _.contains, ResponseHandlers.contains)
  }
  def get(key: Bytes): Batch[Option[Bytes]] = {
    batch(ContainsRequest(header = header(key)), _.get, ResponseHandlers.get)
  }
  def put(key: Bytes, value: Bytes): Batch[Unit] = {
    batch(PutRequest(header = header(key), value = Some(Value(bytes = Some(ByteString.copyFrom(value))))), _.put, ResponseHandlers.put)
  }
  def put(key: Bytes, value: Long): Batch[Unit] = {
    batch(PutRequest(header = header(key), value = Some(Value(integer = Some(value)))), _.put, ResponseHandlers.put)
  }
  def compareAndSet(key: Bytes, previous: Option[Bytes], value: Option[Bytes]): Batch[Unit] = {
    val req = ConditionalPutRequest(
      header = header(key),
      expValue = previous.map(p => Value(bytes = Some(ByteString.copyFrom(p)))),
      value = Value(bytes = value.map(ByteString.copyFrom(_)))
    )
    batch(req, _.conditionalPut,  ResponseHandlers.cas)
  }
  def increment(key: Bytes, amount: Long): Batch[Long] = {
    val req = IncrementRequest(
      header = header(key),
      increment = amount
    )
    batch(req, _.increment,  ResponseHandlers.increment)
  }
  def delete(key: Bytes): Batch[Unit] = {
    val req = DeleteRequest(header = header(key))
    batch(req, _.delete,  ResponseHandlers.delete)
  }
  def deleteRange(from: Bytes, to: Bytes, maxToDelete: Long = Long.MaxValue): Batch[Long] = {
    require(maxToDelete >= 0, "maxToDelete must be >= 0")
    require(from <= to, "from should be less or equal to") // TODO: should from be strictly less than to?
    if(maxToDelete == 0) Batch.const(0) // short-circuit
    else {
      val h = header(from).copy(endKey = Some(ByteString.copyFrom(to)))
      val req = DeleteRangeRequest(header = h, maxEntriesToDelete = maxToDelete)
      batch(req, _.deleteRange,  ResponseHandlers.deleteRange)
    }
  }
  def enqueueMessage(key: Bytes, message: Bytes): Batch[Unit] = {
    val req = EnqueueMessageRequest(header = header(key), msg = Value(bytes = Some(ByteString.copyFrom(message))))
    batch(req, _.enqueueMessage,  ResponseHandlers.enqueueMessage)
  }
  def reapQueue(key: Bytes, maxItems: Int): Batch[Seq[Bytes]] = {
    require(maxItems > 0, "maxItems must be > 0")
    val req = ReapQueueRequest(header = header(key), maxResults = maxItems.toLong)
    batch(req, _.reapQueue,  ResponseHandlers.reapQueue)
  }

  def run[A](batch: Batch[A]): Future[A] = {
    batch.run { input =>
      // TODO: short-circuit batches of zero
      // TODO: short-circuit batches of one (?)
      val batchRequest = BatchRequest(header = header(), requests = input.toVector)
      kv.batchEndpoint(batchRequest)
        .map { r =>
          r.responses.map(Return(_)) // TODO: the Try semantics should be moved down into the response type e.g: Try[ContainsResponse]
        }
    }
  }
}

case class ConditionFailedException(actualValue: Option[Bytes]) extends RuntimeException

case class KvClient(kv: Kv, user: String) extends Client {
  private[this] val someUser = Some(user)
  private[this] def header() = {
    RequestHeader(user = someUser)
  }
  private[this] def header(key: Bytes) = {
    RequestHeader(user = someUser, key = Option(key).map(ByteString.copyFrom(_)))
  }

  def contains(key: Bytes): Future[Boolean] = {
    kv.containsEndpoint(ContainsRequest(header = header(key)))
      .map { ResponseHandlers.contains }
  }

  def get(key: Bytes): Future[Option[Bytes]] = {
    val req = GetRequest(header = header(key))
    kv.getEndpoint(req)
      .map { ResponseHandlers.get }
  }

  def put(key: Bytes, value: Bytes): Future[Unit] = {
    val req = PutRequest(header = header(key), value = Some(Value(bytes = Some(ByteString.copyFrom(value)))))
    kv.putEndpoint(req).map { ResponseHandlers.put }
  }

  def put(key: Bytes, value: Long): Future[Unit] = {
    val req = PutRequest(header = header(key), value = Some(Value(integer = Some(value))))
    kv.putEndpoint(req).map { ResponseHandlers.put }
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
    kv.casEndpoint(req)
      .handle {
        case CockroachException(err, ConditionalPutResponse(_, actual)) => throw ConditionFailedException(actual.flatMap(_.bytes).map(_.toByteArray))
      }
      .unit
  }

  def increment(key: Bytes, amount: Long): Future[Long] = {
    val req = IncrementRequest(header = header(key), increment = amount)
    kv.incrementEndpoint(req).map { ResponseHandlers.increment }
  }

  def delete(key: Bytes): Future[Unit] = {
    val deleteRequest = DeleteRequest(header = header(key))
    kv.deleteEndpoint(deleteRequest).map { ResponseHandlers.delete }
  }

  def deleteRange(from: Bytes, to: Bytes, maxToDelete: Long): Future[Long] = {
    require(maxToDelete >= 0, "maxToDelete must be >= 0")
    require(from <= to, "from should be less or equal to") // TODO: should from be strictly less than to?
    if(maxToDelete == 0) Future.value(0) // short-circuit
    else {
      val h = header(from).copy(endKey = Some(ByteString.copyFrom(to)))
      val req = DeleteRangeRequest(header = h, maxEntriesToDelete = maxToDelete)
      kv.deleteRangeEndpoint(req).map { ResponseHandlers.deleteRange }
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
            case ScanResponse(NoError(_), rows) => rows.collect {
              case KeyValue(key, BytesValue(bytes)) => (key.toByteArray, bytes)
            }
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
    kv.reapQueueEndpoint(req).map { ResponseHandlers.reapQueue }
  }

  def enqueueMessage(key: Bytes, message: Bytes): Future[Unit] = {
    val req = EnqueueMessageRequest(header = header(key), msg = Value(bytes = Some(ByteString.copyFrom(message))))
    kv.enqueueEndpoint(req).map { ResponseHandlers.enqueueMessage }
  }

  // Exponential backoff from 50.ms to 3600.ms then constant 5.seconds
  private[this] val DefaultBackoffs = Backoff.exponential(50.milliseconds, 2).take(6) ++ Backoff.const(5.seconds)

  def tx[T](isolation: IsolationType.EnumVal = IsolationType.SERIALIZABLE)(f: Kv => Future[T]): Future[T] = {
    val txKv = TxKv(kv, isolation = isolation)

    def tryTx(backoffs: Stream[Duration]): Future[T] = {
      f(txKv)
        .rescue {
          case CockroachException(err, _) if(err.readWithinUncertaintyInterval.isDefined) => tryTx(DefaultBackoffs)
        }
        .liftToTry
        .flatMap { result =>
          val endRequest = EndTransactionRequest(header = header(), commit = Some(result.isReturn))
          txKv.endTxEndpoint(endRequest)
            .map {
              case EndTransactionResponse(NoError(_), _) => TxComplete
            }
            .handle {
              case CockroachException(err, _) if(err.transactionRetry.isDefined) => TxRetry
              case CockroachException(err, _) if(err.transactionAborted.isDefined || err.transactionPush.isDefined) => TxAbort
            }
            .map { _ -> result }
        }
        .flatMap {
          case (TxComplete, result) => Future.const(result)
          case (TxRetry, _) => tryTx(DefaultBackoffs)
          case (TxAbort, _) => backoffs match {
            case howlong #:: rest => Future sleep(howlong) before tryTx(rest)
            case _ => Future.exception(new RuntimeException) // This should never happen since the backoff stream is unbounded.
          }
          case _ => Future.exception(new RuntimeException) // TODO: better error semantics
        }
    }

    tryTx(DefaultBackoffs)
  }
}

object Scroach extends App {

  val hosts = flag("hosts", Seq(new InetSocketAddress(8080)), "Host:port of cockroach node(s)")
  val user = flag("user", "root", "Cockroach username.")

  def main() {

    val kv = {
      val http = Httpx.newClient(Name.bound(hosts(): _*), "cockroach").toService
      closeOnExit(http)
      HttpKv(http)
    }

    val client = KvClient(kv, user())

    // TODO: REPL
  }
}
