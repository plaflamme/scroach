package scroach

import com.google.protobuf.{MessageLite, ByteString}
import com.twitter.util.{Throw, Return, Try, Future}
import scroach.proto._

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

case class KvBatchClient(kv: Kv, user: String) extends BatchClient {
  private[this] val someUser = Some(user)
  private[this] def header() = {
    RequestHeader(user = someUser)
  }
  private[this] def header(key: Bytes) = {
    RequestHeader(user = someUser, key = Option(key).map(ByteString.copyFrom(_)))
  }
  private[this] def batch[Req <: MessageLite, Res, R](req: CockroachRequest[Req], ex: ResponseUnion => Option[Res], handler: Res => R) = {
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
    batch(GetRequest(header = header(key)), _.get, ResponseHandlers.get)
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
