package scroach

import com.trueaccord.scalapb.GeneratedMessage
import scroach.proto._
import com.google.protobuf.{MessageLite, ByteString}
import com.twitter.util.{Throw, Return, Try, Future}
import cockroach.proto._

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
  def get(key: Bytes): Batch[Option[Bytes]]
  def getCounter(key: Bytes): Batch[Option[Long]]
  def put(key: Bytes, value: Bytes): Batch[Unit]
  def put(key: Bytes, value: Long): Batch[Unit]
  def compareAndSet(key: Bytes, previous: Option[Bytes], value: Option[Bytes]): Batch[Unit]
  def increment(key: Bytes, amount: Long): Batch[Long]
  def delete(key: Bytes): Batch[Unit]
  def deleteRange(from: Bytes, to: Bytes, maxToDelete: Long = Long.MaxValue): Batch[Long]
  // Would require a Spool in terms of Batch to allow scanning in batches. So this is just a single scan request
  def scan(from: Bytes, to: Bytes, maxResults: Long = Long.MaxValue): Batch[Seq[(Bytes, Bytes)]]

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
  private[this] def batch[Req <: GeneratedMessage, Res, R](req: CockroachRequest[Req], ex: ResponseUnion => Option[Res], handler: Res => R) = {
    Batch.single(req.batched)
      .map { ex(_) }
      .map {
        case Some(response) => handler(response)
        case None => throw new RuntimeException("invalid response type")
      }
  }

  def get(key: Bytes): Batch[Option[Bytes]] = {
    batch(GetRequest(header = header(key)), _.value.get, ResponseHandlers.get)
  }

  def getCounter(key: Bytes): Batch[Option[Long]] = {
    batch(GetRequest(header = header(key)), _.value.get, ResponseHandlers.getCounter)
  }

  def put(key: Bytes, value: Bytes): Batch[Unit] = {
    batch(PutRequest(header = header(key), value = Some(Value(valiu = Value.Valiu.Bytes(ByteString.copyFrom(value))))), _.value.put, ResponseHandlers.put)
  }

  def put(key: Bytes, value: Long): Batch[Unit] = {
    batch(PutRequest(header = header(key), value = Some(Value(valiu = Value.Valiu.Integer(value)))), _.value.put, ResponseHandlers.put)
  }

  def compareAndSet(key: Bytes, previous: Option[Bytes], value: Option[Bytes]): Batch[Unit] = {
    val req = ConditionalPutRequest(
      header = header(key),
      expValue = previous.map(p => Value(valiu = Value.Valiu.Bytes(ByteString.copyFrom(p)))),
      value = Some(Value(valiu = value.map(v => Value.Valiu.Bytes(ByteString.copyFrom(v))).getOrElse(Value.Valiu.Empty)))
    )
    batch(req, _.value.conditionalPut,  ResponseHandlers.cas)
  }

  def increment(key: Bytes, amount: Long): Batch[Long] = {
    val req = IncrementRequest(
      header = header(key),
      increment = Some(amount)
    )
    batch(req, _.value.increment,  ResponseHandlers.increment)
  }

  def delete(key: Bytes): Batch[Unit] = {
    val req = DeleteRequest(header = header(key))
    batch(req, _.value.delete,  ResponseHandlers.delete)
  }

  def deleteRange(from: Bytes, to: Bytes, maxToDelete: Long = Long.MaxValue): Batch[Long] = {
    require(maxToDelete >= 0, "maxToDelete must be >= 0")
    require(from <= to, "from should be less or equal to") // TODO: should from be strictly less than to?
    if(maxToDelete == 0) Batch.const(0) // short-circuit
    else {
      val h = header(from).copy(endKey = Some(ByteString.copyFrom(to)))
      val req = DeleteRangeRequest(header = h, maxEntriesToDelete = Some(maxToDelete))
      batch(req, _.value.deleteRange,  ResponseHandlers.deleteRange)
    }
  }

  def scan(from: Bytes, to: Bytes, maxResults: Long): Batch[Seq[(Bytes, Bytes)]] = {
    require(from <= to, "from should be less or equal to") // TODO: should from be strictly less than to?

    if(from == to) Batch.const(Seq.empty)
    else {
      val h = header(from).copy(endKey = Some(ByteString.copyFrom(to)))
      val req = ScanRequest(header = h, maxResults = Some(maxResults))
      batch(req, _.value.scan, ResponseHandlers.scan)
    }
  }

  def run[A](batch: Batch[A]): Future[A] = {
    batch.run { input =>
      if(input.isEmpty) Future.value(Seq.empty)
      else {
        // TODO: short-circuit batches of one (?). If we do this, we could actually write the normal client in terms of this one.
        val batchRequest = BatchRequest(header = header(), requests = input.toVector)
        kv.batchEndpoint(batchRequest)
          .map {
            _.responses.map(Return(_)) // TODO: the Try semantics should be moved down into the response type e.g: Try[ContainsResponse]
          }
      }
    }
  }
}
