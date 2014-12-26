package scroach

import com.google.protobuf.ByteString
import com.twitter.concurrent.{SpoolSource, Spool}
import com.twitter.finagle.service.Backoff
import com.twitter.util.{Duration, Future}
import com.twitter.conversions.time._
import scroach.proto._

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
        kv.scanEndpoint(req).map(ResponseHandlers.scan)
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
            .map { tx: TxResult => tx -> result } // Typing tx as TxResult helps the compiler with the pattern match below
        }
        .flatMap {
          case (TxComplete, result) => Future.const(result)
          case (TxRetry, _) => tryTx(DefaultBackoffs)
          case (TxAbort, _) => backoffs match {
            case howlong #:: rest => Future sleep(howlong) before tryTx(rest)
            case _ => Future.exception(new RuntimeException) // This should never happen since the backoff stream is unbounded.
          }
        }
    }

    tryTx(DefaultBackoffs)
  }
}
