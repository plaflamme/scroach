package scroach

import scroach.proto._
import com.google.protobuf.ByteString
import com.twitter.concurrent.{SpoolSource, Spool}
import com.twitter.finagle.service.Backoff
import com.twitter.util.{Duration, Future}
import com.twitter.conversions.time._
import cockroach.proto._

// TODO: handle bytes vs. counter values. For example get for a counter would return Future[Long]
// TODO: handle timestamps. Potentially wrap Value into a a union?
trait BaseClient {
  def get(key: Bytes): Future[Option[Bytes]]
  def put(key: Bytes, value: Bytes): Future[Unit]
  def put(key: Bytes, value: Long): Future[Unit]
  def compareAndSet(key: Bytes, previous: Option[Bytes], value: Option[Bytes]): Future[Unit]
  def increment(key: Bytes, amount: Long): Future[Long]
  def delete(key: Bytes): Future[Unit]
  def deleteRange(from: Bytes, to: Bytes, maxToDelete: Long = Long.MaxValue): Future[Long]
  def scan(from: Bytes, to: Bytes, bacthSize: Int = 256): Future[Spool[(Bytes, Bytes)]]
//  def enqueueMessage(key: Bytes, message: Bytes): Future[Unit]
//  def reapQueue(key: Bytes, maxItems: Int): Future[Seq[Bytes]]
  def batched: BatchClient
}
trait TxClient extends BaseClient
trait Client extends BaseClient {
  def tx[T](isolation: IsolationType = IsolationType.SERIALIZABLE, user: Option[String] = None, priority: Option[Int] = None)(f: TxClient => Future[T]): Future[T]
}

case class KvClient(kv: Kv, user: String, priority: Option[Int] = None) extends Client {
  private[this] val someUser = Some(user)
  private[this] def header(): RequestHeader = {
    RequestHeader(user = someUser, userPriority = priority)
  }
  private[this] def header(key: Bytes): RequestHeader = {
    header().copy(key = Option(key).map(ByteString.copyFrom(_)))
  }

  def get(key: Bytes): Future[Option[Bytes]] = {
    val req = GetRequest(header = header(key))
    kv.getEndpoint(req)
      .map { ResponseHandlers.get }
  }

  def put(key: Bytes, value: Bytes): Future[Unit] = {
    val req = PutRequest(header = header(key), value = Some(Value(valiu = Value.Valiu.Bytes(ByteString.copyFrom(value)))))
    kv.putEndpoint(req).map { ResponseHandlers.put }
  }

  def put(key: Bytes, value: Long): Future[Unit] = {
    val req = PutRequest(header = header(key), value = Some(Value(valiu = Value.Valiu.Integer(value))))
    kv.putEndpoint(req).map { ResponseHandlers.put }
  }

  def compareAndSet(key: Bytes, previous: Option[Bytes], value: Option[Bytes]): Future[Unit] = {
    // TODO: verify the semantics of the params:
    // expValue == None means value should be missing or should it be Some(Value(None)) ?
    // value is required, how do you compare-and-delete?
    val req = ConditionalPutRequest(
      header = header(key),
      expValue = previous.map(p => Value(valiu = Value.Valiu.Bytes(ByteString.copyFrom(p)))),
      value = Some(Value(valiu = value.map(v => Value.Valiu.Bytes(ByteString.copyFrom(v))).getOrElse(Value.Valiu.Empty)))
    )
    kv.casEndpoint(req).map { ResponseHandlers.cas }
  }

  def increment(key: Bytes, amount: Long): Future[Long] = {
    val req = IncrementRequest(header = header(key), increment = Some(amount))
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
      val req = DeleteRangeRequest(header = h, maxEntriesToDelete = Some(maxToDelete))
      kv.deleteRangeEndpoint(req).map { ResponseHandlers.deleteRange }
    }
  }

  def scan(from: Bytes, to: Bytes, batchSize: Int): Future[Spool[(Bytes, Bytes)]] = {
    require(batchSize > 0, "batchSize must be > 0")

    // TODO: lazy scan
    val spool = new SpoolSource[(Bytes, Bytes)]()
    def scanBatch(start: Bytes): Future[Unit] = {
      val scan = if(start >= to) Future.value(Seq.empty)
      else {
        val h = header(start).withEndKey(ByteString.copyFrom(to))
        val req = ScanRequest(header = h, maxResults = Some(batchSize))
        kv.scanEndpoint(req).map(ResponseHandlers.scan)
      }
      scan.flatMap { values =>
        if(values.nonEmpty) {
          values.map(spool.offer(_))
          scanBatch(values.last._1.next)
        } else {
          spool.close()
          Future.Done
        }
      }
    }

    val s = spool()
    scanBatch(from) onFailure spool.raise
    s
  }
/*
  def reapQueue(key: Bytes, maxItems: Int): Future[Seq[Bytes]] = {
    require(maxItems > 0, "maxItems must be > 0")
    val req = ReapQueueRequest(header = header(key), maxResults = maxItems.toLong)
    kv.reapQueueEndpoint(req).map { ResponseHandlers.reapQueue }
  }

  def enqueueMessage(key: Bytes, message: Bytes): Future[Unit] = {
    val req = EnqueueMessageRequest(header = header(key), msg = Value(bytes = Some(ByteString.copyFrom(message))))
    kv.enqueueEndpoint(req).map { ResponseHandlers.enqueueMessage }
  }
*/
  // Exponential backoff from 50.ms to 3600.ms then constant 5.seconds
  private[this] val DefaultBackoffs = Backoff.exponential(50.milliseconds, 2).take(6) ++ Backoff.const(5.seconds)

  def tx[T](isolation: IsolationType = IsolationType.SERIALIZABLE, txUser: Option[String] = None, priority: Option[Int] = priority)(f: TxClient => Future[T]): Future[T] = {
    val txKv = TxKv(kv, isolation = isolation)
    val txClient = new KvClient(txKv, txUser getOrElse user, priority) with TxClient

    def tryTx(backoffs: Stream[Duration]): Future[T] = {
      f(txClient)
        .liftToTry
        .flatMap { result =>
          val header = RequestHeader(user = txUser orElse someUser, userPriority = priority)
          val endTxRequest = EndTransactionRequest(header = header, commit = Some(result.isReturn))
          txKv.endTxEndpoint(endTxRequest)
            .map {
              case EndTransactionResponse(NoError(_), _, _) => TxComplete
              case EndTransactionResponse(HasError(err), _, _) if(err.getTransactionRestart == TransactionRestart.IMMEDIATE) => TxRetry
              case EndTransactionResponse(HasError(err), _, _) if(err.getTransactionRestart == TransactionRestart.BACKOFF) => TxBackoff
              case response@EndTransactionResponse(HasError(err), _, _) if(err.getTransactionRestart == TransactionRestart.ABORT) => throw new CockroachException(err, response)
            }
            .map { tx: TxResult => tx -> result } // Typing tx as TxResult helps the compiler with the pattern match below
        }
        .flatMap {
          case (TxComplete, result) => Future.const(result)
          case (TxRetry, _) => tryTx(DefaultBackoffs)
          case (TxBackoff, _) => backoffs match {
            case howlong #:: rest => Future sleep(howlong) before tryTx(rest)
            case _ => Future.exception(new RuntimeException) // This should never happen since the backoff stream is unbounded.
          }
        }
    }

    tryTx(DefaultBackoffs)
  }

  def batched = KvBatchClient(kv, user)
}
