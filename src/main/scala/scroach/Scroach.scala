package scroach

import java.net.InetSocketAddress

import com.google.protobuf.{ByteString}
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
    val req = ContainsRequest(header = header(key))
    kv.containsEndpoint(req)
      .map {
        case ContainsResponse(_, Some(contains)) => contains
        case ContainsResponse(NoError(_), None) => false
        case r => throw new RuntimeException(r.toString) // TODO: better error semantics
      }
  }

  def get(key: Bytes): Future[Option[Bytes]] = {
    val req = GetRequest(header = header(key))
    kv.getEndpoint(req)
      .map {
        case GetResponse(_, Some(BytesValue(bytes))) => Some(bytes)
        case GetResponse(NoError(_), Some(Value(None, _, _, _, _))) => None
        case GetResponse(NoError(_), None) => None
        case r => throw new RuntimeException(r.toString) // TODO: better error semantics
      }
  }

  def put(key: Bytes, value: Bytes): Future[Unit] = {
    val req = PutRequest(header = header(key), value = Some(Value(bytes = Some(ByteString.copyFrom(value)))))
    kv.putEndpoint(req).unit // TODO: error handling
  }

  def put(key: Bytes, value: Long): Future[Unit] = {
    val req = PutRequest(header = header(key), value = Some(Value(integer = Some(value))))
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
    kv.casEndpoint(req)
      .map {
        case ConditionalPutResponse(ResponseHeader(Some(err), _, _), actual) => {
          throw ConditionFailedException(actual.flatMap(_.bytes).map(_.toByteArray))
        }
        case _ => ()
      }
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
    require(maxToDelete >= 0, "maxToDelete must be >= 0")
    require(from <= to, "from should be less or equal to") // TODO: should from be strictly less than to?
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
            case ScanResponse(NoError(_), rows) => rows.collect {
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
        case ReapQueueResponse(NoError(_), values) => values.collect {
          case BytesValue(bytes) => bytes
        }
      }
  }

  def enqueueMessage(key: Bytes, message: Bytes): Future[Unit] = {
    val req = EnqueueMessageRequest(header = header(key), msg = Value(bytes = Some(ByteString.copyFrom(message))))
    kv.enqueueEndpoint(req).unit // TODO: error handling
  }

  // Exponential backoff from 50.ms to 3600.ms then constant 5.seconds
  private[this] val DefaultBackoffs = Backoff.exponential(50.milliseconds, 2).take(6) ++ Backoff.const(5.seconds)

  def tx[T](isolation: IsolationType.EnumVal = IsolationType.SERIALIZABLE)(f: Kv => Future[T]): Future[T] = {
    val txKv = TxKv(kv, isolation = isolation)

    def tryTx(backoffs: Stream[Duration]): Future[T] = {
      f(txKv)
        .liftToTry
        .flatMap { result => // TODO: handle ReadWithinUncertaintyIntervalError as a retry
          val endRequest = EndTransactionRequest(header = header(), commit = Some(result.isReturn))
          txKv.endTxEndpoint(endRequest)
            .map {
              case EndTransactionResponse(ResponseHeader(Some(err), _, _), _) if(err.transactionRetry.isDefined) => TxRetry
              case EndTransactionResponse(ResponseHeader(Some(err), _, _), _) if(err.transactionAborted.isDefined || err.transactionPush.isDefined) => TxAbort
              case EndTransactionResponse(NoError(_), _) => TxComplete
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
