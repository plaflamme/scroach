package scroach

import java.net.InetSocketAddress

import com.google.protobuf.MessageLite
import com.trueaccord.scalapb.GeneratedMessage
import com.twitter.app.App
import com.twitter.finagle._
import scroach.proto._
import cockroach.proto._

private[scroach] object ResponseHandlers {

  def handler[M <: GeneratedMessage <% CockroachResponse[M], T](f: M => T): M => T = { res =>
    res.header match {
      case HasError(err) => throw new CockroachException(err, res)
      case NoError(_) => f(res)
    }
  }

  def noOpHandler[M <: GeneratedMessage <% CockroachResponse[M]] = handler[M, Unit] { _ => () }

  val get = handler[GetResponse, Option[Bytes]] {
    case GetResponse(_, Some(BytesValue(bytes))) => bytes
    case GetResponse(NoError(_), None) => None
  }
  val getCounter = handler[GetResponse, Option[Long]] {
    case GetResponse(_, Some(CounterValue(counter))) => counter
    case GetResponse(NoError(_), None) => None
  }
  val put = noOpHandler[PutResponse]
  val cas: ConditionalPutResponse => Unit = {
    case ConditionalPutResponse(ConditionFailed(ConditionFailedError(actual))) => throw ConditionFailedException(actual.map(_.getBytes).map(_.toByteArray))
    case res@ConditionalPutResponse(HasError(err)) => throw CockroachException(err, res)
    case _ => ()
  }
  val increment = handler[IncrementResponse, Long] {
    case IncrementResponse(_, Some(newValue)) => newValue
  }
  val delete = noOpHandler[DeleteResponse]
  val deleteRange = handler[DeleteRangeResponse, Long] {
    case DeleteRangeResponse(_, Some(deleted)) => deleted
  }
  val scan = handler[ScanResponse, Seq[(Bytes, Bytes)]] {
    case ScanResponse(NoError(_), rows) => rows.collect {
      case KeyValue(key, Some(BytesValue(Some(bytes)))) => (key.toByteArray, bytes)
    }
  }
  val scanCounters = handler[ScanResponse, Seq[(Bytes, Long)]] {
    case ScanResponse(NoError(_), rows) => rows.collect {
      case KeyValue(key, Some(CounterValue(Some(value)))) => (key.toByteArray, value)
    }
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
