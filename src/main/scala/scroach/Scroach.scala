package scroach

import java.net.InetSocketAddress

import com.google.protobuf.MessageLite
import com.twitter.app.App
import com.twitter.finagle._
import proto._

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
