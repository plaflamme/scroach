package scroach

import com.twitter.finagle.Service
import com.twitter.util.Future
import cockroach.proto._

class TestKv extends proto.Kv {
  def noop[Req, Rep]() = Service.mk { (req: Req) => Future.exception[Rep](new UnsupportedOperationException) }
  override val scanEndpoint: Service[ScanRequest, ScanResponse] = noop()
  override val putEndpoint: Service[PutRequest, PutResponse] = noop()
  override val casEndpoint: Service[ConditionalPutRequest, ConditionalPutResponse] = noop()
  override val deleteEndpoint: Service[DeleteRequest, DeleteResponse] = noop()
  override val incrementEndpoint: Service[IncrementRequest, IncrementResponse] = noop()
  override val batchEndpoint: Service[BatchRequest, BatchResponse] = noop()
  override val endTxEndpoint: Service[EndTransactionRequest, EndTransactionResponse] = noop()
  override val deleteRangeEndpoint: Service[DeleteRangeRequest, DeleteRangeResponse] = noop()
  override val getEndpoint: Service[GetRequest, GetResponse] = noop()
}
