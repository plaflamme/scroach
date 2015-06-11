scroach
=======
[![Circle CI](https://circleci.com/gh/plaflamme/scroach.svg?style=shield)](https://circleci.com/gh/plaflamme/scroach)

Scala client for [CockroachDB](https://github.com/cockroachdb/cockroach/) using [Finagle](https://github.com/twitter/finagle/).

Getting it
==========

Scroach is still under development, but some early releases are being published to bintray. You can use that to try it out, but the API is not stable yet, so use at your own risk.

Add the following to your sbt `project/plugins.sbt` file:

```scala
addSbtPlugin("me.lessis" % "bintray-sbt" % "0.3.0")
```

_NOTE_ this plugin targets sbt 0.13.6+

You will need to add the following to your `project/build.properties` file:

    sbt.version=0.13.8

Then add the repository to your `build.sbt` file:

    resolvers += Resolver.bintrayRepo("scroach", "plaflamme")

and finally add it as a dependency to your project:

```
libraryDependencies += "scroach" %% "scroach" % "0.1-alpha-1"
```

Using it
========

This client uses the http transport to talk to the cockroach cluster. Thus, you first need to create a Finagle HTTP `Service`:

```scala
import com.twitter.finagle.Httpx

val service = Httpx.newClient("cockroach-node-01:80,cockroach-node-02:80", "cockroach").toService
```

Then, you can create an instance of `HttpKv`:

```scala
import scroach.proto.HttpKv

val httpKv = HttpKv(service)
```

The class `HttpKv` provides low-level access to the Cockroach API. To get a higher-level client, you can create it like so:

```scala
import scroach.KvClient

// You must provide the username here (e.g.: "root")
val client = KvClient(httpKv, "root")
```

Putting it all together:

```scala
import com.twitter.finagle.Httpx
import scroach.proto.HttpKv
import scroach.KvClient

val service = Httpx.newClient("cockroach-node-01:80,cockroach-node-02:80", "cockroach").toService
val client = KvClient(HttpKv(service), "root")
// Do your thing
service.close
```

API
===

Currently, the richer client api looks like this:

```scala
trait Client {
  type Bytes = Array[Byte]
  
  def get(key: Bytes): Future[Option[Bytes]]
  def getCounters(key: Bytes): Future[Option[Bytes]]
  def put(key: Bytes, value: Bytes): Future[Unit]
  def put(key: Bytes, value: Long): Future[Unit]
  def compareAndSet(key: Bytes, previous: Option[Bytes], value: Option[Bytes]): Future[Unit]
  def increment(key: Bytes, amount: Long): Future[Long]
  def delete(key: Bytes): Future[Unit]
  def deleteRange(from: Bytes, to: Bytes, maxToDelete: Long = Long.MaxValue): Future[Long]
  def scan(from: Bytes, to: Bytes, bacthSize: Int = 256): Future[Spool[(Bytes, Bytes)]]
  def scanCounters(from: Bytes, to: Bytes, bacthSize: Int = 256): Future[Spool[(Bytes, Long)]]
  def tx[T](isolation: IsolationType.EnumVal = IsolationType.SERIALIZABLE)(f: Kv => Future[T]): Future[T]
}
```

These are pretty much self-explanatory except maybe for `scan` and `tx`

## Scanning

`scan` returns a `Spool` which is an asynchronous iterator. So the client will scan in batches and provide values as they become available:

```scala
// Asynchronously computes the size (in bytes) of the values under a certain range of keys
for {
  scanner <- client.scan(from, to)
  result <- scanner.foldLeft(0) { case(size, (key, value)) => size + value.size }
} yield result
```

## Transactions

Cockroach supports transactions which is awesome! It has 2 isolation levels: `SNAPSHOT` and `SERIALIZABLE`. The later is the default because it provides stronger guarantees at the cost of a slight performance hit. Developers need to consciously decide to forfeit the stronger guarantees to get better performance. Scroach exposes the transactional semantics through the `tx` method. Here is its signature:

```scala
  def tx[T](isolation: IsolationType.EnumVal = IsolationType.SERIALIZABLE)(f: Kv => Future[T]): Future[T]
```

It takes an optional parameter to set the isolation level (`SERIALIZABLE` being the default) and a function to execute "within" the transaction. This function is passed a _transactional_ `Kv` instance: one that wraps calls with the proper transactional semantics and behaviour. The transaction is committed if your function returns a successful `Future` and is aborted otherwise. It's important to note that your function needs to be idempotent and should have no side effects since it may be executed multiple times.

Here's an example:

```scala
val client: Client = ???
val key: Bytes = ???
val value: Bytes = ???

client.tx() { kv =>
  val txClient = KvClient(kv)
  // txClient is also a Client, but all of the methods invoked will take part in a single transaction
  for {
    _ <- txClient.put(key, value)
    got <- txClient.get(key)
  } yield got
}
```

## Batching

Another type of client is the `BatchClient` which allows for creating batches of various calls and then to transform an compose them in various ways. It's a very efficient way to interact with Cockroach. The API is equivalent to the normal asynchronous `Client`, but instead of returning `Future`, it returns `Batch`.

You can transform a `Batch[A]` into a `Batch[B]` using `map` and `flatMap` and you can `run` it to turn it into a `Future[A]` to asynchronously obtain its result. The underlying framework will compose `Batch`es together in the minimal set of calls to Cockroach.

Here's a simple example involving a batch of 2 get requests:

```scala
val batchClient: BatchClient = ???
val k1: Bytes = ???
val k2: Bytes = ???

val batchK1: Batch[Option[Bytes]] = batchClient.get(k1)
val batchK2: Batch[Option[Bytes]] = batchClient.get(k2)

val batchK1_2: Batch[(Option[Bytes], Option[Bytes])] = batchK1 join batchK2

val result: Future[(Option[Bytes], Option[Bytes])] = batchClient.run(batchK1_2)
```

You'll note that the batch is only sent to Cockroach when we call ```run```. That is, you can compose arbitrarely complexe `Batch` instances (such as ones involving several round-trips) and the batching framework will create the minimal set of calls to Cockroach to satisfy your batch.

```scala
val batchClient: BatchClient = ???
val keys: Seq[Bytes] = ???
def mkValue(): Bytes = ???

val batches = keys.map { key =>
  batchClient.get(key)
    .flatMap {
      case Some(value) => Batch.const(value)
      case None => {
        val value = mkValue
        batchClient.put(key, value).map { _ => value }
      }
    }
}

// Much like Future.collect, you can turn a Seq[Batch[T]] into a Batch[Seq[T]] using Batch.collect
val result: Batch[Seq[Bytes]] = Batch.collect(batches)
```

In the previous example, the resulting `Batch[Seq[Bytes]]` will execute one or two Cockroach batch requests depending on the data. If there are already values under the keys in `keys`, then a single batch request will be executed, otherwise 2 will be executed and the second will only contain `put` calls for the missing values (hence the second batch may be smaller than the first). This demonstrates that the framework will always run the minimal calls necessary to satisfy the composed batch.
