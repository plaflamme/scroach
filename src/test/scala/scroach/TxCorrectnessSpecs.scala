package scroach

import cockroach.proto.IsolationType
import com.twitter.io.Charsets
import com.twitter.util._

class TxCorrectnessSpecs extends ScroachSpec with CockroachCluster {

  val SSI = IsolationType.SERIALIZABLE
  val SI = IsolationType.SNAPSHOT

  val BothIsolations = Seq(SSI, SI)
  val OnlySerializable = Seq(SSI)
  val OnlySnapshot = Seq(SI)

  def enumIsolations(numTx: Int, isolations: Seq[IsolationType]): Seq[Seq[IsolationType]] = {
    for(i <- 0 until math.pow(isolations.size, numTx).toInt) yield {
      var v = i
      for (t <- 0 until numTx) yield {
        val r = isolations(v % isolations.size)
        v = v / isolations.size
        r
      }
    }
  }

  "enumIsolations" should "enumarate all possible combinations" in {
    val expected = Set(
      Seq(SSI, SSI, SSI),
      Seq(SI, SSI, SSI),
      Seq(SSI, SI, SSI),
      Seq(SSI, SSI, SI),
      Seq(SSI, SI, SI),
      Seq(SI, SSI, SI),
      Seq(SI, SI, SSI),
      Seq(SI, SI, SI)
    )
    // Ordering doesn't matter, so convert to a set
    enumIsolations(3, BothIsolations).toSet should be(expected)
  }

  it should "handle a degenerate case" in {
    enumIsolations(3, OnlySerializable).toSet should be(Set(Seq(SSI, SSI, SSI)))
  }

  sealed trait Cmd
  case class Read(key: String) extends Cmd
  case class Incr(key: String) extends Cmd
  case class Scan(from: String, to: String) extends Cmd
  case class Delete(from: String, to: String) extends Cmd
  case class Sum(key: String) extends Cmd
  case object Commit extends Cmd
  object Cmd {
    val ReadExp = """R\((.+)\)""".r
    val IncrExp = """I\((.+)\)""".r
    val ScanExp = """SC\((.+)-(.+)\)""".r
    val DeleteExp = """DR\((.+)-(.+)\)""".r
    val SumExp = """SUM\((.+)\)""".r
    val CommitExp = "C".r
    def apply(c: String) = {
      c match {
        case ReadExp(key) => Read(key)
        case IncrExp(key) => Incr(key)
        case ScanExp(from, to) => Scan(from, to)
        case DeleteExp(from, to) => Delete(from, to)
        case SumExp(key) => Sum(key)
        case CommitExp() => Commit
      }
    }
  }

  case class Command(cmd: Cmd, txIdx: Int) {
    def apply(txClient: TxClient, historyId: Int, nonce: String) = {
      cmd match {
        case Read(k) => txClient.get(s"$k$historyId$nonce".getBytes(Charsets.Utf8))
        case Incr(k) => txClient.increment(s"$k$historyId$nonce".getBytes(Charsets.Utf8), 1)
        case Commit => Future.Done
      }
    }
    override def toString() = {
      cmd match {
        case Read(k) => s"R$txIdx($k)"
        case Incr(k) => s"I$txIdx($k)"
        case Scan(f,t) => s"SC$txIdx($f-$t)"
        case Delete(f,t) => s"DR$txIdx($f-$t)"
        case Sum(k) => s"SUM$txIdx($k)"
        case Commit => s"C$txIdx"
      }
    }
  }

  case class History(cmds: Seq[Command]) {
    private[this] val historyId = History.nextId
    private[this] def interleave(a: List[Command], b: List[Command], symmetric: Boolean = false): List[List[Command]] = {
      (a, b) match {
        case (xs, Nil) => List(xs)
        case (Nil, ys) => List(ys)
        case(x :: xs, y :: ys) => {
          interleave(xs, y :: ys).map { l => x :: l } ++
            (if(symmetric) List.empty else interleave(x :: xs, ys).map { l => y :: l })
        }
      }
    }

    def interleave(other: History, symmetric: Boolean): Seq[History] = {
      interleave(cmds.toList, other.cmds.toList, symmetric).map {
        History(_)
      }.toSeq
    }

    def read(client: Client, key: String, nonce: String): Future[Long] = {
      client.increment(s"$key$historyId$nonce".getBytes(Charsets.Utf8), 0)
    }

    def run(client: Client, priorities: Seq[Int], isolations: Seq[IsolationType], nonce: String) = {

      case class Ex(cmd: Command, previous: Option[Ex], done: Promise[Unit])

      var previous: Option[Ex] = None
      val plan = cmds
        .map { cmd =>
          val e = Ex(cmd, previous, new Promise[Unit])
          previous = Some(e)
          e
        }

      case class Tx(idx: Int, priority: Int, isolation: IsolationType, cmds: Seq[Ex], commit: Ex)

      def txCmds(txId: Int) = plan.filter {
        case Ex(Command(Commit, id), _, _) if(id == txId) => false
        case Ex(Command(cmd, id), _, _) if(id == txId) => true
        case _ => false
      }
      def txCommit(txId: Int) = plan.filter {
        case Ex(Command(Commit, id), _, _) if(id == txId) => true
        case Ex(Command(cmd, id), _, _) if(id == txId) => false
        case _ => false
      }.head

      val txs = for(i <- 0 until priorities.size) yield {
        Tx(
          i+1,
          priorities(i),
          isolations(i),
          txCmds(i+1),
          txCommit(i+1)
        )
      }

      txs.foreach { tx =>
        println(s"tx${tx.idx} p${tx.priority} ${tx.isolation}: ${tx.cmds.map(_.cmd).mkString(" ")}")
      }

      val results = txs.map { tx =>
        var tried = false
        val txn = client.tx(tx.isolation, priority = Some(-tx.priority)) { txClient =>
          if(tried == true) tx.commit.done.setDone
          tried = true
          val c = Future.collect(tx.cmds.map { case Ex(cmd, previous, done) =>
            previous.foreach { p =>
              println(s"$cmd waiting for ${p.cmd}")
            }
            for {
              _ <- previous.map(_.done).getOrElse(Future.Done)
              _ = println(s"tx${tx.idx} executing $cmd")
              _ <- cmd(txClient, historyId, nonce) ensure { done.setDone }
            } yield ()
          }) ensure { println(s"tx${tx.idx} done") }

          // Wait for our commit's previous step to complete before commiting
          c.flatMap { _ =>
            tx.commit.previous.map(_.done).getOrElse(Future.Done)
          }
        }

        // Signal that our commit is complete
        txn.ensure { tx.commit.done.setDone }
      }
      Future
        .collect(results)
        .liftToTry
        .map {
          case Return(_) => s"Running history ${cmds.mkString(" ")} priorities ${priorities.mkString(",")} isolations ${isolations.mkString(",")}: success"
          case Throw(t) => s"Running history ${cmds.mkString(" ")} priorities ${priorities.mkString(",")} isolations ${isolations.mkString(",")}: failure $t"
        }
    }

    override def toString(): String = {
      f"$historyId%3d: [${cmds.mkString(" ")}]"
    }
  }
  object History {
    // Start with a random value so that we don't reuse the same cells across test runs against the same cluster.
    var id: Int = math.abs(scala.util.Random.nextInt())
    def nextId: Int = {
      id += 1
      id
    }
    def apply(txs: Seq[String]): Seq[History] = {
      txs.zipWithIndex.map { case(h,i) => History(h, i+1) }
    }
    def apply(tx: String, idx: Int): History = {
      val cmds = tx.split(" ").map {
        Cmd(_)
      }.map { Command(_, idx) }
      History(cmds.toSeq)
    }
  }

  def enumerateHistories(txs: Seq[History], symmetric: Boolean): Seq[History] = {
    txs.tail.foldLeft(Seq(txs.head)) { case(histories, h) =>
      histories.flatMap(l => l.interleave(h, symmetric))
    }
  }

  "enumerateHistories" should "correctly enumerate histories" in {
    val notSymmetric = Seq(
      "I1(A) C1 I2(A) C2",
      "I1(A) I2(A) C1 C2",
      "I1(A) I2(A) C2 C1",
      "I2(A) C2 I1(A) C1",
      "I2(A) I1(A) C2 C1",
      "I2(A) I1(A) C1 C2"
    )
    val symmetric = notSymmetric.take(3)

    val histories = History(Seq("I(A) C", "I(A) C"))

    enumerateHistories(histories, false).map { _.cmds.mkString(" ") }.sorted should be(notSymmetric.sorted)
    enumerateHistories(histories, true).map { _.cmds.mkString(" ") }.sorted should be(symmetric.sorted)
  }

  "Tx Correctness" should "inconsistent analysis anomaly" in withClient { client =>
    // See TestTxnDBInconsistentAnalysisAnomaly
    val tx1 = "R(A) R(B) SUM(C) C"
    val tx2 = "I(A) I(B) C"

    def check(isolationLevels: Seq[IsolationType], txs: Seq[String], f: () => Future[Unit]) = {

      val priorities = (for(i <- 1 to txs.size) yield i).permutations.toSeq
      val isolations = enumIsolations(txs.size, isolationLevels)
      val histories = enumerateHistories(History(txs), false)

      for {
        priority <- priorities
        isolation <- isolations
        history <- histories
      } yield {
        Await.result {
          val nonce = scala.util.Random.alphanumeric.take(20).mkString
          for {
            s <- history.run(client, priority, isolation, nonce)
            _ = println(s)
            r <- history.read(client, "A", nonce)
          } yield {
            r should be(2)
          }
        }
      }

    }

    val p = check(BothIsolations, Seq("I(A) C", "I(A) C"), () => Future.Done)
    Future.Done
  }

}
