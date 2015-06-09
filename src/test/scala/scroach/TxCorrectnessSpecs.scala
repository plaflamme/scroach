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

  def interleave[T](a: List[T], b: List[T], symmetric: Boolean = false): List[List[T]] = {
    (a, b) match {
      case (xs, Nil) => List(xs)
      case (Nil, ys) => List(ys)
      case(x :: xs, y :: ys) => {
        interleave(xs, y :: ys).map { l => x :: l } ++
          (if(symmetric) List.empty else interleave(x :: xs, ys).map { l => y :: l })
      }
    }
  }

  "interleave" should "correctly enumerate histories" in {
    val notSymmetric = Seq(
      "I1(A) C1 I2(A) C2",
      "I1(A) I2(A) C1 C2",
      "I1(A) I2(A) C2 C1",
      "I2(A) C2 I1(A) C1",
      "I2(A) I1(A) C2 C1",
      "I2(A) I1(A) C1 C2"
    )
    val symmetric = notSymmetric.take(3)

    val histories = interleave(List("I1(A)", "C1"), List("I2(A)", "C2"), false)

    interleave(List("I1(A)", "C1"), List("I2(A)", "C2"), false).map { _.mkString(" ") }.sorted should be(notSymmetric.sorted)
    interleave(List("I1(A)", "C1"), List("I2(A)", "C2"), true).map { _.mkString(" ") }.sorted should be(symmetric.sorted)
  }

  sealed trait Cmd
  case class Read(key: String) extends Cmd
  case class Incr(key: String) extends Cmd
  case class Scan(from: String, to: String) extends Cmd
  case class Delete(from: String, to: String) extends Cmd
  case class Sum(key: String) extends Cmd
  case object Commit extends Cmd

  // A transaction is a sequence of Cmd
  type Tx = Seq[Cmd]

  type TxState = Map[String, Long]

  // A Cmd within a transaction within a history
  case class TxCmd(cmd: Cmd, txId: Int) {

    def execute(txClient: TxClient, uniqKey: Bijection[String, Bytes], state: TxState): Future[TxState] = cmd match {
      case Read(k) => txClient.getCounter(uniqKey(k)).map(value => state ++ value.map { v => Map(k -> v) }.getOrElse(Map.empty))
      case Incr(k) => txClient.increment(uniqKey(k), 1).map(value => state ++ Map(k -> value))
      case Scan(from, to) => txClient.scanCounters(uniqKey(from), uniqKey(to)).flatMap { _.toSeq }.map { values =>
        state ++ values.map { case(k,v) => uniqKey.invert(k) -> v }.toMap
      }
      case Delete(from, to) => txClient.deleteRange(uniqKey(from), uniqKey(to)).map(_ => state)
      case Sum(k) => { txClient.put(uniqKey(k), state.values.sum).map { _ => state } }
      case Commit => Future.value(state)
    }

    override def toString() = {
      cmd match {
        case Read(k) => s"R$txId($k)"
        case Incr(k) => s"I$txId($k)"
        case Scan(f,t) => s"SC$txId($f-$t)"
        case Delete(f,t) => s"DR$txId($f-$t)"
        case Sum(k) => s"SUM$txId($k)"
        case Commit => s"C$txId"
      }
    }
  }

  // A history is a sequence of interleaving TxCmd
  case class History(id: Int, cmds: List[TxCmd])

  // A TestCase is a single history with an isolation level and a priority for each of its transactions
  case class TestCase(history: History, isolationLevels: Seq[IsolationType], priorities: Seq[Int]) {
    // So that the test keys are unique between runs in the same cluster
    private[this] val nonce = scala.util.Random.alphanumeric.take(20).mkString

    val uniqKey: Bijection[String, Bytes] = new Bijection[String, Bytes] {
      def apply(key: String)  = s"$nonce/${history.id}/$key".getBytes(Charsets.Utf8)
      def invert(bytes: Bytes) = new String(bytes, Charsets.Utf8).split("/").last
    }

    override def toString(): String = {
      val txStr = (isolationLevels zip priorities).zipWithIndex map { case((i,p), id) =>
        s"[tx($id): iso=$i pri=$p]"
      } mkString(" ")
      val cmdStr = history.cmds.mkString("[", " ", "]")

      s"$cmdStr $txStr"
    }
  }

  object Planner {
    def apply(cmds: Seq[Seq[Cmd]], isolationLevels: Seq[IsolationType], symmetric: Boolean): Seq[TestCase] = {
      val txns = cmds
        .zipWithIndex
        .map { case(cmds, id) =>
          cmds.map { cmd => TxCmd(cmd, id) }.toList
        }
      val histories = txns.tail.foldLeft(List(txns.head)) { case(histories, c) =>
        histories.flatMap { h =>
          interleave(h, c, symmetric)
        }
      }.zipWithIndex.map { case(history, id) =>
        History(id, history)
      }

      val priorities = (for(i <- 1 to cmds.size) yield i).permutations.toSeq
      val isolations = enumIsolations(cmds.size, isolationLevels)
      for {
        priority <- priorities
        isolation <- isolations
        history <- histories
      } yield TestCase(history, isolation, priority)
    }
  }

  object Runner {
    case class ExecutionStep(cmd: TxCmd, previous: Option[ExecutionStep], done: Promise[Unit])
    case class Transaction(id: Int, isolationLevel: IsolationType, priority: Int, steps: Seq[ExecutionStep], commit: ExecutionStep)

    def run(client: Client, testCase: TestCase) = {
      var previous: Option[ExecutionStep] = None
      val steps = testCase.history.cmds.map { cmd =>
        val e = ExecutionStep(cmd, previous, new Promise[Unit])
        previous = Some(e)
        e
      }

      val transactions = (testCase.isolationLevels zip testCase.priorities).zipWithIndex.map { case ((isolation, priority), id) =>
        val allCmds = steps.filter {
          _.cmd.txId == id
        }
        val cmds = allCmds.takeWhile(_.cmd.cmd != Commit)
        val commit = allCmds.find(_.cmd.cmd == Commit).head
        Transaction(id, isolation, priority, cmds, commit)
      }

      def runTx(tx: Transaction): Future[TxState] = {
        var triedOnce = false
        val txRun = client.tx(tx.isolationLevel, priority = Some(-tx.priority)) { txClient =>
          if(triedOnce) {
            tx.steps.foreach { _.done.setDone }
            tx.commit.done.setDone
          }
          triedOnce = true
          tx.steps.foldLeft(Future.value(Map.empty[String, Long])) { case(previousState, ExecutionStep(cmd, previous, done)) =>
            for {
              s <- previousState
              _ <- previous.map(_.done).getOrElse(Future.Done)
              state <- cmd.execute(txClient, testCase.uniqKey, s) ensure { done.setDone }
            } yield state
          } flatMap { state =>
            // Wait for our commit's previous step to complete before exiting the transactional block
            tx.commit.previous.map(_.done).getOrElse(Future.Done) before Future.value(state)
          }
        }

        txRun ensure { tx.commit.done.setDone }
      }

      Future.collect(transactions.map(runTx(_).liftToTry))
    }
  }

  object Verifier {
    def apply(client: Client, txs: Seq[Seq[Cmd]], isolations: Seq[IsolationType], symmetric: Boolean)(f: (TestCase, Seq[Try[TxState]]) => Future[Unit]) = {
      Planner(txs, isolations, symmetric)
        .foldLeft(Future.Done) { case(previous, testCase) =>
          for {
            _ <- previous
            _ = println(testCase)
            results <- Runner.run(client, testCase)
            _ <- f(testCase, results)
            _ = println("========")
          } yield ()
        }
    }
  }

  // TODO: until we can change the test cluster's settings, we need to rely on tx timeouts
  // https://github.com/cockroachdb/cockroach/issues/877
  import com.twitter.conversions.time._
  override val TestCaseTimeout = 5.minutes

  "Transaction Correctness" should "handle no anomaly" in withClient { client =>

    val tx = List(Incr("A"), Commit)

    Verifier(client, List(tx, tx), BothIsolations, false) { case(testCase, results) =>
      results.forall(_.isReturn) shouldBe true
      client.getCounter(testCase.uniqKey("A")) map { r =>
        r.value should be(2)
      }
    }
  }

  it should "handle the inconsistent analysis anomaly" in withClient { client =>
    val txn1 = Seq(Read("A"), Read("B"), Sum("C"), Commit)
    val txn2 = Seq(Incr("A"), Incr("B"), Commit)

    Verifier(client, Seq(txn1, txn2), BothIsolations, false) { case(testCase, results) =>
      results.forall(_.isReturn) shouldBe true
      client.getCounter(testCase.uniqKey("C")) map { r =>
        r should contain oneOf (2, 0)
      }
    }
  }

  it should "handle the lost update anomaly" in withClient { client =>
    val txn = Seq(Read("A"), Incr("A"), Commit)

    Verifier(client, Seq(txn, txn), BothIsolations, false) { case(testCase, results) =>
      results.forall(_.isReturn) shouldBe true
      client.getCounter(testCase.uniqKey("A")) map { r =>
        r.value should be(2)
      }
    }
  }

  it should "handle the phantom read anomaly" in withClient { client =>
    val txn1 = Seq(Scan("A", "C"), Sum("D"), Scan("A", "C"), Sum("E"), Commit)
    val txn2 = Seq(Incr("B"), Commit)

    Verifier(client, Seq(txn1, txn2), BothIsolations, false) { case(testCase, results) =>
      results.forall(_.isReturn) shouldBe true
      for {
        (d,e) <- client.getCounter(testCase.uniqKey("D")) join client.getCounter(testCase.uniqKey("E"))
      } yield {
        d.value should be(e.value)
      }
    }
  }

  it should "handle the phantom delete anomaly" in withClient { client =>
    val txn1 = Seq(Delete("A", "C"), Scan("A", "C"), Sum("D"), Commit)
    val txn2 = Seq(Incr("B"), Commit)

    Verifier(client, Seq(txn1, txn2), BothIsolations, false) { case(testCase, results) =>
      results.forall(_.isReturn) shouldBe true
      client.getCounter(testCase.uniqKey("D")) map { r =>
        r.value should be(0)
      }
    }
  }

  it should "handle the write skew anomaly" in withClient { client =>
    val txn1 = Seq(Scan("A", "C"), Incr("A"), Sum("A"), Commit)
    val txn2 = Seq(Scan("A", "C"), Incr("B"), Sum("B"), Commit)

    def verify(allowed: (Int, Int)*) = {
      (testCase: TestCase, results: Seq[Try[TxState]]) => {
        for {
          (a, b) <- client.getCounter(testCase.uniqKey("A")) join client.getCounter(testCase.uniqKey("B"))
        } yield {
          allowed should contain ((a.value, b.value))
        }
      }
    }

    val serializable = Verifier(client, Seq(txn1, txn2), OnlySerializable, false)(verify((1,2), (2,1)))
    val snapshot = Verifier(client, Seq(txn1, txn2), OnlySnapshot, false)(verify((1,2), (2,1), (1,1)))

    serializable join snapshot
  }

}
