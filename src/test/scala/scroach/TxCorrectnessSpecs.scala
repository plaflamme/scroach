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

    def execute(txClient: TxClient, uniqKey: (String) => Bytes, state: TxState): Future[TxState] = cmd match {
      case Read(k) => txClient.get(uniqKey(k)).map(_ => state)
      case Incr(k) => txClient.increment(uniqKey(k), 1).map(_ => state)
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

    def uniqKey(key: String) = s"$key${history.id}$nonce".getBytes(Charsets.Utf8)

    override def toString(): String = {
      val txStr = (isolationLevels zip priorities).zipWithIndex map { case((i,p), id) =>
        s"[tx($id): iso=$i pri=$p]"
      } mkString(" ")
      val cmdStr = history.cmds.mkString("[", " ", "]")

      s"$cmdStr $txStr"
    }
  }

  object Planner {
    def apply(cmds: List[List[Cmd]], isolationLevels: Seq[IsolationType], symmetric: Boolean): Seq[TestCase] = {
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
    def apply(client: Client, txs: List[List[Cmd]], isolations: Seq[IsolationType], symmetric: Boolean)(f: (TestCase, Seq[Try[TxState]]) => Future[Unit]) = {
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

  "Tx Correctness" should "handle no anomaly" in withClient { client =>
    val tx = List(Incr("A"), Commit)

    Verifier(client, List(tx, tx), BothIsolations, false) { case(testCase, results) =>
      // TODO: read counters
      client.increment(testCase.uniqKey("A"), 0) map { r =>
        r should be(2)
      }
    }
  }
}
