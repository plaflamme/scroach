package scroach

import proto.IsolationType

class TxCorrectnessSpecs extends ScroachSpec /* with CockroachCluster*/ {

  val SSI = IsolationType.SERIALIZABLE
  val SI = IsolationType.SNAPSHOT

  val BothIsolations = Seq(SSI, SI)
  val OnlySerializable = Seq(SSI)
  val OnlySnapshot = Seq(SI)

  def enumIsolations(numTx: Int, isolations: Seq[IsolationType.EnumVal]): Seq[Seq[IsolationType.EnumVal]] = {
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
  case class Sum(key: String) extends Cmd
  case object Commit extends Cmd
  object Cmd {
    val ReadExp = """R\((.+)\)""".r
    val IncrExp = """I\((.+)\)""".r
    val ScanExp = """SC\((.+)-(.+)\)""".r
    val SumExp = """SUM\((.+)\)""".r
    val CommitExp = "C".r
    def apply(c: String) = {
      c match {
        case ReadExp(key) => Read(key)
        case IncrExp(key) => Incr(key)
        case ScanExp(from, to) => Scan(from, to)
        case SumExp(key) => Sum(key)
        case CommitExp() => Commit
      }
    }
  }

  case class Command(cmd: Cmd, txIdx: Int) {
    override def toString() = {
      cmd match {
        case Read(k) => s"R$txIdx($k)"
        case Incr(k) => s"I$txIdx($k)"
        case Scan(f,t) => s"SC$txIdx($f-$t)"
        case Sum(k) => s"SUM$txIdx($k)"
        case Commit => s"C$txIdx"
      }
    }
  }

  case class History(cmds: Seq[Command]) {
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
      } toSeq
    }
    override def toString(): String = {
      cmds.mkString(" ")
    }
  }
  object History {
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
    enumerateHistories(histories, false).map(_.toString).toSet should be(notSymmetric.toSet)
    enumerateHistories(histories, true).map(_.toString).toSet should be(symmetric.toSet)
  }

}
