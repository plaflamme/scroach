package scroach

import org.scalatest.{Matchers, FlatSpec}

trait ScroachSpec extends FlatSpec with Matchers {
  def randomBytes = util.Random.alphanumeric.take(20).map(_.toByte).toArray
}
