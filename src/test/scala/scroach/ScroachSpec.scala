package scroach

import org.scalatest.{OptionValues, Matchers, FlatSpec}

trait ScroachSpec extends FlatSpec with Matchers with OptionValues {
  def randomBytes = util.Random.alphanumeric.take(20).map(_.toByte).toArray
}
