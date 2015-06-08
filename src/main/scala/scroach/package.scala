import com.google.common.primitives.UnsignedBytes
import com.twitter.util.JavaTimer

package object scroach {

  /** Used by compare and set to signal the compare failed */
  case class ConditionFailedException(actualValue: Option[Bytes]) extends RuntimeException

  private[scroach] implicit val Timer = new JavaTimer(isDaemon = true)

  type Bytes = Array[Byte]

  val ByteZero = 0x00.toByte

  implicit class Key(val key: Bytes) extends AnyVal {
    /**
     * @return Next possible key in lexicographic sort order
     */
    def next() = {
      key :+ ByteZero
    }

    def compare(k: Bytes) = UnsignedBytes.lexicographicalComparator().compare(key, k)

    def <(o: Bytes) = compare(o) < 0
    def <=(o: Bytes) = compare(o) <= 0
    def >(o: Bytes) = compare(o) > 0
    def >=(o: Bytes) = compare(o) >= 0
    def ==(o: Bytes) = compare(o) == 0
  }
}
