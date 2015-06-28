import com.google.common.primitives.UnsignedBytes
import com.twitter.logging.Logger
import com.twitter.util.JavaTimer

package object scroach {

  /** Used by compare and set to signal the compare failed */
  case class ConditionFailedException(actualValue: Option[Bytes]) extends RuntimeException

  private[scroach] implicit val Timer = new JavaTimer(isDaemon = true)

  private[scroach] val ScroachLog = Logger.get("scroach")

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

  implicit class ScroachValue(val value: Bytes) extends AnyVal {
    // bigendian int64
    def decodeLong(): Long = {
      value(0).toLong << 56 |
        value(1).toLong << 48 |
        value(2).toLong << 40 |
        value(3).toLong << 32 |
        value(4).toLong << 24 |
        value(5).toLong << 16 |
        value(6).toLong << 8 |
        value(7).toLong
    }
  }

  implicit class ScroachLong(val value: Long) extends AnyVal {
    // bigendian int64
    def encodeBytes(): Bytes = {
      val bytes = Array.ofDim[Byte](8)
      bytes(0) = (value >>> 56).toByte
      bytes(1) = (value >>> 48).toByte
      bytes(2) = (value >>> 40).toByte
      bytes(3) = (value >>> 32).toByte
      bytes(4) = (value >>> 24).toByte
      bytes(5) = (value >>> 16).toByte
      bytes(6) = (value >>> 8).toByte
      bytes(7) = value.toByte

      bytes
    }
  }
}
