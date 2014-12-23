import com.google.common.primitives.UnsignedBytes

package object scroach {

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
