package entity

import org.bson.Document

class DocumentDuplication(
                val dup: String,
                val doc: Document) extends Serializable {

  override def hashCode(): Int = {
    var result = 17
    result = result * 31 + dup.hashCode
    result
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: DocumentDuplication =>
        other.dup.equals(this.dup)
      case _ =>
        false
    }
  }
}
