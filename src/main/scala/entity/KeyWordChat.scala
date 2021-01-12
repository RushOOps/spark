package entity

class KeyWordChat(
                val queryText: String,
                val domain: String,
                val keyWord: String) extends Serializable {

  override def hashCode(): Int = {
    var result = 17
    result = result * 31 + queryText.hashCode
    result
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: KeyWordChat =>
        other.queryText.equals(this.queryText)
      case _ =>
        false
    }
  }
}
