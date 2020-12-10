package entity

class DiseaseChat(
                val queryText: String,
                val disease: String) extends Serializable {

  override def hashCode(): Int = {
    var result = 17
    result = result * 31 + queryText.hashCode
    result
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: DiseaseChat =>
        other.queryText.equals(this.queryText)
      case _ =>
        false
    }
  }
}
