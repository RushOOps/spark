package entity

class SmartHome(val device: String,
                val queryText: String,
                val domain: String,
                val intent: String,
                val semantic: String = null) extends Serializable {

  override def hashCode(): Int = {
    var result = 17
    result = result * 31 + queryText.hashCode
    result
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: SmartHome =>
        other.queryText.equals(this.queryText)
      case _ =>
        false
    }
  }

//  override def toString: String = {
//    "device: " + device + "\n" +
//      "queryText: " + queryText + "\n" +
//      "domain: " + domain + "\n" +
//      "intent: " + intent + "\n" +
//      "semantic" + semantic.toString() + "\n"
//  }
}
