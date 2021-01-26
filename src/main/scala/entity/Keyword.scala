package entity

import com.alibaba.fastjson.JSONObject

class Keyword(
               val queryText: String,
               val domain: String,
               val intent: String = null,
               val semantic: JSONObject = null,
               val mac: String = null,
               val keyword: String) extends Serializable {

  override def hashCode(): Int = {
    var result = 17
    result = result * 31 + queryText.hashCode
    result
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: Keyword =>
        other.queryText.equals(this.queryText)
      case _ =>
        false
    }
  }
}
