package entity

import com.alibaba.fastjson.JSONObject

class Semantic(
                val queryText: String,
                val domain: String,
                val intent: String,
                val semantic: JSONObject) extends Serializable {

  override def hashCode(): Int = {
    var result = 17
    result = result * 31 + queryText.hashCode
    result = result * 31 + domain.hashCode
    result
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: Semantic =>
       other.queryText.equals(this.queryText) && other.domain.equals(this.domain)
      case _ =>
        false
    }
  }
}
