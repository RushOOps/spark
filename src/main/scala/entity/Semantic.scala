package entity

import com.alibaba.fastjson.JSONObject

class Semantic(
                val queryText: String,
                val domain: String,
                val intent: String = null,
                val semantic: JSONObject = null,
                val sourceFlag: Int = -1,
                val queryMac: String = null) extends Serializable {

  override def hashCode(): Int = {
    var result = 17
    result = result * 31 + queryText.hashCode
    result = result * 31 + domain.hashCode
    result = result * 31 + queryMac.hashCode
    result
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: Semantic =>
        other.queryText.equals(this.queryText) &&
          other.domain.equals(this.domain) &&
          other.queryMac.equals(this.queryMac)
      case _ =>
        false
    }
  }
}
