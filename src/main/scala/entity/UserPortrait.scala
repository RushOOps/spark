package entity

import com.alibaba.fastjson.JSONObject

class UserPortrait(
                val queryText: String,
                val domain: String,
                val intent: String,
                val semantic: JSONObject,
                val mac: String) extends Serializable {

  override def hashCode(): Int = {
    var result = 17
    result = result * 31 + queryText.hashCode
    result = result * 31 + domain.hashCode
    result = result * 31 + mac.hashCode
    result
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: UserPortrait =>
        other.queryText.equals(this.queryText) &&
          other.domain.equals(this.domain) &&
          other.mac.equals(this.mac)
      case _ =>
        false
    }
  }
}
