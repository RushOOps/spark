package entity

import com.alibaba.fastjson.JSONObject

class SemanticWithoutDomain(
                             val queryText: String,
                             val intent: String,
                             val semantic: JSONObject) extends Serializable {

  override def hashCode(): Int = {
    var result = 17
    result = result * 31 + queryText.hashCode
    result
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: SemanticWithoutDomain =>
        other.queryText.equals(this.queryText)
      case _ =>
        false
    }
  }

}
