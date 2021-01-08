package entity

import com.alibaba.fastjson.JSONObject

class SemanticText(
                val queryText: String,
                val domain: String,
                val intent: String,
                val semantic: JSONObject,
                val sourceFlag: Int) extends Serializable {

  override def hashCode(): Int = {
    var result = 17
    result = result * 31 + queryText.hashCode
    result
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: SemanticText =>
        other.queryText.equals(this.queryText)
      case _ =>
        false
    }
  }
}
