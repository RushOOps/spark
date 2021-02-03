package spark.scalautil

import scala.annotation.tailrec

object ScalaUtil {
  @tailrec
  def hasKeyword(arr: Array[String], txt: String, index: Int = 0): Boolean = {
    index<arr.length && (txt.contains(arr(index)) || hasKeyword(arr, txt, index+1))
  }
}
