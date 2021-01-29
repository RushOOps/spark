package spark.rdd

import com.alibaba.fastjson.JSONObject
import com.mongodb.spark.MongoSpark
import entity.DocumentDuplication
import org.apache.spark.sql.{Row, SparkSession}
import org.bson.Document

/**
 * 根据mac地址和请求文本去重的表，将请求聚合后形成另一张表
 */

object MongoDataProcess2 {

  def main(args: Array[String]): Unit = {

    val sc = SparkSession.builder()
      .config("spark.mongodb.input.uri", "mongodb://10.66.188.17/semantic."+args(0))
      .config("spark.mongodb.output.uri", "mongodb://10.66.188.17/semantic."+args(1))
      .getOrCreate()

    val rdd = MongoSpark.load(sc)

    val result = rdd.rdd
      .map(record => {
        val semantic = record.getAs[Row]("semantic")
        val json = new JSONObject()
        val map = semantic.getValuesMap(semantic.schema.fieldNames)
        map.foreach(record => {
          if(record._2 != null){
            json.put(record._1, record._2)
          }
        })
        new Document()
          .append("query_text", record.getAs[String]("query_text"))
          .append("domain", record.getAs[String]("domain"))
          .append("intent", record.getAs[String]("intent"))
          .append("semantic", json)
          .append("source_flag", record.getAs[Int]("source_flag"))
          .append("count", record.getAs[Int]("count"))
      })
      .map(record => (new DocumentDuplication(record.getString("query_text"), record), 1))
      .reduceByKey(_+_)
      .map(record => record._1.doc.append("count", record._2))

    MongoSpark.save(result)

    sc.stop()
  }

}
