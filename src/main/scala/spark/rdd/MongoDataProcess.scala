package spark.rdd

import com.alibaba.fastjson.JSONObject
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.WriteConfig
import org.apache.spark.sql.{Row, SparkSession}
import org.bson.Document

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * 根据mac地址和请求文本去重的表，将mac地址count为1的和大于1的请求分离成两个表
 */

object MongoDataProcess {

  def main(args: Array[String]): Unit = {
    val sc = SparkSession.builder()
      .config("spark.mongodb.input.uri", "mongodb://10.66.188.17/semantic." + args(0))
      .config("spark.mongodb.output.uri", "mongodb://10.66.188.17/semantic.placeholder")
      .getOrCreate()

    val writeOverrides = mutable.Map[String, String]()
    writeOverrides.put("collection", args(0)+"_single")
    writeOverrides.put("writeConcern.w", "majority")
    var writeConfig = WriteConfig.create(sc.sparkContext).withOptions(writeOverrides)

    val rdd = MongoSpark.load(sc)

    val cache = rdd.rdd
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
          .append("mac", record.getAs[String]("mac"))
          .append("count", record.getAs[Int]("count"))
      })
      .map(record => (record.getString("mac"), record))
      .combineByKey(
        v => ArrayBuffer[Document](v),
        (c: ArrayBuffer[Document], v: Document) => c += v,
        (c1: ArrayBuffer[Document], c2: ArrayBuffer[Document]) => c1 ++ c2
      )
      .cache()

    val result = cache
      .filter(record => record._2.size == 1)
      .map(record => record._2(0))

    MongoSpark.save(result, writeConfig)

    writeOverrides.put("collection", args(0)+"_many")
    writeConfig = WriteConfig.create(sc.sparkContext).withOptions(writeOverrides)

    val result2 = cache
      .filter(record => record._2.size > 1)
      .map(record => {
        var count = 0
        record._2.foreach(doc => count += doc.getInteger("count"))
        new Document().append("mac", record._1).append("count", count)
      })

    MongoSpark.save(result2, writeConfig)

    sc.stop()
  }

}
