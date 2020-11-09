package spark

import com.alibaba.fastjson.JSON
import com.mongodb.spark.MongoSpark
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.Document
import util.StringUtil

import scala.collection.mutable.ArrayBuffer

object Music {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().set("spark.mongodb.output.uri", "mongodb://10.66.188.17:27017/semantic.semantic_lyrics")
    val sc = new SparkContext(conf)

    val input = sc.textFile("hdfs://hadoop1:9000"+args(0))

    val result = input.map(record => JSON.parseObject(record))
      .filter(record => {
        val domain = record.getString("return_domain")
        val semantic = record.getJSONObject("return_semantic")
        StringUtil.isNotEmpty(domain) &&
          domain.equals("MUSIC") &&
          semantic != null &&
          semantic.size() != 0
      })
      .flatMap(record => {
        val semantic = record.getJSONObject("return_semantic").entrySet().iterator()
        val returnSeq = new ArrayBuffer[(String, Int)]()
        while(semantic.hasNext){
          val keyValue = semantic.next()
          returnSeq.append((keyValue.getKey+"@@@"+keyValue.getValue, 1))
        }
        returnSeq
      })
      .reduceByKey(_+_)
      .map(record => {
        val keyValue = record._1.split("@@@")
        new Document()
          .append("property", keyValue(0))
          .append("value", keyValue(1))
          .append("count", record._2)
      })

    MongoSpark.save(result)

    sc.stop()

  }
}

