package spark.rdd

import com.alibaba.fastjson.JSON
import com.mongodb.spark.MongoSpark
import org.apache.spark.{SparkConf, SparkContext}
import util.StringUtil

/**
 * 统计"isNew":"0"的mac地址数量，去重聚合后列表输出到mongo
 */

object Common {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().set("spark.mongodb.output.uri", "mongodb://semantic:semantic22s2@172.17.1.181:27017/semantic."+args(0))
    val sc = new SparkContext(conf)

    val input = sc.textFile("hdfs://hadoop1:9000/execDir")

    val result = input
      .map(JSON.parseObject)
      .filter(record => {
        val query = record.getJSONObject("query")
        query != null && query.getString("isNew").equals("0") && StringUtil.isNotEmpty(record.getString("query_mac"))
      })
      .map(record => (record.getString("query_mac"),1))
      .reduceByKey(_+_)

    MongoSpark.save(result)

    sc.stop()
  }

}
