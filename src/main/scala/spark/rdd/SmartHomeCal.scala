package spark.rdd

import com.alibaba.fastjson.JSON
import org.apache.spark.{SparkConf, SparkContext}
import util.StringUtil

object SmartHomeCal {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    val input = sc.textFile("hdfs://hadoop1:9000/execDir")
    val result = input
      .map(JSON.parseObject)
      .filter(record => {
        val domain = record.getString("return_domain")
        try{
          StringUtil.isNotEmpty(domain) &&
            domain.equals("SmartHome") &&
            StringUtil.isNotEmpty(record.getJSONObject("return_semantic").getJSONArray("voiceCommand").getJSONObject(0).getString("device_name"))
        } catch {
          case _: Exception => false
        }
      })
      .map(record => {
        ((record.getString("query_text"), record.getJSONObject("return_semantic").getJSONArray("voiceCommand").getJSONObject(0).getString("device_name")), 1)
      })
      .reduceByKey(_+_)
      .sortBy(_._2, ascending = false)
      .take(10000)

    sc
      .parallelize(result, 1)
      .map(record => record._1._1+"\t"+record._1._2+"\t"+record._2)
      .saveAsTextFile("hdfs://hadoop1:9000/output")

    sc.stop()
  }
}
