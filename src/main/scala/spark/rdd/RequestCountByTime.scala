package spark.rdd

import com.alibaba.fastjson.JSON
import org.apache.spark.{SparkConf, SparkContext}
import util.StringUtil

object RequestCountByTime {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    val input = sc.textFile("hdfs://hadoop1:9000/execDir")
    val requireDomain = List("DISEASE", "RECIPE", "FOOD")

    input
      .map(JSON.parseObject)
      .filter(record => {
        val domain = record.getString("return_domain")
        StringUtil.isNotEmpty(domain) && requireDomain.contains(domain)
      })
      .map(record => ((record.getString("time").substring(5, 7), record.getString("return_domain")), 1))
      .reduceByKey(_+_)
      .sortByKey(ascending = true, 1)
      .saveAsTextFile("hdfs://hadoop1:9000/output")

    sc.stop
  }

}
