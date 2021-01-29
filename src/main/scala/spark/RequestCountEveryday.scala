package spark

import com.alibaba.fastjson.JSON
import org.apache.spark.{SparkConf, SparkContext}

object RequestCountEveryday {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    val input = sc.textFile("hdfs://hadoop1:9000/execDir")

    input
      .map(JSON.parseObject)
      .filter(record => record.containsKey("type") &&
        (record.getString("type").equals("Errorsuggest") || record.getString("type").equals("ErrorCache")))
      .map(record => ((record.getString("time").substring(8, 10), record.getString("type")), 1))
      .reduceByKey(_+_)
      .sortByKey(ascending = true, 1)
      .saveAsTextFile("hdfs://hadoop1:9000/output")

    sc.stop
  }

}
