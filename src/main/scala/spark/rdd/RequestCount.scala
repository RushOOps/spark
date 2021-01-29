package spark.rdd

import com.alibaba.fastjson.JSON
import org.apache.spark.{SparkConf, SparkContext}

import java.time.LocalDate

object RequestCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf
    val sc = new SparkContext(conf)

    val input = sc.textFile("hdfs://hadoop1:9000/execDir")
    val domains = sc.textFile(System.getenv("SPARK_YARN_STAGING_DIR")+"/domains.txt").collect.toList

    val domainsBC = sc.broadcast(domains)

    input
      .map(JSON.parseObject)
      .filter(record => domainsBC.value.contains(record.getString("return_domain")))
      .map(record => (record.getString("return_domain"), 1))
      .reduceByKey(_+_)
      .repartition(1)
      .saveAsTextFile("hdfs://hadoop1:9000/output_"+LocalDate.now)

    sc.stop()
  }
}
