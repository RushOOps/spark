package spark.rdd

import com.alibaba.fastjson.JSON
import org.apache.spark.{SparkConf, SparkContext}

import java.time.LocalDate

object TopN {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    val input = sc.textFile("hdfs://hadoop1:9000/execDir")

    val result = input.map(record => JSON.parseObject(record))
      .map(record => {
        (record.getString("query_text"), 1)
      })
      .reduceByKey(_+_)
      .sortBy(_._2, ascending = false)
      .map(record => record._1+" "+record._2)
      .take(10000)

    sc.parallelize(result, 1).saveAsTextFile("hdfs://hadoop1:9000/output_"+LocalDate.now)

    sc.stop()

  }
}

