package spark

import com.alibaba.fastjson.JSON
import org.apache.spark.{SparkConf, SparkContext}
import util.StringUtil

import java.time.LocalDate
import scala.collection.mutable.ArrayBuffer

object Word {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    //  val conf = new SparkConf().setMaster("local[*]").setAppName("test")
    val sc = new SparkContext(conf)

    val words = sc.textFile(System.getenv("SPARK_YARN_STAGING_DIR")+"/words.txt").collect.toList
    val broadcast = sc.broadcast(words)

    val input = sc.textFile("hdfs://hadoop1:9000/execDir")
    input
      .map(JSON.parseObject)
      .filter(record => StringUtil.isNotEmpty(record.getString("query_text")))
      .map(record => (record.getString("query_text"), 1))
      .reduceByKey(_+_)
      .flatMap(record => {
        val result = new ArrayBuffer[(String, Int)]()
        for(s <- broadcast.value){
          if(record._1.contains(s.split(" ")(0))){
            result.append((s, record._2))
          }
        }
        result
      })
      .reduceByKey(_+_)
      .map(record => record._1+" "+record._2)
      .repartition(1)
      .saveAsTextFile("hdfs://hadoop1:9000/output_"+LocalDate.now)

    sc.stop()
  }

}
