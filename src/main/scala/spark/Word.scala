package spark

import com.alibaba.fastjson.JSON
import org.apache.spark.{SparkConf, SparkContext}
import util.StringUtil

import scala.collection.mutable.ArrayBuffer

object Word {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    //  val conf = new SparkConf().setMaster("local[*]").setAppName("test")
    val sc = new SparkContext(conf)
    val processCount = sc.longAccumulator("processCount")

    val words = sc.textFile(System.getenv("SPARK_YARN_STAGING_DIR")+args(1)).collect().toList
    val broadcast = sc.broadcast(words)

    val input = sc.textFile(args(0))
    //  val input = sc.textFile(args(0))
    input
      .map(JSON.parseObject)
      .filter(record => StringUtil.isNotEmpty(record.getString("query_text")))
      .map(record => record.getString("query_text"))
      .distinct
      .flatMap(record => {
        processCount.add(1)
        val result = new ArrayBuffer[(String, Int)]()
        for(s <- broadcast.value){
          if(record.contains(s.split(" ")(0))){
            result.append((s, 1))
          }
        }
        result
      })
      .reduceByKey(_+_)
      .map(record => record._1+" "+record._2)
      .repartition(1)
      .saveAsTextFile(args(2))

    sc.stop()
  }

}
