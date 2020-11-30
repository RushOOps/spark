package spark

import com.alibaba.fastjson.JSON
import org.apache.spark.{SparkConf, SparkContext}
import util.StringUtil

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

object Word extends App {

  val conf = new SparkConf()
//  val conf = new SparkConf().setMaster("local[*]").setAppName("test")
  val sc = new SparkContext(conf)

  val wordFile = Source.fromFile("hdfs://hadoop1:9000"+args(1))
//  val wordFile = Source.fromFile(args(1))
  val broadcast = sc.broadcast(wordFile.getLines().toList)
  wordFile.close

  val input = sc.textFile("hdfs://hadoop1:9000"+args(0))
//  val input = sc.textFile(args(0))
  input
    .map(JSON.parseObject)
    .filter(record => StringUtil.isNotEmpty(record.getString("query_text")))
    .map(record => record.getString("query_text"))
    .distinct
    .flatMap(record => {
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
    .saveAsTextFile("hdfs://hadoop1:9000"+args(2))
//    .saveAsTextFile(args(2))

  sc.stop()
}
