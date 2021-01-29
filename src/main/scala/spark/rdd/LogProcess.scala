package spark.rdd

import com.alibaba.fastjson.JSON
import org.apache.spark.{SparkConf, SparkContext}

import java.time.LocalDate

object LogProcess {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf
//    val conf = new SparkConf().setMaster("local[*]").setAppName("test")
    val sc = new SparkContext(conf)

    val bc = sc.broadcast('\001')

    val input = sc.textFile("hdfs://hadoop1:9000/execDir")
//    val input = sc.textFile("/Users/crash/Desktop/test.txt")
    input
      .map(record => {
        val recordJson = JSON.parseObject(record)
        val semantic = recordJson.getJSONObject("return_semantic")
        var result = recordJson.getString("query_text")+bc.value+
          recordJson.getString("return_domain")+bc.value+
          recordJson.getString("return_intent")+bc.value
        result = if(semantic==null){
           result + "{}" + bc.value
        }else{
          result + semantic.toString() + bc.value
        }
        result + recordJson.getString("source_flag") + bc.value + recordJson.getString("time")
      })
      .repartition(1)
      .saveAsTextFile("hdfs://hadoop1:9000/output_"+LocalDate.now)
//      .saveAsTextFile("/Users/crash/Desktop/test_out.txt")

    sc.stop()
  }
}

