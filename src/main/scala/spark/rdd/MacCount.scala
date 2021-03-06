package spark.rdd

import com.alibaba.fastjson.JSON
import org.apache.spark.{SparkConf, SparkContext}
import util.StringUtil

object MacCount {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    val macRecord = sc.longAccumulator("MacRecord")
    val emptyMac = sc.longAccumulator("emptyMac")

    val input = sc.textFile("hdfs://hadoop1:9000/execDir")

    input.map(record => JSON.parseObject(record))
      .filter(record => {
        if(StringUtil.isEmpty(record.getString("query_mac"))){
          emptyMac.add(1)
          false
        }else{
          true
        }
      })
      .map(record => record.getString("query_mac"))
      .distinct()
      .foreach(_ => macRecord.add(1))

    sc.stop()

  }
}

