package spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Durations, StreamingContext}

object StreamingTransform {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("test")
    val ssc = new StreamingContext(conf, Durations.seconds(20))

    val arr = List("Lisa", "Jack", "Lucy")
    val blacklist = ssc.sparkContext.parallelize(arr).map(r => (r, 2))

    val dStream = ssc.socketTextStream("47.93.6.72", 2333)

    // 将流里面的每批次数据作为rdd处理，返回一个rdd
    dStream.transform(rdd => {
      val out = rdd.flatMap(r => r.split(" ")).map(r => (r, 1)).reduceByKey(_+_)
      out.leftOuterJoin(blacklist)
    }).print

    ssc.start()
    ssc.awaitTermination()


  }

}
