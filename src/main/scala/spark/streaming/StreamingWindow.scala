package spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Durations, StreamingContext}

object StreamingWindow {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("test")
    val ssc = new StreamingContext(conf, Durations.seconds(10))
    ssc.checkpoint("/tmp/spark-checkpoint")

    val input = ssc.socketTextStream("47.93.6.72", 2333)

    val step1 = input.flatMap(_.split(" ")).map((_, 1))
    // 计算从当前批次开始的往前总共3个批次，每次都计算
//    step1.reduceByKeyAndWindow(_+_, Durations.seconds(30)).print()
    // 优化，不重复计算中间的批次，需要额外提供过时批次的计算，还需要提供checkpoint
    step1.reduceByKeyAndWindow(_+_, _-_, Durations.seconds(30)).print()

    ssc.start()
    ssc.awaitTermination()

  }

}
