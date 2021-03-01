package spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Durations, StreamingContext}

object StreamingUpdateByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("test")
    val ssc = new StreamingContext(conf, Durations.seconds(10))

    // 如果需要使用updateStateByKey，需要指定checkpoint
    ssc.checkpoint("/tmp/spark-checkpoint")

    val input = ssc.socketTextStream("47.93.6.72", 2333)
    val pairs = input.flatMap(_.split(" ")).map((_, 1))

    // 只需要提供新旧值的计算方式，底层源码已实现对每个key的新旧值计算调用该函数，再更新key的state
    pairs.updateStateByKey(updateFunction).print()

    ssc.start()
    ssc.awaitTermination()

  }

  def updateFunction(newValues: Seq[Int], runningCount: Option[Int]): Option[Int] = {
    val newCount = newValues.sum + runningCount.getOrElse(0)
    Some(newCount)
  }

}
