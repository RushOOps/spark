package spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Durations, StreamingContext}

object StreamingDev {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("spark-kafka-test")
    val streamingContext = new StreamingContext(conf, Durations.seconds(20))

    val batch = streamingContext.socketTextStream("localhost", 9999)

    batch.flatMap(_.split(" ")).print()

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
